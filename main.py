"""Асинхронная проверка уникальных пар через JSON POST. Python 3.11+."""

import asyncio
from collections import Counter, OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import sqlite3
import ssl
import sys
from time import monotonic
from urllib.parse import unquote, urlsplit

import httpx
import certifi
from httpx_socks import AsyncProxyTransport
from loguru import logger
from python_socks import ProxyConnectionError, ProxyError, ProxyType
from rich.console import Console

import config as cfg
from terminal import Dashboard, configure_logging

BASE = Path(__file__).resolve().parent


def read_values(filename):
    # Убираем только перевод строки: пробелы могут быть частью значения.
    lines = (BASE / filename).read_text(encoding="utf-8-sig").splitlines()
    values = list(dict.fromkeys(line for line in lines if line != ""))
    if not values:
        raise ValueError(f"Файл {filename} пуст.")
    return values


def read_proxies():
    if not cfg.USE_PROXIES:
        return [None]
    proxies = []
    for number, value in enumerate(read_values(cfg.PROXY_FILE), 1):
        value = value.strip()
        if "://" not in value:
            value = "http://" + value
        try:
            parsed = urlsplit(value)
            valid = (
                parsed.scheme in {"http", "https", "socks4", "socks5", "socks5h"}
                and parsed.hostname and parsed.port
                and parsed.path in {"", "/"}
                and not parsed.query and not parsed.fragment
            )
        except ValueError:
            valid = False
        if not valid:
            raise ValueError(f"Некорректный прокси в строке {number}.")
        proxies.append(value)
    return proxies


@lru_cache(maxsize=2)
def tls_setting(verify):
    # Загрузка хранилища сертификатов один раз, а не при каждой смене прокси.
    return ssl.create_default_context(cafile=certifi.where()) if verify else False


def create_client(proxy):
    limits = httpx.Limits(max_connections=cfg.CONCURRENCY,
                         max_keepalive_connections=cfg.CONCURRENCY)
    options = {"proxy": proxy}
    if proxy is not None and urlsplit(proxy).scheme == "socks4":
        # Встроенный SOCKS-транспорт HTTPX поддерживает только SOCKS5.
        parsed = urlsplit(proxy)
        options = {"transport": AsyncProxyTransport(
            proxy_type=ProxyType.SOCKS4, proxy_host=parsed.hostname,
            proxy_port=parsed.port, username=unquote(parsed.username or ""),
            rdns=False, verify=tls_setting(cfg.VERIFY_TLS), limits=limits, trust_env=False,
        )}
    return httpx.AsyncClient(
        **options, headers=cfg.HEADERS, verify=tls_setting(cfg.VERIFY_TLS),
        timeout=cfg.TIMEOUT_SECONDS, trust_env=False,
        follow_redirects=False, limits=limits,
    )


@dataclass
class ClientEntry:
    client: httpx.AsyncClient
    active: int = 0


class ClientPool:
    """LRU-кэш соединений; активный клиент никогда не закрывается при вытеснении."""

    def __init__(self):
        self.entries = OrderedDict()

    @asynccontextmanager
    async def lease(self, proxy):
        entry = self.entries.get(proxy)
        if entry is None:
            entry = ClientEntry(create_client(proxy))
            self.entries[proxy] = entry
        self.entries.move_to_end(proxy)
        entry.active += 1
        try:
            yield entry.client
        finally:
            entry.active -= 1
            while len(self.entries) > cfg.MAX_CACHED_CLIENTS:
                idle = next((key for key, value in self.entries.items() if value.active == 0), ...)
                if idle is ...:
                    break
                # Удаление до await исключает выдачу уже закрывающегося клиента.
                old = self.entries.pop(idle)
                await old.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await asyncio.gather(*(entry.client.aclose() for entry in self.entries.values()))
        self.entries.clear()


def classify_status(code):
    if code == 200:
        return "match"
    if code == 400:
        return "no_match"
    if code == 429:
        return "rate_limited"
    return "http_error"


def json_matches(actual, expected):
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        if cfg.RESPONSE_MATCH_MODE == "exact" and actual.keys() != expected.keys():
            return False
        return all(key in actual and json_matches(actual[key], value)
                   for key, value in expected.items())
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(
            json_matches(a, b) for a, b in zip(actual, expected))
    return actual == expected


def classify_json(data):
    match = json_matches(data, cfg.MATCH_JSON)
    no_match = json_matches(data, cfg.NO_MATCH_JSON)
    if match == no_match:
        return "unknown"
    return "match" if match else "no_match"


async def read_json_result(response):
    body = bytearray()
    async for chunk in response.aiter_bytes():
        body.extend(chunk)
        if len(body) > cfg.MAX_RESPONSE_BYTES:
            return "response_error", "Превышен MAX_RESPONSE_BYTES"
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return "response_error", "Ответ не является JSON"
    return classify_json(data), json.dumps(data, ensure_ascii=False)


def profile_id():
    # Профиль режима status сохраняется для совместимости с имеющейся историей.
    settings = ["http-status-v1", cfg.RUN_NAMESPACE, cfg.API_URL, cfg.PARAM1_NAME,
                cfg.PARAM2_NAME, cfg.EXTRA_JSON, cfg.HEADERS]
    if cfg.RESULT_MODE == "json":
        settings[0] = "json-response-v1"
        settings.extend([cfg.MATCH_JSON, cfg.NO_MATCH_JSON, cfg.RESPONSE_MATCH_MODE])
    return hashlib.sha256(json.dumps(
        settings, sort_keys=True, ensure_ascii=False
    ).encode()).hexdigest()


class Storage:
    def __init__(self):
        self.profile = profile_id()
        self.db = sqlite3.connect(BASE / cfg.DATABASE_FILE)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS results (
                profile TEXT NOT NULL,
                first_value TEXT NOT NULL,
                second_value TEXT NOT NULL,
                status TEXT NOT NULL,
                http_status INTEGER,
                detail TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (profile, first_value, second_value)
            )
        """)
        self.db.commit()

    def completed(self, first, second):
        row = self.db.execute(
            "SELECT status FROM results WHERE profile=? AND first_value=? AND second_value=?",
            (self.profile, first, second),
        ).fetchone()
        return row is not None and (
            not cfg.RETRY_ERRORS_ON_NEXT_RUN or row[0] in {"match", "no_match"}
        )

    def completed_seconds(self, first):
        query = "SELECT second_value FROM results WHERE profile=? AND first_value=?"
        if cfg.RETRY_ERRORS_ON_NEXT_RUN:
            query += " AND status IN ('match', 'no_match')"
        return {row[0] for row in self.db.execute(query, (self.profile, first))}

    def save(self, first, second, status, http_status, detail):
        with self.db:
            self.db.execute("""
                INSERT INTO results
                    (profile, first_value, second_value, status, http_status, detail)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile, first_value, second_value) DO UPDATE SET
                    status=excluded.status, http_status=excluded.http_status,
                    detail=excluded.detail, updated_at=CURRENT_TIMESTAMP
            """, (self.profile, first, second, status, http_status, detail))

    def export_matches(self):
        destination = BASE / cfg.MATCHES_FILE
        temporary = destination.with_name(destination.name + ".tmp")
        with temporary.open("w", encoding="utf-8") as output:
            for first, second in self.db.execute(
                "SELECT first_value, second_value FROM results WHERE profile=? AND status='match'",
                (self.profile,),
            ):
                output.write(json.dumps({cfg.PARAM1_NAME: first, cfg.PARAM2_NAME: second},
                                        ensure_ascii=False) + "\n")
        temporary.replace(destination)

    def close(self):
        self.db.close()


async def check_pair(client, semaphore, storage, pair, counts, dashboard=None):
    first, second = pair
    payload = {**cfg.EXTRA_JSON, cfg.PARAM1_NAME: first, cfg.PARAM2_NAME: second}
    status, code, detail = "network_error", None, ""
    async with semaphore:
        started = monotonic()
        try:
            async with asyncio.timeout(cfg.TIMEOUT_SECONDS):
                async with client.stream("POST", cfg.API_URL, json=payload) as response:
                    code = response.status_code
                    status, detail = classify_status(code), f"HTTP {code}"
                    if code == 429:
                        detail = "HTTP 429: сервер ограничил частоту запросов"
                        logger.warning("HTTP 429: слишком много запросов. Уменьшите CONCURRENCY.")
                    elif cfg.RESULT_MODE == "json" and (200 <= code < 300 or 400 <= code < 500):
                        status, detail = await read_json_result(response)
        except (httpx.RequestError, ProxyConnectionError, ProxyError, TimeoutError) as exc:
            # Не включаем текст исключения: он может содержать пароль прокси.
            status, detail = "network_error", type(exc).__name__
        storage.save(first, second, status, code, detail)
        counts[status] += 1
        elapsed = monotonic() - started
        if dashboard is not None:
            dashboard.result(counts, elapsed)
        # Идентификатор позволяет связать записи без раскрытия входных значений.
        pair_id = hashlib.sha256(json.dumps(pair, ensure_ascii=False).encode()).hexdigest()[:12]
        logger.debug("pair={} status={} http={} elapsed={:.0f}ms reason={}",
                     pair_id, status, code or "—", elapsed * 1000,
                     detail if status in {"network_error", "http_error", "rate_limited", "response_error"} else "—")


def validate_settings():
    parsed = urlsplit(cfg.API_URL)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Укажите корректный API_URL в config.py.")
    if parsed.hostname == "example.com":
        raise ValueError("Замените пример API_URL в config.py своим адресом.")
    if not cfg.PARAM1_NAME or not cfg.PARAM2_NAME or cfg.PARAM1_NAME == cfg.PARAM2_NAME:
        raise ValueError("Имена двух JSON-параметров должны быть непустыми и разными.")
    if cfg.CONCURRENCY < 1 or cfg.REQUESTS_PER_PROXY < 1 or cfg.MAX_CACHED_CLIENTS < 1:
        raise ValueError("CONCURRENCY, REQUESTS_PER_PROXY и MAX_CACHED_CLIENTS должны быть >= 1.")
    if cfg.UI_REFRESH_PER_SECOND <= 0:
        raise ValueError("UI_REFRESH_PER_SECOND должен быть положительным.")
    if cfg.TIMEOUT_SECONDS <= 0:
        raise ValueError("Таймаут должен быть положительным.")
    if cfg.RESULT_MODE not in {"json", "status"}:
        raise ValueError("RESULT_MODE: json или status.")
    if cfg.RESULT_MODE == "json":
        if cfg.RESPONSE_MATCH_MODE not in {"subset", "exact"}:
            raise ValueError("RESPONSE_MATCH_MODE: subset или exact.")
        if cfg.MAX_RESPONSE_BYTES < 1:
            raise ValueError("MAX_RESPONSE_BYTES должен быть положительным.")
        if cfg.MATCH_JSON == cfg.NO_MATCH_JSON:
            raise ValueError("MATCH_JSON и NO_MATCH_JSON должны различаться.")


def pending_pairs(first_values, second_values, storage, dashboard):
    for first in first_values:
        # Один индексный SELECT на строку первого файла вместо SELECT на каждую пару.
        completed = storage.completed_seconds(first)
        for second in second_values:
            if second in completed:
                dashboard.skip(1)
            else:
                yield first, second


async def run(console=None):
    validate_settings()
    first_values = read_values(cfg.FIRST_FILE)
    second_values = read_values(cfg.SECOND_FILE)
    proxies = read_proxies()
    storage = Storage()
    counts = Counter()
    semaphore = asyncio.Semaphore(cfg.CONCURRENCY)
    total = len(first_values) * len(second_values)
    dashboard = Dashboard(total, len(proxies) if cfg.USE_PROXIES else 0, console)
    pending = iter(enumerate(pending_pairs(first_values, second_values, storage, dashboard)))
    logger.info("Старт: {} уникальных пар, параллельность {}, прокси {}", total,
                cfg.CONCURRENCY, len(proxies) if cfg.USE_PROXIES else 0)

    async def worker(pool):
        while item := next(pending, None):
            index, pair = item
            proxy_index = (index // cfg.REQUESTS_PER_PROXY) % len(proxies)
            dashboard.active += 1
            try:
                async with pool.lease(proxies[proxy_index]) as client:
                    await check_pair(client, semaphore, storage, pair, counts, dashboard)
            finally:
                dashboard.active -= 1

    try:
        with dashboard:
            async with ClientPool() as pool:
                # Фиксированное число воркеров: задачи не создаются на всё произведение файлов.
                async with asyncio.TaskGroup() as group:
                    for _ in range(min(cfg.CONCURRENCY, total)):
                        group.create_task(worker(pool))
    finally:
        try:
            storage.export_matches()
        finally:
            storage.close()
    dashboard.summary()
    logger.info("Результаты экспортированы; история сохранена.")


if __name__ == "__main__":
    console = Console()
    configure_logging(BASE, console)
    try:
        asyncio.run(run(console))
    except KeyboardInterrupt:
        logger.warning("Остановлено. Завершённые проверки сохранены.")
    except (OSError, ValueError, sqlite3.Error) as exc:
        logger.error("Ошибка: {}", exc)
        sys.exit(1)
    except Exception:
        # Не выводим traceback с телами запросов, токенами или паролями прокси.
        logger.error("Внутренняя ошибка. Завершённые проверки сохранены; проверьте конфигурацию.")
        sys.exit(1)
    finally:
        logger.complete()
