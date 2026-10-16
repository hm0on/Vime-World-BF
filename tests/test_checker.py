import asyncio
from collections import Counter
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

import config
import main


class TestConfig:
    def setUp(self):
        # Пользователь может свободно менять config.py — тесты от него не зависят.
        settings = patch.multiple(
            config, API_URL="https://test.invalid/check", PARAM1_NAME="param1",
            PARAM2_NAME="param2", EXTRA_JSON={}, HEADERS={}, FIRST_FILE="1.txt",
            SECOND_FILE="2.txt", PROXY_FILE="proxys.txt", DATABASE_FILE="results.sqlite3",
            MATCHES_FILE="matches.jsonl", MATCH_JSON={"match": True},
            NO_MATCH_JSON={"match": False}, RESPONSE_MATCH_MODE="subset",
            RESULT_MODE="status",
            CONCURRENCY=5, REQUESTS_PER_PROXY=5, TIMEOUT_SECONDS=3,
            MAX_RESPONSE_BYTES=1048576, VERIFY_TLS=True, USE_PROXIES=True,
            RUN_NAMESPACE="test", RETRY_ERRORS_ON_NEXT_RUN=False,
            MAX_CACHED_CLIENTS=4, SHOW_PROGRESS=False, UI_REFRESH_PER_SECOND=4,
        )
        settings.start()
        self.addCleanup(settings.stop)
        for module in ("main", "terminal"):
            main.logger.disable(module)
            self.addCleanup(main.logger.enable, module)


class CheckerTests(TestConfig, unittest.TestCase):
    def test_result_modes_have_separate_history(self):
        status_profile = main.profile_id()
        with patch.object(config, "MATCH_JSON", {"different": True}):
            self.assertEqual(status_profile, main.profile_id())
        with patch.object(config, "RESULT_MODE", "json"):
            json_profile = main.profile_id()
            self.assertNotEqual(status_profile, json_profile)
            with patch.object(config, "MATCH_JSON", {"different": True}):
                self.assertNotEqual(json_profile, main.profile_id())
        self.assertEqual(status_profile, main.profile_id())

    def test_json_mode_handles_rules_errors_and_rate_limit(self):
        cases = [
            (200, {"match": True, "extra": 1}, "match"),
            (200, {"match": False}, "no_match"),
            (400, {"match": False}, "no_match"),
            (200, {"match": 1}, "unknown"),
            (200, {"other": True}, "unknown"),
            (429, {"match": True}, "rate_limited"),
            (503, {"match": True}, "http_error"),
            (200, None, "response_error"),
        ]

        async def exercise(storage):
            for index, (code, data, expected) in enumerate(cases):
                response = httpx.Response(code, text="not json") if data is None else httpx.Response(code, json=data)
                async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response)) as client:
                    counts = Counter()
                    await main.check_pair(client, asyncio.Semaphore(1), storage, ("one", str(index)), counts)
                    self.assertEqual(dict(counts), {expected: 1})

        with tempfile.TemporaryDirectory() as directory, patch.object(main, "BASE", Path(directory)), \
                patch.object(config, "RESULT_MODE", "json"):
            storage = main.Storage()
            try:
                asyncio.run(exercise(storage))
            finally:
                storage.close()

    def test_json_subset_exact_and_ambiguous_rules(self):
        data = {"result": {"ok": True, "extra": 7}}
        with patch.multiple(config, MATCH_JSON={"result": {"ok": True}},
                            NO_MATCH_JSON={"result": {"ok": False}}):
            self.assertEqual(main.classify_json(data), "match")
            with patch.object(config, "RESPONSE_MATCH_MODE", "exact"):
                self.assertEqual(main.classify_json(data), "unknown")
        with patch.multiple(config, MATCH_JSON={"a": True}, NO_MATCH_JSON={"b": True}):
            self.assertEqual(main.classify_json({"a": True, "b": True}), "unknown")
        self.assertFalse(main.json_matches([1], [True]))

    def test_rolling_queue_does_not_wait_for_slowest_in_group(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / "1.txt").write_text("one\n")
            (base / "2.txt").write_text("\n".join(str(n) for n in range(20)))
            routes, created = {}, []
            original_client = httpx.AsyncClient
            sixth_started = asyncio.Event()

            async def handler(request, proxy):
                index = int(json.loads(request.content)["param2"])
                routes[index] = proxy
                if index == 0:
                    # Старая барьерная очередь не смогла бы отправить шестой запрос.
                    await asyncio.wait_for(sixth_started.wait(), timeout=1)
                elif index == 5:
                    sixth_started.set()
                await asyncio.sleep(0)
                return httpx.Response(200, json={"match": False})

            def factory(**kwargs):
                proxy = kwargs.pop("proxy")
                created.append(proxy)
                return original_client(transport=httpx.MockTransport(lambda req: handler(req, proxy)), **kwargs)

            proxies = ["http://localhost:9001", "http://localhost:9002"]
            with patch.object(main, "BASE", base), patch.object(main, "read_proxies", return_value=proxies), \
                    patch.object(main.httpx, "AsyncClient", factory), redirect_stdout(io.StringIO()):
                asyncio.run(main.run())
            self.assertTrue(sixth_started.is_set())
            self.assertEqual(len(routes), 20)
            for index, proxy in routes.items():
                self.assertEqual(proxy, proxies[index // 5 % 2])
            self.assertEqual(created, proxies)  # Клиенты переиспользуются при новом круге.

    def test_client_pool_does_not_evict_active_client(self):
        from unittest.mock import AsyncMock, Mock
        clients = []

        def create(proxy):
            client = Mock(aclose=AsyncMock())
            clients.append(client)
            return client

        async def exercise():
            async with main.ClientPool() as pool:
                async with pool.lease("first") as first:
                    async with pool.lease("second") as second:
                        first.aclose.assert_not_awaited()
                        second.aclose.assert_not_awaited()
                    second.aclose.assert_awaited_once()
                    first.aclose.assert_not_awaited()
                self.assertEqual(len(pool.entries), 1)

        with patch.object(config, "MAX_CACHED_CLIENTS", 1), patch.object(main, "create_client", create):
            asyncio.run(exercise())
        for client in clients:
            client.aclose.assert_awaited_once()

    def test_mixed_proxy_file(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(main, "BASE", Path(directory)):
            values = ["socks4://user@localhost:9001", "socks5://user:pass@localhost:9002",
                      "localhost:9003", "socks5h://localhost:9004"]
            (Path(directory) / config.PROXY_FILE).write_text("\n".join(values))
            with patch.object(config, "USE_PROXIES", True):
                self.assertEqual(main.read_proxies(), [*values[:2], "http://localhost:9003", values[3]])

    def test_http_status_classification(self):
        for code, status in [(200, "match"), (400, "no_match"), (429, "rate_limited"),
                             (201, "http_error"), (204, "http_error"), (302, "http_error"),
                             (401, "http_error"), (503, "http_error")]:
            with self.subTest(code=code):
                self.assertEqual(main.classify_status(code), status)

    def test_cartesian_dedup_rotation_resume_and_export(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / "1.txt").write_text("\ufeff1\n2\n1\n\n", encoding="utf-8")
            (base / "2.txt").write_text("1\n2\n3\n4\n2\n", encoding="utf-8")
            (base / "proxys.txt").write_text("http://localhost:9001\nhttp://localhost:9002\n")
            calls, clients, routes = [], [], {}
            active, peak = 0, 0
            original_client = httpx.AsyncClient

            async def handler(request, proxy):
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0.001)
                payload = json.loads(request.content)
                routes[tuple(payload.values())] = proxy
                calls.append(payload)
                active -= 1
                return httpx.Response(200 if payload["param1"] == payload["param2"] else 400)

            def factory(**kwargs):
                proxy = kwargs.pop("proxy")
                clients.append(proxy)
                return original_client(transport=httpx.MockTransport(lambda request: handler(request, proxy)), **kwargs)

            with patch.object(main, "BASE", base), patch.multiple(
                config, API_URL="https://test.invalid/check", USE_PROXIES=True,
                MATCH_JSON={"match": True}, NO_MATCH_JSON={"match": False},
                REQUESTS_PER_PROXY=5, CONCURRENCY=5,
                RETRY_ERRORS_ON_NEXT_RUN=False,
            ), patch.object(main.httpx, "AsyncClient", factory), redirect_stdout(io.StringIO()):
                asyncio.run(main.run())
                self.assertEqual(len(calls), 8)
                self.assertEqual(len({tuple(p.values()) for p in calls}), 8)
                self.assertEqual(clients, ["http://localhost:9001", "http://localhost:9002"])
                for index, pair in enumerate((a, b) for a in ["1", "2"] for b in ["1", "2", "3", "4"]):
                    self.assertEqual(routes[pair], f"http://localhost:{9001 + index // 5}")
                self.assertEqual(peak, 5)
                exported = [json.loads(line) for line in (base / config.MATCHES_FILE).read_text().splitlines()]
                self.assertEqual(exported, [{"param1": "1", "param2": "1"}, {"param1": "2", "param2": "2"}])
                asyncio.run(main.run())
                self.assertEqual(len(calls), 8)

    def test_status_overrides_body_and_errors_are_not_matches(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(main, "BASE", Path(directory)):
            storage = main.Storage()

            def handler(request):
                value = json.loads(request.content)["param2"]
                if value == "network":
                    raise httpx.ConnectError("offline", request=request)
                if value == "socks":
                    raise main.ProxyError("proxy rejected connection")
                if value == "http":
                    return httpx.Response(503, json={"match": True})
                if value == "invalid":
                    return httpx.Response(200, text="not json")
                if value == "no_match":
                    return httpx.Response(400, json={"match": True})
                if value == "limited":
                    return httpx.Response(429, json={"match": True})
                if value == "created":
                    return httpx.Response(201, json={"match": True})
                return httpx.Response(200, json={"match": False})

            async def exercise():
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    counts = Counter()
                    for value in ["network", "socks", "http", "invalid", "empty_rule", "no_match", "limited", "created"]:
                        await main.check_pair(client, asyncio.Semaphore(5), storage, ("1", value), counts)
                    return counts

            try:
                counts = asyncio.run(exercise())
                self.assertEqual(dict(counts), {"network_error": 2, "http_error": 2,
                                                "match": 2, "no_match": 1, "rate_limited": 1})
                self.assertTrue(storage.completed("1", "network"))
                with patch.object(config, "RETRY_ERRORS_ON_NEXT_RUN", True):
                    self.assertFalse(storage.completed("1", "network"))
                    self.assertFalse(storage.completed("1", "limited"))
                    self.assertTrue(storage.completed("1", "no_match"))
                storage.export_matches()
                exported = [json.loads(line)["param2"] for line in
                            (Path(directory) / config.MATCHES_FILE).read_text().splitlines()]
                self.assertEqual(set(exported), {"invalid", "empty_rule"})
            finally:
                storage.close()


class SocksIntegrationTests(TestConfig, unittest.IsolatedAsyncioTestCase):
    async def test_real_socks4_and_socks5_handshakes_and_rotation(self):
        received = []
        failures = []

        async def serve(reader, writer, version):
            try:
                if version == 4:
                    self.assertEqual(await reader.readexactly(2), b"\x04\x01")
                    await reader.readexactly(6)  # Порт и IPv4 назначения.
                    self.assertEqual(await reader.readuntil(b"\x00"), b"user\x00")
                    writer.write(b"\x00\x5a\x00\x00\x00\x00\x00\x00")
                else:
                    self.assertEqual(await reader.readexactly(1), b"\x05")
                    methods = await reader.readexactly((await reader.readexactly(1))[0])
                    self.assertIn(2, methods)
                    writer.write(b"\x05\x02")
                    await writer.drain()
                    self.assertEqual(await reader.readexactly(1), b"\x01")
                    user = await reader.readexactly((await reader.readexactly(1))[0])
                    password = await reader.readexactly((await reader.readexactly(1))[0])
                    self.assertEqual((user, password), (b"user", b"pass"))
                    writer.write(b"\x01\x00")
                    await writer.drain()
                    self.assertEqual(await reader.readexactly(4), b"\x05\x01\x00\x01")
                    await reader.readexactly(6)
                    writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
                headers = (await reader.readuntil(b"\r\n\r\n")).decode("ascii")
                length = next(int(line.split(":", 1)[1]) for line in headers.split("\r\n")
                              if line.lower().startswith("content-length:"))
                payload = json.loads(await reader.readexactly(length))
                received.append((version, payload))
                body = b'{"match": true}'
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                             b"Connection: close\r\nContent-Length: " + str(len(body)).encode()
                             + b"\r\n\r\n" + body)
                await writer.drain()
            except Exception as exc:
                failures.append(exc)
            finally:
                writer.close()
                await writer.wait_closed()

        socks4 = await asyncio.start_server(lambda r, w: serve(r, w, 4), "127.0.0.1", 0)
        socks5 = await asyncio.start_server(lambda r, w: serve(r, w, 5), "127.0.0.1", 0)
        async with socks4, socks5:
            port4 = socks4.sockets[0].getsockname()[1]
            port5 = socks5.sockets[0].getsockname()[1]
            with tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                (base / "1.txt").write_text("one\n")
                (base / "2.txt").write_text("\n".join(str(n) for n in range(10)))
                (base / "proxys.txt").write_text(
                    f"socks4://user@127.0.0.1:{port4}\nsocks5://user:pass@127.0.0.1:{port5}\n")
                with patch.object(main, "BASE", base), patch.multiple(
                    config, API_URL="http://127.0.0.1:8080/check", USE_PROXIES=True,
                    CONCURRENCY=5, REQUESTS_PER_PROXY=5, TIMEOUT_SECONDS=3,
                ), redirect_stdout(io.StringIO()):
                    await main.run()
                self.assertEqual(failures, [])
                self.assertEqual(len(received), 10)
                for version, payload in received:
                    self.assertEqual(version, 4 if int(payload["param2"]) < 5 else 5)
                self.assertEqual(len((base / config.MATCHES_FILE).read_text().splitlines()), 10)


if __name__ == "__main__":
    unittest.main()
