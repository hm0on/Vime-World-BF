"""Rich-интерфейс и Loguru; сетевые данные в консоль и логи не попадают."""

from pathlib import Path
from time import monotonic

from loguru import logger
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, MofNCompleteColumn, Progress, ProgressColumn, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.text import Text

import config as cfg


def configure_logging(base, console):
    logger.remove()

    def console_sink(message):
        record = message.record
        styles = {"ERROR": "red", "WARNING": "yellow", "SUCCESS": "green"}
        text = Text(record["time"].strftime("%H:%M:%S"), style="dim")
        text.append(f"  {record['level'].name:<7} ", styles.get(record["level"].name, "cyan"))
        text.append(record["message"])
        console.print(text)

    logger.add(console_sink, level=cfg.CONSOLE_LOG_LEVEL, diagnose=False, backtrace=False)
    destination = Path(base) / cfg.LOG_FILE
    destination.parent.mkdir(parents=True, exist_ok=True)
    logger.add(destination, level=cfg.FILE_LOG_LEVEL, rotation=cfg.LOG_ROTATION,
               retention=cfg.LOG_RETENTION, encoding="utf-8", enqueue=True,
               diagnose=False, backtrace=False,
               format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<7} | {message}")


class RequestRateColumn(ProgressColumn):
    def render(self, task):
        return Text(f"{task.fields.get('rate', 0):.1f} req/s", style="cyan")


class RemainingColumn(ProgressColumn):
    def render(self, task):
        rate = task.fields.get("rate", 0)
        remaining = max(0, task.total - task.completed)
        if remaining == 0:
            return Text("ETA 00:00:00", style="dim")
        if rate <= 0:
            return Text("ETA --:--:--", style="dim")
        seconds = int(remaining / rate)
        return Text(f"ETA {seconds // 3600:02}:{seconds // 60 % 60:02}:{seconds % 60:02}", style="dim")


class Dashboard:
    def __init__(self, total, proxy_count, console=None):
        self.console = console or Console()
        self.total = total
        self.proxy_count = proxy_count
        self.completed = self.skipped = self.active = 0
        self.counts = {}
        self.latency_total = 0.0
        self.started = monotonic()
        self.progress = Progress(
            SpinnerColumn(), TextColumn("[bold cyan]Проверка"), BarColumn(bar_width=None),
            TaskProgressColumn(), MofNCompleteColumn(), RequestRateColumn(),
            TimeElapsedColumn(), RemainingColumn(), console=self.console,
            auto_refresh=False, expand=True,
        )
        self.task = self.progress.add_task("Пары", total=total, rate=0.0)
        self.live = Live(console=self.console, get_renderable=self.render,
                         refresh_per_second=cfg.UI_REFRESH_PER_SECOND,
                         auto_refresh=cfg.SHOW_PROGRESS)

    @property
    def rate(self):
        return self.completed / max(monotonic() - self.started, 0.001)

    def render(self):
        self.progress.update(self.task, completed=self.completed + self.skipped, rate=self.rate)
        stats = Table.grid(expand=True, padding=(0, 2))
        stats.add_column()
        stats.add_column()
        stats.add_column()
        errors = sum(value for key, value in self.counts.items() if key not in {"match", "no_match"})
        stats.add_row(f"[green]Совпадения: {self.counts.get('match', 0)}[/]",
                      f"Несовпадения: {self.counts.get('no_match', 0)}",
                      f"[yellow]Ошибки / неизвестные: {errors}[/]")
        average_ms = self.latency_total * 1000 / max(self.completed, 1)
        stats.add_row(f"В работе: {self.active} / {cfg.CONCURRENCY}",
                      f"Из истории: {self.skipped}", f"Средний ответ: {average_ms:.0f} ms")
        stats.add_row(f"Прокси: {self.proxy_count}", f"Смена через {cfg.REQUESTS_PER_PROXY} отправок",
                      f"Завершено за запуск: {self.completed}")
        return Panel(Group(self.progress, Text(""), stats), title="[bold cyan]POST · PAIR CHECKER[/]",
                     subtitle="[dim]Ctrl+C — остановить и сохранить результаты[/]", border_style="cyan")

    def skip(self, amount):
        self.skipped += amount

    def result(self, counts, elapsed):
        self.completed += 1
        self.counts = counts
        self.latency_total += elapsed

    def __enter__(self):
        self.started = monotonic()
        if cfg.SHOW_PROGRESS:
            self.live.start(refresh=True)
        return self

    def __exit__(self, *args):
        if cfg.SHOW_PROGRESS:
            self.live.stop()

    def summary(self):
        if not cfg.SHOW_PROGRESS:
            self.console.print(self.render())
        logger.success("Завершено: {} проверок, {} совпадений, {:.1f} req/s; из истории: {}",
                       self.completed, self.counts.get("match", 0), self.rate, self.skipped)
