from collections import Counter
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from rich.console import Console

import config
from terminal import Dashboard, configure_logging, logger


class TerminalTests(unittest.TestCase):
    def test_dashboard_renders_metrics(self):
        output = io.StringIO()
        console = Console(file=output, width=140, force_terminal=False)
        with patch.multiple(config, SHOW_PROGRESS=False, CONCURRENCY=20,
                            REQUESTS_PER_PROXY=5, UI_REFRESH_PER_SECOND=4):
            dashboard = Dashboard(10, 2, console)
            dashboard.skip(3)
            dashboard.result(Counter(match=1, no_match=1, network_error=1), 0.05)
            dashboard.active = 2
            console.print(dashboard.render())
            self.assertEqual(dashboard.progress.tasks[0].completed, 4)
        text = output.getvalue()
        for label in ["POST", "req/s", "ETA", "Совпадения: 1", "Из истории: 3", "2 / 20"]:
            self.assertIn(label, text)

    def test_loguru_file_and_console_sinks(self):
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, patch.multiple(
            config, LOG_FILE="logs/checker.log", CONSOLE_LOG_LEVEL="INFO",
            FILE_LOG_LEVEL="DEBUG", LOG_ROTATION="10 MB", LOG_RETENTION="7 days",
        ):
            try:
                configure_logging(directory, Console(file=output, width=140))
                logger.debug("test status=network_error")
                logger.info("Старт проверки")
                logger.complete()
                contents = (Path(directory) / config.LOG_FILE).read_text()
                self.assertIn("status=network_error", contents)
                self.assertIn("Старт проверки", contents)
                self.assertNotIn("status=network_error", output.getvalue())
                self.assertIn("Старт проверки", output.getvalue())
            finally:
                logger.remove()
