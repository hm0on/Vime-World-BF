"""Все пользовательские настройки. Пути считаются от папки программы."""

API_URL = "https://cp.vimeworld.com/api/login" 
PARAM1_NAME = "username"
PARAM2_NAME = "password"
EXTRA_JSON = {}  # Дополнительные постоянные параметры тела запроса.
HEADERS = {"Refer": "https://cp.vimeworld.com/login"}

FIRST_FILE = "1.txt"
SECOND_FILE = "2.txt"
PROXY_FILE = "proxys.txt"
DATABASE_FILE = "results.sqlite3"
MATCHES_FILE = "matches.jsonl"

# Сравнение по указанным ключам: лишние поля ответа игнорируются.
# Например, {"result": {"valid": True}} проверит вложенный ключ.
# Для полного сравнения JSON установите RESPONSE_MATCH_MODE = "exact".
MATCH_JSON = {"response": {"type": "invalid_credentials"}}
NO_MATCH_JSON = {"success": True}
RESPONSE_MATCH_MODE = "subset"  # "subset" или "exact"

REQUESTS_PER_PROXY = 5
CONCURRENCY = 50  # Общий лимит запросов одновременно, независимо от размера группы.
TIMEOUT_SECONDS = 15.0  # Максимальное время всей попытки, включая тело ответа.
MAX_RESPONSE_BYTES = 1_048_576
VERIFY_TLS = True
USE_PROXIES = True  # False удобно для локального тестирования.
# Измените, чтобы намеренно проверить все пары заново.
# Адрес, имена параметров, EXTRA_JSON и правила ответа также разделяют историю.
RUN_NAMESPACE = "default"

# Ошибки сети/HTTP и неизвестный JSON сохраняются отдельно, без автоповтора.
# True повторяет такие пары при СЛЕДУЮЩЕМ запуске; успешные и неуспешные
# проверки в любом случае пропускаются.
RETRY_ERRORS_ON_NEXT_RUN = True

# Соединения с недавно использованными прокси сохраняются для повторного использования.
MAX_CACHED_CLIENTS = 32

# Интерфейс и логи. В файл пишутся статусы попыток, без значений и паролей.
SHOW_PROGRESS = True
UI_REFRESH_PER_SECOND = 4
LOG_FILE = "logs/checker.log"
CONSOLE_LOG_LEVEL = "INFO"
FILE_LOG_LEVEL = "DEBUG"
LOG_ROTATION = "10 MB"
LOG_RETENTION = "7 days"
