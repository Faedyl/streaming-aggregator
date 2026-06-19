import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL     = os.getenv("DATABASE_URL", "postgres://uts:utspass@storage:5432/utsdb")
REDIS_URL        = os.getenv("REDIS_URL",    "redis://broker:6379/0")
CONSUMER_WORKERS = int(os.getenv("CONSUMER_WORKERS", "3"))
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO")
STREAM_NAME      = "events"
GROUP_NAME       = "agg"
