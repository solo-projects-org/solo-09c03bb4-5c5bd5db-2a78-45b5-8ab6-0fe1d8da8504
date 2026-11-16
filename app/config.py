"""运行期配置，全部可通过环境变量覆盖。"""
import os


class Settings:
    def __init__(self) -> None:
        self.db_path = os.environ.get("WQ_DB_PATH", "/data/wq.db")
        # SQLite 写锁等待上限（毫秒），配合应用层重试
        self.busy_timeout_ms = int(os.environ.get("WQ_BUSY_TIMEOUT_MS", "5000"))
        self.lock_retries = int(os.environ.get("WQ_LOCK_RETRIES", "20"))
        self.lock_retry_delay = float(os.environ.get("WQ_LOCK_RETRY_DELAY", "0.05"))


settings = Settings()
