"""运行期配置，全部可通过环境变量覆盖。"""
import os


class Settings:
    def __init__(self) -> None:
        self.db_path = os.environ.get("WQ_DB_PATH", "/data/wq.db")
        # SQLite 写锁等待上限（毫秒），配合应用层重试
        self.busy_timeout_ms = int(os.environ.get("WQ_BUSY_TIMEOUT_MS", "5000"))
        self.lock_retries = int(os.environ.get("WQ_LOCK_RETRIES", "20"))
        self.lock_retry_delay = float(os.environ.get("WQ_LOCK_RETRY_DELAY", "0.05"))
        # 站点网关（模拟服务）地址与 HTTP 超时
        self.gateway_base_url = os.environ.get(
            "WQ_GATEWAY_URL", "http://gateway:8100"
        ).rstrip("/")
        self.gateway_timeout_sec = float(
            os.environ.get("WQ_GATEWAY_TIMEOUT_SEC", "10")
        )
        # 单批最大行数（业务上限，网关与采集端双侧强制）
        self.max_rows_per_batch = int(os.environ.get("WQ_MAX_ROWS_PER_BATCH", "100"))


settings = Settings()
