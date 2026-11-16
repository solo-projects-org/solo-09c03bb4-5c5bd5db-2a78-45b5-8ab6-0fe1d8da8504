import os
import socket
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.config import settings  # noqa: E402
from app.db import init_db  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(settings, "db_path", db_path)
    init_db()
    with TestClient(app) as c:
        yield c


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def live_server(tmp_path_factory):
    """启动一个真实 uvicorn 进程内实例（WAL + 文件库），供并发测试使用。"""
    import httpx
    import uvicorn

    db_dir = tmp_path_factory.mktemp("live")
    db_path = str(db_dir / "live.db")
    settings.db_path = db_path
    init_db()

    config = uvicorn.Config(app, host="127.0.0.1", port=_free_port(),
                            log_level="warning", lifespan="on")
    server = uvicorn.Server(config)

    import threading
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{config.port}"

    with httpx.Client(base_url=base, timeout=30) as http:
        for _ in range(100):
            try:
                if http.get("/health").status_code == 200:
                    break
            except httpx.TransportError:
                pass
            import time
            time.sleep(0.05)
        yield http

    server.should_exit = True
    thread.join(timeout=10)
