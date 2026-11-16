"""FastAPI 应用入口：uvicorn app.main:app"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from .db import init_db
from .errors import register_exception_handlers
from .routers.api import router as api_router
from .routers.collection import router as collection_router


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="水质数仓 · 温度探头观测追溯 API",
    version="1.1.0",
    lifespan=lifespan,
    description=(
        "接收带观测号、站号、UTC 时刻与温度的原始读数，管理各测站探头的"
        "半开装机时段表，并按故障探头追溯受影响读数。\n\n"
        "- 读数：同号同值补传幂等，同号异文拒收\n"
        "- 装机：按站整版提交 + 期望版本（乐观并发），同站不重叠、"
        "同探头不同站不双挂，冲突整版拒绝\n"
        "- 追溯：按探头 + UTC 区间，在同一已提交快照内返回装机依据与归属/未归属读数\n"
        "- 网关采集：按站整批拉取温度观测，坏行隔离不卡正常数据，"
        "本地持久化后才确认网关，支持断线/回执丢失/重启重试对账"
    ),
)

register_exception_handlers(app)
app.include_router(api_router)
app.include_router(collection_router)


@app.get("/health", tags=["运维"], summary="健康检查")
def health() -> dict:
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")
