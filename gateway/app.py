"""站点温度观测网关（模拟服务）。

用途：在 Docker Compose 内扮演测站网关，供数仓采集端练习整批拉取与确认协议。

协议：
- ``POST /test/stations/{station_id}/backlog`` 投放一批观测（最多 100 行，
  行号与原文原样保留），进入该站积压队列；
- ``GET  /stations/{station_id}/batches/next`` 拉取队头批次，
  未确认前重复拉取返回同一批（at-least-once 重投递）；积压为空返回 204；
- ``POST /stations/{station_id}/batches/{batch_id}/ack`` 确认清退队头批次，
  批次被物理清出队列，下一批成为队头；重复确认返回 410；
- ``POST /test/faults`` 注入故障：``disconnect``（网关不可用）与
  ``ack_lost``（网关已清退但回执丢失，返回 502）；
- ``GET  /test/state`` / ``POST /test/reset`` 测试观察与复位。

状态持久化在 JSON 文件（原子替换写入），容器重启后积压不丢。
"""
from __future__ import annotations

import json
import math
import os
import threading
import uuid
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field, field_validator

MAX_ROWS_PER_BATCH = 100
DATA_PATH = os.environ.get("GATEWAY_DATA_PATH", "/data/gateway_state.json")

_LOCK = threading.RLock()


class Fault(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: str = Field(pattern="^(none|disconnect|ack_lost)$")
    # 故障作用范围：None 表示全站生效；否则只对指定测站生效
    station_id: str | None = None
    # 可选的人工延迟（秒），用于验证采集端等待期间其他网关仍可采集
    delay_sec: float = Field(default=0.0, ge=0.0, le=60.0)


def _reject_constant(constant: str):
    raise ValueError(f"含非标准 JSON 常量 {constant}（NaN/Infinity 不允许）")


def _has_nonfinite(value: Any) -> bool:
    """递归检查结构中是否含 NaN/Infinity（标准 JSON 无法表达）。"""
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, dict):
        return any(_has_nonfinite(v) for v in value.values())
    if isinstance(value, list):
        return any(_has_nonfinite(v) for v in value)
    return False


class SeedBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_id: str | None = Field(default=None, min_length=1, max_length=128)
    # 同时接受结构化对象与原始 JSON 文本（原始文本用于保留坏行原文）
    rows: list[dict[str, Any] | str] = Field(min_length=1, max_length=MAX_ROWS_PER_BATCH)

    @field_validator("rows")
    @classmethod
    def _non_empty_lines(cls, v: list) -> list:
        for i, row in enumerate(v, start=1):
            if isinstance(row, str):
                if not row.strip():
                    raise ValueError(f"第 {i} 行为空，空行不得投放")
                # 非标准 JSON（含 NaN/Infinity 等）在投放层直接拒绝，
                # 保证网关后续能以标准 JSON 原样回传
                try:
                    json.loads(row, parse_constant=_reject_constant)
                except json.JSONDecodeError:
                    pass  # 普通坏 JSON 保留给采集端隔离
            elif _has_nonfinite(row):
                raise ValueError(f"第 {i} 行含 NaN/Infinity，标准 JSON 无法承载")
        return v


def _error(status: int, code: str, message: str, details: dict | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": {"code": code, "message": message, "details": details or {}}},
    )


class GatewayState:
    """内存状态 + JSON 文件持久化，所有方法在锁内调用。"""

    def __init__(self) -> None:
        self.queues: dict[str, list[dict[str, Any]]] = {}
        # 已确认清退的批次号留痕，便于测试断言“重复确认不能误删下一批”
        self.acked: dict[str, list[str]] = {}
        self.fault = Fault(mode="none")
        self._load()

    # ------------------------- 持久化 -------------------------
    def _load(self) -> None:
        try:
            with open(DATA_PATH, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except FileNotFoundError:
            return
        except (json.JSONDecodeError, OSError):
            return
        self.queues = data.get("queues", {})
        self.acked = data.get("acked", {})

    def _save(self) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(DATA_PATH)), exist_ok=True)
        tmp = f"{DATA_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"queues": self.queues, "acked": self.acked},
                      fh, ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, DATA_PATH)

    # ------------------------- 故障注入 -------------------------
    def _fault_active(self, mode: str, station_id: str) -> bool:
        f = self.fault
        if f.mode != mode:
            return False
        return f.station_id is None or f.station_id == station_id

    # ------------------------- 业务操作 -------------------------
    def seed(self, station_id: str, payload: SeedBatch) -> dict[str, Any]:
        batch_id = payload.batch_id or f"B-{uuid.uuid4().hex[:12]}"
        rows: list[dict[str, Any]] = []
        for line_no, row in enumerate(payload.rows, start=1):
            if isinstance(row, str):
                raw = row
                parsed: Any
                try:
                    parsed = json.loads(row)
                except json.JSONDecodeError:
                    parsed = None
                if isinstance(parsed, dict):
                    rows.append({"line_no": line_no, "raw": raw, "data": parsed})
                else:
                    rows.append({"line_no": line_no, "raw": raw, "data": None})
            else:
                rows.append({
                    "line_no": line_no,
                    "raw": json.dumps(row, ensure_ascii=False),
                    "data": row,
                })
        batch = {"batch_id": batch_id, "rows": rows}
        self.queues.setdefault(station_id, []).append(batch)
        self._save()
        return {"station_id": station_id, "batch_id": batch_id,
                "row_count": len(rows)}

    def next_batch(self, station_id: str) -> tuple[int, Any]:
        queue = self.queues.get(station_id, [])
        if not queue:
            return 204, None
        head = queue[0]
        return 200, {
            "station_id": station_id,
            "batch_id": head["batch_id"],
            "row_count": len(head["rows"]),
            "rows": [
                {"line_no": r["line_no"], "raw": r["raw"], "data": r["data"]}
                for r in head["rows"]
            ],
        }

    def ack(self, station_id: str, batch_id: str) -> tuple[int, dict]:
        queue = self.queues.get(station_id, [])
        if not queue:
            return 410, {"error": {
                "code": "batch_not_found",
                "message": f"测站 {station_id} 没有待确认批次（可能已清退）",
                "details": {"station_id": station_id, "batch_id": batch_id},
            }}
        head = queue[0]
        if head["batch_id"] != batch_id:
            # 典型场景：上一批已清退后又重复确认旧批号——绝不能误删当前队头
            return 410, {"error": {
                "code": "batch_not_head",
                "message": "该批号不是当前队头批次，拒绝确认以防误删下一批",
                "details": {"station_id": station_id, "requested_batch": batch_id,
                            "current_head": head["batch_id"]},
            }}
        queue.pop(0)
        self.acked.setdefault(station_id, []).append(batch_id)
        if not queue:
            self.queues.pop(station_id, None)
        self._save()
        return 200, {"station_id": station_id, "batch_id": batch_id,
                     "status": "cleared"}

    def snapshot(self) -> dict[str, Any]:
        return {
            "queues": {
                station: [{"batch_id": b["batch_id"], "row_count": len(b["rows"])}
                          for b in queue]
                for station, queue in self.queues.items()
            },
            "acked": self.acked,
            "fault": self.fault.model_dump(),
        }

    def reset(self) -> None:
        self.queues = {}
        self.acked = {}
        self.fault = Fault(mode="none")
        try:
            os.remove(DATA_PATH)
        except FileNotFoundError:
            pass


state = GatewayState()

app = FastAPI(
    title="站点观测网关（模拟）",
    version="1.0.0",
    description="供数仓采集端对接的模拟测站网关：投放积压、整批拉取、确认清退、"
                "断线与回执丢失故障注入。",
)


@app.exception_handler(RequestValidationError)
async def _validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    # pydantic 的 errors() 在 ctx 里可能带原始异常对象，清洗为纯 JSON 结构
    clean_errors = []
    for err in exc.errors():
        clean_errors.append({
            "type": str(err.get("type", "")),
            "loc": [str(part) for part in err.get("loc", [])],
            "msg": str(err.get("msg", "")),
            "input": _json_safe(err.get("input")),
        })
    return JSONResponse(
        status_code=422,
        content={"error": {
            "code": "validation_error",
            "message": "投放报文校验失败",
            "details": {"errors": clean_errors}}},
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    return str(value)


class _DisconnectMiddleware:
    """纯 ASGI 断线故障中间件。

    刻意不使用 ``BaseHTTPMiddleware``：它对响应体的流式转发与
    ServerErrorMiddleware 存在已知的 Content-Length 交互问题。
    这里只在断线时短路返回 503，其余请求原样透传、不触碰响应消息。
    控制面 /test/* 始终可达，便于注入/解除故障。
    """

    def __init__(self, app_) -> None:
        self.app = app_

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        parts = path.split("/")
        # /stations/{sid}/batches/{bid}/ack -> ['', 'stations', sid, 'batches', bid, 'ack']
        is_ack_endpoint = (
            len(parts) == 6 and parts[1] == "stations"
            and parts[3] == "batches" and parts[5] == "ack"
        )
        is_station = path.startswith("/stations/")
        station_id = parts[2] if is_station and len(parts) > 2 else ""

        with _LOCK:
            disconnect = is_station and state._fault_active(
                "disconnect", station_id)
            # 回执丢失只作用于确认端点：/stations/{sid}/batches/{bid}/ack
            ack_lost = (
                is_ack_endpoint and scope.get("method") == "POST"
                and state._fault_active("ack_lost", station_id)
            )

        if disconnect:
            await self._short_circuit(send, 503, {
                "error": {
                    "code": "gateway_unavailable",
                    "message": f"测站 {station_id} 断线中，网关拒绝服务",
                    "details": {}}})
            return

        if ack_lost:
            batch_id = parts[4]
            with _LOCK:
                status, body = state.ack(station_id, batch_id)
            if status != 200:
                # 批次本身无法确认（如重复确认）：按正常错误响应透传
                await self._short_circuit(send, status, body)
                return
            # 已成功清退，仅回执丢失：原始字节直送，绕过错误中间件重写
            await self._short_circuit(send, 502, {
                "error": {
                    "code": "ack_lost",
                    "message": "网关已清退该批次，但确认回执在链路上丢失",
                    "details": {}}})
            return

        await self.app(scope, receive, send)

    @staticmethod
    async def _short_circuit(send, status_code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        await send({"type": "http.response.start", "status": status_code,
                    "headers": [(b"content-type", b"application/json"),
                                (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})


app.add_middleware(_DisconnectMiddleware)


@app.get("/health", tags=["运维"])
def health() -> dict:
    return {"status": "ok", "service": "gateway"}


@app.post("/test/faults", tags=["测试故障注入"],
          summary="注入/解除断线或回执丢失故障")
def inject_fault(fault: Fault) -> dict:
    with _LOCK:
        # 控制面始终可达：即使正在模拟断线，也要允许注入 none 解除故障
        if fault.delay_sec:
            import time
            time.sleep(fault.delay_sec)
        state.fault = fault
        return {"status": "configured", "fault": fault.model_dump()}


@app.post("/test/reset", tags=["测试故障注入"], summary="清空全部积压与故障")
def reset_state() -> dict:
    with _LOCK:
        state.reset()
    return {"status": "reset"}


@app.get("/test/state", tags=["测试故障注入"], summary="查看网关积压与故障状态")
def get_state() -> dict:
    with _LOCK:
        return state.snapshot()


@app.post("/test/stations/{station_id}/backlog", tags=["测试投放"],
          summary="向指定测站网关投放一批积压观测")
def seed_backlog(station_id: str, payload: SeedBatch) -> dict:
    with _LOCK:
        return state.seed(station_id, payload)


@app.get("/stations/{station_id}/batches/next", tags=["采集协议"],
         summary="拉取该测站队头批次（未确认前重复拉取返回同一批）")
def pull_next(station_id: str):
    with _LOCK:
        status, body = state.next_batch(station_id)
        if status == 204:
            # 用无内容的纯 Response 表达 204：JSONResponse(None) 仍带
            # content-type 且在 keep-alive 下会触发 Content-Length 不匹配
            return Response(status_code=204)
        return body


@app.post("/stations/{station_id}/batches/{batch_id}/ack", tags=["采集协议"],
          summary="确认清退队头批次；重复确认/批号不符返回 410")
def ack_batch(station_id: str, batch_id: str):
    with _LOCK:
        status, body = state.ack(station_id, batch_id)
        return JSONResponse(status_code=status, content=body)
