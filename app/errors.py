"""统一的业务错误类型与处理程序。"""
import math

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


class APIError(Exception):
    def __init__(self, status_code: int, code: str, message: str, details: dict | None = None):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details or {}
        super().__init__(message)


def _sanitize(value):
    """把非有限浮点及无法合规 JSON 序列化的回显值替换为安全表示。"""
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, dict):
        return {str(k): _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize(v) for v in value]
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    return str(value)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(APIError)
    async def _handle_api_error(_: Request, exc: APIError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "details": _sanitize(exc.details),
                }
            },
        )

    @app.exception_handler(RequestValidationError)
    async def _handle_validation_error(_: Request,
                                       exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "validation_error",
                    "message": "请求参数校验失败",
                    "details": {"errors": _sanitize(exc.errors())},
                }
            },
        )
