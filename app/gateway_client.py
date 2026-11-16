"""站点网关 HTTP 客户端（采集端侧）。

只负责协议收发与故障分类，不触碰本地数据库，便于在 service 层组合
“本地事务提交后才确认网关”的顺序。所有异常都归类为 :class:`GatewayError`，
上层据此返回 502/504，绝不能把网关故障误报为采集完成。

拉取与确认都是幂等操作，因此对传输层的连接重置/陈旧连接做一次自动重试；
重试仍失败时抛出 :class:`GatewayError`，本地批次保持 ``persisted``，
由下一次采集对账收敛，不漏行也不会重复清退。
"""
from __future__ import annotations

from typing import Any, Callable

import httpx

from .config import settings

# 传输层可安全重试的异常：连接被对端关闭/重置、读到陈旧连接等
_RETRIABLE_TRANSPORT = (
    httpx.RemoteProtocolError,
    httpx.ReadError,
    httpx.WriteError,
    httpx.ConnectError,
    httpx.ReadTimeout,
)


class GatewayError(Exception):
    """网关不可达/超时/协议异常。"""

    def __init__(self, code: str, message: str, *, status_code: int | None = None):
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class GatewayClient:
    def __init__(self, base_url: str | None = None,
                 timeout: float | None = None,
                 transport: httpx.BaseTransport | None = None) -> None:
        self.base_url = (base_url or settings.gateway_base_url).rstrip("/")
        self.timeout = timeout or settings.gateway_timeout_sec
        # 测试可注入自定义 transport（如 ASGI/模拟 transport）
        self._transport = transport

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout,
            transport=self._transport,
        )

    def _request_with_retry(self, send: Callable[[httpx.Client], httpx.Response]
                            ) -> httpx.Response:
        """执行一次 HTTP 调用；传输层连接故障时用全新连接重试一次。"""
        try:
            with self._client() as http:
                return send(http)
        except _RETRIABLE_TRANSPORT:
            # 陈旧 keep-alive 连接被对端关闭等：丢弃连接，重开一次
            with self._client() as http:
                return send(http)

    # ------------------------- 拉取 -------------------------
    def pull_next_batch(self, station_id: str) -> dict[str, Any] | None:
        """返回队头批次；积压为空返回 None。

        网关对未确认批次重复拉取返回同一批号同一内容（at-least-once），
        因此本函数天然可安全重试。
        """
        try:
            resp = self._request_with_retry(
                lambda http: http.get(f"/stations/{station_id}/batches/next")
            )
        except httpx.TimeoutException as exc:
            raise GatewayError("gateway_timeout",
                               f"拉取网关批次超时：{exc}") from exc
        except httpx.HTTPError as exc:
            raise GatewayError("gateway_unavailable",
                               f"网关不可达：{exc}") from exc

        if resp.status_code == 204:
            return None
        if resp.status_code >= 400:
            code = "gateway_pull_failed"
            try:
                code = resp.json().get("error", {}).get("code", code)
            except ValueError:
                pass
            raise GatewayError(
                code,
                f"网关拉取失败：HTTP {resp.status_code}",
                status_code=resp.status_code,
            )
        try:
            batch = resp.json()
        except ValueError as exc:
            raise GatewayError("gateway_protocol_error",
                               "网关返回的批次不是合法 JSON") from exc
        required = {"station_id", "batch_id", "rows"}
        if not isinstance(batch, dict) or not required.issubset(batch):
            raise GatewayError("gateway_protocol_error",
                               "网关批次报文缺少 station_id/batch_id/rows 字段")
        if batch["station_id"] != station_id:
            raise GatewayError(
                "gateway_protocol_error",
                "网关返回批次的站号与请求站号不一致",
            )
        return batch

    # ------------------------- 确认 -------------------------
    def ack_batch(self, station_id: str, batch_id: str) -> str:
        """确认清退。返回结果分类：

        - ``acked``：网关确认已清退；
        - ``already_cleared``：网关侧已无此批次（410），视为确认对账成功，
          覆盖“已清退但回执丢失后重试”的场景；
        - 其他失败抛 :class:`GatewayError`，本地保持 persisted 等待下次对账。
        """
        try:
            resp = self._request_with_retry(
                lambda http: http.post(
                    f"/stations/{station_id}/batches/{batch_id}/ack"
                )
            )
        except httpx.TimeoutException as exc:
            raise GatewayError("gateway_timeout",
                               f"批次确认超时：{exc}") from exc
        except httpx.HTTPError as exc:
            # 连接重置等传输错误：网关状态未知，按“未确认”处理，
            # 批次保留 persisted 由下次对账收敛（幂等，不会重复清退）
            raise GatewayError("gateway_unavailable",
                               f"批次确认链路异常：{exc}") from exc

        if resp.status_code == 200:
            return "acked"
        if resp.status_code == 410:
            # 批次已不在队头/队列：此前的确认实际上已生效，回执只是丢了
            return "already_cleared"
        code = "gateway_ack_failed"
        try:
            code = resp.json().get("error", {}).get("code", code)
        except ValueError:
            pass
        raise GatewayError(
            code,
            f"网关确认失败：HTTP {resp.status_code}",
            status_code=resp.status_code,
        )
