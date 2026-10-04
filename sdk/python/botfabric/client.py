"""BotFabric 外部进程插件双向 gRPC Session 客户端。"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import grpc
from google.protobuf import json_format, struct_pb2


@dataclass(slots=True)
class Event:
    id: str
    topic: str
    schema: str
    timestamp_unix_millis: int
    source: str
    bot_id: str
    endpoint_id: str = ""
    required_reply_bot: str = ""
    response_mode: str = ""
    response_timeout_millis: int = 0
    fallback_bots: list[str] = field(default_factory=list)
    correlation_id: str = ""
    trace_id: str = ""
    headers: dict[str, str] = field(default_factory=dict)
    payload: bytes = b""


@dataclass(slots=True)
class Request:
    service: str
    method: str
    caller: str = ""
    trace_id: str = ""
    correlation_id: str = ""
    metadata: dict[str, str] = field(default_factory=dict)
    payload: bytes = b""


@dataclass(slots=True)
class Response:
    payload: bytes = b""
    error: ServiceError | None = None


@dataclass(slots=True)
class ServiceError(Exception):
    code: str
    message: str
    retryable: bool = False
    details: dict[str, str] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


EventHandler = Callable[[Event], Awaitable[None]]
InvokeServiceHandler = Callable[[Request], Awaitable[Response]]
HealthHandler = Callable[[], Awaitable[tuple[str, str, Mapping[str, str]]]]
ShutdownHandler = Callable[[], Awaitable[None]]


class SessionClient:
    """Runtime 托管的 Unix Socket 双向 Session 客户端。

    使用 ``BOTFABRIC_SOCKET`` 等 Runtime 注入的环境变量，不应自行连接外部地址。
    ``subscriptions`` 的回调按 Topic 精确匹配；``services`` 使用 ``(service, method)`` 键。
    """

    def __init__(
        self,
        *,
        plugin_id: str | None = None,
        plugin_version: str | None = None,
        protocol_version: str | None = None,
        socket_path: str | None = None,
        session_token: str | None = None,
        subscriptions: Mapping[str, EventHandler] | None = None,
        services: Mapping[tuple[str, str], InvokeServiceHandler] | None = None,
        health: HealthHandler | None = None,
        shutdown: ShutdownHandler | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.plugin_id = plugin_id or os.environ.get("BOTFABRIC_PLUGIN_ID", "")
        self.plugin_version = plugin_version or os.environ.get(
            "BOTFABRIC_PLUGIN_VERSION", ""
        )
        self.protocol_version = protocol_version or os.environ.get(
            "BOTFABRIC_PROTOCOL_VERSION", ""
        )
        self.socket_path = socket_path or os.environ.get("BOTFABRIC_SOCKET", "")
        self.session_token = session_token or os.environ.get(
            "BOTFABRIC_SESSION_TOKEN", ""
        )
        self.subscriptions = dict(subscriptions or {})
        self.services = dict(services or {})
        self.health_handler = health
        self.shutdown_handler = shutdown
        self.timeout = timeout
        self._channel: grpc.aio.Channel | None = None
        self._call: Any = None
        self._reader: asyncio.Task[None] | None = None
        self._ready_future: asyncio.Future[dict[str, Any]] | None = None
        self._write_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._inbound_tasks: set[asyncio.Task[None]] = set()
        self._closed = False
        self._connected = False
        self._shutdown_event = asyncio.Event()

    async def connect(self) -> None:
        """连接 Runtime、完成身份握手并启动收帧循环。"""
        if self._connected:
            raise RuntimeError("Session 已连接")
        if self._closed:
            raise RuntimeError("Session 已关闭，不支持重连")
        missing = [
            name
            for name, value in (
                ("BOTFABRIC_PLUGIN_ID", self.plugin_id),
                ("BOTFABRIC_PLUGIN_VERSION", self.plugin_version),
                ("BOTFABRIC_PROTOCOL_VERSION", self.protocol_version),
                ("BOTFABRIC_SOCKET", self.socket_path),
                ("BOTFABRIC_SESSION_TOKEN", self.session_token),
            )
            if not value
        ]
        if missing:
            raise RuntimeError("缺少 Runtime 注入的环境变量: " + ", ".join(missing))
        self._channel = grpc.aio.insecure_channel(
            "unix:" + self.socket_path,
            options=(
                ("grpc.max_receive_message_length", 4 * 1024 * 1024),
                ("grpc.max_send_message_length", 4 * 1024 * 1024),
            ),
        )
        try:
            await asyncio.wait_for(self._channel.channel_ready(), timeout=self.timeout)
        except BaseException:
            await self.close()
            raise
        method = self._channel.stream_stream(
            "/botfabric.v1.ProcessPlugin/Session",
            request_serializer=_serialize_struct,
            response_deserializer=_deserialize_struct,
        )
        self._call = method(timeout=self.timeout)
        self._ready_future = asyncio.get_running_loop().create_future()
        self._reader = asyncio.create_task(
            self._read_loop(), name="botfabric-session-reader"
        )
        try:
            await self._send(
                {
                    "type": "hello",
                    "plugin_id": self.plugin_id,
                    "plugin_version": self.plugin_version,
                    "protocol_version": self.protocol_version,
                    "session_token": self.session_token,
                }
            )
            ready = self._ready_future
            if ready is None:
                raise ConnectionError("Session reader 未启动")
            result = await asyncio.wait_for(asyncio.shield(ready), timeout=self.timeout)
            if result.get("type") != "ready" or result.get("accepted") is not True:
                raise RuntimeError("Runtime 拒绝外部插件握手")
        except BaseException:
            await self.close()
            raise
        self._connected = True

    async def serve_forever(self) -> None:
        """保持 Session 运行；连接断开或客户端关闭时返回。"""
        if self._reader is None:
            raise RuntimeError("请先调用 connect()")
        try:
            await self._shutdown_event.wait()
        finally:
            await self.close()

    async def publish(self, event: Event, *, timeout: float | None = None) -> None:
        """向 Runtime 发布事件，并等待策略/投递结果。"""
        result = await self._request(
            "publish_result",
            {
                "type": "publish_event",
                "event": _event_to_wire(event),
            },
            timeout=timeout,
        )
        _raise_wire_error(result.get("error"))

    async def invoke_host(
        self, request: Request, *, timeout: float | None = None
    ) -> Response:
        """调用 Manifest 授权给插件的 Host 服务。"""
        result = await self._request(
            "invoke_host_response",
            {
                "type": "invoke_host",
                "request": _request_to_wire(request),
            },
            timeout=timeout,
        )
        return _response_from_wire(result.get("response"))

    async def close(self) -> None:
        """关闭 Session，取消挂起请求并释放 gRPC channel。"""
        if self._closed:
            return
        self._closed = True
        error = ConnectionError("BotFabric Session 已关闭")
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(error)
        self._pending.clear()
        ready = self._ready_future
        if ready is not None and not ready.done():
            ready.set_exception(error)
        call = self._call
        self._call = None
        if call is not None:
            call.cancel()
        inbound_tasks = tuple(self._inbound_tasks)
        self._inbound_tasks.clear()
        for task in inbound_tasks:
            task.cancel()
        if inbound_tasks:
            await asyncio.gather(*inbound_tasks, return_exceptions=True)
        reader = self._reader
        self._reader = None
        if reader is not None and reader is not asyncio.current_task():
            reader.cancel()
            try:
                await reader
            except asyncio.CancelledError:
                pass
        channel = self._channel
        self._channel = None
        if channel is not None:
            await channel.close()
        self._connected = False
        self._shutdown_event.set()

    async def _read_loop(self) -> None:
        try:
            async for frame in self._call:
                await self._dispatch(frame)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._fail_pending(exc)
            ready = self._ready_future
            if ready is not None and not ready.done():
                ready.set_exception(exc)
        finally:
            self._fail_pending(ConnectionError("BotFabric Session 已断开"))
            ready = self._ready_future
            if ready is not None and not ready.done():
                ready.set_exception(ConnectionError("Runtime 在握手前关闭了 Session"))
            self._shutdown_event.set()

    async def _dispatch(self, frame: dict[str, Any]) -> None:
        frame_type = frame.get("type", "")
        request_id = frame.get("request_id", "")
        if frame.get("type") == "ready":
            self._connected = frame.get("accepted") is True
            ready = self._ready_future
            if ready is not None and not ready.done():
                ready.set_result(frame)
            return
        if frame_type in {
            "publish_result",
            "invoke_host_response",
            "event_ack",
            "invoke_plugin_response",
            "health_response",
            "shutdown_ack",
        }:
            future = self._pending.pop(request_id, None)
            if future is not None and not future.done():
                future.set_result(frame)
            return
        if frame_type == "shutdown":
            self._spawn_inbound(self._handle_shutdown(request_id))
            return
        if frame_type == "health":
            self._spawn_inbound(self._handle_health(request_id))
            return
        if frame_type == "event":
            self._spawn_inbound(self._handle_event(frame))
            return
        if frame_type == "invoke_plugin":
            self._spawn_inbound(self._handle_invoke(frame))
            return
        if frame_type == "ready":
            return
        if frame_type == "error":
            raise RuntimeError(
                str(frame.get("error", {}).get("message", "Runtime 返回协议错误"))
            )
        raise RuntimeError(f"Runtime 发送了未知帧类型: {frame_type}")

    def _spawn_inbound(self, coroutine: Awaitable[None]) -> None:
        task = asyncio.create_task(coroutine)
        self._inbound_tasks.add(task)
        task.add_done_callback(self._inbound_done)

    def _inbound_done(self, task: asyncio.Task[None]) -> None:
        self._inbound_tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._fail_pending(error)
            if self._call is not None:
                self._call.cancel()
            self._shutdown_event.set()

    async def _handle_shutdown(self, request_id: str) -> None:
        accepted = True
        if self.shutdown_handler is not None:
            try:
                await self.shutdown_handler()
            except Exception:  # noqa: BLE001
                accepted = False
        await self._send(
            {"type": "shutdown_ack", "request_id": request_id, "accepted": accepted}
        )
        self._shutdown_event.set()

    async def _handle_health(self, request_id: str) -> None:
        state, message, details = ("healthy", "", {})
        if self.health_handler is not None:
            try:
                state, message, details = await self.health_handler()
            except Exception as exc:  # noqa: BLE001
                state, message, details = "unhealthy", str(exc), {}
        await self._send(
            {
                "type": "health_response",
                "request_id": request_id,
                "state": state,
                "message": message,
                "details": dict(details),
            }
        )

    async def _handle_event(self, frame: dict[str, Any]) -> None:
        event = _event_from_wire(frame.get("event"))
        handler = self.subscriptions.get(event.topic)
        error: dict[str, Any] | None = None
        if handler is not None:
            try:
                await handler(event)
            except Exception as exc:  # noqa: BLE001
                error = _error_to_wire(exc)
        reply: dict[str, Any] = {
            "type": "event_ack",
            "request_id": frame.get("request_id", ""),
        }
        if error is not None:
            reply["error"] = error
        await self._send(reply)

    async def _handle_invoke(self, frame: dict[str, Any]) -> None:
        request = _request_from_wire(frame.get("request"))
        handler = self.services.get((request.service, request.method))
        if handler is None:
            response = Response(
                error=ServiceError("unavailable", "Python 插件未注册此服务")
            )
        else:
            try:
                response = await handler(request)
            except Exception as exc:  # noqa: BLE001
                response = Response(error=_error_to_exception(exc))
        await self._send(
            {
                "type": "invoke_plugin_response",
                "request_id": frame.get("request_id", ""),
                "response": _response_to_wire(response),
            }
        )

    async def _request(
        self,
        response_type: str,
        frame: dict[str, Any],
        *,
        timeout: float | None,
    ) -> dict[str, Any]:
        if not self._connected or self._closed:
            raise ConnectionError("BotFabric Session 尚未连接或已关闭")
        request_id = uuid.uuid4().hex
        future: asyncio.Future[dict[str, Any]] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[request_id] = future
        frame["request_id"] = request_id
        try:
            await self._send(frame)
            result = await asyncio.wait_for(
                future, timeout=self.timeout if timeout is None else timeout
            )
            if result.get("type") != response_type:
                raise RuntimeError(f"Runtime 响应类型错误: {result.get('type', '')}")
            if result.get("error"):
                _raise_wire_error(result["error"])
            return result
        finally:
            self._pending.pop(request_id, None)

    async def _send(self, frame: dict[str, Any]) -> None:
        if self._closed or self._call is None:
            raise ConnectionError("BotFabric Session 已关闭")
        async with self._write_lock:
            await self._call.write(_to_struct(frame))

    def _fail_pending(self, error: Exception) -> None:
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(error)
        self._pending.clear()


# `base64` 保留为显式 wire helper，确保 payload bytes 使用 Go encoding/json 的规范形态。
def _to_struct(frame: Mapping[str, Any]) -> struct_pb2.Struct:
    normalized = json.loads(json.dumps(frame, default=_json_default))
    value = struct_pb2.Struct()
    json_format.ParseDict(normalized, value)
    return value


def _serialize_struct(value: struct_pb2.Struct) -> bytes:
    return value.SerializeToString()


def _deserialize_struct(data: bytes) -> dict[str, Any]:
    value = struct_pb2.Struct.FromString(data)
    return json_format.MessageToDict(value, preserving_proto_field_name=True)


def _json_default(value: Any) -> Any:
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    raise TypeError(f"不支持的 wire 值: {type(value)!r}")


def _encode_bytes(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _decode_bytes(value: str | None) -> bytes:
    if not value:
        return b""
    return base64.b64decode(value, validate=True)


def _event_to_wire(event: Event) -> dict[str, Any]:
    return {
        "id": event.id,
        "topic": event.topic,
        "schema": event.schema,
        "timestamp_unix_millis": event.timestamp_unix_millis,
        "source": event.source,
        "bot_id": event.bot_id,
        "endpoint_id": event.endpoint_id,
        "required_reply_bot": event.required_reply_bot,
        "response_mode": event.response_mode,
        "response_timeout_millis": event.response_timeout_millis,
        "fallback_bots": event.fallback_bots,
        "correlation_id": event.correlation_id,
        "trace_id": event.trace_id,
        "headers": event.headers,
        "payload": _encode_bytes(event.payload),
    }


def _event_from_wire(value: Mapping[str, Any] | None) -> Event:
    value = value or {}
    return Event(
        id=str(value.get("id", "")),
        topic=str(value.get("topic", "")),
        schema=str(value.get("schema", "")),
        timestamp_unix_millis=int(value.get("timestamp_unix_millis", 0)),
        source=str(value.get("source", "")),
        bot_id=str(value.get("bot_id", "")),
        endpoint_id=str(value.get("endpoint_id", "")),
        required_reply_bot=str(value.get("required_reply_bot", "")),
        response_mode=str(value.get("response_mode", "")),
        response_timeout_millis=int(value.get("response_timeout_millis", 0)),
        fallback_bots=list(value.get("fallback_bots", [])),
        correlation_id=str(value.get("correlation_id", "")),
        trace_id=str(value.get("trace_id", "")),
        headers={str(k): str(v) for k, v in value.get("headers", {}).items()},
        payload=_decode_bytes(value.get("payload")),
    )


def _request_to_wire(request: Request) -> dict[str, Any]:
    return {
        "service": request.service,
        "method": request.method,
        "caller": request.caller,
        "trace_id": request.trace_id,
        "correlation_id": request.correlation_id,
        "metadata": request.metadata,
        "payload": _encode_bytes(request.payload),
    }


def _request_from_wire(value: Mapping[str, Any] | None) -> Request:
    value = value or {}
    return Request(
        service=str(value.get("service", "")),
        method=str(value.get("method", "")),
        caller=str(value.get("caller", "")),
        trace_id=str(value.get("trace_id", "")),
        correlation_id=str(value.get("correlation_id", "")),
        metadata={str(k): str(v) for k, v in value.get("metadata", {}).items()},
        payload=_decode_bytes(value.get("payload")),
    )


def _response_to_wire(response: Response) -> dict[str, Any]:
    wire: dict[str, Any] = {"payload": _encode_bytes(response.payload)}
    if response.error is not None:
        wire["error"] = _error_to_wire(response.error)
    return wire


def _response_from_wire(value: Mapping[str, Any] | None) -> Response:
    value = value or {}
    error = value.get("error")
    return Response(
        payload=_decode_bytes(value.get("payload")),
        error=ServiceError(
            code=str(error.get("code", "internal")),
            message=str(error.get("message", "")),
            retryable=bool(error.get("retryable", False)),
            details={str(k): str(v) for k, v in error.get("details", {}).items()},
        )
        if error
        else None,
    )


def _raise_wire_error(value: Mapping[str, Any] | None) -> None:
    if value:
        raise ServiceError(
            code=str(value.get("code", "internal")),
            message=str(value.get("message", "")),
            retryable=bool(value.get("retryable", False)),
            details={str(k): str(v) for k, v in value.get("details", {}).items()},
        )


def _error_to_exception(error: Exception) -> ServiceError:
    if isinstance(error, ServiceError):
        return error
    return ServiceError("internal", str(error))


def _error_to_wire(error: Exception) -> dict[str, Any]:
    error = _error_to_exception(error)
    return {
        "code": error.code,
        "message": error.message,
        "retryable": error.retryable,
        "details": error.details,
    }


__all__ = [
    "Event",
    "EventHandler",
    "HealthHandler",
    "InvokeServiceHandler",
    "Request",
    "Response",
    "ServiceError",
    "SessionClient",
    "ShutdownHandler",
]
