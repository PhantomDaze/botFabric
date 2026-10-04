# Python 外部进程插件 SDK

Go 插件可使用 `sdk/go`；Python 插件可使用 `sdk/python`，通过 Runtime 注入的 Unix Socket gRPC 双向 Session 与 BotFabric 通信。Python SDK 使用 `grpcio` 和 `protobuf`，不需要为 `ProcessPlugin` 生成语言绑定。

## 安装

在项目环境中安装本地 SDK：

```bash
python -m pip install ./sdk/python
```

或仅安装运行依赖：

```bash
python -m pip install -r sdk/python/requirements.txt
```

## 最小插件

完整示例见 [`examples/minimal_plugin.py`](examples/minimal_plugin.py)：

```python
import asyncio

from botfabric import Event, Request, Response, SessionClient


async def on_event(event: Event) -> None:
    print(event.topic, event.payload)


async def echo(request: Request) -> Response:
    return Response(payload=request.payload)


async def main() -> None:
    client = SessionClient(
        subscriptions={"message.group": on_event},
        services={("example.echo", "Run"): echo},
    )
    await client.connect()
    try:
        await client.serve_forever()
    finally:
        await client.close()


asyncio.run(main())
```

SDK 从 `BOTFABRIC_SOCKET`、`BOTFABRIC_PLUGIN_ID`、`BOTFABRIC_PLUGIN_VERSION`、`BOTFABRIC_PROTOCOL_VERSION` 和 `BOTFABRIC_SESSION_TOKEN` 读取 Runtime 注入值，完成首帧身份握手后运行 Session。插件不应将会话令牌写入日志或转发给其他服务。

## 双向通信

- `subscriptions` 用 Topic 精确匹配入站事件；回调成功后自动回 `event_ack`。
- `services` 以 `(service, method)` 为键，处理 Runtime 发来的远程调用。
- `publish(Event(...))` 向 Host 发布事件。
- `invoke_host(Request(...))` 调用 Manifest 授权的 Host 服务；Caller 身份由 Runtime 确认，不能通过自报字段提升权限。
- 回调在独立异步任务中处理，因此可以安全地从事件、服务、Health 或 Shutdown 回调中再次调用 Host API；任务会在 `close()` 时取消并等待退出。
- Payload 在公开对象中为 `bytes`；SDK 按 Session Struct wire 格式进行 Base64 编解码。业务 payload 是否为 JSON 由插件自行决定。
- 所有 I/O API 均为 `async`。默认单次等待上限为 30 秒，可在构造 `SessionClient(timeout=...)` 时调整，也可对单次 `publish`、`invoke_host` 指定 `timeout`。

运行 SDK 自测：

```bash
python -m pip install -e ./sdk/python
python -m unittest discover -s sdk/python/tests -v
```

该互操作测试启动真实 Unix Socket gRPC 回环服务，验证握手、事件 ACK、Host 调用、Health 与 Struct payload 编码。
