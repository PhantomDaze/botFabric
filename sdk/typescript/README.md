# BotFabric TypeScript 外部进程插件 SDK

Node.js/TypeScript 外部进程插件可使用本 SDK，通过 Runtime 注入的 Unix Socket gRPC 双向 Session 与 BotFabric 通信。SDK 使用 protobuf `Struct` 承载帧，不需要生成项目专属 protobuf 代码。

## 要求与安装

要求 Node.js 20 或更高版本。安装本地包：

```bash
npm install ./sdk/typescript
```

## 最小插件

```typescript
import { SessionClient, type Event } from "@botfabric/sdk";

async function onMessage(event: Event): Promise<void> {
  console.log(event.topic, Buffer.from(event.payload ?? []).toString());
}

const client = new SessionClient({
  subscriptions: { "message.group": onMessage },
  services: {
    "example.echo.Run": async (request) => ({ payload: request.payload ?? Buffer.alloc(0) }),
  },
});

await client.connect();
try {
  await client.serveForever();
} finally {
  await client.close();
}
```

Runtime 注入 `BOTFABRIC_SOCKET`、`BOTFABRIC_PLUGIN_ID`、`BOTFABRIC_PLUGIN_VERSION`、`BOTFABRIC_PROTOCOL_VERSION` 和 `BOTFABRIC_SESSION_TOKEN`。SDK 使用这些值完成首帧身份握手；不要将会话令牌写入日志或转发给其他服务。

## 双向通信

- `subscriptions` 使用 Topic 精确匹配 Runtime 投递的事件，回调成功后自动发送 `event_ack`。
- `services` 使用 `"service.method"` 为键，处理 Runtime 发起的远程调用。
- `publish(event)` 向 Host 发布事件；`invokeHost(request)` 调用 Manifest 授权的 Host 服务。
- 所有入站回调都携带可选 `AbortSignal`，在 `close()` 或流断开时会触发取消；`close()` 最多等待默认请求超时时间以回收回调，之后会继续关闭传输。
- `payload` 在公开对象中为 `Uint8Array`/`Buffer`，在线路 Struct 帧中使用 Base64 字符串。
- 请求默认超时为 30 秒，可通过构造选项 `timeout` 或单次方法选项调整。

运行 SDK 构建、类型检查和 Unix Socket 回环互操作测试：

```bash
cd sdk/typescript
npm install
npm run build
npm run typecheck
npm test
```

本 SDK 当前只支持 Node.js 外部进程与 Unix Socket；不覆盖 Windows transport、同进程插件或高级沙箱。
