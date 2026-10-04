import * as grpc from "@grpc/grpc-js";
import { randomUUID } from "node:crypto";
import { decodeStruct, encodeStruct } from "./struct.js";

export interface Event {
  id: string;
  topic: string;
  schema: string;
  timestamp_unix_millis: number;
  source: string;
  bot_id: string;
  endpoint_id?: string;
  required_reply_bot?: string;
  response_mode?: string;
  response_timeout_millis?: number;
  fallback_bots?: string[];
  correlation_id?: string;
  trace_id?: string;
  headers?: Record<string, string>;
  payload?: Uint8Array;
}

export interface Request {
  service: string;
  method: string;
  caller?: string;
  trace_id?: string;
  correlation_id?: string;
  metadata?: Record<string, string>;
  payload?: Uint8Array;
}

export interface Response {
  payload: Buffer;
  error?: ServiceErrorData;
}

export interface ServiceErrorData {
  code: string;
  message: string;
  retryable?: boolean;
  details?: Record<string, string>;
}

export class ServiceError extends Error implements ServiceErrorData {
  code: string;
  retryable: boolean;
  details: Record<string, string>;

  constructor(data: ServiceErrorData) {
    super(data.message);
    this.name = "ServiceError";
    this.code = data.code;
    this.retryable = data.retryable ?? false;
    this.details = data.details ?? {};
  }
}

export type EventHandler = (event: Event, signal?: AbortSignal) => void | Promise<void>;
export type ServiceHandler = (request: Request, signal?: AbortSignal) => Response | Promise<Response>;
export type HealthHandler = (signal?: AbortSignal) => { state: string; message?: string; details?: Record<string, string> } | Promise<{ state: string; message?: string; details?: Record<string, string> }>;
export type ShutdownHandler = (signal?: AbortSignal) => void | Promise<void>;

export interface SessionClientOptions {
  pluginId?: string;
  pluginVersion?: string;
  protocolVersion?: string;
  socketPath?: string;
  sessionToken?: string;
  subscriptions?: Record<string, EventHandler>;
  services?: Record<string, ServiceHandler>;
  health?: HealthHandler;
  shutdown?: ShutdownHandler;
  timeout?: number;
}

interface Frame extends Record<string, unknown> {
  type: string;
  request_id?: string;
}

interface PendingRequest {
  expected: string;
  resolve: (frame: Frame) => void;
  reject: (error: Error) => void;
  timer: NodeJS.Timeout;
}

interface SessionStream extends NodeJS.EventEmitter {
  write(value: Buffer, callback?: (error?: Error | null) => void): boolean;
  end(): void;
  cancel(): void;
}

interface SessionGrpcClient extends grpc.Client {
  Session(): SessionStream;
}

const sessionDefinition: grpc.ServiceDefinition = {
  Session: {
    path: "/botfabric.v1.ProcessPlugin/Session",
    requestStream: true,
    responseStream: true,
    requestSerialize: (value: Buffer) => value,
    requestDeserialize: (value: Buffer) => value,
    responseSerialize: (value: Buffer) => value,
    responseDeserialize: (value: Buffer) => value,
  },
};
const SessionGrpcClientConstructor = grpc.makeGenericClientConstructor(sessionDefinition, "ProcessPlugin") as unknown as {
  new (address: string, credentials: grpc.ChannelCredentials): SessionGrpcClient;
};

export class SessionClient {
  private readonly pluginId: string;
  private readonly pluginVersion: string;
  private readonly protocolVersion: string;
  private readonly socketPath: string;
  private readonly sessionToken: string;
  private readonly subscriptions: Record<string, EventHandler>;
  private readonly services: Record<string, ServiceHandler>;
  private readonly health: HealthHandler | undefined;
  private readonly shutdown: ShutdownHandler | undefined;
  private readonly timeout: number;
  private readonly pending = new Map<string, PendingRequest>();
  private readonly inbound = new Set<Promise<void>>();
  private readonly inboundAbort = new AbortController();
  private client: SessionGrpcClient | undefined;
  private stream: SessionStream | undefined;
  private connected = false;
  private closed = false;
  private closing: Promise<void> | undefined;
  private shutdownResolve: (() => void) | undefined;
  private readyResolve: ((frame: Frame) => void) | undefined;
  private readyReject: ((error: Error) => void) | undefined;
  private readonly shutdownDone: Promise<void>;
  private inboundStopped = false;

  constructor(options: SessionClientOptions = {}) {
    this.pluginId = options.pluginId ?? process.env.BOTFABRIC_PLUGIN_ID ?? "";
    this.pluginVersion = options.pluginVersion ?? process.env.BOTFABRIC_PLUGIN_VERSION ?? "";
    this.protocolVersion = options.protocolVersion ?? process.env.BOTFABRIC_PROTOCOL_VERSION ?? "";
    this.socketPath = options.socketPath ?? process.env.BOTFABRIC_SOCKET ?? "";
    this.sessionToken = options.sessionToken ?? process.env.BOTFABRIC_SESSION_TOKEN ?? "";
    this.subscriptions = options.subscriptions ?? {};
    this.services = options.services ?? {};
    this.health = options.health;
    this.shutdown = options.shutdown;
    this.timeout = options.timeout ?? 30_000;
    this.shutdownDone = new Promise<void>((resolve) => { this.shutdownResolve = resolve; });
    if (!(this.timeout > 0 && Number.isFinite(this.timeout))) throw new RangeError("timeout must be positive");
  }

  async connect(): Promise<void> {
    if (this.connected) throw new Error("Session 已连接");
    if (this.closed) throw new Error("Session 已关闭，不支持重连");
    const missing = [
      ["BOTFABRIC_PLUGIN_ID", this.pluginId],
      ["BOTFABRIC_PLUGIN_VERSION", this.pluginVersion],
      ["BOTFABRIC_PROTOCOL_VERSION", this.protocolVersion],
      ["BOTFABRIC_SOCKET", this.socketPath],
      ["BOTFABRIC_SESSION_TOKEN", this.sessionToken],
    ].filter(([, value]) => !value).map(([name]) => name);
    if (missing.length) throw new Error(`缺少 Runtime 注入的环境变量: ${missing.join(", ")}`);

    const socketTarget = this.socketPath.startsWith("/") ? `unix://${this.socketPath}` : `unix:${this.socketPath}`;
    const client = new SessionGrpcClientConstructor(socketTarget, grpc.credentials.createInsecure());
    this.client = client;
    const stream = client.Session();
    this.stream = stream;
    stream.on("data", (data: Buffer) => {
      try {
        this.dispatch(decodeStruct(data) as Frame);
      } catch (error) {
        this.fail(error instanceof Error ? error : new Error(String(error)));
      }
    });
    stream.on("error", (error: Error) => this.fail(error));
    stream.on("end", () => {
      const error = new Error("BotFabric Session 已断开");
      this.inboundAbort.abort(error);
      this.inboundStopped = true;
      this.failPending(error);
      this.finish();
      void this.close();
    });
    stream.on("status", (status: grpc.StatusObject) => {
      if (status.code !== grpc.status.OK && status.code !== grpc.status.CANCELLED) {
        this.fail(new Error(`Session gRPC 结束: ${status.details || status.code}`));
      }
    });

    try {
      const readyPromise = new Promise<Frame>((resolve, reject) => {
        this.readyResolve = (frame) => {
          clearTimeout(timer);
          resolve(frame);
        };
        this.readyReject = (error) => {
          clearTimeout(timer);
          reject(error);
        };
        const timer = setTimeout(() => {
          this.readyResolve = undefined;
          this.readyReject = undefined;
          reject(new Error(`Session 握手超时 (${this.timeout}ms)`));
        }, this.timeout);
        this.stream?.write(encodeStruct({
          type: "hello",
          plugin_id: this.pluginId,
          plugin_version: this.pluginVersion,
          protocol_version: this.protocolVersion,
          session_token: this.sessionToken,
        }), (error) => {
          if (error) this.readyReject?.(error);
        });
      });
      const response = await readyPromise;
      if (response.accepted !== true) throw new Error("Runtime 拒绝外部插件握手");
      this.connected = true;
    } catch (error) {
      await this.close();
      throw error;
    }
  }

  async serveForever(): Promise<void> {
    if (!this.stream) throw new Error("请先调用 connect()");
    await this.shutdownDone;
    await this.close();
  }

  async publish(event: Event, options: { timeout?: number } = {}): Promise<void> {
    const frame = await this.request("publish_result", {
      type: "publish_event",
      event: eventToWire(event),
    }, options.timeout);
    if (frame.error && typeof frame.error === "object") throw new ServiceError(frame.error as ServiceErrorData);
  }

  async invokeHost(request: Request, options: { timeout?: number } = {}): Promise<Response> {
    const frame = await this.request("invoke_host_response", {
      type: "invoke_host",
      request: requestToWire(request),
    }, options.timeout);
    const response = frame.response as Record<string, unknown> | undefined;
    if (!response) throw new Error("Host 响应缺少 response");
    if (response.error && typeof response.error === "object") throw new ServiceError(response.error as ServiceErrorData);
    return { payload: decodeBytes(response.payload) };
  }

  async close(): Promise<void> {
    if (this.closing) return this.closing;
    if (this.closed) return;
    this.closed = true;
    this.closing = this.closeOnce();
    return this.closing;
  }

  private async closeOnce(): Promise<void> {
    const error = new Error("BotFabric Session 已关闭");
    this.readyReject?.(error);
    this.readyResolve = undefined;
    this.readyReject = undefined;
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
    this.inboundAbort.abort(error);
    this.stream?.cancel();
    this.inboundStopped = true;
    this.client?.close();
    const inboundTasks = [...this.inbound];
    if (inboundTasks.length > 0) {
      let timer: NodeJS.Timeout | undefined;
      await Promise.race([
        Promise.allSettled(inboundTasks),
        new Promise<void>((resolve) => {
          timer = setTimeout(resolve, this.timeout);
        }),
      ]);
      if (timer) clearTimeout(timer);
    }
    this.inbound.clear();
    this.stream = undefined;
    this.client = undefined;
    this.finish();
  }

  private request(expected: string, frame: Record<string, unknown>, timeout = this.timeout): Promise<Frame> {
    if (this.closed) return Promise.reject(new Error("BotFabric Session 已关闭"));
    if (!this.stream) return Promise.reject(new Error("BotFabric Session 尚未连接"));
    if (!(timeout > 0 && Number.isFinite(timeout))) return Promise.reject(new RangeError("timeout must be positive"));
    const requestId = randomUUID();
    const stream = this.stream;
    return new Promise<Frame>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId);
        reject(new Error(`Session 请求超时 (${timeout}ms)`));
      }, timeout);
      this.pending.set(requestId, { expected, resolve, reject, timer });
      try {
        stream.write(encodeStruct({ ...frame, request_id: requestId }), (error) => {
          if (!error) return;
          clearTimeout(timer);
          this.pending.delete(requestId);
          reject(error);
        });
      } catch (error) {
        clearTimeout(timer);
        this.pending.delete(requestId);
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  private dispatch(frame: Frame): void {
    if (frame.type === "ready") {
      if (frame.accepted === false) this.readyReject?.(new Error("Runtime 拒绝外部插件握手"));
      else this.readyResolve?.(frame);
      this.readyResolve = undefined;
      this.readyReject = undefined;
      return;
    }
    switch (frame.type) {
      case "event": this.spawn(this.handleEvent(frame)); break;
      case "invoke_plugin": this.spawn(this.handleInvoke(frame)); break;
      case "health": this.spawn(this.handleHealth(frame)); break;
      case "shutdown": this.spawn(this.handleShutdown(frame)); break;
      case "publish_result":
      case "invoke_plugin_response":
      case "invoke_host_response":
      case "error":
        this.resolve(frame);
        break;
      default:
        void this.send({ type: "error", request_id: frame.request_id ?? "", error: { code: "invalid_argument", message: `未知外部插件帧类型: ${frame.type}` } });
    }
  }

  private resolve(frame: Frame): void {
    const id = frame.request_id;
    if (typeof id !== "string") return;
    const pending = this.pending.get(id);
    if (!pending) return;
    this.pending.delete(id);
    clearTimeout(pending.timer);
    if (frame.type === "error") pending.reject(new ServiceError((frame.error ?? { code: "internal", message: "Session failed" }) as ServiceErrorData));
    else if (frame.type !== pending.expected) pending.reject(new Error(`Session 响应类型不匹配: ${frame.type}`));
    else pending.resolve(frame);
  }

  private spawn(task: Promise<void>): void {
    if (this.inboundStopped) return;
    this.inbound.add(task);
    void task.catch((error: unknown) => {
      this.fail(error instanceof Error ? error : new Error(String(error)));
    }).finally(() => this.inbound.delete(task));
  }

  private async handleEvent(frame: Frame): Promise<void> {
    const event = frame.event as Record<string, unknown> | undefined;
    const requestId = typeof frame.request_id === "string" ? frame.request_id : "";
    if (!event || typeof event.topic !== "string") {
      await this.send({ type: "event_ack", request_id: requestId, error: { code: "invalid_argument", message: "event 帧缺少有效 event" } });
      return;
    }
    try {
      const handler = Object.hasOwn(this.subscriptions, event.topic) ? this.subscriptions[event.topic] : undefined;
      if (handler) await handler(eventFromWire(event), this.inboundAbort.signal);
      await this.send({ type: "event_ack", request_id: requestId, accepted: true });
    } catch (error) {
      await this.send({ type: "event_ack", request_id: requestId, accepted: false, error: errorToWire(error) });
    }
  }

  private async handleInvoke(frame: Frame): Promise<void> {
    const request = frame.request as Record<string, unknown> | undefined;
    const requestId = typeof frame.request_id === "string" ? frame.request_id : "";
    if (!request) {
      await this.send({ type: "invoke_plugin_response", request_id: requestId, response: { error: { code: "invalid_argument", message: "invoke_plugin 缺少 request" } } });
      return;
    }
    try {
      const key = `${String(request.service ?? "")}.${String(request.method ?? "")}`;
      const handler = Object.hasOwn(this.services, key) ? this.services[key] : undefined;
      if (!handler) throw new ServiceError({ code: "not_found", message: `未注册服务: ${key}` });
      const response = await handler(requestFromWire(request), this.inboundAbort.signal);
      await this.send({ type: "invoke_plugin_response", request_id: requestId, response: responseToWire(response) });
    } catch (error) {
      await this.send({ type: "invoke_plugin_response", request_id: requestId, response: { error: errorToWire(error) } });
    }
  }

  private async handleHealth(frame: Frame): Promise<void> {
    const requestId = typeof frame.request_id === "string" ? frame.request_id : "";
    try {
      const result = this.health ? await this.health(this.inboundAbort.signal) : { state: "healthy", message: "" };
      await this.send({ type: "health_response", request_id: requestId, state: result.state, message: result.message ?? "", details: result.details ?? {} });
    } catch (error) {
      await this.send({ type: "health_response", request_id: requestId, state: "unhealthy", message: errorMessage(error) });
    }
  }

  private async handleShutdown(frame: Frame): Promise<void> {
    const requestId = typeof frame.request_id === "string" ? frame.request_id : "";
    try {
      await this.shutdown?.(this.inboundAbort.signal);
      await this.send({ type: "shutdown_ack", request_id: requestId, accepted: true });
    } catch (error) {
      await this.send({ type: "shutdown_ack", request_id: requestId, accepted: false, error: errorToWire(error) });
    } finally {
      setImmediate(() => this.finish());
    }
  }

  private send(frame: Record<string, unknown>): Promise<void> {
    if (this.closed || !this.stream) return Promise.resolve();
    const stream = this.stream;
    return new Promise<void>((resolve, reject) => {
      try {
        stream.write(encodeStruct(frame), (error) => error ? reject(error) : resolve());
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  private fail(error: Error): void {
    this.failPending(error);
    this.readyReject?.(error);
    this.readyResolve = undefined;
    this.readyReject = undefined;
    this.closed = true;
    this.inboundAbort.abort(error);
    this.inboundStopped = true;
    this.finish();
    this.stream?.cancel();
    this.client?.close();
  }

  private failPending(error: Error): void {
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
  }

  private finish(): void {
    const resolve = this.shutdownResolve;
    this.shutdownResolve = undefined;
    resolve?.();
    if (!this.connected) {
      this.readyReject?.(new Error("BotFabric Session 在握手前断开"));
      this.readyResolve = undefined;
      this.readyReject = undefined;
    }
  }
}

function eventToWire(event: Event): Record<string, unknown> {
  return { ...event, payload: encodeBytes(event.payload) };
}
function eventFromWire(event: Record<string, unknown>): Event {
  return { ...event, payload: decodeBytes(event.payload) } as unknown as Event;
}
function requestToWire(request: Request): Record<string, unknown> {
  return { ...request, payload: encodeBytes(request.payload) };
}
function requestFromWire(request: Record<string, unknown>): Request {
  return { ...request, payload: decodeBytes(request.payload) } as unknown as Request;
}
function responseToWire(response: Response): Record<string, unknown> {
  return { payload: encodeBytes(response.payload), ...(response.error ? { error: response.error } : {}) };
}
function encodeBytes(value: Uint8Array | undefined): string {
  return Buffer.from(value ?? []).toString("base64");
}
function decodeBytes(value: unknown): Buffer {
  if (typeof value !== "string") return Buffer.alloc(0);
  return Buffer.from(value, "base64");
}
function errorToWire(error: unknown): ServiceErrorData {
  if (error instanceof ServiceError) return { code: error.code, message: error.message, retryable: error.retryable, details: error.details };
  return { code: "internal", message: errorMessage(error) };
}
function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
