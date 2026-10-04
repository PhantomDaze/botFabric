import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import * as grpc from "@grpc/grpc-js";
import {
  SessionClient,
  decodeStruct,
  encodeStruct,
  type Event,
  type Request,
  type Response,
} from "../src/index.js";

const service: grpc.ServiceDefinition = {
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

test("Struct wire round-trip handles nested values", () => {
  const input = { type: "hello", accepted: true, payload: "AP8=", nested: [1, null, { ok: false }] };
  assert.deepEqual(decodeStruct(encodeStruct(input)), input);
  assert.throws(() => encodeStruct({ value: Number.NaN }), /finite/);
});

test("Unix Socket Session supports bidirectional calls from callbacks", async () => {
  const directory = await mkdtemp(join(tmpdir(), "botfabric-ts-sdk-"));
  const socketPath = join(directory, "runtime.sock");
  const server = new grpc.Server();
  const seen: Record<string, unknown>[] = [];
  let activeCall: grpc.ServerDuplexStream<Buffer, Buffer> | undefined;
  let resolveRuntime!: () => void;
  let rejectRuntime!: (error: Error) => void;
  const runtimeDone = new Promise<void>((resolve, reject) => {
    resolveRuntime = resolve;
    rejectRuntime = reject;
  });
  const serverReady = new Promise<void>((resolve, reject) => {
    server.addService(service, {
      Session: (call: grpc.ServerDuplexStream<Buffer, Buffer>) => {
        activeCall = call;
        call.on("data", (data) => {
          const frame = decodeStruct(data);
          seen.push(frame);
          if (frame.type === "hello") {
            call.write(encodeStruct({ type: "ready", accepted: true }));
            call.write(encodeStruct({
              type: "event",
              request_id: "event-1",
              event: { id: "evt", topic: "events.test", schema: "test/v1", timestamp_unix_millis: 123, source: "runtime", bot_id: "bot", payload: "AAEC" },
            }));
            return;
          }
          if (frame.type === "invoke_host") {
            const request = frame.request as Record<string, unknown>;
            call.write(encodeStruct({ type: "invoke_host_response", request_id: frame.request_id, response: { payload: "aG9zdC1vaw==" } }));
            if (request.payload === "ZXZlbnQ=") {
              call.write(encodeStruct({ type: "invoke_plugin", request_id: "plugin-1", request: { service: "demo.echo", method: "Run", payload: "eHl6" } }));
            }
          } else if (frame.type === "event_ack") {
            assert.equal(frame.request_id, "event-1");
          } else if (frame.type === "invoke_plugin_response") {
            assert.equal((frame.response as Record<string, unknown>).payload, "b2s=");
            call.write(encodeStruct({ type: "health", request_id: "health-1" }));
          } else if (frame.type === "health_response") {
            assert.equal(frame.state, "healthy");
            call.write(encodeStruct({ type: "shutdown", request_id: "shutdown-1" }));
          } else if (frame.type === "shutdown_ack") {
            assert.equal(frame.accepted, true);
            resolveRuntime();
          } else if (frame.type === "publish_event") {
            assert.equal((frame.event as Record<string, unknown>).payload, "AP8=");
            call.write(encodeStruct({ type: "publish_result", request_id: frame.request_id }));
          }
        });
        call.on("error", rejectRuntime);
      },
    });
    server.bindAsync(`unix:${socketPath}`, grpc.ServerCredentials.createInsecure(), (error) => {
      if (error) reject(error);
      else { server.start(); resolve(); }
    });
  });
  await serverReady;

  const received: Buffer[] = [];
  const hostRequests: Buffer[] = [];
  let client!: SessionClient;
  client = new SessionClient({
    pluginId: "plugin", pluginVersion: "1.0", protocolVersion: "v1", socketPath, sessionToken: "secret", timeout: 2_000,
    subscriptions: {
      "events.test": async (event: Event, signal?: AbortSignal) => {
        assert.equal(signal?.aborted, false);
        received.push(Buffer.from(event.payload ?? []));
        const response = await client.invokeHost({ service: "host", method: "Run", payload: Buffer.from("event") });
        assert.equal(response.payload.toString(), "host-ok");
      },
    },
    services: {
      "demo.echo.Run": async (request: Request, signal?: AbortSignal): Promise<Response> => {
        assert.equal(signal?.aborted, false);
        assert.equal(Buffer.from(request.payload ?? []).toString(), "xyz");
        const response = await client.invokeHost({ service: "host", method: "Run", payload: Buffer.from("service") });
        hostRequests.push(response.payload);
        return { payload: Buffer.from("ok") };
      },
    },
    health: async () => {
      const response = await client.invokeHost({ service: "host", method: "Run", payload: Buffer.from("health") });
      hostRequests.push(response.payload);
      return { state: "healthy", message: "ready", details: { worker: "ok" } };
    },
    shutdown: async () => {
      const response = await client.invokeHost({ service: "host", method: "Run", payload: Buffer.from("shutdown") });
      hostRequests.push(response.payload);
    },
  });

  try {
    await client.connect();
    const serving = client.serveForever();
    await client.publish({ id: "evt-publish", topic: "outbound", schema: "test/v1", timestamp_unix_millis: 456, source: "plugin", bot_id: "bot", payload: Buffer.from([0, 255]) });
    await Promise.all([serving, runtimeDone]);
    assert.deepEqual(received, [Buffer.from([0, 1, 2])]);
    assert.deepEqual(hostRequests.map((value) => value.toString()), ["host-ok", "host-ok", "host-ok"]);
    assert.ok(seen.some((frame) => frame.type === "hello" && frame.session_token === "secret"));
    assert.ok(activeCall);
  } finally {
    await client.close();
    server.forceShutdown();
    await rm(directory, { recursive: true, force: true });
  }
});

test("timeout rejects pending requests and close ends the Session", async () => {
  const directory = await mkdtemp(join(tmpdir(), "botfabric-ts-timeout-"));
  const socketPath = join(directory, "runtime.sock");
  const server = new grpc.Server();
  const serverReady = new Promise<void>((resolve, reject) => {
    server.addService(service, {
      Session: (call: grpc.ServerDuplexStream<Buffer, Buffer>) => {
        call.on("data", (data) => {
          if (decodeStruct(data).type === "hello") call.write(encodeStruct({ type: "ready", accepted: true }));
        });
      },
    });
    server.bindAsync(`unix:${socketPath}`, grpc.ServerCredentials.createInsecure(), (error) => {
      if (error) reject(error);
      else { server.start(); resolve(); }
    });
  });
  await serverReady;
  const client = new SessionClient({ pluginId: "plugin", pluginVersion: "1", protocolVersion: "v1", socketPath, sessionToken: "secret", timeout: 50 });
  try {
    await client.connect();
    const pending = client.invokeHost({ service: "host", method: "Run" });
    await assert.rejects(pending, /超时/);
    const closing = client.invokeHost({ service: "host", method: "Run" });
    await client.close();
    await assert.rejects(closing, /已关闭/);
  } finally {
    await client.close();
    server.forceShutdown();
    await rm(directory, { recursive: true, force: true });
  }
});
