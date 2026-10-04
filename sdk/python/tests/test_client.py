"""Python SDK 的 wire codec 和本地回环测试。"""

from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from collections.abc import AsyncIterator

import grpc
from botfabric import Event, Request, Response, SessionClient
from botfabric.client import (
    _decode_bytes,
    _deserialize_struct,
    _encode_bytes,
    _event_from_wire,
    _event_to_wire,
    _request_from_wire,
    _request_to_wire,
    _serialize_struct,
    _to_struct,
)
from google.protobuf import json_format, struct_pb2


class CodecTests(unittest.TestCase):
    def test_payload_uses_json_base64_string_inside_struct(self) -> None:
        frame = {
            "type": "publish_event",
            "event": _event_to_wire(
                Event("e1", "topic", "s1", 1, "src", "bot", payload=b"\x00\xff")
            ),
        }
        proto = _to_struct(frame)
        wire = json_format.MessageToDict(proto, preserving_proto_field_name=True)
        self.assertEqual(wire["event"]["payload"], "AP8=")
        decoded = _event_from_wire(wire["event"])
        self.assertEqual(decoded.payload, b"\x00\xff")
        self.assertEqual(_decode_bytes(_encode_bytes(decoded.payload)), decoded.payload)

    def test_request_round_trip_preserves_metadata_and_payload(self) -> None:
        request = Request(
            "svc", "run", caller="plugin", metadata={"k": "v"}, payload=b"payload"
        )
        self.assertEqual(_request_from_wire(_request_to_wire(request)), request)

    def test_struct_serializer_round_trip(self) -> None:
        frame = {"type": "hello", "plugin_id": "plugin", "accepted": True}
        self.assertEqual(
            _deserialize_struct(_serialize_struct(_to_struct(frame))), frame
        )


class SessionInteropTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.socket_path = os.path.join(self.tempdir.name, "runtime.sock")
        self.server = grpc.aio.server()
        handler = grpc.stream_stream_rpc_method_handler(
            self.runtime_session,
            request_deserializer=struct_pb2.Struct.FromString,
            response_serializer=_serialize_struct,
        )
        self.server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    "botfabric.v1.ProcessPlugin", {"Session": handler}
                ),
            )
        )
        self.server.add_insecure_port("unix:" + self.socket_path)
        await self.server.start()
        self.client = SessionClient(
            plugin_id="plugin",
            plugin_version="1.0",
            protocol_version="v1",
            socket_path=self.socket_path,
            session_token="secret",
            subscriptions={"events.test": self.on_event},
            services={("demo.echo", "Run"): self.on_service},
            health=self.on_health,
            shutdown=self.on_shutdown,
        )
        self.inbound_event = asyncio.Event()
        self.host_call_complete = asyncio.Event()
        self.received_event: Event | None = None
        self.host_responses: list[bytes] = []

    async def asyncTearDown(self) -> None:
        await self.client.close()
        await self.server.stop(0)
        self.tempdir.cleanup()

    async def runtime_session(
        self,
        request_iterator: AsyncIterator[struct_pb2.Struct],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[struct_pb2.Struct]:
        first = json_format.MessageToDict(
            await request_iterator.__anext__(), preserving_proto_field_name=True
        )
        self.assertEqual(first["type"], "hello")
        self.assertEqual(first["session_token"], "secret")
        yield _to_struct({"type": "ready", "accepted": True})
        yield _to_struct(
            {
                "type": "event",
                "request_id": "event-1",
                "event": {
                    "id": "evt",
                    "topic": "events.test",
                    "schema": "test/v1",
                    "timestamp_unix_millis": 123,
                    "source": "source",
                    "bot_id": "bot",
                    "payload": "AAEC",
                },
            }
        )
        host_invoke = json_format.MessageToDict(
            await request_iterator.__anext__(), preserving_proto_field_name=True
        )
        self.assertEqual(host_invoke["type"], "invoke_host")
        yield _to_struct(
            {
                "type": "invoke_host_response",
                "request_id": host_invoke["request_id"],
                "response": {"payload": "aG9zdC1vaw=="},
            }
        )
        event_ack = json_format.MessageToDict(
            await request_iterator.__anext__(), preserving_proto_field_name=True
        )
        self.assertEqual(event_ack["type"], "event_ack")
        self.assertEqual(event_ack["request_id"], "event-1")
        invoke = json_format.MessageToDict(
            await request_iterator.__anext__(), preserving_proto_field_name=True
        )
        self.assertEqual(invoke["type"], "invoke_host")
        self.assertEqual(invoke["request"]["payload"], "eHl6")
        yield _to_struct(
            {
                "type": "invoke_host_response",
                "request_id": invoke["request_id"],
                "response": {"payload": "b2s="},
            }
        )
        yield _to_struct({"type": "health", "request_id": "health-1"})
        health_call = json_format.MessageToDict(
            await request_iterator.__anext__(), preserving_proto_field_name=True
        )
        self.assertEqual(health_call["type"], "invoke_host")
        self.assertEqual(health_call["request"]["payload"], "ZnJvbS1oZWFsdGg=")
        yield _to_struct(
            {
                "type": "invoke_host_response",
                "request_id": health_call["request_id"],
                "response": {"payload": "aGVhbHRoLW9r"},
            }
        )
        health = json_format.MessageToDict(
            await request_iterator.__anext__(), preserving_proto_field_name=True
        )
        self.assertEqual(health["type"], "health_response")
        self.assertEqual(health["state"], "healthy")
        yield _to_struct({"type": "shutdown", "request_id": "shutdown-1"})
        shutdown_call = json_format.MessageToDict(
            await request_iterator.__anext__(), preserving_proto_field_name=True
        )
        self.assertEqual(shutdown_call["type"], "invoke_host")
        self.assertEqual(shutdown_call["request"]["payload"], "ZnJvbS1zaHV0ZG93bg==")
        yield _to_struct(
            {
                "type": "invoke_host_response",
                "request_id": shutdown_call["request_id"],
                "response": {"payload": "c2h1dGRvd24tb2s="},
            }
        )
        shutdown = json_format.MessageToDict(
            await request_iterator.__anext__(), preserving_proto_field_name=True
        )
        self.assertEqual(shutdown["type"], "shutdown_ack")
        self.assertTrue(shutdown["accepted"])
        await context.abort(grpc.StatusCode.CANCELLED, "test complete")

    async def on_event(self, event: Event) -> None:
        self.received_event = event
        response = await self.client.invoke_host(
            Request("host", "Run", payload=b"from-event"), timeout=2
        )
        self.host_responses.append(response.payload)
        self.host_call_complete.set()
        self.inbound_event.set()

    async def on_service(self, request: Request) -> Response:
        self.assertEqual(request.payload, b"xyz")
        return Response(payload=b"ok")

    async def on_shutdown(self) -> None:
        response = await self.client.invoke_host(
            Request("host", "Run", payload=b"from-shutdown"), timeout=2
        )
        self.host_responses.append(response.payload)

    async def on_health(self) -> tuple[str, str, dict[str, str]]:
        response = await self.client.invoke_host(
            Request("host", "Run", payload=b"from-health"), timeout=2
        )
        self.host_responses.append(response.payload)
        return "healthy", "ready", {"worker": "ok"}

    async def test_handshake_inbound_event_host_call_health_and_shutdown(self) -> None:
        await self.client.connect()
        await asyncio.wait_for(self.inbound_event.wait(), timeout=2)
        await asyncio.wait_for(self.host_call_complete.wait(), timeout=2)
        self.assertIsNotNone(self.received_event)
        assert self.received_event is not None
        self.assertEqual(self.received_event.payload, b"\x00\x01\x02")
        response = await self.client.invoke_host(
            Request("host", "Run", payload=b"xyz"), timeout=2
        )
        self.assertEqual(response, Response(payload=b"ok"))
        await self.client.serve_forever()
        self.assertEqual(
            self.host_responses, [b"host-ok", b"health-ok", b"shutdown-ok"]
        )
        self.assertTrue(self.client._closed)


if __name__ == "__main__":
    unittest.main()
