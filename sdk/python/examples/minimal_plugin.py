from __future__ import annotations

import asyncio

from botfabric import Event, Request, Response, SessionClient


async def on_group_message(event: Event) -> None:
    print(event.topic, event.payload.decode("utf-8", errors="replace"))


async def echo(request: Request) -> Response:
    return Response(payload=request.payload)


async def main() -> None:
    client = SessionClient(
        subscriptions={"message.group": on_group_message},
        services={("example.echo", "Run"): echo},
    )
    await client.connect()
    try:
        await client.serve_forever()
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
