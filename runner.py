"""Runner — runs on YOUR machine, next to Appium and the device.

It dials OUT to the broker over a websocket (so no inbound ports/firewall), then
loops: receive a command, execute it through MobileCore against the real device,
send the result back. Reconnects with backoff if the broker drops.

    mobilemcp-runner --broker wss://your-app.up.railway.app/runner \
                     --token  $MOBILEMCP_TOKEN \
                     --room   default

Protocol (see PROTOCOL.md):
    -> {"type":"hello","token":..,"room":..,"runner":..}
    <- {"type":"command","id":..,"tool":..,"args":{..}}
    -> {"type":"result","id":..,"ok":true,"data":..}  | {"ok":false,"error":..}
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os

from .core import MobileCore

__version__ = "0.1.0"


async def _run(broker: str, token: str, room: str, store_root: str):
    import websockets  # lazy import so the package imports without it

    # The Cloudflare broker routes /runner to a Durable Object by ?room=.
    # Harmless for brokers that ignore the query (e.g. the Render one).
    if "room=" not in broker:
        broker = broker + ("&" if "?" in broker else "?") + f"room={room}"

    core = MobileCore(store_root=store_root)
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(broker, max_size=8 * 1024 * 1024) as ws:
                await ws.send(json.dumps({
                    "type": "hello", "token": token, "room": room,
                    "runner": f"mobilemcp/{__version__}",
                }))
                ack = json.loads(await ws.recv())
                if ack.get("type") != "welcome":
                    raise RuntimeError(f"broker rejected runner: {ack}")
                print(f"[runner] connected to {broker} (room={room})")
                backoff = 1.0
                await _serve(ws, core)
        except Exception as e:  # noqa: BLE001 - we want to retry on anything
            print(f"[runner] disconnected ({type(e).__name__}: {e}); "
                  f"retrying in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


async def _serve(ws, core: MobileCore):
    async for raw in ws:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if msg.get("type") != "command":
            continue
        cmd_id, tool, args = msg.get("id"), msg.get("tool"), msg.get("args", {})
        # device calls are blocking; run them off the event loop
        try:
            data = await asyncio.to_thread(core.dispatch, tool, args)
            reply = {"type": "result", "id": cmd_id, "ok": True, "data": data}
        except Exception as e:  # noqa: BLE001
            reply = {"type": "result", "id": cmd_id, "ok": False,
                     "error": f"{type(e).__name__}: {e}"}
        await ws.send(json.dumps(reply))


def main():
    ap = argparse.ArgumentParser(prog="mobilemcp-runner")
    ap.add_argument("--broker", default=os.environ.get("MOBILEMCP_BROKER"),
                    help="wss:// URL of the broker /runner endpoint")
    ap.add_argument("--token", default=os.environ.get("MOBILEMCP_TOKEN"),
                    help="shared secret; must match the broker's token")
    ap.add_argument("--room", default=os.environ.get("MOBILEMCP_ROOM", "default"),
                    help="logical room so multiple runners/devices can coexist")
    ap.add_argument("--store", default="./tests/store",
                    help="local directory for saved test cases")
    args = ap.parse_args()
    if not args.broker or not args.token:
        ap.error("--broker and --token are required "
                 "(or set MOBILEMCP_BROKER / MOBILEMCP_TOKEN)")
    try:
        asyncio.run(_run(args.broker, args.token, args.room, args.store))
    except KeyboardInterrupt:
        print("\n[runner] stopped")


if __name__ == "__main__":
    main()
