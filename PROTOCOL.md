# Broker ↔ Runner wire protocol

A single WebSocket between the Railway **broker** and your local **runner**.
JSON text frames. The runner always dials out to the broker (no inbound ports).

## Handshake

Runner connects to `wss://<broker>/runner` and sends:

```json
{ "type": "hello", "token": "<shared secret>", "room": "default",
  "runner": "mobilemcp/0.1.0" }
```

Broker validates `token` against its `MOBILEMCP_TOKEN` env var, then replies:

```json
{ "type": "welcome", "room": "default" }
```

On bad token: `{ "type": "error", "error": "auth failed" }` and the socket closes
with code `4401`.

## Command / result

When Claude calls an MCP tool, the broker forwards it:

```json
{ "type": "command", "id": "<uuid>", "tool": "act_by_intent",
  "args": { "intent": "tap the login button" } }
```

The runner executes it through `MobileCore` and replies with the **same id**:

```json
{ "type": "result", "id": "<uuid>", "ok": true,
  "data": { "ok": true, "action": "tap", "matched_text": "Log In" } }
```

On failure:

```json
{ "type": "result", "id": "<uuid>", "ok": false,
  "error": "NoSuchElementException: ..." }
```

The broker correlates by `id` (concurrent calls are safe), and surfaces the
`data` to the MCP client, or an error if `ok` is false / the runner times out /
the runner disconnects mid-call.

## Rooms

`room` lets several device runners share one broker (e.g. `pixel`, `iphone15`).
MCP tools take an optional `room` arg (default `"default"`) so Claude can target
a specific device.

## Liveness

WebSocket ping/pong (built into the client and server libraries) keeps the
connection alive. If it drops, the runner reconnects with exponential backoff.
