/**
 * mobilemcp broker — Cloudflare Worker.
 *
 * Two Durable Objects:
 *   MobileMcp  (McpAgent)  — serves /mcp to Claude over Streamable HTTP.
 *   RunnerHub  (DO)        — holds the runner's WebSocket + correlates requests.
 *
 * The MCP tools own no device; each forwards to the RunnerHub, which relays the
 * command over the websocket to your local `mobilemcp-runner` and awaits the
 * matching result. Hibernation keeps idle objects from incurring charges, so
 * this stays inside the free tier.
 *
 * The Python runner, wire protocol, and engine are unchanged from the stdio /
 * Render setups — only the broker is rewritten here.
 */
import { McpAgent } from "agents/mcp";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { DurableObject } from "cloudflare:workers";
import { z } from "zod";

export interface Env {
  MOBILE_MCP: DurableObjectNamespace<MobileMcp>;
  RUNNER_HUB: DurableObjectNamespace<RunnerHub>;
  MOBILEMCP_TOKEN: string;
  MOBILEMCP_TIMEOUT?: string;
}

type Pending = { resolve: (v: unknown) => void; reject: (e: unknown) => void };

/* ------------------------------------------------------------------ *
 * RunnerHub: one per `room`. Holds the runner websocket (hibernatable)
 * and a pending-request map. forward() is called via RPC by the MCP tools.
 * ------------------------------------------------------------------ */
export class RunnerHub extends DurableObject<Env> {
  // In-memory only; non-empty solely while a forward() awaits, during which the
  // DO is pinned by the active request, so hibernation never drops a live entry.
  private pending = new Map<string, Pending>();

  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("expected websocket upgrade", { status: 426 });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server); // hibernatable accept
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    let msg: any;
    try {
      const text = typeof message === "string" ? message : new TextDecoder().decode(message);
      msg = JSON.parse(text);
    } catch {
      return;
    }
    if (msg.type === "hello") {
      if (msg.token !== this.env.MOBILEMCP_TOKEN) {
        ws.send(JSON.stringify({ type: "error", error: "auth failed" }));
        ws.close(4401, "auth failed");
        return;
      }
      ws.send(JSON.stringify({ type: "welcome", room: msg.room ?? "default" }));
      return;
    }
    if (msg.type === "result") {
      const p = this.pending.get(msg.id);
      if (p) {
        this.pending.delete(msg.id);
        p.resolve(msg);
      }
    }
  }

  async webSocketClose(): Promise<void> {
    for (const [, p] of this.pending) p.reject(new Error("runner disconnected"));
    this.pending.clear();
  }

  private runnerSocket(): WebSocket | undefined {
    const all = this.ctx.getWebSockets();
    return all.find((w) => w.readyState === WebSocket.READY_STATE_OPEN) ?? all[0];
  }

  /** RPC entrypoint used by the MCP tools. Returns the runner's `data`, or an
   *  error object on no-runner / runner-error / timeout / disconnect. */
  async forward(tool: string, args: unknown): Promise<unknown> {
    const ws = this.runnerSocket();
    if (!ws) {
      return {
        ok: false,
        error: "no runner connected. Start mobilemcp-runner on your machine.",
      };
    }
    const id = crypto.randomUUID();
    const timeoutMs = Number(this.env.MOBILEMCP_TIMEOUT ?? "120") * 1000;
    const wait = new Promise<any>((resolve, reject) =>
      this.pending.set(id, { resolve, reject })
    );
    ws.send(JSON.stringify({ type: "command", id, tool, args }));
    try {
      const msg = await Promise.race([
        wait,
        new Promise<never>((_, rej) =>
          setTimeout(() => rej(new Error("timeout")), timeoutMs)
        ),
      ]);
      if (!msg.ok) return { ok: false, error: msg.error ?? "runner error" };
      return msg.data;
    } catch (e: any) {
      this.pending.delete(id);
      const m = e?.message;
      return {
        ok: false,
        error: m === "timeout" ? `runner timed out after ${timeoutMs / 1000}s` : String(m ?? e),
      };
    }
  }
}

/* ------------------------------------------------------------------ *
 * MobileMcp: the MCP server. Every tool forwards to the RunnerHub.
 * ------------------------------------------------------------------ */
function hub(env: Env, room: string) {
  return env.RUNNER_HUB.get(env.RUNNER_HUB.idFromName(room));
}

export class MobileMcp extends McpAgent<Env> {
  server = new McpServer({ name: "mobile-automation-mcp", version: "0.1.0" });

  async init(): Promise<void> {
    const fwd = async (tool: string, args: unknown, room: string) => {
      const data = await hub(this.env, room).forward(tool, args);
      return { content: [{ type: "text" as const, text: JSON.stringify(data) }] };
    };

    this.server.tool(
      "start_session",
      {
        platform: z.string(),
        capabilities: z.record(z.string(), z.any()),
        server_url: z.string().optional(),
        room: z.string().default("default"),
      },
      async ({ platform, capabilities, server_url, room }) =>
        fwd("start_session",
          { platform, capabilities, server_url: server_url ?? "http://127.0.0.1:4723" },
          room)
    );

    this.server.tool(
      "get_ui_tree",
      { room: z.string().default("default") },
      async ({ room }) => fwd("get_ui_tree", {}, room)
    );

    this.server.tool(
      "find_by_intent",
      { intent: z.string(), top_k: z.number().default(5), room: z.string().default("default") },
      async ({ intent, top_k, room }) => fwd("find_by_intent", { intent, top_k }, room)
    );

    this.server.tool(
      "act_by_intent",
      {
        intent: z.string(),
        candidate_index: z.number().optional(),
        room: z.string().default("default"),
      },
      async ({ intent, candidate_index, room }) =>
        fwd("act_by_intent", { intent, candidate_index }, room)
    );

    this.server.tool(
      "start_recording",
      { name: z.string(), platform: z.string().default("any"), room: z.string().default("default") },
      async ({ name, platform, room }) => fwd("start_recording", { name, platform }, room)
    );

    this.server.tool(
      "stop_recording_and_save",
      { room: z.string().default("default") },
      async ({ room }) => fwd("stop_recording_and_save", {}, room)
    );

    this.server.tool(
      "list_tests",
      { room: z.string().default("default") },
      async ({ room }) => fwd("list_tests", {}, room)
    );

    this.server.tool(
      "run_test",
      { name: z.string(), room: z.string().default("default") },
      async ({ name, room }) => fwd("run_test", { name }, room)
    );

    this.server.tool(
      "heal_test",
      { name: z.string(), room: z.string().default("default") },
      async ({ name, room }) => fwd("heal_test", { name }, room)
    );

    this.server.tool(
      "screenshot",
      { room: z.string().default("default") },
      async ({ room }) => fwd("screenshot", {}, room)
    );
  }
}

// Serve BOTH transports so any MCP client works regardless of which it picks:
//   /mcp           -> Streamable HTTP (current spec, preferred)
//   /sse, /sse/message -> SSE (older clients / fallback)
const httpHandler = MobileMcp.serve("/mcp", { binding: "MOBILE_MCP" });
const sseHandler = MobileMcp.serveSSE("/sse", { binding: "MOBILE_MCP" });

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      return Response.json({ status: "ok" });
    }
    if (url.pathname === "/runner") {
      const room = url.searchParams.get("room") ?? "default";
      return hub(env, room).fetch(request);
    }
    if (url.pathname === "/mcp") {
      return httpHandler.fetch(request, env, ctx);
    }
    if (url.pathname === "/sse" || url.pathname === "/sse/message") {
      return sseHandler.fetch(request, env, ctx);
    }
    return new Response(
      "mobilemcp broker is running. Point an MCP client at /mcp (Streamable HTTP) " +
        "or /sse (SSE). This URL is not meant to be opened in a browser.",
      { status: 404 }
    );
  },
};