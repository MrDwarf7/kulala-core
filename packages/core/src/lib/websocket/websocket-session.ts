import { writeSync } from "node:fs";
import { createInterface } from "node:readline";
import { normalizeSentPayload } from "./display";
import type { WebSocketScriptMessage } from "./script-messages";

export type WebSocketConnectOptions = {
  url: string;
  body?: string;
  headers?: Record<string, string>;
  messages?: WebSocketScriptMessage[];
  /** Maximum milliseconds per server-message gate; omitted means no timeout. */
  timeoutMs?: number;
};

type OutboundMessage =
  | { type: "ready" }
  | { type: "message"; data: string }
  | { type: "sent"; data: string }
  | { type: "waiting"; remaining: number }
  | { type: "script-done" }
  | { type: "error"; error: string }
  | { type: "closed"; code?: number };

function isEagain(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as NodeJS.ErrnoException).code === "EAGAIN"
  );
}

/**
 * Compact NDJSON to stdout. Uses writeSync (same rationale as writePayloadToFd):
 * async process.stdout.write can deadlock / throw under parent pipe backpressure.
 */
function writeOutbound(msg: OutboundMessage): void {
  const buffer = Buffer.from(`${JSON.stringify(msg)}\n`);
  let offset = 0;
  while (offset < buffer.length) {
    try {
      const written = writeSync(1, buffer, offset, buffer.length - offset);
      if (written <= 0) {
        throw new Error("failed to write WebSocket outbound message");
      }
      offset += written;
    } catch (error) {
      if (isEagain(error)) continue;
      throw error;
    }
  }
}

/** DOM lib types only allow protocols as the 2nd arg; Bun accepts `{ headers }`. */
type BunClientWebSocket = {
  new (
    url: string | URL,
    options?: { headers?: Record<string, string> },
  ): WebSocket;
};

function openClientWebSocket(
  url: string,
  headers?: Record<string, string>,
): WebSocket {
  if (headers && Object.keys(headers).length > 0) {
    const Ws = WebSocket as unknown as BunClientWebSocket;
    return new Ws(url, { headers });
  }
  return new WebSocket(url);
}

function normalizeWsUrl(method: string, target: string): string {
  // Defensive: strip a trailing HTTP version if a caller passed a raw request line.
  const withoutVersion = target
    .replace(/\s+HTTP\/\d+(?:\.\d+)?\s*$/i, "")
    .trim();
  const t = withoutVersion;
  if (/^wss?:\/\//i.test(t)) return t;
  const scheme = method.toUpperCase() === "WSS" ? "wss" : "ws";
  return `${scheme}://${t}`;
}

function errorEventMessage(ev: Event): string {
  if (ev instanceof ErrorEvent) {
    if (ev.message) return ev.message;
    const nested = (ev as ErrorEvent & { error?: unknown }).error;
    if (nested instanceof Error && nested.message) return nested.message;
    if (typeof nested === "string" && nested) return nested;
  }
  return "WebSocket error";
}

function closeCodeHint(code: number): string {
  if (code === 1002) return "WebSocket handshake failed (protocol error)";
  if (code === 1006) return "WebSocket connection closed abnormally";
  return `WebSocket closed before handshake (code ${code})`;
}

/** Read HTTP status/body when the server rejects the WebSocket upgrade (e.g. 429 rate limit). */
async function describeHandshakeFailure(url: string): Promise<string> {
  const httpUrl = url.replace(/^ws/i, "http");
  try {
    const res = await fetch(httpUrl, {
      headers: {
        Connection: "Upgrade",
        Upgrade: "websocket",
        "Sec-WebSocket-Version": "13",
        "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
      },
      redirect: "manual",
    });
    if (res.status === 101) {
      return "WebSocket handshake failed unexpectedly";
    }
    const body = (await res.text()).trim();
    if (body) return `HTTP ${res.status}: ${body}`;
    return `HTTP ${res.status} (expected 101 Switching Protocols)`;
  } catch (e) {
    return e instanceof Error ? e.message : "WebSocket handshake failed";
  }
}

function isGenericHandshakeError(message: string): boolean {
  return (
    message === "WebSocket error" ||
    message.includes("Expected 101") ||
    message.includes("Unexpected server response")
  );
}

/**
 * Long-lived WebSocket session for kulala.nvim (replaces websocat).
 * Invoked via `kulala-core --websocket -i <connect.json>`.
 * Reads JSON lines from stdin: `{ "op": "send", "data": "..." }`, `{ "op": "close" }`.
 */
export async function runWebSocketSession(
  connect: WebSocketConnectOptions,
): Promise<void> {
  const url = normalizeWsUrl("WS", connect.url);

  await new Promise<void>((resolve, reject) => {
    let ws: WebSocket;
    try {
      ws = openClientWebSocket(url, connect.headers);
    } catch (e) {
      reject(e);
      return;
    }

    const rl = createInterface({ input: process.stdin, terminal: false });
    let opened = false;
    let errorSent = false;
    let finished = false;
    let handshakeError: Promise<void> | undefined;
    type ActiveWebSocketWait = {
      remaining: number;
      resolve: (received: boolean) => void;
      timer?: NodeJS.Timeout;
    };
    let activeWait: ActiveWebSocketWait | undefined;

    const settleWait = (received: boolean) => {
      if (!activeWait) return;
      const wait = activeWait;
      activeWait = undefined;
      clearTimeout(wait.timer);
      wait.resolve(received);
    };

    const cleanup = () => {
      rl.close();
      try {
        ws.close();
      } catch {
        // ignore
      }
    };

    const finish = (code?: number) => {
      if (finished) return;
      finished = true;
      settleWait(false);
      cleanup();
      writeOutbound({
        type: "closed",
        ...(code !== undefined ? { code } : {}),
      });
      resolve();
    };

    const emitHandshakeError = async (fallback: string): Promise<void> => {
      if (errorSent) return;
      errorSent = true;
      let message = fallback;
      if (isGenericHandshakeError(fallback)) {
        message = await describeHandshakeFailure(url);
      }
      writeOutbound({ type: "error", error: message });
    };

    const sendPayload = (data: string) => {
      const payload = data.endsWith("\n") ? data : data + "\n";
      ws.send(payload);
      writeOutbound({ type: "sent", data: normalizeSentPayload(data) });
    };

    const waitForServer = (count: number): Promise<boolean> => {
      const { promise, resolve: resolveWait } =
        Promise.withResolvers<boolean>();
      const wait: ActiveWebSocketWait = {
        remaining: count,
        resolve: resolveWait,
      };
      activeWait = wait;
      writeOutbound({ type: "waiting", remaining: wait.remaining });

      if (connect.timeoutMs !== undefined) {
        let remainingMs = connect.timeoutMs;
        const scheduleTimeout = () => {
          const startedAt = performance.now();
          const delay = Math.ceil(Math.min(remainingMs, 2_147_483_647));
          wait.timer = setTimeout(() => {
            if (activeWait !== wait) return;
            remainingMs -= performance.now() - startedAt;
            if (remainingMs > 0) {
              scheduleTimeout();
              return;
            }
            activeWait = undefined;
            writeOutbound({
              type: "error",
              error: `WebSocket script timed out after ${connect.timeoutMs}ms waiting for server messages`,
            });
            wait.resolve(false);
          }, delay);
        };
        scheduleTimeout();
      }
      return promise;
    };

    const runScript = async () => {
      for (const message of connect.messages ?? []) {
        if (finished) return;
        if (message.waitForServer && message.waitForServer > 0) {
          if (!(await waitForServer(message.waitForServer)) || finished) return;
        }
        sendPayload(message.data);
      }
      if (!finished) writeOutbound({ type: "script-done" });
    };

    ws.addEventListener("open", () => {
      opened = true;
      writeOutbound({ type: "ready" });
      if (connect.messages !== undefined) {
        void runScript().catch((error: unknown) => {
          writeOutbound({
            type: "error",
            error: error instanceof Error ? error.message : String(error),
          });
          finish();
        });
      } else if (connect.body && connect.body.trim()) {
        try {
          sendPayload(connect.body);
        } catch (error) {
          writeOutbound({
            type: "error",
            error: error instanceof Error ? error.message : String(error),
          });
          finish();
        }
      }
    });

    ws.addEventListener("message", (ev) => {
      if (finished) return;
      const data =
        typeof ev.data === "string"
          ? ev.data
          : ev.data instanceof ArrayBuffer
            ? new TextDecoder().decode(ev.data)
            : String(ev.data);
      writeOutbound({ type: "message", data });
      if (activeWait) {
        activeWait.remaining -= 1;
        if (activeWait.remaining === 0) {
          settleWait(true);
        } else {
          writeOutbound({ type: "waiting", remaining: activeWait.remaining });
        }
      }
    });

    ws.addEventListener("error", (ev) => {
      if (finished) return;
      const message = errorEventMessage(ev);
      if (opened) {
        if (!errorSent) {
          errorSent = true;
          writeOutbound({ type: "error", error: message });
        }
        finish();
        return;
      }
      handshakeError = emitHandshakeError(message);
      void handshakeError.then(() => finish());
    });

    ws.addEventListener("close", (ev) => {
      if (finished) return;
      void (async () => {
        if (!opened) {
          if (handshakeError) {
            await handshakeError;
          } else if (!errorSent) {
            await emitHandshakeError(closeCodeHint(ev.code));
          }
        }
        finish(ev.code);
      })();
    });

    rl.on("line", (line) => {
      const trimmed = line.trim();
      if (!trimmed) return;
      try {
        const cmd = JSON.parse(trimmed) as { op?: string; data?: string };
        if (cmd.op === "close") {
          finish();
          return;
        }
        if (cmd.op === "send" && cmd.data != null) {
          sendPayload(cmd.data);
        }
      } catch {
        writeOutbound({ type: "error", error: "Invalid stdin command JSON" });
      }
    });

    // Do not close the socket when stdin hits EOF. Neovim (and other parents) may
    // deliver EOF before the stdin pipe is fully wired; closing here disconnects
    // immediately with no echoed messages. Shutdown is driven by `{ op: "close" }`
    // or process termination.
  });
}
