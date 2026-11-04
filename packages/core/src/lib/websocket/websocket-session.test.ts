import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { WebSocketConnectOptions } from "./websocket-session";

type OutboundMessage = {
  type: string;
  data?: string;
  error?: string;
  code?: number;
};

class StdoutCollector {
  readonly events: OutboundMessage[] = [];
  private buf = "";
  private readonly waiters: Array<() => void> = [];
  private done = false;
  private readonly pump: Promise<void>;

  constructor(stream: ReadableStream<Uint8Array>) {
    this.pump = this.read(stream);
  }

  private notify(): void {
    const waiters = this.waiters.splice(0);
    for (const wake of waiters) wake();
  }

  private async read(stream: ReadableStream<Uint8Array>): Promise<void> {
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        this.buf += decoder.decode(value, { stream: true });
        let nl: number;
        while ((nl = this.buf.indexOf("\n")) >= 0) {
          const line = this.buf.slice(0, nl).trim();
          this.buf = this.buf.slice(nl + 1);
          if (line) this.events.push(JSON.parse(line) as OutboundMessage);
        }
        this.notify();
      }
    } finally {
      this.done = true;
      this.notify();
    }
  }

  async waitUntil(
    predicate: (events: OutboundMessage[]) => boolean,
    label: string,
    timeoutMs = 10_000,
  ): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (!predicate(this.events)) {
      if (this.done) {
        throw new Error(
          `stdout ended before ${label}; events=${JSON.stringify(this.events)}`,
        );
      }
      if (Date.now() >= deadline) {
        throw new Error(
          `timed out waiting for ${label}; events=${JSON.stringify(this.events)}`,
        );
      }
      await new Promise<void>((resolve) => {
        this.waiters.push(resolve);
        setTimeout(resolve, 50);
      });
    }
  }

  async finish(): Promise<OutboundMessage[]> {
    await this.pump;
    return this.events;
  }
}

function launchSession(connect: WebSocketConnectOptions) {
  const dir = mkdtempSync(join(tmpdir(), "kulala-ws-script-test-"));
  const connectFile = join(dir, "connect.json");
  writeFileSync(connectFile, JSON.stringify(connect));
  const child = Bun.spawn(
    [
      "bun",
      "run",
      join(import.meta.dir, "../../cli.ts"),
      "--websocket",
      "-i",
      connectFile,
    ],
    {
      cwd: join(import.meta.dir, "../../../../../"),
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  const stderrChunks: string[] = [];
  void child.stderr.pipeTo(
    new WritableStream({
      write(chunk) {
        stderrChunks.push(new TextDecoder().decode(chunk));
      },
    }),
  );
  const collector = new StdoutCollector(child.stdout);

  return {
    child,
    collector,
    stderrChunks,
    async stop() {
      try {
        if (child.exitCode === null) {
          child.stdin.write(`${JSON.stringify({ op: "close" })}\n`);
          child.stdin.end();
          await collector.waitUntil(
            (events) => events.some((event) => event.type === "closed"),
            "closed",
            3000,
          );
        }
      } catch {
        if (child.exitCode === null) child.kill();
      } finally {
        if (child.exitCode === null) child.kill();
        await child.exited;
        await collector.finish();
        rmSync(dir, { recursive: true, force: true });
      }
    },
  };
}

describe("runWebSocketSession", () => {
  let server: ReturnType<typeof Bun.serve> | undefined;

  afterEach(() => {
    server?.stop(true);
    server = undefined;
  });

  test("emits sent for initial body and stdin send", async () => {
    server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch(req, srv) {
        if (srv.upgrade(req, { data: undefined })) return;
        return new Response("Upgrade failed", { status: 500 });
      },
      websocket: {
        open() {},
        message(ws, message) {
          ws.send(String(message));
        },
      },
    });

    const dir = mkdtempSync(join(tmpdir(), "kulala-ws-test-"));
    const connectFile = join(dir, "connect.json");
    writeFileSync(
      connectFile,
      JSON.stringify({
        url: `ws://127.0.0.1:${server.port}`,
        body: '{"name":"world"}',
      }),
    );

    const cliPath = join(import.meta.dir, "../../cli.ts");
    const child = Bun.spawn(
      ["bun", "run", cliPath, "--websocket", "-i", connectFile],
      {
        cwd: join(import.meta.dir, "../../../../../"),
        stdin: "pipe",
        stdout: "pipe",
        stderr: "pipe",
      },
    );

    const stderrChunks: string[] = [];
    void child.stderr.pipeTo(
      new WritableStream({
        write(chunk) {
          stderrChunks.push(new TextDecoder().decode(chunk));
        },
      }),
    );

    const collector = new StdoutCollector(child.stdout);

    try {
      await collector.waitUntil(
        (events) =>
          events.some((e) => e.type === "ready") &&
          events.filter((e) => e.type === "sent").length >= 1 &&
          events.filter((e) => e.type === "message").length >= 1,
        "initial sent+echo",
      );

      child.stdin.write(
        `${JSON.stringify({ op: "send", data: '{"foo":1}' })}\n`,
      );

      await collector.waitUntil(
        (events) =>
          events.filter((e) => e.type === "sent").length >= 2 &&
          events.filter((e) => e.type === "message").length >= 2,
        "stdin sent+echo",
      );

      child.stdin.write(`${JSON.stringify({ op: "close" })}\n`);
      child.stdin.end();

      await collector.waitUntil(
        (events) => events.some((e) => e.type === "closed"),
        "closed",
      );
    } catch (error) {
      child.kill();
      const stderr = stderrChunks.join("").trim();
      throw new Error(
        `${error instanceof Error ? error.message : String(error)}${
          stderr ? `\nstderr: ${stderr}` : ""
        }`,
      );
    }

    const events = await collector.finish();
    const exitCode = await child.exited;
    expect(exitCode).toBe(0);

    expect(events.map((e) => e.type)).toEqual([
      "ready",
      "sent",
      "message",
      "sent",
      "message",
      "closed",
    ]);
    expect(events.filter((e) => e.type === "sent").map((e) => e.data)).toEqual([
      '{"name":"world"}',
      '{"foo":1}',
    ]);
  }, 15000);

  test("sends immediate and gated messages in order", async () => {
    let cleanupSession = async () => {};
    try {
      server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        fetch(req, srv) {
          if (srv.upgrade(req, { data: undefined })) return;
          return new Response("Upgrade failed", { status: 500 });
        },
        websocket: {
          open() {},
          message(ws, message) {
            const data = String(message).trim();
            if (data === "first") ws.send("ack-first");
            if (data === "second") {
              ws.send("ack-second-a");
              ws.send("ack-second-b");
            }
          },
        },
      });
      const session = launchSession({
        url: `ws://127.0.0.1:${server.port}`,
        messages: [
          { data: "first" },
          { data: "second", waitForServer: 1 },
          { data: "third", waitForServer: 2 },
        ],
      });
      cleanupSession = session.stop;
      await session.collector.waitUntil(
        (events) =>
          events.some((event) => event.type === "script-done") &&
          events.filter((event) => event.type === "sent").length === 3,
        "script completion",
        5000,
      );

      const events = session.collector.events;
      const sentIndex = (data: string) =>
        events.findIndex(
          (event) => event.type === "sent" && event.data === data,
        );
      const messageIndex = (data: string) =>
        events.findIndex(
          (event) => event.type === "message" && event.data === data,
        );
      expect(sentIndex("second")).toBeGreaterThan(messageIndex("ack-first"));
      expect(sentIndex("third")).toBeGreaterThan(messageIndex("ack-second-b"));
    } finally {
      await cleanupSession();
      server?.stop(true);
      server = undefined;
    }
  }, 15000);

  test("resets gate timeout and stays open while idle", async () => {
    let cleanupSession = async () => {};
    try {
      server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        fetch(req, srv) {
          if (srv.upgrade(req, { data: undefined })) return;
          return new Response("Upgrade failed", { status: 500 });
        },
        websocket: {
          open() {},
          message(ws, message) {
            const data = String(message).trim();
            // Real server delays exercise the session's actual per-gate timeout.
            if (data === "first") setTimeout(() => ws.send("ack-first"), 140);
            if (data === "second") setTimeout(() => ws.send("ack-second"), 140);
          },
        },
      });
      const session = launchSession({
        url: `ws://127.0.0.1:${server.port}`,
        messages: [
          { data: "first" },
          { data: "second", waitForServer: 1 },
          { data: "third", waitForServer: 1 },
        ],
        timeoutMs: 240,
      });
      cleanupSession = session.stop;
      await session.collector.waitUntil(
        (events) =>
          events.some((event) => event.type === "script-done") &&
          events.filter((event) => event.type === "sent").length === 3,
        "script completion",
        5000,
      );
      // Allow longer than timeoutMs to prove idle listening has no timeout.
      await new Promise((resolve) => setTimeout(resolve, 260));
      expect(
        session.collector.events.some(
          (event) =>
            event.type === "error" && event.error?.includes("timed out"),
        ),
      ).toBe(false);
    } finally {
      await cleanupSession();
      server?.stop(true);
      server = undefined;
    }
  }, 15000);

  test("keeps interactive sends available after timeout", async () => {
    let cleanupSession = async () => {};
    try {
      server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        fetch(req, srv) {
          if (srv.upgrade(req, { data: undefined })) return;
          return new Response("Upgrade failed", { status: 500 });
        },
        websocket: {
          open() {},
          message(ws, message) {
            if (String(message).trim() === "interactive") {
              ws.send("interactive-ack");
            }
          },
        },
      });
      const session = launchSession({
        url: `ws://127.0.0.1:${server.port}`,
        messages: [
          { data: "script-start" },
          { data: "after-timeout", waitForServer: 1 },
        ],
        timeoutMs: 100,
      });
      cleanupSession = session.stop;
      await session.collector.waitUntil(
        (events) =>
          events.some(
            (event) =>
              event.type === "error" && event.error?.includes("timed out"),
          ),
        "script timeout",
        5000,
      );
      session.child.stdin.write(
        `${JSON.stringify({ op: "send", data: "interactive" })}\n`,
      );
      await session.collector.waitUntil(
        (events) =>
          events.some(
            (event) => event.type === "sent" && event.data === "interactive",
          ) &&
          events.some(
            (event) =>
              event.type === "message" && event.data === "interactive-ack",
          ),
        "interactive send after timeout",
        5000,
      );
    } finally {
      await cleanupSession();
      server?.stop(true);
      server = undefined;
    }
  }, 15000);

  test("ends a gated script on early close", async () => {
    let cleanupSession = async () => {};
    try {
      server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        fetch(req, srv) {
          if (srv.upgrade(req, { data: undefined })) return;
          return new Response("Upgrade failed", { status: 500 });
        },
        websocket: {
          open() {},
          message(ws, message) {
            if (String(message).trim() === "first") {
              ws.close(1000, "done");
            }
          },
        },
      });
      const session = launchSession({
        url: `ws://127.0.0.1:${server.port}`,
        messages: [{ data: "first" }, { data: "never-sent", waitForServer: 1 }],
      });
      cleanupSession = session.stop;
      await session.collector.waitUntil(
        (events) => events.some((event) => event.type === "closed"),
        "early close",
        5000,
      );
      expect(await session.child.exited).toBe(0);
    } finally {
      await cleanupSession();
      server?.stop(true);
      server = undefined;
    }
  }, 15000);

  test("reports a failed upgrade and ends without hanging", async () => {
    let cleanupSession = async () => {};
    try {
      server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        fetch() {
          return new Response("Upgrade denied", { status: 403 });
        },
      });
      const session = launchSession({
        url: `ws://127.0.0.1:${server.port}`,
      });
      cleanupSession = session.stop;
      await session.collector.waitUntil(
        (events) => events.some((event) => event.type === "error"),
        "handshake error",
        5000,
      );
      await session.collector.waitUntil(
        (events) => events.some((event) => event.type === "closed"),
        "closed after handshake error",
        5000,
      );
      expect(await session.child.exited).toBe(0);
    } finally {
      await cleanupSession();
      server?.stop(true);
      server = undefined;
    }
  }, 15000);
});
