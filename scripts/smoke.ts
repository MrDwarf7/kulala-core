import assert from "node:assert/strict";
import { execFile, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import type { KulalaHttpStreamEvent } from "../packages/core/src/lib/runner/types";

const binary = resolve(process.argv[2] ?? "packages/core/dist/kulala-core");
const plugin = process.argv[3] && resolve(process.argv[3]);
const dir = mkdtempSync(join(tmpdir(), "kulala-smoke-"));
let finishStream: (() => void) | undefined;
let mutations = 0;
const server = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === "/mutate") {
      mutations++;
      return new Response("mutated");
    }
    if (path === "/finish") {
      finishStream?.();
      return new Response("ok");
    }
    if (path === "/stream") {
      let closed = false;
      let controller: ReadableStreamDefaultController<Uint8Array>;
      const close = () => {
        if (closed) return;
        closed = true;
        controller.close();
      };
      const stream = new ReadableStream<Uint8Array>({
        start(value) {
          controller = value;
          value.enqueue(new TextEncoder().encode("first\n"));
        },
        cancel() {
          closed = true;
        },
      });
      finishStream = () => {
        if (!closed) controller.enqueue(new TextEncoder().encode("last\n"));
        close();
      };
      request.signal.addEventListener("abort", close, { once: true });
      return new Response(stream, {
        headers: { "content-type": "text/plain" },
      });
    }
    return Response.json({
      method: request.method,
      body: await request.text(),
      marker: request.headers.get("x-smoke"),
    });
  },
});
const env: NodeJS.ProcessEnv = {
  ...process.env,
  KULALA_CORE_DATA_DIR: join(dir, "data"),
  KULALA_CORE_CACHE_DIR: join(dir, "cache"),
  XDG_CONFIG_HOME: join(dir, "config"),
  XDG_DATA_HOME: join(dir, "xdg-data"),
  XDG_STATE_HOME: join(dir, "state"),
  XDG_CACHE_HOME: join(dir, "xdg-cache"),
  NO_PROXY: "127.0.0.1,localhost",
  no_proxy: "127.0.0.1,localhost",
};

function invoke(
  command: string,
  args: string[],
  input?: unknown,
  onStream?: (event: KulalaHttpStreamEvent, child: ChildProcess) => void,
) {
  const { promise, resolve, reject } = Promise.withResolvers<string>();
  const child = execFile(
    command,
    args,
    { cwd: dir, env, timeout: 30000, maxBuffer: 8 * 1024 * 1024 },
    (error, stdout, stderr) => {
      if (error) reject(new Error(`${error.message}\n${stderr}`));
      else resolve(stdout);
    },
  );
  if (onStream && child.stdout) {
    createInterface({ input: child.stdout }).on("line", (line) => {
      let event;
      try {
        event = JSON.parse(line);
      } catch {
        return;
      }
      if (event?.type !== "http-stream") return;
      try {
        onStream(event, child);
      } catch (error) {
        child.kill("SIGKILL");
        reject(error);
      }
    });
  }
  child.stdin?.end(input === undefined ? undefined : JSON.stringify(input));
  return promise;
}

function finalResponse(output: string) {
  return JSON.parse(
    output
      .split("\n")
      .filter((line) => {
        try {
          return JSON.parse(line)?.type !== "http-stream";
        } catch {
          return true;
        }
      })
      .join("\n"),
  );
}

try {
  const filepath = join(dir, "smoke.http");
  writeFileSync(
    join(dir, "http-client.env.json"),
    JSON.stringify({
      default: { base: server.url.toString(), marker: "smoke" },
    }),
  );
  const content = [
    "### get",
    "GET {{base}}get",
    "X-Smoke: {{marker}}",
    "",
    "### post",
    "POST {{base}}post",
    "Content-Type: application/json",
    "",
    '{"hello":"world"}',
  ].join("\n");
  writeFileSync(filepath, content);
  const parsed = JSON.parse(
    await invoke(binary, [], { action: "parse", filepath, content }),
  );
  assert.equal(parsed.blocks.length, 2);
  const result = JSON.parse(
    await invoke(binary, [], {
      action: "run",
      filepath,
      content,
      env: "default",
    }),
  );
  assert.equal(result.type, "responses");
  assert.equal(result.data.length, 2);
  for (const response of result.data) {
    assert.equal(response.success, true, JSON.stringify(response));
    assert.equal(response.status, 200);
  }
  assert.equal(result.data[0].body.content.method, "GET");
  assert.equal(result.data[0].body.content.marker, "smoke");
  assert.deepEqual(JSON.parse(result.data[1].body.content.body), {
    hello: "world",
  });
  const streamPayload = {
    action: "run",
    content: `### stream\n# @kulala-keep-alive-stream\nGET ${server.url}stream`,
  };
  const events: KulalaHttpStreamEvent[] = [];
  const streamed = finalResponse(
    await invoke(binary, [], streamPayload, (event) => {
      events.push(event);
      if (event.event === "chunk") finishStream?.();
    }),
  );
  assert.equal(events[0]?.event, "headers");
  assert.equal(
    events.find((event) => event.event === "chunk")?.data,
    "first\n",
  );
  assert.equal(streamed.data[0].body.content, "first\nlast\n");
  const cancelledEvents: KulalaHttpStreamEvent[] = [];
  const cancelled = finalResponse(
    await invoke(
      binary,
      [],
      {
        ...streamPayload,
        content:
          streamPayload.content + `\n\n### mutate\nPOST ${server.url}mutate`,
      },
      (event, child) => {
        cancelledEvents.push(event);
        if (event.event === "chunk") child.kill("SIGTERM");
      },
    ),
  );
  assert(
    cancelledEvents.some((event) => event.event === "error"),
    "Cancelled stream did not emit an error",
  );
  assert.equal(cancelled.data[0].success, false);
  assert.equal(cancelled.data[0].cancelled, true);
  assert.equal(cancelled.data.length, 1);
  assert.equal(mutations, 0, "Cancellation executed a later request");
  // Exercise output larger than a pipe buffer to catch truncated CLI JSON.
  const large =
    "### large\nPOST " + server.url + "large\n\n" + "x".repeat(262144);
  const largeResult = JSON.parse(
    await invoke(binary, [], { action: "run", content: large }),
  );
  assert.equal(largeResult.data[0].body.content.body.length, 262144);
  if (plugin) {
    env.KULALA_CORE_PATH = binary;
    env.KULALA_SMOKE_FILE = filepath;
    env.KULALA_PLUGIN_DIR = plugin;
    env.KULALA_SMOKE_URL = server.url.toString();
    console.log(
      await invoke("nvim", [
        "--headless",
        "-u",
        "NONE",
        "-i",
        "NONE",
        "-l",
        join(plugin, "tests/core_integration.lua"),
      ]),
    );
  }
  console.log(
    "Core smoke passed: parse, env, GET, JSON POST, live stream, cancellation, large output",
  );
} finally {
  await server.stop(true);
  rmSync(dir, { recursive: true, force: true });
}
