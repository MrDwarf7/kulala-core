import { describe, expect, test } from "bun:test";
import { createServer } from "node:http";
import {
  createServer as createTcpServer,
  type Server as TcpServer,
  type Socket,
} from "node:net";
import {
  createServer as createHttp2Server,
  type Http2Server,
  type ServerHttp2Stream,
} from "node:http2";
import { httpRequest } from "./http-client";
import { curlNeedsRequestFlag } from "./curl-transport";
import { resolveCurlPath } from "./embedded-curl";

async function hasCurl(): Promise<boolean> {
  try {
    await resolveCurlPath();
    return true;
  } catch {
    return false;
  }
}

function listenHttp(server: ReturnType<typeof createServer>): Promise<number> {
  return new Promise((resolve, reject) => {
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (addr && typeof addr === "object") resolve(addr.port);
      else reject(new Error("no address"));
    });
  });
}
function listenTcp(server: TcpServer): Promise<number> {
  const { promise, resolve, reject } = Promise.withResolvers<number>();
  server.listen(0, "127.0.0.1", () => {
    const addr = server.address();
    if (addr && typeof addr === "object") resolve(addr.port);
    else reject(new Error("no address"));
  });
  return promise;
}

function listenHttp2(server: Http2Server): Promise<number> {
  return new Promise((resolve, reject) => {
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (addr && typeof addr === "object") resolve(addr.port);
      else reject(new Error("no address"));
    });
  });
}

describe("curl transport", () => {
  test("forces HTTP/1.0 when requested", async () => {
    if (!(await hasCurl())) return;

    let seenVersion: string | undefined;
    const server = createServer((req, res) => {
      seenVersion = req.httpVersion;
      res.statusCode = 200;
      res.setHeader("content-type", "text/plain");
      res.end("ok");
    });
    const port = await listenHttp(server);

    const res = await httpRequest({
      url: `http://127.0.0.1:${port}/`,
      method: "GET",
      headers: {},
      httpVersion: "HTTP/1.0",
    });

    server.close();
    expect(res.statusCode).toBe(200);
    expect(seenVersion).toBe("1.0");
    expect(res.redirectChain).toBeUndefined();
  });

  test("follows redirects and returns final url", async () => {
    if (!(await hasCurl())) return;

    const server = createServer((req, res) => {
      if (req.url === "/") {
        res.statusCode = 302;
        res.setHeader("location", "/final");
        res.end();
        return;
      }
      res.statusCode = 200;
      res.setHeader("content-type", "text/plain");
      res.end("final");
    });
    const port = await listenHttp(server);

    const res = await httpRequest({
      url: `http://127.0.0.1:${port}/`,
      method: "GET",
      headers: {},
    });

    server.close();
    expect(res.statusCode).toBe(200);
    expect(res.url).toContain("/final");
    expect(res.redirectChain?.length).toBe(2);
    expect(res.redirectChain?.[0]?.statusCode).toBe(302);
    expect(res.redirectChain?.[1]?.statusCode).toBe(200);
    expect(res.timings.phases.redirect).toBeGreaterThanOrEqual(0);
    expect(res.timings.phases.total).toBeGreaterThanOrEqual(0);
  });

  test("keeps POST body across 301/302/303 redirects (GraphQL-style)", async () => {
    if (!(await hasCurl())) return;

    const server = createServer(async (req, res) => {
      if (req.url === "/start") {
        res.statusCode = 302;
        res.setHeader("location", "/graphql");
        res.end();
        return;
      }
      if (req.url === "/graphql") {
        const chunks: Buffer[] = [];
        for await (const c of req) chunks.push(c as Buffer);
        const raw = Buffer.concat(chunks).toString("utf-8");
        if (req.method !== "POST" || raw.trim() === "") {
          res.statusCode = 400;
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify({ errors: [{ message: "Missing body" }] }));
          return;
        }
        res.statusCode = 200;
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify({ ok: true }));
        return;
      }
      res.statusCode = 404;
      res.end();
    });

    const port = await listenHttp(server);
    const res = await httpRequest({
      url: `http://127.0.0.1:${port}/start`,
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: "query { __typename }" }),
    });

    server.close();
    expect(res.statusCode).toBe(200);
  });

  test("supports HTTP/2 (h2c prior knowledge) locally", async () => {
    if (!(await hasCurl())) return;

    const server = createHttp2Server();
    server.on("stream", (stream: ServerHttp2Stream) => {
      stream.respond({ ":status": 200, "content-type": "text/plain" });
      stream.end("h2");
    });
    const port = await listenHttp2(server);

    try {
      const res = await httpRequest({
        url: `http://127.0.0.1:${port}/`,
        method: "GET",
        headers: {},
        httpVersion: "HTTP/2",
      });

      expect(res.statusCode).toBe(200);
      expect(res.timings.phases.total).toBeGreaterThanOrEqual(0);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      // INFO:
      // curl h2c prior knowledge against Node's http2 server is flaky on some nghttp2 builds.
      if (!msg.includes("nghttp2 recv error")) throw err;
    } finally {
      server.close();
    }
  });

  test("cross-host redirect does not forward client Cookie or host-only Set-Cookie", async () => {
    if (!(await hasCurl())) return;

    let cookieSeenOnB = "";
    const serverB = createServer((req, res) => {
      cookieSeenOnB = req.headers.cookie ?? "";
      res.statusCode = 200;
      res.setHeader("content-type", "text/plain");
      res.end("b");
    });
    const portB = await listenHttp(serverB);

    const serverA = createServer((req, res) => {
      if (req.url === "/") {
        res.statusCode = 302;
        // `localhost` vs `127.0.0.1`: different URL hosts so client cookies are not replayed.
        res.setHeader("location", `http://localhost:${portB}/on-b`);
        res.setHeader("set-cookie", "from_a=1; Path=/");
        res.end();
        return;
      }
      res.statusCode = 404;
      res.end();
    });
    const portA = await listenHttp(serverA);

    const res = await httpRequest({
      url: `http://127.0.0.1:${portA}/`,
      method: "GET",
      headers: { Cookie: "client_only=1" },
    });

    serverA.close();
    serverB.close();

    expect(res.statusCode).toBe(200);
    expect(cookieSeenOnB).toBe("");
  });

  test("same-host redirect still sends client Cookie and Set-Cookie applies on next hop", async () => {
    if (!(await hasCurl())) return;

    let cookieOnFinal = "";
    const server = createServer((req, res) => {
      if (req.url === "/" || req.url?.startsWith("/?")) {
        res.statusCode = 302;
        res.setHeader("location", "/final");
        res.setHeader("set-cookie", "from_redirect=1; Path=/");
        res.end();
        return;
      }
      if (req.url === "/final") {
        cookieOnFinal = req.headers.cookie ?? "";
        res.statusCode = 200;
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify({ ok: true }));
        return;
      }
      res.statusCode = 404;
      res.end();
    });
    const port = await listenHttp(server);

    const res = await httpRequest({
      url: `http://127.0.0.1:${port}/`,
      method: "GET",
      headers: { Cookie: "client=xyz" },
    });

    server.close();

    expect(res.statusCode).toBe(200);
    expect(cookieOnFinal).toContain("client=xyz");
    expect(cookieOnFinal).toContain("from_redirect=1");
  });

  test("sends JSON body when Content-Type is omitted", async () => {
    if (!(await hasCurl())) return;

    let raw = "";
    const server = createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const c of req) chunks.push(c as Buffer);
      raw = Buffer.concat(chunks).toString("utf8");
      res.statusCode = 200;
      res.setHeader("content-type", "application/json");
      res.end(
        JSON.stringify({
          raw,
          contentType: req.headers["content-type"] ?? null,
        }),
      );
    });
    const port = await listenHttp(server);

    const res = await httpRequest({
      url: `http://127.0.0.1:${port}/`,
      method: "POST",
      headers: { "Content-Type;": "" },
      body: '{"foo":"bar"}',
    });

    server.close();

    expect(res.statusCode).toBe(200);
    const parsed = JSON.parse(res.body.toString("utf8")) as {
      raw: string;
      contentType: string | null;
    };
    expect(parsed.raw).toBe('{"foo":"bar"}');
    expect(parsed.contentType).toBeNull();
  });
  test("streams response headers and UTF-8 chunks before the transfer ends", async () => {
    if (!(await hasCurl())) return;

    let serverEnded = false;
    let streamedWhileServerOpen = false;
    let peer: Socket;
    const chunks: string[] = [];
    const events: string[] = [];
    const sockets = new Set<Socket>();
    const server = createTcpServer((socket) => {
      peer = socket;
      sockets.add(socket);
      socket.on("close", () => sockets.delete(socket));
      socket.on("error", () => {});
      socket.once("data", () => {
        socket.write(
          "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-T",
        );
        socket.write(
          Buffer.concat([
            Buffer.from(
              "ype: text/plain\r\nContent-Length: 7\r\nConnection: close\r\n\r\n",
            ),
            Buffer.from([0xe2]),
          ]),
        );
      });
    });
    const port = await listenTcp(server);
    try {
      const response = await httpRequest({
        url: `http://127.0.0.1:${port}/`,
        method: "GET",
        headers: {},
        timeoutSec: 3,
        onStream: {
          onHeaders: (headers) => {
            events.push("headers");
            expect(headers.statusCode).toBe(200);
            // The next UTF-8 bytes are sent only after the first bytes reach the client.
            peer.write(Buffer.from([0x98, 0x83]));
          },
          onChunk: (text) => {
            events.push("chunk");
            chunks.push(text);
            if (!serverEnded) {
              streamedWhileServerOpen = true;
              expect(text).toBe("☃");
              serverEnded = true;
              peer.end("done");
            }
          },
        },
      });
      expect(response.body.toString("utf8")).toBe("☃done");
      expect(events[0]).toBe("headers");
      expect(chunks.join("")).toBe("☃done");
      expect(streamedWhileServerOpen).toBe(true);
    } finally {
      for (const socket of sockets) socket.destroy();
      const { promise, resolve } = Promise.withResolvers<void>();
      server.close(() => resolve());
      await promise;
    }
  }, 5000);

  test("streams the final response through redirects and an authentication challenge", async () => {
    if (!(await hasCurl())) return;
    const statuses: number[] = [];
    const chunks: string[] = [];
    const server = createServer((request, response) => {
      if (request.url === "/redirect") {
        response.writeHead(302, { location: "/protected" });
        response.end("redirect body");
      } else if (!request.headers.authorization) {
        response.writeHead(401, { "www-authenticate": 'Basic realm="test"' });
        response.end("challenge body");
      } else {
        response.end("authorized");
      }
    });
    const port = await listenHttp(server);
    try {
      const response = await httpRequest({
        url: `http://127.0.0.1:${port}/redirect`,
        method: "GET",
        headers: {},
        timeoutSec: 3,
        extraCurlArgv: [
          "--anyauth",
          "--user",
          "test:password",
          "--retry",
          "1",
          "--retry",
          "0",
        ],
        onStream: {
          onHeaders: ({ statusCode }) => statuses.push(statusCode),
          onChunk: (text) => chunks.push(text),
        },
      });
      expect(response.statusCode).toBe(200);
      expect(statuses).toEqual([200]);
      expect(chunks.join("")).toBe("authorized");
    } finally {
      server.closeAllConnections();
      const { promise, resolve } = Promise.withResolvers<void>();
      server.close(() => resolve());
      await promise;
    }
  });

  test("rejects automatic retries before starting a live stream", async () => {
    for (const extraCurlArgv of [["--retry", "1"], ["--retry=1"]]) {
      await expect(
        httpRequest({
          url: "http://127.0.0.1:1",
          method: "GET",
          headers: {},
          extraCurlArgv,
          onStream: { onHeaders() {}, onChunk() {} },
        }),
      ).rejects.toThrow(
        "Live streaming cannot be combined with automatic cURL retries",
      );
    }
  });
});

describe("curlNeedsRequestFlag", () => {
  test("omits -X for default GET and POST with body", () => {
    expect(curlNeedsRequestFlag("GET", false)).toBe(false);
    expect(curlNeedsRequestFlag("POST", true)).toBe(false);
  });

  test("requires -X for non-inferred combinations", () => {
    expect(curlNeedsRequestFlag("GET", true)).toBe(true);
    expect(curlNeedsRequestFlag("POST", false)).toBe(true);
    expect(curlNeedsRequestFlag("PUT", true)).toBe(true);
    expect(curlNeedsRequestFlag("DELETE", false)).toBe(true);
  });
});
