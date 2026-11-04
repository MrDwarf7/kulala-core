import { describe, expect, test } from "bun:test";
import { prepareWebSocketConnect } from "./prepare-connect";

describe("prepareWebSocketConnect", () => {
  test("passes through already-resolved connect options", () => {
    expect(
      prepareWebSocketConnect({
        url: "wss://ws.ifelse.io",
        body: '{"name":"world"}',
      }),
    ).toEqual({
      url: "wss://ws.ifelse.io",
      body: '{"name":"world"}',
    });
  });

  test("substitutes variables when vars are provided", () => {
    expect(
      prepareWebSocketConnect({
        url: "{{websocket.addr}}",
        vars: { "websocket.addr": "wss://ws.ifelse.io" },
      }),
    ).toEqual({
      url: "wss://ws.ifelse.io",
    });
  });

  test("throws when templates remain and no vars are provided", () => {
    expect(() =>
      prepareWebSocketConnect({ url: "{{websocket.addr}}" }),
    ).toThrow(/unresolved/);
  });

  test("forwards script messages and only forwards an explicit timeout", () => {
    const messages = [{ data: "first" }, { data: "second", waitForServer: 2 }];
    expect(
      prepareWebSocketConnect({
        url: "ws://localhost",
        messages,
        timeoutMs: 250,
      }),
    ).toMatchObject({
      url: "ws://localhost",
      messages,
      timeoutMs: 250,
    });
    expect(
      prepareWebSocketConnect({ url: "ws://localhost", messages }),
    ).not.toHaveProperty("timeoutMs");
  });

  test("substitutes variables in scripted message data", () => {
    expect(
      prepareWebSocketConnect({
        url: "ws://localhost",
        messages: [{ data: "{{name}}", waitForServer: 1 }],
        vars: { name: "world" },
      }),
    ).toMatchObject({
      messages: [{ data: "world", waitForServer: 1 }],
    });
  });

  test("rejects malformed messages and waits", () => {
    expect(() =>
      prepareWebSocketConnect({
        url: "ws://localhost",
        messages: "not-an-array" as unknown as { data: string }[],
      }),
    ).toThrow(/array/);
    expect(() =>
      prepareWebSocketConnect({
        url: "ws://localhost",
        messages: [null as unknown as { data: string }],
      }),
    ).toThrow(/object/);
    expect(() =>
      prepareWebSocketConnect({
        url: "ws://localhost",
        messages: [{ data: 3 } as unknown as { data: string }],
      }),
    ).toThrow(/data/);
    for (const waitForServer of [
      -1,
      1.5,
      Number.POSITIVE_INFINITY,
      Number.NaN,
    ]) {
      expect(() =>
        prepareWebSocketConnect({
          url: "ws://localhost",
          messages: [{ data: "x", waitForServer }],
        }),
      ).toThrow(/waitForServer/);
    }
  });

  test("rejects nonpositive or nonfinite explicit timeouts", () => {
    for (const timeoutMs of [0, -1, Number.POSITIVE_INFINITY, Number.NaN]) {
      expect(() =>
        prepareWebSocketConnect({ url: "ws://localhost", timeoutMs }),
      ).toThrow(/timeoutMs/);
    }
  });
});
