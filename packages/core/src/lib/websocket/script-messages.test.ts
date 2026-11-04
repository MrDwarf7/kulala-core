import { describe, expect, test } from "bun:test";
import { parseWebSocketScriptMessages } from "./script-messages";

describe("parseWebSocketScriptMessages", () => {
  test("returns undefined when the body has no script delimiter", () => {
    expect(parseWebSocketScriptMessages('{"name":"world"}')).toBeUndefined();
  });

  test("splits line-delimited messages and identifies the first message", () => {
    expect(
      parseWebSocketScriptMessages("first\nline\n===\nsecond\r\nline"),
    ).toEqual({
      messages: [{ data: "first\nline" }, { data: "second\nline" }],
      initialMessage: "first\nline",
    });
  });

  test("counts repeated server waits before the next message", () => {
    expect(
      parseWebSocketScriptMessages(
        "first\n=== wait-for-server\n=== wait-for-server\nsecond",
      ),
    ).toEqual({
      messages: [{ data: "first" }, { data: "second", waitForServer: 2 }],
      initialMessage: "first",
    });
  });
});
