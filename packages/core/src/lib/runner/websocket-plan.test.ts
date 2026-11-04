import { expect, test } from "bun:test";
import { getDocument } from "../parser/parser";
import { runDocument } from "./index";

function websocketPlan(body: string) {
  const content = `@who=world\n\n### chat\nWEBSOCKET ws://127.0.0.1:1\n\n${body}`;
  return getDocument(content).then((doc) =>
    runDocument(doc, undefined, { content }),
  );
}

test("WebSocket plans resolve variables before splitting scripted JSON messages", async () => {
  const wrapper = await websocketPlan(
    '{"hello":"{{who}}"}\n=== wait-for-server\n=== wait-for-server\n{"next":true}',
  );
  expect(wrapper.type).toBe("responses");
  const plan = wrapper.data[0];
  expect(plan).toMatchObject({
    success: true,
    protocol: "websocket",
    initialMessage: '{"hello":"world"}',
    messages: [
      { data: '{"hello":"world"}' },
      { data: '{"next":true}', waitForServer: 2 },
    ],
  });
});

test("ordinary WebSocket bodies keep the existing single-message plan", async () => {
  const wrapper = await websocketPlan("hello {{who}}");
  const plan = wrapper.data[0];
  expect(plan).toMatchObject({
    success: true,
    protocol: "websocket",
    initialMessage: "hello world",
  });
  expect(plan).not.toHaveProperty("messages");
});
