import {
  substituteInObject,
  substituteInString,
} from "../variables/substitute";
import type { WebSocketConnectOptions } from "./websocket-session";

export type WebSocketConnectInput = WebSocketConnectOptions & {
  /** Fallback variables for unresolved URL, body, headers, or script messages. */
  vars?: Record<string, string>;
};

function validateConnectInput(input: WebSocketConnectInput): void {
  if (
    typeof input !== "object" ||
    input === null ||
    Array.isArray(input) ||
    typeof input.url !== "string" ||
    input.url.trim() === ""
  ) {
    throw new Error("WebSocket connect input must include a URL");
  }

  if (input.messages !== undefined) {
    if (!Array.isArray(input.messages)) {
      throw new Error("WebSocket messages must be an array");
    }
    input.messages.forEach((message, index) => {
      if (
        typeof message !== "object" ||
        message === null ||
        Array.isArray(message)
      ) {
        throw new Error(`WebSocket message ${index} must be an object`);
      }
      if (typeof message.data !== "string") {
        throw new Error(`WebSocket message ${index} data must be a string`);
      }
      if (
        message.waitForServer !== undefined &&
        (!Number.isFinite(message.waitForServer) ||
          !Number.isInteger(message.waitForServer) ||
          message.waitForServer < 0)
      ) {
        throw new Error(
          `WebSocket message ${index} waitForServer must be a nonnegative integer`,
        );
      }
    });
  }

  if (
    input.timeoutMs !== undefined &&
    (!Number.isFinite(input.timeoutMs) || input.timeoutMs <= 0)
  ) {
    throw new Error("WebSocket timeoutMs must be a finite positive number");
  }
}

/**
 * Apply variable substitution to a WebSocket connect payload before opening the session.
 * Callers should pass values already resolved by kulala-core run when possible; `vars`
 * is a fallback for direct `--websocket` invocations.
 */
export function prepareWebSocketConnect(
  input: WebSocketConnectInput,
): WebSocketConnectOptions {
  validateConnectInput(input);
  const vars = input.vars ?? {};
  const hasTemplates =
    input.url.includes("{{") ||
    (input.body?.includes("{{") ?? false) ||
    Object.values(input.headers ?? {}).some((v) => v.includes("{{")) ||
    (input.messages?.some((message) => message.data.includes("{{")) ?? false);

  if (!hasTemplates) {
    return {
      url: input.url,
      body: input.body,
      headers: input.headers,
      ...(input.messages !== undefined ? { messages: input.messages } : {}),
      ...(input.timeoutMs !== undefined ? { timeoutMs: input.timeoutMs } : {}),
    };
  }

  if (Object.keys(vars).length === 0) {
    throw new Error(
      "WebSocket connect URL or payload contains unresolved {{variables}}; resolve the request in kulala-core first",
    );
  }

  const headers = input.headers
    ? (substituteInObject(input.headers, vars) as Record<string, string>)
    : undefined;
  const body =
    input.body != null
      ? (substituteInObject(input.body, vars) as string)
      : undefined;
  const messages = input.messages?.map((message) => ({
    ...message,
    data: substituteInString(message.data, vars),
  }));

  return {
    url: substituteInString(input.url, vars),
    ...(body !== undefined ? { body } : {}),
    ...(headers !== undefined ? { headers } : {}),
    ...(messages !== undefined ? { messages } : {}),
    ...(input.timeoutMs !== undefined ? { timeoutMs: input.timeoutMs } : {}),
  };
}
