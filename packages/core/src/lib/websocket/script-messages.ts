/** Message shape accepted by the scripted WebSocket session runner. */
export type WebSocketScriptMessage = {
  data: string;
  /** Number of server messages that must arrive before this message is sent. */
  waitForServer?: number;
};

/** Parsed body-script representation used by the WebSocket session runner. */
export type ParsedWebSocketScript = {
  messages: WebSocketScriptMessage[];
  initialMessage?: string;
};

/** Split the documented line-delimited `===` WebSocket body syntax. */
export function parseWebSocketScriptMessages(
  body: string,
): ParsedWebSocketScript | undefined {
  const messages: WebSocketScriptMessage[] = [];
  let segment: string[] = [];
  let waitForServer = 0;
  let hasDelimiter = false;

  const flush = (): void => {
    const data = segment.join("\n").replace(/^\n+|\n+$/g, "");
    segment = [];
    if (data.trim() === "") return;
    messages.push({ data, ...(waitForServer > 0 ? { waitForServer } : {}) });
    waitForServer = 0;
  };

  for (const line of body.split(/\r\n|\n|\r/)) {
    const marker = line.trim();
    if (marker === "===" || marker === "=== wait-for-server") {
      hasDelimiter = true;
      flush();
      if (marker === "=== wait-for-server") waitForServer++;
    } else {
      segment.push(line);
    }
  }
  flush();

  if (!hasDelimiter) return undefined;
  const initialMessage = messages[0]?.data;
  return {
    messages,
    ...(initialMessage !== undefined ? { initialMessage } : {}),
  };
}
