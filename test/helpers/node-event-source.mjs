// Minimal EventSource-compatible client over fetch, for driving the real
// recovery client against a real server in tests (no browser involved).
export class NodeEventSource {
  constructor(url) {
    this.url = String(url);
    this.listeners = {};
    this.readyState = 0;
    this.closed = false;
    this.reader = null;
    this.started = this.start();
  }

  addEventListener(type, fn) {
    (this.listeners[type] ??= []).push(fn);
  }

  async start() {
    try {
      const response = await fetch(this.url);
      if (!response.ok || !response.body) throw new Error(`stream status ${response.status}`);
      this.readyState = 1;
      this.onopen?.();
      const reader = response.body.getReader();
      this.reader = reader;
      const decoder = new TextDecoder();
      let buffer = '';
      while (true) {
        const {done, value} = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, {stream: true});
        let index;
        while ((index = buffer.indexOf('\n\n')) >= 0) {
          const frame = buffer.slice(0, index);
          buffer = buffer.slice(index + 2);
          this.dispatch(frame);
        }
      }
      if (!this.closed) {
        this.readyState = 2;
        this.onerror?.(new Error('stream ended'));
      }
    } catch (error) {
      if (!this.closed) {
        this.readyState = 2;
        this.onerror?.(error);
      }
    }
  }

  dispatch(frame) {
    const data = [];
    let event = 'message';
    let id;
    for (const line of frame.split('\n')) {
      if (line === '' || line.startsWith(':')) continue;
      const colon = line.indexOf(':');
      const field = colon < 0 ? line : line.slice(0, colon);
      const value = colon < 0 ? '' : line.slice(colon + 1).replace(/^ /, '');
      if (field === 'data') data.push(value);
      else if (field === 'event') event = value;
      else if (field === 'id') id = value;
    }
    if (data.length === 0) return;
    const message = {data: data.join('\n'), lastEventId: id, type: event};
    if (event === 'message') this.onmessage?.(message);
    for (const fn of this.listeners[event] ?? []) fn(message);
  }

  close() {
    this.closed = true;
    this.readyState = 2;
    this.reader?.cancel().catch(() => {});
  }
}

export function mapStorage(map = new Map()) {
  return {
    get: key => (map.has(key) ? map.get(key) : null),
    set: (key, value) => map.set(key, String(value)),
    map,
  };
}

export async function waitFor(predicate, {timeout = 3000, interval = 5} = {}) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise(resolve => setTimeout(resolve, interval));
  }
  throw new Error('waitFor timed out');
}

export async function postJson(url, body) {
  const response = await fetch(url, {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify(body),
  });
  return {status: response.status, body: await response.json()};
}

export async function getJson(url) {
  const response = await fetch(url);
  return {status: response.status, body: await response.json()};
}
