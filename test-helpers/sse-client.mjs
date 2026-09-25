import {
  applySnapshot, createClientStore, noteAck,
  onCaughtUp, onChange, onDisconnected, onHello, onResyncRequired,
} from '../public/client-store.mjs';

// Async generator of SSE frames from a fetch response body.
export async function* readFrames(body) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  try {
    while (true) {
      const {done, value} = await reader.read();
      if (done) return;
      buffer += decoder.decode(value, {stream: true});
      let index;
      while ((index = buffer.indexOf('\n\n')) !== -1) {
        const raw = buffer.slice(0, index);
        buffer = buffer.slice(index + 2);
        const frame = parseFrame(raw);
        if (frame) yield frame;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

function parseFrame(raw) {
  let event = 'message', data = '', id;
  for (const line of raw.split('\n')) {
    if (line.startsWith(':')) continue; // heartbeat comment
    const colon = line.indexOf(':');
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? '' : line.slice(colon + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    if (field === 'event') event = value;
    else if (field === 'data') data = data ? `${data}\n${value}` : value;
    else if (field === 'id') id = value;
  }
  if (!data && event === 'message') return null;
  return {event, data, id};
}

export async function waitFor(predicate, {timeout = 5000, interval = 10} = {}) {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeout) throw new Error('waitFor: timeout');
    await new Promise(resolve => setTimeout(resolve, interval));
  }
}

// A simulated browser tab: wires SSE frames into the same client-store
// the page uses, acknowledges applied events, and full-syncs when told.
export class TestClient {
  constructor(base, user = 'tab') {
    this.base = base;
    this.user = user;
    this.store = createClientStore();
    this.controller = null;
    this.lastAck = null;
  }

  connect(cursor) {
    this.disconnect(false);
    this.store.status = 'connecting';
    this.controller = new AbortController();
    const url = cursor === null
      ? `${this.base}/api/stream`
      : `${this.base}/api/stream?cursor=${cursor}`;
    this.pump(fetch(url, {signal: this.controller.signal}));
  }

  async pump(request) {
    try {
      const response = await request;
      for await (const frame of readFrames(response.body)) {
        if (frame.event === 'hello') onHello(this.store, JSON.parse(frame.data));
        else if (frame.event === 'change') {
          const ackId = onChange(this.store, JSON.parse(frame.data));
          if (ackId !== null) await this.ack(ackId);
        } else if (frame.event === 'caught-up') onCaughtUp(this.store, JSON.parse(frame.data));
        else if (frame.event === 'resync-required') onResyncRequired(this.store, JSON.parse(frame.data));
        if (this.store.status === 'resync-required') return await this.fullSync();
      }
    } catch (error) {
      if (error.name !== 'AbortError') onDisconnected(this.store);
    }
  }

  async ack(cursor) {
    const response = await fetch(`${this.base}/api/ack`, {
      method: 'POST',
      headers: {'content-type': 'application/json'},
      body: JSON.stringify({user: this.user, cursor}),
    });
    this.lastAck = await response.json();
    noteAck(this.store, this.lastAck.cursor);
  }

  async fullSync() {
    const snapshot = await (await fetch(`${this.base}/api/state`)).json();
    applySnapshot(this.store, snapshot);
    this.connect(snapshot.cursor);
  }

  disconnect(record = true) {
    if (!this.controller) return;
    this.controller.abort();
    this.controller = null;
    if (record) onDisconnected(this.store);
  }
}

export async function startServer(options) {
  const {createApp} = await import('../server.mjs');
  const server = createApp({heartbeatMs: 60000, ...options});
  await new Promise(resolve => server.app.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.app.address().port}`;
  const close = () => new Promise(resolve => {
    server.app.closeAllConnections();
    server.app.close(resolve);
  });
  return {...server, base, close};
}
