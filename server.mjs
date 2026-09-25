import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {join, extname, sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {
  after, canResume, createEventLog, firstRetained, publish, reduceEvent, tip,
} from './src/event-log.mjs';
import {broadcast, createHub, subscribe, unsubscribe} from './src/hub.mjs';
import {acknowledge, ackSnapshot, createAckRegistry} from './src/acks.mjs';

const PUBLIC_DIR = fileURLToPath(new URL('./public', import.meta.url));
const MIME = {'.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8'};

function json(res, code, value) {
  res.writeHead(code, {'content-type': 'application/json'});
  res.end(JSON.stringify(value));
}

function sseFrame(event, data, id) {
  let frame = '';
  if (id !== undefined) frame += `id: ${id}\n`;
  if (event) frame += `event: ${event}\n`;
  frame += `data: ${JSON.stringify(data)}\n\n`;
  return frame;
}

export function createApp({limit = 100, heartbeatMs = 15000} = {}) {
  // Server state: event sequence + retention window, live subscriptions,
  // derived state snapshot, and per-user ack cursors.
  let log = createEventLog(limit);
  let state = {};
  const hub = createHub();
  const acks = createAckRegistry();

  function doPublish(type, data) {
    log = publish(log, type, data);
    const event = log.events.at(-1);
    state = reduceEvent(state, event);
    broadcast(hub, event);
    return event;
  }

  function handleStream(req, res, url) {
    // Last-Event-ID (sent automatically on EventSource reconnects) wins over
    // the query parameter: it is the fresher position of this connection.
    const header = req.headers['last-event-id'];
    const param = url.searchParams.get('cursor');
    const raw = header ?? param;
    if (raw !== null && raw !== undefined && !Number.isFinite(Number(raw))) {
      return json(res, 400, {error: 'invalid cursor'});
    }
    const cursor = raw === null || raw === undefined ? null : Number(raw);

    res.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
    });

    let conn = null;
    if (cursor === null) {
      // Legacy client without a cursor: live-only stream, no replay.
      res.write(sseFrame('hello', {mode: 'live', cursor: tip(log)}));
      conn = subscribe(hub, log, tip(log), event => res.write(sseFrame('change', event, event.id)));
    } else if (!canResume(log, cursor)) {
      // Retention window has moved past the cursor: tell the client
      // explicitly that it must perform a full resync, then close.
      res.write(sseFrame('resync-required', {
        reason: 'cursor-expired',
        cursor,
        firstAvailable: firstRetained(log),
        tip: tip(log),
      }));
      res.end();
      return;
    } else {
      res.write(sseFrame('hello', {mode: 'replay', from: cursor, to: tip(log)}));
      conn = subscribe(hub, log, cursor, event => res.write(sseFrame('change', event, event.id)));
      res.write(sseFrame('caught-up', {cursor: tip(log)}));
    }

    const heartbeat = setInterval(() => res.write(': ping\n\n'), heartbeatMs);
    heartbeat.unref();
    req.on('close', () => {
      clearInterval(heartbeat);
      unsubscribe(hub, conn);
    });
  }

  const app = createServer((req, res) => {
    const url = new URL(req.url ?? '/', 'http://localhost');

    if (url.pathname === '/api/publish' && req.method === 'POST') {
      let text = '';
      req.on('data', part => text += part);
      req.on('end', () => {
        const input = JSON.parse(text || '{}');
        json(res, 201, doPublish(input.type || 'state', input.data || {}));
      });
      return;
    }

    // Legacy JSON polling endpoint, unchanged for old clients.
    if (url.pathname === '/api/events') {
      const cursor = url.searchParams.get('cursor');
      if (!canResume(log, cursor)) return json(res, 409, {error: 'cursor expired'});
      return json(res, 200, {events: after(log, cursor), cursor: tip(log)});
    }

    if (url.pathname === '/api/stream') return handleStream(req, res, url);

    // Snapshot for a full resync: derived state plus the cursor it is valid at.
    if (url.pathname === '/api/state') {
      return json(res, 200, {state, cursor: tip(log)});
    }

    if (url.pathname === '/api/ack' && req.method === 'POST') {
      let text = '';
      req.on('data', part => text += part);
      req.on('end', () => {
        try {
          const input = JSON.parse(text || '{}');
          const result = acknowledge(acks, input.user ?? 'anonymous', input.cursor);
          json(res, 200, {user: String(input.user ?? 'anonymous'), ...result});
        } catch (error) {
          json(res, 400, {error: error.message});
        }
      });
      return;
    }

    if (url.pathname === '/api/acks') return json(res, 200, {acks: ackSnapshot(acks)});

    if (url.pathname === '/api/status') {
      return json(res, 200, {
        service: 'sse-recovery',
        cursor: tip(log),
        connections: hub.connections.size,
        acks: ackSnapshot(acks),
      });
    }

    if (url.pathname === '/' && !req.headers.accept?.includes('text/html')) {
      // Legacy status response for non-browser clients.
      return json(res, 200, {service: 'sse-recovery', cursor: tip(log)});
    }

    return serveStatic(url.pathname === '/' ? '/index.html' : url.pathname, res);
  });

  return {app, hub, acks, publish: doPublish, getLog: () => log, getState: () => state};
}

async function serveStatic(pathname, res) {
  const file = join(PUBLIC_DIR, pathname);
  if (!file.startsWith(PUBLIC_DIR + sep)) return json(res, 404, {error: 'not found'});
  try {
    const body = await readFile(file);
    res.writeHead(200, {'content-type': MIME[extname(file)] ?? 'application/octet-stream'});
    res.end(body);
  } catch {
    json(res, 404, {error: 'not found'});
  }
}

const {app} = createApp({heartbeatMs: Number(process.env.SSE_HEARTBEAT_MS ?? 15000)});
if (import.meta.url === `file://${process.argv[1]}`) app.listen(Number(process.env.PORT ?? 4180));
export {app};
