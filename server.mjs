import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {after, canResume, createEventLog, firstAvailable, latestCursor, publish} from './src/event-log.mjs';
import {createCursorStore, createFileCursorPersistence} from './src/cursor-store.mjs';
import {attachStream, createHub} from './src/sse.mjs';

const root = fileURLToPath(new URL('.', import.meta.url));

const staticFiles = new Map([
  ['/', ['index.html', 'text/html; charset=utf-8']],
  ['/index.html', ['index.html', 'text/html; charset=utf-8']],
  ['/app.js', ['app.js', 'text/javascript; charset=utf-8']],
  ['/client.mjs', ['client.mjs', 'text/javascript; charset=utf-8']],
]);

function json(res, code, value) {
  res.writeHead(code, {'content-type': 'application/json'});
  res.end(JSON.stringify(value));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let text = '';
    req.on('data', part => text += part);
    req.on('end', () => resolve(text));
    req.on('error', reject);
  });
}

export function createApp({eventLimit = 10, cursorFile = 'data/cursors.json', heartbeatMs = 15000} = {}) {
  let log = createEventLog(eventLimit);
  const persistence = cursorFile ? createFileCursorPersistence(cursorFile) : null;
  const cursors = createCursorStore(persistence ?? {});
  const hub = createHub();
  const heartbeat = heartbeatMs > 0 ? setInterval(() => hub.heartbeat(), heartbeatMs) : null;
  heartbeat?.unref?.();

  function publishEvent(type, data) {
    log = publish(log, type, data);
    const event = log.events.at(-1);
    hub.broadcast(event);
    return event;
  }

  const app = createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? '/', 'http://localhost');
      const pathname = url.pathname;

      if (req.method === 'GET' && staticFiles.has(pathname)) {
        const [file, type] = staticFiles.get(pathname);
        try {
          const content = await readFile(join(root, 'public', file));
          res.writeHead(200, {'content-type': type});
          res.end(content);
        } catch {
          json(res, 404, {error: 'not found'});
        }
        return;
      }

      if (pathname === '/api/info' && req.method === 'GET') {
        return json(res, 200, {service: 'sse-recovery', cursor: latestCursor(log)});
      }

      if (pathname === '/api/publish' && req.method === 'POST') {
        let input;
        try {
          input = JSON.parse(await readBody(req) || '{}');
        } catch {
          return json(res, 400, {error: 'invalid json'});
        }
        return json(res, 201, publishEvent(input.type || 'state', input.data || {}));
      }

      // Legacy cursor-less JSON endpoint: behaviour unchanged for old clients.
      if (pathname === '/api/events' && req.method === 'GET') {
        const cursor = url.searchParams.get('cursor');
        if (!canResume(log, cursor)) {
          return json(res, 409, {error: 'cursor expired', cursor: latestCursor(log), firstAvailable: firstAvailable(log)});
        }
        return json(res, 200, {events: after(log, cursor), cursor: latestCursor(log)});
      }

      // Resumable SSE stream. Cursor comes from ?cursor= or Last-Event-ID.
      if (pathname === '/api/events/stream' && req.method === 'GET') {
        const cursor = url.searchParams.get('cursor') ?? req.headers['last-event-id'] ?? 0;
        res.writeHead(200, {
          'content-type': 'text/event-stream; charset=utf-8',
          'cache-control': 'no-cache',
          connection: 'keep-alive',
          'x-accel-buffering': 'no',
        });
        res.write('retry: 1000\n\n');
        attachStream({res, log, cursor, hub});
        return;
      }

      // Full-sync snapshot: always succeeds, even when the cursor is expired.
      if (pathname === '/api/state' && req.method === 'GET') {
        return json(res, 200, {
          cursor: latestCursor(log),
          firstAvailable: firstAvailable(log),
          limit: log.limit,
          events: log.events,
        });
      }

      // Shared per-user ack boundary. Monotonic: a late ack never regresses it.
      if (pathname === '/api/cursor/ack' && req.method === 'POST') {
        let input;
        try {
          input = JSON.parse(await readBody(req) || '{}');
        } catch {
          return json(res, 400, {error: 'invalid json'});
        }
        const user = String(input.user ?? 'default');
        return json(res, 200, {user, cursor: cursors.ack(user, input.cursor)});
      }

      if (pathname === '/api/cursor' && req.method === 'GET') {
        const user = url.searchParams.get('user') ?? 'default';
        return json(res, 200, {user, cursor: cursors.get(user)});
      }

      return json(res, 404, {error: 'not found'});
    } catch (error) {
      json(res, 500, {error: String(error?.message ?? error)});
    }
  });

  const extras = {
    hub,
    cursors,
    getLog: () => log,
    publish: publishEvent,
    stop() { if (heartbeat) clearInterval(heartbeat); },
  };
  return {app, extras};
}

const {app, extras} = createApp({
  eventLimit: Number(process.env.EVENT_LIMIT ?? 10),
  cursorFile: process.env.CURSOR_FILE ?? 'data/cursors.json',
});

if (import.meta.url === `file://${process.argv[1]}`) {
  const port = Number(process.env.PORT ?? 4180);
  app.listen(port, () => console.log(`sse-recovery listening on http://localhost:${port}`));
}

export {app, extras};
