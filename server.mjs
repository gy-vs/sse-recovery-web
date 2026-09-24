import {createServer} from 'node:http';
import {after, canResume, createEventLog, publish} from './src/event-log.mjs';
let log = createEventLog(10);
function json(res, code, value) { res.writeHead(code, {'content-type': 'application/json'}); res.end(JSON.stringify(value)); }
const app = createServer((req, res) => { const url = new URL(req.url ?? '/', 'http://localhost'); if (url.pathname === '/api/publish' && req.method === 'POST') { let text = ''; req.on('data', part => text += part); req.on('end', () => { const input = JSON.parse(text || '{}'); log = publish(log, input.type || 'state', input.data || {}); json(res, 201, log.events.at(-1)); }); return; } if (url.pathname === '/api/events') { const cursor = url.searchParams.get('cursor'); if (!canResume(log, cursor)) return json(res, 409, {error: 'cursor expired'}); return json(res, 200, {events: after(log, cursor), cursor: log.events.at(-1)?.id ?? 0}); } if (url.pathname === '/') return json(res, 200, {service: 'sse-recovery', cursor: log.events.at(-1)?.id ?? 0}); return json(res, 404, {error: 'not found'}); });
if (import.meta.url === `file://${process.argv[1]}`) app.listen(Number(process.env.PORT ?? 4180));
export {app};
