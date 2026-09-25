import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {createApp} from '../server.mjs';
import {getJson, postJson, waitFor} from './helpers/node-event-source.mjs';

async function startApp(options = {}) {
  const {app, extras} = createApp({cursorFile: null, heartbeatMs: 0, ...options});
  await new Promise(resolve => app.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${app.address().port}`;
  return {app, extras, base, close: () => new Promise(resolve => { extras.stop(); app.close(resolve); })};
}

// Collect SSE frames from a raw HTTP response until `stop` says enough.
function readFrames(url, {headers = {}, stop} = {}) {
  const frames = [];
  const req = http.get(url, {headers}, res => {
    res.setEncoding('utf8');
    let buffer = '';
    res.on('data', chunk => {
      buffer += chunk;
      let index;
      while ((index = buffer.indexOf('\n\n')) >= 0) {
        frames.push(buffer.slice(0, index));
        buffer = buffer.slice(index + 2);
      }
      if (stop?.(frames)) req.destroy();
    });
    res.on('end', () => req.emit('done'));
  });
  req.frames = frames;
  return req;
}

test('legacy cursor-less JSON endpoint behaviour is preserved', async () => {
  const {base, close} = await startApp({eventLimit: 2});
  await postJson(`${base}/api/publish`, {type: 'one', data: {value: 1}});
  await postJson(`${base}/api/publish`, {type: 'two', data: {value: 2}});

  const all = await getJson(`${base}/api/events?cursor=0`);
  assert.equal(all.status, 200);
  assert.deepEqual(all.body.events.map(e => e.type), ['one', 'two']);
  assert.equal(all.body.cursor, 2);

  const tail = await getJson(`${base}/api/events?cursor=1`);
  assert.deepEqual(tail.body.events.map(e => e.type), ['two']);

  await postJson(`${base}/api/publish`, {type: 'three', data: {value: 3}});
  const expired = await getJson(`${base}/api/events?cursor=0`);
  assert.equal(expired.status, 409, 'retention window moved past cursor 0');
  assert.equal(expired.body.error, 'cursor expired');
  await close();
});

test('snapshot endpoint always succeeds and reports the retention boundary', async () => {
  const {base, close} = await startApp({eventLimit: 2});
  for (let i = 1; i <= 4; i++) await postJson(`${base}/api/publish`, {type: 'n', data: {i}});
  const snap = await getJson(`${base}/api/state`);
  assert.equal(snap.status, 200);
  assert.equal(snap.body.cursor, 4);
  assert.equal(snap.body.firstAvailable, 3);
  assert.deepEqual(snap.body.events.map(e => e.id), [3, 4]);
  await close();
});

test('ack endpoint is monotonic and shared per user', async () => {
  const {base, close} = await startApp();
  assert.equal((await postJson(`${base}/api/cursor/ack`, {user: 'u', cursor: 5})).body.cursor, 5);
  assert.equal((await postJson(`${base}/api/cursor/ack`, {user: 'u', cursor: 2})).body.cursor, 5, 'late ack ignored');
  assert.equal((await getJson(`${base}/api/cursor?user=u`)).body.cursor, 5);
  assert.equal((await getJson(`${base}/api/cursor?user=other`)).body.cursor, 0);
  await close();
});

test('SSE stream replays from Last-Event-ID when no query cursor is given', async () => {
  const {base, close} = await startApp();
  await postJson(`${base}/api/publish`, {type: 'a', data: {}});
  await postJson(`${base}/api/publish`, {type: 'b', data: {}});
  const req = readFrames(`${base}/api/events/stream`, {
    headers: {'last-event-id': '1'},
    stop: frames => frames.some(f => f.includes('event: sync')),
  });
  await waitFor(() => req.frames.some(f => f.includes('event: sync')));
  const dataFrames = req.frames.filter(f => f.startsWith('id: '));
  assert.deepEqual(dataFrames.map(f => f.match(/^id: (\d+)/)[1]), ['2'], 'only events after the header cursor');
  req.destroy();
  await close();
});

test('SSE stream sends resync-required then closes when retention is insufficient', async () => {
  const {base, close} = await startApp({eventLimit: 1});
  await postJson(`${base}/api/publish`, {type: 'a', data: {}});
  await postJson(`${base}/api/publish`, {type: 'b', data: {}});
  const req = readFrames(`${base}/api/events/stream?cursor=0`);
  await waitFor(() => req.frames.some(f => f.includes('resync-required')));
  assert.match(req.frames.find(f => f.includes('resync-required')), /cursor-expired/);
  req.destroy();
  await close();
});

test('a live subscriber receives events published after subscribing', async () => {
  const {base, close} = await startApp();
  const req = readFrames(`${base}/api/events/stream?cursor=0`, {
    stop: frames => frames.filter(f => f.startsWith('id: ')).length >= 1,
  });
  await waitFor(() => req.frames.some(f => f.includes('event: sync')));
  await postJson(`${base}/api/publish`, {type: 'live', data: {n: 1}});
  await waitFor(() => req.frames.some(f => f.startsWith('id: 1')));
  assert.match(req.frames.find(f => f.startsWith('id: 1')), /"type":"live"/);
  req.destroy();
  await close();
});

test('the page and client module are served', async () => {
  const {base, close} = await startApp();
  const page = await fetch(`${base}/`);
  assert.equal(page.status, 200);
  assert.match(await page.text(), /实时订阅/);
  const client = await fetch(`${base}/client.mjs`);
  assert.equal(client.status, 200);
  assert.match(client.headers.get('content-type'), /javascript/);
  const legacy = await getJson(`${base}/api/info`);
  assert.equal(legacy.body.service, 'sse-recovery');
  await close();
});
