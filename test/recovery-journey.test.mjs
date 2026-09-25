import test from 'node:test';
import assert from 'node:assert/strict';
import {createApp} from '../server.mjs';
import {createRecoveryClient, STATES} from '../public/client.mjs';
import {NodeEventSource, getJson, mapStorage, postJson, waitFor} from './helpers/node-event-source.mjs';

async function startApp(options = {}) {
  const {app, extras} = createApp({cursorFile: null, heartbeatMs: 0, ...options});
  await new Promise(resolve => app.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${app.address().port}`;
  return {app, extras, base, close: () => new Promise(resolve => { extras.stop(); app.close(resolve); })};
}

function makeClient(base, {user = 'journey', storage = mapStorage()} = {}) {
  return createRecoveryClient({
    user,
    storage,
    eventSourceImpl: url => new NodeEventSource(url),
    streamUrl: `${base}/api/events/stream`,
    snapshotUrl: `${base}/api/state`,
    ackUrl: `${base}/api/cursor/ack`,
    reconnectDelay: () => 100,
  });
}

const eventIds = client => client.record.filter(r => r.kind === 'event').map(r => r.id);

test('mid-stream drop: events published while away are replayed exactly once', async () => {
  const {base, extras, close} = await startApp({eventLimit: 10});
  const client = makeClient(base);
  await client.connect();
  await waitFor(() => client.state === STATES.LIVE);

  await postJson(`${base}/api/publish`, {type: 'note', data: {n: 1}});
  await postJson(`${base}/api/publish`, {type: 'note', data: {n: 2}});
  await waitFor(() => client.cursor === 2);

  // The connection dies mid-stream; three events arrive while the client is away.
  extras.hub.dropAll();
  await waitFor(() => client.state === STATES.RECONNECTING);
  await postJson(`${base}/api/publish`, {type: 'note', data: {n: 3}});
  await postJson(`${base}/api/publish`, {type: 'note', data: {n: 4}});
  await postJson(`${base}/api/publish`, {type: 'note', data: {n: 5}});

  await waitFor(() => client.state === STATES.LIVE && client.cursor === 5);
  assert.deepEqual(eventIds(client), [1, 2, 3, 4, 5], 'no event skipped, none applied twice');
  assert.equal(client.duplicates, 0);
  assert.deepEqual(client.record.filter(r => r.kind === 'event').map(r => r.origin),
    ['live', 'live', 'replay', 'replay', 'replay'], 'the missed range came back as replay');
  assert.ok(client.transitions.some(t => t.to === STATES.RECONNECTING), 'the drop was observable');

  // Server and client agree on the confirmed boundary.
  await waitFor(() => extras.cursors.get('journey') === 5);
  const boundary = await getJson(`${base}/api/cursor?user=journey`);
  assert.equal(boundary.body.cursor, 5);
  assert.equal(client.sharedCursor, 5);
  client.disconnect();
  await close();
});

test('insufficient retention: explicit resync-required, observable full-sync, then live', async () => {
  const {base, extras, close} = await startApp({eventLimit: 3});
  const client = makeClient(base);
  await client.connect();
  await waitFor(() => client.state === STATES.LIVE);
  for (let i = 1; i <= 3; i++) await postJson(`${base}/api/publish`, {type: 'note', data: {n: i}});
  await waitFor(() => client.cursor === 3);

  client.disconnect();
  assert.equal(client.state, STATES.CLOSED);

  // The retention window (3) slides past the client's cursor while it is away.
  for (let i = 4; i <= 7; i++) await postJson(`${base}/api/publish`, {type: 'note', data: {n: i}});

  await client.connect();
  await waitFor(() => client.state === STATES.LIVE && client.cursor === 7);
  assert.ok(client.transitions.some(t => t.to === STATES.RESYNCING && t.reason === 'cursor-expired'),
    'the resync step is visible in the state log');
  assert.equal(client.record[0].kind, 'marker');
  assert.equal(client.record[0].marker, 'full-sync');
  assert.equal(client.record[0].fromCursor, 3);
  assert.deepEqual(eventIds(client), [5, 6, 7], 'record is the server snapshot, not a silent reset');
  assert.deepEqual(client.record.slice(1).map(r => r.origin), ['snapshot', 'snapshot', 'snapshot']);

  await waitFor(() => extras.cursors.get('journey') === 7);
  assert.equal(client.sharedCursor, 7);
  client.disconnect();
  await close();
});

test('events published while a client is replaying arrive live after the sync marker', async () => {
  const {base, close} = await startApp();
  await postJson(`${base}/api/publish`, {type: 'note', data: {n: 1}});
  await postJson(`${base}/api/publish`, {type: 'note', data: {n: 2}});

  const client = makeClient(base);
  await client.connect();
  await waitFor(() => client.state === STATES.LIVE && client.cursor === 2);
  await postJson(`${base}/api/publish`, {type: 'note', data: {n: 3}});
  await waitFor(() => client.cursor === 3);

  assert.deepEqual(eventIds(client), [1, 2, 3]);
  assert.deepEqual(client.record.filter(r => r.kind === 'event').map(r => r.origin), ['replay', 'replay', 'live']);
  assert.equal(client.duplicates, 0);
  client.disconnect();
  await close();
});

test('two tabs on one user: the shared boundary is monotonic across the network', async () => {
  const {base, extras, close} = await startApp();
  const storage = mapStorage(); // same origin, same user: both tabs share this
  const tabA = makeClient(base, {user: 'tabs', storage});
  await tabA.connect();
  await waitFor(() => tabA.state === STATES.LIVE);
  for (let i = 1; i <= 3; i++) await postJson(`${base}/api/publish`, {type: 'note', data: {n: i}});
  await waitFor(() => tabA.cursor === 3);

  // A second tab opens later: it full-syncs from the shared cursor, then joins live.
  const tabB = makeClient(base, {user: 'tabs', storage});
  await tabB.connect();
  await waitFor(() => tabB.state === STATES.LIVE && tabB.cursor === 3);
  assert.equal(tabB.record[0].marker, 'full-sync', 'the late tab visibly starts from a snapshot');

  for (let i = 4; i <= 5; i++) await postJson(`${base}/api/publish`, {type: 'note', data: {n: i}});
  await waitFor(() => tabA.cursor === 5 && tabB.cursor === 5);
  await waitFor(() => extras.cursors.get('tabs') === 5);

  // A late ack (as if a suspended tab finally woke up) cannot drag anyone back.
  const late = await postJson(`${base}/api/cursor/ack`, {user: 'tabs', cursor: 2});
  assert.equal(late.body.cursor, 5);
  assert.equal((await getJson(`${base}/api/cursor?user=tabs`)).body.cursor, 5);
  assert.equal(storage.map.get('sse-recovery:cursor:tabs'), '5');
  assert.equal(tabA.cursor, 5);
  assert.equal(tabB.cursor, 5);
  tabA.disconnect();
  tabB.disconnect();
  await close();
});

test('the legacy cursor-less flow keeps working while streams are attached', async () => {
  const {base, close} = await startApp();
  const client = makeClient(base);
  await client.connect();
  await waitFor(() => client.state === STATES.LIVE);
  await postJson(`${base}/api/publish`, {type: 'note', data: {n: 1}});
  await waitFor(() => client.cursor === 1);

  const legacy = await getJson(`${base}/api/events?cursor=0`);
  assert.equal(legacy.status, 200);
  assert.deepEqual(legacy.body.events.map(e => e.id), [1]);
  assert.equal(legacy.body.cursor, 1);
  client.disconnect();
  await close();
});
