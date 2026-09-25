import test from 'node:test';
import assert from 'node:assert/strict';
import {createRecoveryClient, STATES} from '../public/client.mjs';
import {createCursorStore} from '../src/cursor-store.mjs';
import {mapStorage, waitFor} from './helpers/node-event-source.mjs';

class FakeEventSource {
  static instances = [];
  static reset() { FakeEventSource.instances = []; }
  static last() { return FakeEventSource.instances.at(-1); }
  constructor(url) {
    this.url = url;
    this.listeners = {};
    this.closed = false;
    FakeEventSource.instances.push(this);
    queueMicrotask(() => { if (!this.closed) this.onopen?.(); });
  }
  addEventListener(type, fn) { (this.listeners[type] ??= []).push(fn); }
  message(event) { this.onmessage?.({data: JSON.stringify(event)}); }
  named(type, data) { for (const fn of this.listeners[type] ?? []) fn({data: JSON.stringify(data)}); }
  error() { this.onerror?.({}); }
  close() { this.closed = true; }
}

const tick = () => new Promise(resolve => setTimeout(resolve, 0));

function makeClient({store = createCursorStore(), snapshot = () => ({cursor: 0, events: []}), storage = mapStorage(), user = 'u'} = {}) {
  const calls = {ack: [], snapshot: 0};
  const fetchImpl = async (url, options = {}) => {
    if (url.endsWith('/api/state')) {
      calls.snapshot += 1;
      return {ok: true, json: async () => snapshot()};
    }
    if (url.endsWith('/api/cursor/ack')) {
      const body = JSON.parse(options.body);
      calls.ack.push(body);
      return {ok: true, json: async () => ({user: body.user, cursor: store.ack(body.user, body.cursor)})};
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const client = createRecoveryClient({
    user,
    fetchImpl,
    eventSourceImpl: url => new FakeEventSource(url),
    storage,
    reconnectDelay: () => 0,
    sleep: () => Promise.resolve(),
  });
  return {client, calls, store, storage};
}

async function openAndCatchUp(client, events, syncCursor) {
  await client.connect();
  await tick();
  const source = FakeEventSource.last();
  for (const event of events) source.message(event);
  source.named('sync', {cursor: syncCursor, firstAvailable: 1});
  return source;
}

test('connect replays to the sync marker: idle → connecting → catching-up → live', async () => {
  FakeEventSource.reset();
  const {client, store, storage} = makeClient();
  await openAndCatchUp(client, [
    {id: 1, type: 'note', data: {n: 1}},
    {id: 2, type: 'note', data: {n: 2}},
  ], 2);
  assert.equal(client.state, STATES.LIVE);
  assert.deepEqual(client.record.map(r => r.id), [1, 2]);
  assert.deepEqual(client.record.map(r => r.origin), ['replay', 'replay']);
  assert.deepEqual(client.transitions.map(t => t.to), [STATES.CONNECTING, STATES.CATCHING_UP, STATES.LIVE]);
  await client.lastAck;
  assert.equal(store.get('u'), 2, 'server-side ack boundary matches the client cursor');
  assert.equal(storage.map.get('sse-recovery:cursor:u'), '2', 'shared storage cursor matches');
  assert.equal(client.cursor, 2);

  FakeEventSource.last().message({id: 3, type: 'note', data: {n: 3}});
  assert.equal(client.record.at(-1).origin, 'live');
});

test('a duplicate frame is counted and skipped, never applied twice', async () => {
  FakeEventSource.reset();
  const {client} = makeClient();
  const source = await openAndCatchUp(client, [{id: 1, type: 'n', data: {}}, {id: 2, type: 'n', data: {}}], 2);
  source.message({id: 2, type: 'n', data: {}});
  source.message({id: 1, type: 'n', data: {}});
  assert.equal(client.duplicates, 2);
  assert.equal(client.cursor, 2);
  assert.deepEqual(client.record.filter(r => r.kind === 'event').map(r => r.id), [1, 2]);
  assert.deepEqual(client.record.filter(r => r.kind === 'duplicate').map(r => r.id), [2, 1]);
});

test('a forward gap triggers an observable full resync, then resumes live', async () => {
  FakeEventSource.reset();
  const snapshot = () => ({cursor: 5, firstAvailable: 3, events: [
    {id: 3, type: 'n', data: {n: 3}},
    {id: 4, type: 'n', data: {n: 4}},
    {id: 5, type: 'n', data: {n: 5}},
  ]});
  const {client, calls} = makeClient({snapshot});
  const source = await openAndCatchUp(client, [{id: 1, type: 'n', data: {}}, {id: 2, type: 'n', data: {}}], 2);

  source.message({id: 5, type: 'n', data: {n: 5}});
  assert.equal(client.gaps, 1);
  assert.equal(client.state, STATES.RESYNCING);
  assert.equal(source.closed, true, 'broken stream is closed');

  await waitFor(() => FakeEventSource.instances.length === 2);
  assert.equal(calls.snapshot, 1);
  assert.match(FakeEventSource.last().url, /cursor=5$/);
  assert.equal(client.record[0].kind, 'marker');
  assert.equal(client.record[0].marker, 'full-sync');
  assert.equal(client.record[0].reason, 'gap:2->5');
  assert.deepEqual(client.record.slice(1).map(r => r.id), [3, 4, 5], 'record replaced by the snapshot');
  assert.equal(client.cursor, 5);

  await tick();
  FakeEventSource.last().named('sync', {cursor: 5, firstAvailable: 3});
  assert.equal(client.state, STATES.LIVE);
  assert.ok(client.transitions.some(t => t.from === STATES.RESYNCING && t.to === STATES.CONNECTING));
});

test('a server resync-required frame drives the same full-sync flow', async () => {
  FakeEventSource.reset();
  const snapshot = () => ({cursor: 2, firstAvailable: 1, events: [{id: 1, type: 'n', data: {}}, {id: 2, type: 'n', data: {}}]});
  const {client} = makeClient({snapshot});
  const source = await openAndCatchUp(client, [], 0);
  source.named('resync-required', {reason: 'cursor-expired', cursor: 2, firstAvailable: 1});
  assert.equal(client.state, STATES.RESYNCING);
  await waitFor(() => FakeEventSource.instances.length === 2);
  assert.equal(client.record[0].reason, 'cursor-expired');
  assert.deepEqual(client.record.slice(1).map(r => r.id), [1, 2]);
});

test('a sync marker behind the client cursor means the server lost history: resync', async () => {
  FakeEventSource.reset();
  const snapshot = () => ({cursor: 3, firstAvailable: 1, events: [{id: 1, type: 'n', data: {}}, {id: 2, type: 'n', data: {}}, {id: 3, type: 'n', data: {}}]});
  const {client} = makeClient({snapshot});
  const source = await openAndCatchUp(client, [{id: 1, type: 'n', data: {}}, {id: 2, type: 'n', data: {}}, {id: 3, type: 'n', data: {}}], 3);
  assert.equal(client.state, STATES.LIVE);

  // Reconnect: the server restarted empty and cannot honour cursor 3.
  source.error();
  await waitFor(() => FakeEventSource.instances.length === 2);
  await tick();
  FakeEventSource.last().named('sync', {cursor: 0, firstAvailable: 0});
  assert.equal(client.state, STATES.RESYNCING);
  await waitFor(() => client.record[0]?.marker === 'full-sync');
  assert.equal(client.record[0].reason, 'server-cursor-behind');
});

test('a dropped stream reconnects with the last applied cursor, not the shared one', async () => {
  FakeEventSource.reset();
  const {client} = makeClient();
  const source = await openAndCatchUp(client, [{id: 1, type: 'n', data: {}}, {id: 2, type: 'n', data: {}}], 2);
  source.error();
  assert.equal(client.state, STATES.RECONNECTING);
  await waitFor(() => FakeEventSource.instances.length === 2);
  assert.match(FakeEventSource.last().url, /cursor=2$/);
  assert.ok(client.transitions.some(t => t.to === STATES.RECONNECTING && t.reason === 'stream-error'));
});

test('a fresh tab with a shared cursor full-syncs first instead of skipping history', async () => {
  FakeEventSource.reset();
  const storage = mapStorage(new Map([['sse-recovery:cursor:u', '7']]));
  const snapshot = () => ({cursor: 9, firstAvailable: 8, events: [{id: 8, type: 'n', data: {}}, {id: 9, type: 'n', data: {}}]});
  const {client, calls} = makeClient({storage, snapshot});
  await client.connect();
  await waitFor(() => FakeEventSource.instances.length === 1);
  assert.equal(calls.snapshot, 1, 'snapshot fetched before opening any stream');
  assert.equal(client.record[0].reason, 'shared-cursor-ahead');
  assert.match(FakeEventSource.last().url, /cursor=9$/);
  assert.deepEqual(client.record.slice(1).map(r => r.id), [8, 9]);
});

test('two tabs share the user cursor; a late ack never moves the boundary backwards', async () => {
  FakeEventSource.reset();
  const store = createCursorStore();       // server-side shared boundary
  const storage = mapStorage();            // localStorage, shared by both tabs
  const tabA = makeClient({store, storage, user: 'u'});
  const tabB = makeClient({store, storage, user: 'u'});

  // Tab A races ahead and confirms up to 5.
  const sourceA = await openAndCatchUp(tabA.client, [1, 2, 3, 4, 5].map(id => ({id, type: 'n', data: {}})), 5);
  await tabA.client.lastAck;
  assert.equal(store.get('u'), 5);

  // Tab B is behind: its acks arrive late and must not regress anything.
  const sourceB = await openAndCatchUp(tabB.client, [1, 2, 3].map(id => ({id, type: 'n', data: {}})), 5);
  await tabB.client.lastAck;
  assert.equal(store.get('u'), 5, 'late acks from the slow tab are ignored');
  assert.equal(storage.map.get('sse-recovery:cursor:u'), '5', 'shared storage stays at the high-water mark');
  assert.equal(tabB.client.cursor, 3, 'but the tab keeps its own position');
  assert.equal(tabB.client.sharedCursor, 5, 'and learns the authoritative boundary');

  // When the slow tab reconnects it resumes from its own cursor, not 5.
  // (tab A opened one stream; tab B snapshot-synced first, then opened one.)
  sourceB.error();
  await waitFor(() => FakeEventSource.instances.length === 3);
  assert.match(FakeEventSource.last().url, /cursor=3$/);
  assert.equal(tabA.client.cursor, 5, 'the other tab is untouched');
  assert.equal(sourceA.closed, false);
});

test('reconnectWithCursor re-requests events, which are applied once', async () => {
  FakeEventSource.reset();
  const {client} = makeClient();
  await openAndCatchUp(client, [1, 2, 3].map(id => ({id, type: 'n', data: {}})), 3);

  client.reconnectWithCursor(1);
  await waitFor(() => FakeEventSource.instances.length === 2);
  assert.match(FakeEventSource.last().url, /cursor=1$/);
  const source = FakeEventSource.last();
  await tick();
  source.message({id: 2, type: 'n', data: {}});
  source.message({id: 3, type: 'n', data: {}});
  source.named('sync', {cursor: 3, firstAvailable: 1});
  assert.equal(client.cursor, 3);
  source.message({id: 3, type: 'n', data: {}});
  assert.equal(client.duplicates, 1, 'an already-applied event is not applied twice');
  assert.deepEqual(client.record.filter(r => r.kind === 'event').map(r => r.id), [1, 2, 3, 2, 3]);
});

test('an intentional disconnect stops reconnecting and ignores late errors', async () => {
  FakeEventSource.reset();
  const {client} = makeClient();
  const source = await openAndCatchUp(client, [{id: 1, type: 'n', data: {}}], 1);
  client.disconnect();
  assert.equal(client.state, STATES.CLOSED);
  source.error();
  await tick();
  assert.equal(FakeEventSource.instances.length, 1, 'no reconnect after disconnect');
});

test('disconnecting mid-snapshot does not reopen the stream afterwards', async () => {
  FakeEventSource.reset();
  let releaseSnapshot;
  const snapshotReady = new Promise(resolve => { releaseSnapshot = resolve; });
  const {client} = makeClient({
    snapshot: async () => {
      await snapshotReady;
      return {cursor: 1, events: [{id: 1, type: 'n', data: {}}]};
    },
  });
  const source = await openAndCatchUp(client, [], 0);
  source.named('resync-required', {reason: 'cursor-expired'});
  assert.equal(client.state, STATES.RESYNCING);
  client.disconnect();
  releaseSnapshot();
  await tick();
  await tick();
  assert.equal(client.state, STATES.CLOSED);
  assert.equal(FakeEventSource.instances.length, 1, 'no new stream after disconnect');
});
