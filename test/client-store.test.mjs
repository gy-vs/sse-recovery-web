import test from 'node:test';
import assert from 'node:assert/strict';
import {
  STATUS, applySnapshot, createClientStore, noteAck,
  onCaughtUp, onChange, onDisconnected, onHello, onResyncRequired,
  reduceEvent as clientReduce,
} from '../public/client-store.mjs';
import {reduceEvent as serverReduce} from '../src/event-log.mjs';

const set = (id, key, value) => ({id, type: 'set', data: {key, value}});

test('a replay hello moves the client to catching-up with an observable step', () => {
  const store = createClientStore();
  onHello(store, {mode: 'replay', from: 2, to: 5});
  assert.equal(store.status, STATUS.CATCHING_UP);
  assert.deepEqual(store.replay, {from: 2, to: 5, remaining: 3});
  assert.equal(store.records.at(-1).step, 'catch-up-begin');
});

test('a live hello adopts the server tip without replaying history', () => {
  const store = createClientStore();
  onHello(store, {mode: 'live', cursor: 7});
  assert.equal(store.status, STATUS.LIVE);
  assert.equal(store.cursor, 7);
  // The next live event applies cleanly instead of looking like a gap.
  assert.equal(onChange(store, set(8, 'a', 1)), 8);
  assert.deepEqual(store.state, {a: 1});
});

test('a cursor ahead of the server tip is an ordering error, not a silent reset', () => {
  const store = createClientStore();
  store.cursor = 9;
  onHello(store, {mode: 'replay', from: 9, to: 5});
  assert.equal(store.status, STATUS.RESYNC_REQUIRED);
  assert.equal(store.records.at(-1).step, 'clock-error');
});

test('events apply in order and each applied event is acknowledged', () => {
  const store = createClientStore();
  onHello(store, {mode: 'replay', from: 0, to: 2});
  assert.equal(onChange(store, set(1, 'a', 1)), 1);
  assert.equal(onChange(store, set(2, 'b', 2)), 2);
  onCaughtUp(store, {cursor: 2});
  assert.equal(store.status, STATUS.LIVE);
  assert.deepEqual(store.state, {a: 1, b: 2});
  assert.equal(store.cursor, 2);
});

test('a duplicate event is skipped, counted and never acknowledged twice', () => {
  const store = createClientStore();
  onHello(store, {mode: 'live', cursor: 0});
  assert.equal(onChange(store, set(1, 'a', 1)), 1);
  assert.equal(onChange(store, set(1, 'a', 1)), null);
  assert.equal(store.counters.duplicates, 1);
  assert.equal(store.counters.applied, 1);
  assert.deepEqual(store.state, {a: 1});
  assert.equal(store.records.at(-1).step, 'duplicate-skipped');
});

test('a gap in the stream forces an explicit resync instead of applying past it', () => {
  const store = createClientStore();
  onHello(store, {mode: 'live', cursor: 0});
  onChange(store, set(1, 'a', 1));
  assert.equal(onChange(store, set(3, 'b', 2)), null);
  assert.equal(store.status, STATUS.RESYNC_REQUIRED);
  assert.equal(store.counters.gaps, 1);
  assert.deepEqual(store.state, {a: 1}); // event 3 was not applied
  assert.equal(store.records.at(-1).step, 'gap-detected');
});

test('a replay that misses frames fails the caught-up check', () => {
  const store = createClientStore();
  onHello(store, {mode: 'replay', from: 0, to: 3});
  onChange(store, set(1, 'a', 1));
  onChange(store, set(2, 'b', 2));
  // Event 3 never arrives but the server announces caught-up at 3.
  onCaughtUp(store, {cursor: 3});
  assert.equal(store.status, STATUS.RESYNC_REQUIRED);
  assert.equal(store.records.at(-1).step, 'catch-up-mismatch');
});

test('a replay of already-applied events dedupes and still lands exactly on the tip', () => {
  const store = createClientStore();
  onHello(store, {mode: 'live', cursor: 0});
  for (const event of [set(1, 'a', 1), set(2, 'b', 2), set(3, 'c', 3)]) onChange(store, event);
  // Reconnect with a stale cursor: the server replays 2..5, we had 1..3.
  onDisconnected(store);
  onHello(store, {mode: 'replay', from: 1, to: 5});
  assert.equal(onChange(store, set(2, 'b', 2)), null);
  assert.equal(onChange(store, set(3, 'c', 3)), null);
  assert.equal(onChange(store, set(4, 'd', 4)), 4);
  assert.equal(onChange(store, set(5, 'e', 5)), 5);
  onCaughtUp(store, {cursor: 5});
  assert.equal(store.status, STATUS.LIVE);
  assert.equal(store.counters.duplicates, 2);
  assert.deepEqual(store.state, {a: 1, b: 2, c: 3, d: 4, e: 5});
});

test('full sync replaces state at the snapshot cursor as a recorded step', () => {
  const store = createClientStore();
  onHello(store, {mode: 'live', cursor: 0});
  onChange(store, set(1, 'stale', true));
  onResyncRequired(store, {reason: 'cursor-expired', tip: 8});
  assert.equal(store.status, STATUS.RESYNC_REQUIRED);
  applySnapshot(store, {state: {fresh: 1}, cursor: 8});
  assert.equal(store.status, STATUS.CONNECTING);
  assert.equal(store.cursor, 8);
  assert.deepEqual(store.state, {fresh: 1});
  assert.deepEqual(store.records.map(r => r.step).slice(-2), ['resync-required', 'full-sync']);
  // The stream resumes cleanly from the snapshot cursor.
  onHello(store, {mode: 'replay', from: 8, to: 8});
  onCaughtUp(store, {cursor: 8});
  assert.equal(onChange(store, set(9, 'next', 1)), 9);
  assert.deepEqual(store.state, {fresh: 1, next: 1});
});

test('local ack tracking is monotonic like the server registry', () => {
  const store = createClientStore();
  assert.equal(noteAck(store, 4), 4);
  assert.equal(noteAck(store, 2), 4);
  assert.equal(noteAck(store, 6), 6);
});

test('client and server folds agree on a mixed op sequence', () => {
  const ops = [
    set(1, 'a', 1), set(2, 'b', 2), {id: 3, type: 'delete', data: {key: 'a'}},
    set(4, 'a', 9), {id: 5, type: 'mystery', data: {key: 'b', value: 0}},
    {id: 6, type: 'delete', data: {key: 'missing'}},
  ];
  let clientState = {};
  let serverState = {};
  for (const event of ops) {
    clientState = clientReduce(clientState, event);
    serverState = serverReduce(serverState, event);
    assert.deepEqual(clientState, serverState);
  }
});
