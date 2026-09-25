import test from 'node:test';
import assert from 'node:assert/strict';
import {createEventLog, publish} from '../src/event-log.mjs';
import {broadcast, createHub, subscribe, unsubscribe} from '../src/hub.mjs';

function logOf(count, limit = 100) {
  let log = createEventLog(limit);
  for (let i = 1; i <= count; i += 1) log = publish(log, 'set', {key: `k${i}`, value: i});
  return log;
}

test('a subscriber replays missed events in order, then receives live ones', () => {
  const hub = createHub();
  const sent = [];
  const conn = subscribe(hub, logOf(3), 1, event => sent.push(event.id));
  assert.deepEqual(sent, [2, 3]);
  broadcast(hub, {id: 4, type: 'set', data: {}});
  assert.deepEqual(sent, [2, 3, 4]);
  assert.equal(conn.live, true);
});

test('an event broadcast during replay is buffered and flushed once, in order', () => {
  const hub = createHub();
  const log = logOf(3);
  const sent = [];
  subscribe(hub, log, 0, event => {
    sent.push(event.id);
    if (event.id === 1) {
      // A publish lands while this connection is still replaying.
      broadcast(hub, {id: 4, type: 'set', data: {}});
    }
  });
  assert.deepEqual(sent, [1, 2, 3, 4]);
});

test('a cursor outside the retention window cannot subscribe', () => {
  const hub = createHub();
  const conn = subscribe(hub, logOf(5, 2), 0, () => {});
  assert.equal(conn, null);
  assert.equal(hub.connections.size, 0);
});

test('an unsubscribed connection receives nothing', () => {
  const hub = createHub();
  const sent = [];
  const conn = subscribe(hub, logOf(1), 1, event => sent.push(event.id));
  unsubscribe(hub, conn);
  broadcast(hub, {id: 2, type: 'set', data: {}});
  assert.deepEqual(sent, []);
  assert.equal(hub.connections.size, 0);
});
