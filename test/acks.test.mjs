import test from 'node:test';
import assert from 'node:assert/strict';
import {acknowledge, ackedCursor, ackSnapshot, createAckRegistry} from '../src/acks.mjs';

test('the acknowledged cursor only moves forward', () => {
  const registry = createAckRegistry();
  assert.deepEqual(acknowledge(registry, 'user', 5), {cursor: 5, advanced: true});
  assert.deepEqual(acknowledge(registry, 'user', 3), {cursor: 5, advanced: false});
  assert.deepEqual(acknowledge(registry, 'user', 5), {cursor: 5, advanced: false});
  assert.deepEqual(acknowledge(registry, 'user', 9), {cursor: 9, advanced: true});
  assert.equal(ackedCursor(registry, 'user'), 9);
});

test('a late ack from one tab cannot move the shared cursor of another tab', () => {
  const registry = createAckRegistry();
  // Two tabs share the user "shared". Tab A races ahead and confirms 10.
  acknowledge(registry, 'shared', 10);
  // Tab B was behind; its late confirmation of 4 must not regress the cursor.
  const late = acknowledge(registry, 'shared', 4);
  assert.deepEqual(late, {cursor: 10, advanced: false});
  assert.equal(ackedCursor(registry, 'shared'), 10);
});

test('ack cursors are tracked per user', () => {
  const registry = createAckRegistry();
  acknowledge(registry, 'alice', 7);
  acknowledge(registry, 'bob', 3);
  assert.equal(ackedCursor(registry, 'alice'), 7);
  assert.equal(ackedCursor(registry, 'bob'), 3);
  assert.equal(ackedCursor(registry, 'carol'), 0);
  assert.deepEqual(ackSnapshot(registry), {alice: 7, bob: 3});
});

test('invalid cursors are rejected', () => {
  const registry = createAckRegistry();
  assert.throws(() => acknowledge(registry, 'user', 'abc'), TypeError);
  assert.throws(() => acknowledge(registry, 'user', -1), TypeError);
});
