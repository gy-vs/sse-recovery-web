import test from 'node:test';
import assert from 'node:assert/strict';
import {after, canResume, createEventLog, publish} from '../src/event-log.mjs';
test('a reconnect receives events after its acknowledged cursor', () => { let log = publish(createEventLog(), 'one', {value: 1}); log = publish(log, 'two', {value: 2}); assert.deepEqual(after(log, 1).map(event => event.type), ['two']); });
test('an old cursor is rejected after the retention window moves', () => { let log = createEventLog(1); log = publish(log, 'one', {}); log = publish(log, 'two', {}); assert.equal(canResume(log, 0), false); });
