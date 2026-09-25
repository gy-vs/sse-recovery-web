import test from 'node:test';
import assert from 'node:assert/strict';
import {
  after, canResume, createEventLog, firstRetained, publish, reduceEvent, tip,
} from '../src/event-log.mjs';

test('a reconnect receives events after its acknowledged cursor', () => {
  let log = publish(createEventLog(), 'one', {value: 1});
  log = publish(log, 'two', {value: 2});
  assert.deepEqual(after(log, 1).map(event => event.type), ['two']);
});

test('an old cursor is rejected after the retention window moves', () => {
  let log = createEventLog(1);
  log = publish(log, 'one', {});
  log = publish(log, 'two', {});
  assert.equal(canResume(log, 0), false);
});

test('the cursor exactly at the retention boundary still resumes', () => {
  let log = createEventLog(2);
  for (const type of ['a', 'b', 'c']) log = publish(log, type, {});
  // Retained: b(2), c(3). Cursor 1 is the id just before the window.
  assert.equal(firstRetained(log), 2);
  assert.equal(canResume(log, 1), true);
  assert.equal(canResume(log, 0), false);
  assert.deepEqual(after(log, 1).map(event => event.id), [2, 3]);
});

test('an empty log resumes any cursor and reports tip zero', () => {
  const log = createEventLog();
  assert.equal(canResume(log, 42), true);
  assert.equal(tip(log), 0);
  assert.equal(firstRetained(log), null);
  assert.deepEqual(after(log, 42), []);
});

test('ids keep increasing across the retention boundary', () => {
  let log = createEventLog(2);
  for (let i = 0; i < 5; i += 1) log = publish(log, 'set', {key: 'k', value: i});
  assert.deepEqual(log.events.map(event => event.id), [4, 5]);
  assert.equal(tip(log), 5);
});

test('reduceEvent folds set and delete, ignoring unknown types', () => {
  let state = {};
  state = reduceEvent(state, {id: 1, type: 'set', data: {key: 'a', value: 1}});
  state = reduceEvent(state, {id: 2, type: 'set', data: {key: 'b', value: 2}});
  state = reduceEvent(state, {id: 3, type: 'delete', data: {key: 'a'}});
  state = reduceEvent(state, {id: 4, type: 'other', data: {key: 'b', value: 99}});
  assert.deepEqual(state, {b: 2});
});
