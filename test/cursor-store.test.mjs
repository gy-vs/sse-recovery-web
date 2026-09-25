import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createCursorStore, createFileCursorPersistence} from '../src/cursor-store.mjs';

test('acks only move the boundary forward; a late ack cannot regress it', () => {
  const store = createCursorStore();
  assert.equal(store.ack('u', 5), 5);
  assert.equal(store.ack('u', 3), 5, 'late ack from a slow tab is ignored');
  assert.equal(store.ack('u', 9), 9);
  assert.equal(store.ack('u', 9), 9);
  assert.equal(store.get('u'), 9);
});

test('cursors are tracked per user', () => {
  const store = createCursorStore();
  store.ack('a', 7);
  store.ack('b', 2);
  assert.equal(store.get('a'), 7);
  assert.equal(store.get('b'), 2);
  assert.equal(store.get('nobody'), 0);
});

test('invalid acks are ignored and report the current boundary', () => {
  const store = createCursorStore();
  store.ack('u', 4);
  assert.equal(store.ack('u', 'junk'), 4);
  assert.equal(store.ack('u', -1), 4);
  assert.equal(store.ack('u', undefined), 4);
});

test('the ack boundary survives a restart through file persistence', () => {
  const file = join(mkdtempSync(join(tmpdir(), 'cursors-')), 'cursors.json');
  const first = createCursorStore(createFileCursorPersistence(file));
  first.ack('u', 7);
  assert.deepEqual(JSON.parse(readFileSync(file, 'utf8')), {u: 7});

  const second = createCursorStore(createFileCursorPersistence(file));
  assert.equal(second.get('u'), 7, 'reloaded after restart');
  assert.equal(second.ack('u', 4), 7, 'a pre-restart late ack still cannot regress');
  assert.equal(second.ack('u', 8), 8);
});

test('a missing or corrupt persistence file starts empty', () => {
  const file = join(mkdtempSync(join(tmpdir(), 'cursors-')), 'nope', 'cursors.json');
  const store = createCursorStore(createFileCursorPersistence(file));
  assert.equal(store.get('u'), 0);
});
