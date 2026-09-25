import test from 'node:test';
import assert from 'node:assert/strict';
import {createEventLog, publish} from '../src/event-log.mjs';
import {attachStream, createHub} from '../src/sse.mjs';

function fakeResponse() {
  return {
    frames: [],
    ended: false,
    destroyed: false,
    handlers: {},
    write(chunk) { this.frames.push(chunk); return true; },
    end() { this.ended = true; },
    destroy() { this.destroyed = true; this.handlers.close?.(); },
    on(event, fn) { this.handlers[event] = fn; },
  };
}

function logOf(count, limit = 100) {
  let log = createEventLog(limit);
  for (let i = 1; i <= count; i++) log = publish(log, 'note', {n: i});
  return log;
}

test('a resumable cursor replays exactly the missed events then a sync marker', () => {
  const res = fakeResponse();
  const hub = createHub();
  const result = attachStream({res, log: logOf(3), cursor: 1, hub});
  assert.equal(result.resumed, true);
  assert.equal(result.replayed, 2);
  assert.match(res.frames[0], /^id: 2\n/);
  assert.match(res.frames[0], /"n":2/);
  assert.match(res.frames[1], /^id: 3\n/);
  assert.match(res.frames[2], /^event: sync\ndata: \{"cursor":3,"firstAvailable":1\}/);
  assert.equal(res.ended, false, 'stream stays open for live events');
  assert.equal(hub.size, 1);
});

test('an expired cursor gets an explicit resync-required frame and the stream closes', () => {
  const res = fakeResponse();
  const hub = createHub();
  const result = attachStream({res, log: logOf(5, 2), cursor: 1, hub});
  assert.equal(result.resumed, false);
  assert.match(res.frames[0], /^event: resync-required\n/);
  assert.match(res.frames[0], /"reason":"cursor-expired"/);
  assert.match(res.frames[0], /"firstAvailable":4/);
  assert.equal(res.ended, true);
  assert.equal(hub.size, 0, 'expired connections are not subscribed');
});

test('a cursor ahead of the log (server lost history) is rejected explicitly', () => {
  const res = fakeResponse();
  const result = attachStream({res, log: logOf(2), cursor: 9, hub: createHub()});
  assert.equal(result.resumed, false);
  assert.match(res.frames[0], /"reason":"cursor-ahead-of-log"/);
  assert.equal(res.ended, true);
});

test('a cursor-less client replays the retained window then goes live', () => {
  const res = fakeResponse();
  const result = attachStream({res, log: logOf(2), cursor: undefined, hub: createHub()});
  assert.equal(result.resumed, true);
  assert.equal(result.replayed, 2);
});

test('events published after attach arrive live, in order, after the sync marker', () => {
  const res = fakeResponse();
  const hub = createHub();
  let log = logOf(2);
  attachStream({res, log, cursor: 0, hub});
  log = publish(log, 'note', {n: 3});
  hub.broadcast(log.events.at(-1));
  assert.equal(res.frames.length, 4, 'replay 1, replay 2, sync, live 3');
  assert.match(res.frames[2], /event: sync/);
  assert.match(res.frames[3], /^id: 3\n/);
});

test('a closed connection is unsubscribed and no longer receives broadcasts', () => {
  const res = fakeResponse();
  const hub = createHub();
  attachStream({res, log: logOf(0), cursor: 0, hub});
  assert.equal(hub.size, 1);
  res.destroy();
  assert.equal(hub.size, 0);
  hub.broadcast({id: 1, type: 'note', data: {}});
  assert.equal(res.frames.length, 1, 'only the sync frame was ever written');
});

test('dropAll severs every subscriber as if the network dropped', () => {
  const a = fakeResponse();
  const b = fakeResponse();
  const hub = createHub();
  attachStream({res: a, log: logOf(0), cursor: 0, hub});
  attachStream({res: b, log: logOf(0), cursor: 0, hub});
  hub.dropAll();
  assert.equal(hub.size, 0);
  assert.equal(a.destroyed, true);
  assert.equal(b.destroyed, true);
});
