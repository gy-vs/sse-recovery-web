import test from 'node:test';
import assert from 'node:assert/strict';
import {readFrames, startServer, waitFor} from '../test-helpers/sse-client.mjs';

async function post(base, path, body) {
  const response = await fetch(`${base}${path}`, {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify(body),
  });
  return {status: response.status, body: await response.json()};
}

async function collect(base, path, {headers = {}, frames: wanted}) {
  const controller = new AbortController();
  const frames = [];
  try {
    const response = await fetch(`${base}${path}`, {signal: controller.signal, headers});
    for await (const frame of readFrames(response.body)) {
      frames.push(frame);
      if (frames.length >= wanted) break;
    }
  } catch (error) {
    if (error.name !== 'AbortError') throw error;
  } finally {
    controller.abort();
  }
  return frames;
}

test('legacy JSON polling keeps working, including the 409 on expired cursors', async () => {
  const server = await startServer({limit: 2});
  try {
    await post(server.base, '/api/publish', {type: 'set', data: {key: 'a', value: 1}});
    await post(server.base, '/api/publish', {type: 'set', data: {key: 'b', value: 2}});

    const all = await (await fetch(`${server.base}/api/events?cursor=0`)).json();
    assert.deepEqual(all.events.map(e => e.id), [1, 2]);
    assert.equal(all.cursor, 2);

    const tail = await (await fetch(`${server.base}/api/events?cursor=1`)).json();
    assert.deepEqual(tail.events.map(e => e.id), [2]);

    await post(server.base, '/api/publish', {type: 'set', data: {key: 'c', value: 3}});
    const expired = await fetch(`${server.base}/api/events?cursor=0`);
    assert.equal(expired.status, 409);
    assert.deepEqual(await expired.json(), {error: 'cursor expired'});

    const status = await (await fetch(`${server.base}/`)).json();
    assert.equal(status.service, 'sse-recovery');
    assert.equal(status.cursor, 3);
  } finally {
    await server.close();
  }
});

test('a cursor-less stream is live-only and does not replay history', async () => {
  const server = await startServer();
  try {
    await post(server.base, '/api/publish', {type: 'set', data: {key: 'a', value: 1}});
    await post(server.base, '/api/publish', {type: 'set', data: {key: 'b', value: 2}});

    const controller = new AbortController();
    const frames = [];
    const pump = (async () => {
      const response = await fetch(`${server.base}/api/stream`, {signal: controller.signal});
      for await (const frame of readFrames(response.body)) frames.push(frame);
    })().catch(error => assert.equal(error.name, 'AbortError'));

    await waitFor(() => frames.some(f => f.event === 'hello'));
    assert.deepEqual(JSON.parse(frames[0].data), {mode: 'live', cursor: 2});
    assert.equal(frames.filter(f => f.event === 'change').length, 0);

    await post(server.base, '/api/publish', {type: 'set', data: {key: 'c', value: 3}});
    await waitFor(() => frames.some(f => f.event === 'change'));
    const change = frames.find(f => f.event === 'change');
    assert.equal(change.id, '3');
    assert.deepEqual(JSON.parse(change.data).data, {key: 'c', value: 3});

    controller.abort();
    await pump;
  } finally {
    await server.close();
  }
});

test('a stream with a cursor replays missed events, announces caught-up, then goes live', async () => {
  const server = await startServer();
  try {
    for (let i = 1; i <= 3; i += 1) {
      await post(server.base, '/api/publish', {type: 'set', data: {key: `k${i}`, value: i}});
    }
    const controller = new AbortController();
    const frames = [];
    const pump = (async () => {
      const response = await fetch(`${server.base}/api/stream?cursor=1`, {signal: controller.signal});
      for await (const frame of readFrames(response.body)) frames.push(frame);
    })().catch(error => assert.equal(error.name, 'AbortError'));

    await waitFor(() => frames.some(f => f.event === 'caught-up'));
    assert.deepEqual(JSON.parse(frames[0].data), {mode: 'replay', from: 1, to: 3});
    const replayed = frames.filter(f => f.event === 'change').map(f => Number(f.id));
    assert.deepEqual(replayed, [2, 3]);
    assert.deepEqual(JSON.parse(frames.at(-1).data), {cursor: 3});

    await post(server.base, '/api/publish', {type: 'set', data: {key: 'k4', value: 4}});
    await waitFor(() => frames.filter(f => f.event === 'change').length === 3);
    assert.equal(frames.filter(f => f.event === 'change').at(-1).id, '4');

    controller.abort();
    await pump;
  } finally {
    await server.close();
  }
});

test('the Last-Event-ID header is honoured on reconnect', async () => {
  const server = await startServer();
  try {
    for (let i = 1; i <= 4; i += 1) {
      await post(server.base, '/api/publish', {type: 'set', data: {key: `k${i}`, value: i}});
    }
    const frames = await collect(server.base, '/api/stream', {
      headers: {'last-event-id': '2'},
      frames: 4, // hello + 2 changes + caught-up
    });
    assert.deepEqual(JSON.parse(frames[0].data), {mode: 'replay', from: 2, to: 4});
    assert.deepEqual(frames.filter(f => f.event === 'change').map(f => f.id), ['3', '4']);
  } finally {
    await server.close();
  }
});

test('an expired cursor is told explicitly to resync instead of hanging', async () => {
  const server = await startServer({limit: 2});
  try {
    for (let i = 1; i <= 4; i += 1) {
      await post(server.base, '/api/publish', {type: 'set', data: {key: `k${i}`, value: i}});
    }
    const response = await fetch(`${server.base}/api/stream?cursor=1`);
    const frames = [];
    for await (const frame of readFrames(response.body)) frames.push(frame);
    assert.equal(frames.length, 1);
    assert.equal(frames[0].event, 'resync-required');
    assert.deepEqual(JSON.parse(frames[0].data), {
      reason: 'cursor-expired', cursor: 1, firstAvailable: 3, tip: 4,
    });
  } finally {
    await server.close();
  }
});

test('the state snapshot carries the cursor it is valid at', async () => {
  const server = await startServer();
  try {
    await post(server.base, '/api/publish', {type: 'set', data: {key: 'a', value: 1}});
    await post(server.base, '/api/publish', {type: 'set', data: {key: 'b', value: 2}});
    await post(server.base, '/api/publish', {type: 'delete', data: {key: 'a'}});
    const snapshot = await (await fetch(`${server.base}/api/state`)).json();
    assert.deepEqual(snapshot, {state: {b: 2}, cursor: 3});
  } finally {
    await server.close();
  }
});

test('acks over HTTP are monotonic per user and observable', async () => {
  const server = await startServer();
  try {
    assert.deepEqual((await post(server.base, '/api/ack', {user: 'u', cursor: 5})).body,
      {user: 'u', cursor: 5, advanced: true});
    assert.deepEqual((await post(server.base, '/api/ack', {user: 'u', cursor: 2})).body,
      {user: 'u', cursor: 5, advanced: false});
    assert.deepEqual((await post(server.base, '/api/ack', {user: 'u', cursor: 8})).body,
      {user: 'u', cursor: 8, advanced: true});
    const acks = await (await fetch(`${server.base}/api/acks`)).json();
    assert.deepEqual(acks, {acks: {u: 8}});
    const bad = await post(server.base, '/api/ack', {user: 'u', cursor: 'nope'});
    assert.equal(bad.status, 400);
  } finally {
    await server.close();
  }
});

test('closing a stream removes its subscription', async () => {
  const server = await startServer();
  try {
    const controller = new AbortController();
    const done = (async () => {
      const response = await fetch(`${server.base}/api/stream`, {signal: controller.signal});
      for await (const _ of readFrames(response.body)) { /* discard */ }
    })().catch(error => assert.equal(error.name, 'AbortError'));

    await waitFor(() => server.hub.connections.size === 1);
    controller.abort();
    await done;
    await waitFor(() => server.hub.connections.size === 0);
  } finally {
    await server.close();
  }
});

test('events published while a client reconnects arrive exactly once, in order', async () => {
  const server = await startServer();
  try {
    for (let i = 1; i <= 5; i += 1) {
      await post(server.base, '/api/publish', {type: 'set', data: {key: `k${i}`, value: i}});
    }
    const controller = new AbortController();
    const changes = [];
    const pump = (async () => {
      const response = await fetch(`${server.base}/api/stream?cursor=0`, {signal: controller.signal});
      for await (const frame of readFrames(response.body)) {
        if (frame.event === 'change') changes.push(Number(frame.id));
      }
    })().catch(error => assert.equal(error.name, 'AbortError'));

    // Publish more events while the reconnecting client is being served.
    for (let i = 6; i <= 10; i += 1) {
      await post(server.base, '/api/publish', {type: 'set', data: {key: `k${i}`, value: i}});
    }
    await waitFor(() => changes.length === 10);
    assert.deepEqual(changes, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    controller.abort();
    await pump;
  } finally {
    await server.close();
  }
});
