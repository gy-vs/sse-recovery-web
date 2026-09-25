import test from 'node:test';
import assert from 'node:assert/strict';
import {TestClient, startServer, waitFor} from '../test-helpers/sse-client.mjs';

async function publish(base, type, data) {
  const response = await fetch(`${base}/api/publish`, {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify({type, data}),
  });
  assert.equal(response.status, 201);
}

const serverState = async base => (await (await fetch(`${base}/api/state`)).json()).state;
const serverAck = async (base, user) =>
  (await (await fetch(`${base}/api/acks`)).json()).acks[user] ?? 0;
const steps = client => client.store.records.map(r => r.step);

test('disconnect mid-stream, publish, reconnect with an old cursor: nothing lost, nothing twice', async () => {
  const server = await startServer({limit: 100});
  const client = new TestClient(server.base, 'demo-user');
  try {
    // Phase 1: connect and apply five events live.
    client.connect(0);
    await waitFor(() => client.store.status === 'live');
    for (let i = 1; i <= 5; i += 1) await publish(server.base, 'set', {key: `k${i}`, value: i});
    await waitFor(() => client.store.cursor === 5);
    assert.deepEqual(client.store.state, {k1: 1, k2: 2, k3: 3, k4: 4, k5: 5});
    // Ack boundary: the server confirms exactly what the client applied.
    await waitFor(async () => (await serverAck(server.base, 'demo-user')) === 5);
    assert.equal(client.store.acked, 5);

    // Phase 2: the connection drops; three events are published meanwhile.
    client.disconnect();
    assert.equal(client.store.status, 'disconnected');
    for (let i = 6; i <= 8; i += 1) await publish(server.base, 'set', {key: `k${i}`, value: i});
    assert.equal(client.store.cursor, 5); // the tab missed 6..8

    // Phase 3: reconnect with a stale cursor (older than what we applied).
    client.connect(3);
    await waitFor(() => client.store.status === 'live' && client.store.cursor === 8);

    // Events 4..5 were replayed but already applied: deduped, not applied twice.
    assert.equal(client.store.counters.duplicates, 2);
    assert.equal(client.store.counters.applied, 8);
    assert.equal(client.store.counters.gaps, 0);
    assert.deepEqual(client.store.state, await serverState(server.base));

    // Ack boundary stays consistent through the recovery.
    await waitFor(async () => (await serverAck(server.base, 'demo-user')) === 8);
    assert.equal(client.store.acked, 8);

    // Every recovery step is observable in the record.
    assert.deepEqual(steps(client).filter(s => s !== 'applied'), [
      'catch-up-begin', 'caught-up', // initial connect
      'disconnected',
      'catch-up-begin', 'duplicate-skipped', 'duplicate-skipped', 'caught-up',
    ]);
  } finally {
    client.disconnect();
    await server.close();
  }
});

test('a cursor that fell out of the retention window triggers an explicit full sync', async () => {
  const server = await startServer({limit: 3});
  const client = new TestClient(server.base, 'demo-user');
  try {
    for (let i = 1; i <= 5; i += 1) await publish(server.base, 'set', {key: `k${i}`, value: i});

    // Cursor 0 is older than the retained window (events 3..5): the server
    // cannot replay, so the client must visibly pass through resync-required.
    client.connect(0);
    await waitFor(() => client.store.status === 'live');

    assert.deepEqual(steps(client).filter(s => s !== 'applied'), [
      'resync-required', 'full-sync', 'catch-up-begin', 'caught-up',
    ]);
    assert.equal(client.store.cursor, 5);
    assert.deepEqual(client.store.state, await serverState(server.base));

    // After the full sync the stream is live again.
    await publish(server.base, 'set', {key: 'k6', value: 6});
    await waitFor(() => client.store.cursor === 6);
    assert.deepEqual(client.store.state, await serverState(server.base));
  } finally {
    client.disconnect();
    await server.close();
  }
});

test('a late ack from one tab never repositions another tab sharing the user cursor', async () => {
  const server = await startServer({limit: 100});
  const tabA = new TestClient(server.base, 'shared-user');
  const tabB = new TestClient(server.base, 'shared-user');
  try {
    tabA.connect(0);
    tabB.connect(0);
    await waitFor(() => tabA.store.status === 'live' && tabB.store.status === 'live');
    for (let i = 1; i <= 5; i += 1) await publish(server.base, 'set', {key: `k${i}`, value: i});
    await waitFor(() => tabA.store.cursor === 5 && tabB.store.cursor === 5);
    await waitFor(async () => (await serverAck(server.base, 'shared-user')) === 5);

    // Tab B loses its connection; tab A keeps going and confirms up to 10.
    tabB.disconnect();
    for (let i = 6; i <= 10; i += 1) await publish(server.base, 'set', {key: `k${i}`, value: i});
    await waitFor(() => tabA.store.cursor === 10);
    await waitFor(async () => (await serverAck(server.base, 'shared-user')) === 10);

    // Tab B reconnects with its own cursor (5), not the shared ack cursor.
    tabB.connect(5);
    await waitFor(() => tabB.store.status === 'live' && tabB.store.cursor === 10);

    // Tab B received its own missing events 6..10 — the shared ack cursor
    // at 10 did not push it past them.
    assert.deepEqual(tabB.store.state, await serverState(server.base));
    assert.equal(tabB.store.counters.gaps, 0);

    // Tab B's late acks are all stale: the shared cursor never regresses.
    assert.equal(tabB.lastAck.advanced, false);
    assert.equal(await serverAck(server.base, 'shared-user'), 10);
  } finally {
    tabA.disconnect();
    tabB.disconnect();
    await server.close();
  }
});

test('events published during replay are neither skipped nor applied twice', async () => {
  const server = await startServer({limit: 100});
  const client = new TestClient(server.base, 'demo-user');
  try {
    for (let i = 1; i <= 3; i += 1) await publish(server.base, 'set', {key: `k${i}`, value: i});
    client.connect(0);
    // Publish while the client is still catching up on the backlog.
    for (let i = 4; i <= 6; i += 1) await publish(server.base, 'set', {key: `k${i}`, value: i});
    await waitFor(() => client.store.status === 'live' && client.store.cursor === 6);

    assert.equal(client.store.counters.applied, 6);
    assert.equal(client.store.counters.duplicates, 0);
    assert.equal(client.store.counters.gaps, 0);
    assert.deepEqual(
      client.store.records.filter(r => r.step === 'applied').map(r => r.id),
      [1, 2, 3, 4, 5, 6],
    );
    assert.deepEqual(client.store.state, await serverState(server.base));
  } finally {
    client.disconnect();
    await server.close();
  }
});
