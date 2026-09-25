// Client-side recovery state machine. Pure logic, no DOM: app.js wires it
// to EventSource and the page, tests drive it directly against the server.
//
// Statuses: connecting -> catching-up -> live, with resync-required as the
// explicit "must full-sync" state and disconnected for a dropped stream.
// Every transition appends a record so recovery steps stay observable.

export const STATUS = {
  CONNECTING: 'connecting',
  CATCHING_UP: 'catching-up',
  LIVE: 'live',
  RESYNC_REQUIRED: 'resync-required',
  DISCONNECTED: 'disconnected',
};

// Client-side fold, kept consistent with the server's reduceEvent
// (test/fold consistency is covered by the test suite).
export function reduceEvent(state, event) {
  const next = {...state};
  if (event.type === 'set') next[event.data.key] = event.data.value;
  else if (event.type === 'delete') delete next[event.data.key];
  return next;
}

export function createClientStore() {
  return {
    status: STATUS.CONNECTING,
    cursor: 0,          // last applied event id; sent as the reconnect cursor
    acked: 0,           // highest id this client has confirmed to the server
    state: {},
    records: [],        // observable recovery steps + applied events
    counters: {applied: 0, duplicates: 0, gaps: 0},
    replay: null,       // {from, to, remaining} while catching up
  };
}

function record(store, step, detail = {}) {
  store.records.push({step, ...detail});
}

function transition(store, status, step, detail) {
  store.status = status;
  record(store, step, detail);
}

// Server greeting. `mode: replay` means missed events follow; `mode: live`
// means the stream starts at the tip (legacy no-cursor connect).
export function onHello(store, hello) {
  const tip = hello.mode === 'replay' ? hello.to : hello.cursor;
  if (store.cursor > tip) {
    // Our cursor is ahead of the server tip: ordering is broken, resync.
    return transition(store, STATUS.RESYNC_REQUIRED, 'clock-error', {
      cursor: store.cursor, tip,
    });
  }
  if (hello.mode === 'replay') {
    store.replay = {from: hello.from, to: hello.to, remaining: hello.to - hello.from};
    return transition(store, STATUS.CATCHING_UP, 'catch-up-begin', {
      from: hello.from, to: hello.to,
    });
  }
  // Live-only connect: history before the tip is deliberately skipped.
  store.cursor = hello.cursor;
  store.replay = null;
  return transition(store, STATUS.LIVE, 'connected-live', {cursor: hello.cursor});
}

// One streamed event. Returns the id to acknowledge, or null when the
// event was skipped (duplicate) or rejected (gap).
export function onChange(store, event) {
  if (store.replay) store.replay.remaining -= 1;
  if (event.id <= store.cursor) {
    store.counters.duplicates += 1;
    record(store, 'duplicate-skipped', {id: event.id, cursor: store.cursor});
    return null;
  }
  if (event.id > store.cursor + 1) {
    store.counters.gaps += 1;
    transition(store, STATUS.RESYNC_REQUIRED, 'gap-detected', {
      expected: store.cursor + 1, got: event.id,
    });
    return null;
  }
  store.state = reduceEvent(store.state, event);
  store.cursor = event.id;
  store.counters.applied += 1;
  record(store, 'applied', {id: event.id, type: event.type, data: event.data});
  return event.id;
}

// Server finished replaying. Every announced replay frame must have
// arrived and advanced us exactly to the announced tip; anything else
// means events were lost on the way.
export function onCaughtUp(store, info) {
  const replay = store.replay;
  store.replay = null;
  const incomplete = replay !== null && replay.remaining !== 0;
  const expected = replay?.to ?? info.cursor;
  if (incomplete || store.cursor !== expected || store.cursor !== info.cursor) {
    store.counters.gaps += 1;
    return transition(store, STATUS.RESYNC_REQUIRED, 'catch-up-mismatch', {
      cursor: store.cursor, expected, announced: info.cursor,
    });
  }
  return transition(store, STATUS.LIVE, 'caught-up', {cursor: info.cursor});
}

// Server told us the cursor fell out of the retention window.
export function onResyncRequired(store, info) {
  store.replay = null;
  return transition(store, STATUS.RESYNC_REQUIRED, 'resync-required', {...info});
}

// Full sync: replace local state with the server snapshot. This is an
// explicit, recorded transition — never a silent background reset.
export function applySnapshot(store, snapshot) {
  store.state = structuredClone(snapshot.state);
  store.cursor = snapshot.cursor;
  store.replay = null;
  return transition(store, STATUS.CONNECTING, 'full-sync', {
    cursor: snapshot.cursor, keys: Object.keys(snapshot.state).length,
  });
}

export function onDisconnected(store) {
  store.replay = null;
  return transition(store, STATUS.DISCONNECTED, 'disconnected', {cursor: store.cursor});
}

// Local ack tracking is monotonic, mirroring the server-side registry.
export function noteAck(store, cursor) {
  if (cursor > store.acked) store.acked = cursor;
  return store.acked;
}
