// Recovery client for the SSE stream. DOM-free: the page wires it to real
// EventSource/fetch/localStorage, tests inject fakes. Every transition and
// every applied/duplicate/gap event is observable through onChange.

export const STATES = Object.freeze({
  IDLE: 'idle',
  CONNECTING: 'connecting',
  CATCHING_UP: 'catching-up',
  LIVE: 'live',
  RESYNCING: 'resyncing',
  RECONNECTING: 'reconnecting',
  CLOSED: 'closed',
});

export function createRecoveryClient(options = {}) {
  const {
    user = 'default',
    fetchImpl = (...args) => fetch(...args),
    eventSourceImpl = url => new EventSource(url),
    storage = null, // shared across tabs: {get(key), set(key, value)}
    streamUrl = '/api/events/stream',
    snapshotUrl = '/api/state',
    ackUrl = '/api/cursor/ack',
    reconnectDelay = attempt => Math.min(500 * 2 ** attempt, 8000),
    sleep = ms => new Promise(resolve => setTimeout(resolve, ms)),
    onChange = () => {},
  } = options;

  const storageKey = `sse-recovery:cursor:${user}`;
  const appliedIds = new Set();

  const client = {
    user,
    state: STATES.IDLE,
    cursor: 0,        // this tab's contiguous high-water mark
    sharedCursor: 0,  // server-authoritative ack boundary (display only)
    record: [],       // visible applied entries: events, duplicates, markers
    duplicates: 0,
    gaps: 0,
    attempts: 0,
    transitions: [],
    lastAck: Promise.resolve(),
    connect,
    disconnect,
    reconnectWithCursor,
    applyEvent,
  };

  let source = null;
  let intentionalClose = false;
  let resyncing = false;

  function view() {
    return {
      user,
      state: client.state,
      cursor: client.cursor,
      sharedCursor: client.sharedCursor,
      duplicates: client.duplicates,
      gaps: client.gaps,
      record: client.record,
    };
  }

  function emit(activity) { onChange(view(), activity); }

  function transition(to, reason) {
    if (client.state === to) return;
    const from = client.state;
    client.state = to;
    client.transitions.push({from, to, reason});
    emit({kind: 'state', from, to, reason});
  }

  function readStoredCursor() {
    try {
      return Number(storage?.get(storageKey) ?? 0) || 0;
    } catch {
      return 0;
    }
  }

  // Cross-tab monotonic guard: a tab only ever raises the shared value.
  function writeStoredCursor(value) {
    try {
      if (value > readStoredCursor()) storage?.set(storageKey, String(value));
    } catch { /* storage unavailable */ }
  }

  function closeSource() {
    if (!source) return;
    const current = source;
    source = null;
    try {
      current.close();
    } catch { /* already closed */ }
  }

  async function connect() {
    if (resyncing) return;
    closeSource();
    // A fresh tab with no local record but a shared cursor is behind by
    // definition: full-sync first instead of silently skipping history.
    if (client.record.length === 0 && appliedIds.size === 0 && client.cursor === 0 && readStoredCursor() > 0) {
      await startResync('shared-cursor-ahead');
      return;
    }
    intentionalClose = false;
    transition(STATES.CONNECTING, 'open-stream');
    source = eventSourceImpl(`${streamUrl}?cursor=${client.cursor}`);
    source.onopen = () => {
      client.attempts = 0;
      transition(STATES.CATCHING_UP, 'stream-open');
    };
    source.onmessage = frame => {
      try {
        applyEvent(JSON.parse(frame.data));
      } catch { /* malformed frame: ignore */ }
    };
    source.addEventListener('sync', frame => {
      const info = JSON.parse(frame.data);
      if (Number(info.cursor) < client.cursor) {
        // The server is behind what we already applied (e.g. it restarted
        // and lost the log): our cursor can never be honoured, full-sync.
        void startResync('server-cursor-behind');
        return;
      }
      transition(STATES.LIVE, 'caught-up');
      ack(client.cursor);
    });
    source.addEventListener('resync-required', frame => {
      let info = {};
      try {
        info = JSON.parse(frame.data || '{}');
      } catch { /* keep default reason */ }
      void startResync(info.reason ?? 'server-requested');
    });
    source.onerror = () => {
      if (intentionalClose || resyncing || client.state === STATES.CLOSED || source === null) return;
      closeSource();
      void reconnect('stream-error');
    };
  }

  async function reconnect(reason) {
    if (client.state === STATES.CLOSED || resyncing) return;
    const attempt = client.attempts++;
    transition(STATES.RECONNECTING, reason);
    await sleep(reconnectDelay(attempt));
    if (client.state !== STATES.RECONNECTING || resyncing) return;
    await connect();
  }

  function applyEvent(event) {
    const id = Number(event?.id);
    if (!Number.isFinite(id)) return 'ignored';
    if (appliedIds.has(id)) {
      client.duplicates += 1;
      client.record.push({kind: 'duplicate', id, event});
      emit({kind: 'duplicate', id});
      return 'duplicate';
    }
    if (id > client.cursor + 1) {
      // A forward gap means events were lost between server and client.
      client.gaps += 1;
      client.record.push({kind: 'gap', id, expected: client.cursor + 1});
      emit({kind: 'gap', id, expected: client.cursor + 1});
      void startResync(`gap:${client.cursor}->${id}`);
      return 'gap';
    }
    appliedIds.add(id);
    client.cursor = Math.max(client.cursor, id);
    const origin = client.state === STATES.CATCHING_UP ? 'replay' : 'live';
    client.record.push({kind: 'event', id, event, origin});
    writeStoredCursor(id);
    ack(id);
    emit({kind: 'event', id, origin});
    return 'applied';
  }

  function ack(cursor) {
    writeStoredCursor(cursor);
    const promise = (async () => {
      try {
        const response = await fetchImpl(ackUrl, {
          method: 'POST',
          headers: {'content-type': 'application/json'},
          body: JSON.stringify({user, cursor}),
        });
        if (!response?.ok) return;
        const body = await response.json();
        const authoritative = Number(body?.cursor ?? 0);
        if (authoritative > client.sharedCursor) {
          client.sharedCursor = authoritative;
          emit({kind: 'ack', cursor: authoritative});
        }
      } catch { /* ack is best-effort; the next applied event retries */ }
    })();
    client.lastAck = promise;
    return promise;
  }

  async function startResync(reason) {
    if (resyncing || client.state === STATES.CLOSED) return;
    resyncing = true;
    closeSource();
    transition(STATES.RESYNCING, reason);
    try {
      const response = await fetchImpl(snapshotUrl);
      if (!response?.ok) throw new Error(`snapshot failed with status ${response?.status}`);
      const snap = await response.json();
      const events = Array.isArray(snap?.events) ? snap.events : [];
      const fromCursor = client.cursor;
      appliedIds.clear();
      client.cursor = 0;
      // The reset is part of the visible record, never a silent swap.
      client.record = [{kind: 'marker', marker: 'full-sync', reason, fromCursor, toCursor: Number(snap?.cursor ?? 0)}];
      for (const event of events) {
        const id = Number(event?.id);
        if (!Number.isFinite(id)) continue;
        appliedIds.add(id);
        client.cursor = Math.max(client.cursor, id);
        client.record.push({kind: 'event', id, event, origin: 'snapshot'});
      }
      client.cursor = Math.max(client.cursor, Number(snap?.cursor ?? 0));
      writeStoredCursor(client.cursor);
      ack(client.cursor);
      emit({kind: 'resync', fromCursor, toCursor: client.cursor, events: events.length});
    } catch (error) {
      resyncing = false;
      client.record.push({kind: 'marker', marker: 'snapshot-failed', reason: String(error?.message ?? error)});
      emit({kind: 'snapshot-failed'});
      void reconnect('snapshot-failed');
      return;
    }
    resyncing = false;
    if (client.state === STATES.CLOSED) return; // user disconnected mid-snapshot
    await connect();
  }

  function disconnect() {
    intentionalClose = true;
    closeSource();
    transition(STATES.CLOSED, 'client-disconnect');
  }

  // Demo/debug aid: pretend this tab only confirmed up to `cursor` and
  // reconnect. Events beyond it are re-delivered by the server.
  function reconnectWithCursor(cursor) {
    const numeric = Math.max(0, Math.floor(Number(cursor) || 0));
    closeSource();
    for (const id of [...appliedIds]) if (id > numeric) appliedIds.delete(id);
    client.cursor = numeric;
    client.attempts = 0;
    client.record.push({kind: 'marker', marker: 'manual-cursor', cursor: numeric});
    emit({kind: 'cursor-reset', cursor: numeric});
    void connect();
  }

  return client;
}
