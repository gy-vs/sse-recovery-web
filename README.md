# sse-recovery-web

SSE event stream with resumable cursors. The server keeps an event sequence
with a bounded retention window, live subscriptions, and per-user ack
cursors; the browser reconnects with its last confirmed event and either
catches up from the retained window or is told explicitly to full-sync.

## Run

```
npm start          # PORT=4180 by default, SSE_HEARTBEAT_MS=15000
npm test
```

Open http://localhost:4180/ in two tabs to see shared-user acks.

## API

- `POST /api/publish {type, data}` — append an event (201, returns it).
  `set {key,value}` / `delete {key}` also fold into the state snapshot.
- `GET /api/stream` — SSE. Cursor from `Last-Event-ID` header (preferred,
  sent automatically by EventSource reconnects) or `?cursor=N`.
  - no cursor: legacy live-only stream, no replay;
  - resumable cursor: `hello{mode:"replay",from,to}` → missed `change`
    events → `caught-up{cursor}` → live `change` events;
  - expired cursor: a terminal `resync-required` event, then close.
- `GET /api/events?cursor=N` — legacy JSON polling (409 on expired cursor).
- `GET /api/state` — full-sync snapshot `{state, cursor}`.
- `POST /api/ack {user, cursor}` — confirm applied events; per-user cursor
  only moves forward, late acks return `{advanced: false}`.
- `GET /api/acks`, `GET /api/status` — observability.

## Client states

`connecting → catching-up → live`, plus `resync-required` (server cannot
replay: retention moved past the cursor, a gap was detected, or the cursor
is ahead of the server tip) and `disconnected`. Full sync replaces local
state from `/api/state` as an explicit, recorded step — never a silent
reset. Duplicates (id ≤ cursor) are skipped and counted; every transition
and applied event lands in the on-page recovery record.

Manual recovery drill: open the page, press **disconnect**, publish a few
events, enter an old cursor and press **reconnect with cursor** — the record
shows `catch-up-begin`, `duplicate-skipped`/`applied` steps and `caught-up`.
