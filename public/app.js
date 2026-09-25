import {
  STATUS, applySnapshot, createClientStore, noteAck,
  onCaughtUp, onChange, onDisconnected, onHello, onResyncRequired,
} from './client-store.mjs';

const store = createClientStore();
let source = null;
const user = () => document.querySelector('#user').value || 'demo-user';

async function ack(cursor) {
  const response = await fetch('/api/ack', {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify({user: user(), cursor}),
  });
  const result = await response.json();
  noteAck(store, result.cursor);
  document.querySelector('#server-ack').textContent =
    `${result.cursor} (${result.advanced ? 'advanced' : 'stale, ignored'})`;
}

function connect(cursor) {
  if (source) source.close();
  store.status = STATUS.CONNECTING;
  const url = cursor === null ? '/api/stream' : `/api/stream?cursor=${cursor}`;
  source = new EventSource(url);

  source.addEventListener('hello', event => {
    onHello(store, JSON.parse(event.data));
    if (store.status === STATUS.RESYNC_REQUIRED) return fullSync();
    render();
  });
  source.addEventListener('change', event => {
    const ackId = onChange(store, JSON.parse(event.data));
    if (ackId !== null) ack(ackId);
    if (store.status === STATUS.RESYNC_REQUIRED) return fullSync();
    render();
  });
  source.addEventListener('caught-up', event => {
    onCaughtUp(store, JSON.parse(event.data));
    if (store.status === STATUS.RESYNC_REQUIRED) return fullSync();
    render();
  });
  source.addEventListener('resync-required', event => {
    onResyncRequired(store, JSON.parse(event.data));
    source.close(); // stop EventSource auto-retry with the stale cursor
    fullSync();
  });
  source.onerror = () => {
    if (source.readyState === EventSource.CLOSED) {
      onDisconnected(store);
    } else {
      store.status = STATUS.CONNECTING;
    }
    render();
  };
  render();
}

async function fullSync() {
  const snapshot = await (await fetch('/api/state')).json();
  applySnapshot(store, snapshot);
  render();
  connect(snapshot.cursor);
}

async function publish(type, data) {
  await fetch('/api/publish', {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify({type, data}),
  });
}

function render() {
  document.querySelector('#status').textContent = store.status;
  document.querySelector('#status').dataset.status = store.status;
  document.querySelector('#cursor').textContent = store.cursor;
  document.querySelector('#acked').textContent = store.acked;
  document.querySelector('#counters').textContent =
    `applied ${store.counters.applied} · duplicates ${store.counters.duplicates} · gaps ${store.counters.gaps}`;
  document.querySelector('#state').textContent = JSON.stringify(store.state, null, 2);
  document.querySelector('#records').textContent = store.records
    .map((r, i) => `${i + 1}. ${r.step} ${JSON.stringify({...r, step: undefined})}`).join('\n');
  document.querySelector('#reconnect-cursor').placeholder = String(store.cursor);
}

document.querySelector('#set').onclick = () =>
  publish('set', {key: document.querySelector('#key').value, value: document.querySelector('#value').value});
document.querySelector('#delete').onclick = () =>
  publish('delete', {key: document.querySelector('#key').value});
document.querySelector('#disconnect').onclick = () => {
  if (source) source.close();
  onDisconnected(store);
  render();
};
document.querySelector('#reconnect').onclick = () => {
  const input = document.querySelector('#reconnect-cursor');
  connect(Number(input.value || input.placeholder));
};
document.querySelector('#connect-live').onclick = () => connect(null);
document.querySelector('#full-sync').onclick = () => fullSync();
// Legacy polling client, kept from the previous version.
document.querySelector('#read').onclick = async () => {
  const response = await fetch('/api/events?cursor=0');
  document.querySelector('#events').textContent = JSON.stringify(await response.json(), null, 2);
};

fullSync();
