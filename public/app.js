import {createRecoveryClient, STATES} from '/client.mjs';

const $ = id => document.getElementById(id);

const stateLabels = {
  [STATES.IDLE]: ['空闲', 'wait'],
  [STATES.CONNECTING]: ['连接中', 'wait'],
  [STATES.CATCHING_UP]: ['追赶中', 'catch'],
  [STATES.LIVE]: ['已实时', 'live'],
  [STATES.RESYNCING]: ['需要全量同步', 'resync'],
  [STATES.RECONNECTING]: ['重连中', 'catch'],
  [STATES.CLOSED]: ['已断开', 'wait'],
};

// localStorage is shared by every tab of this origin: this is the user's
// cross-tab cursor. The client only ever raises it (monotonic guard inside).
const sharedStorage = {
  get: key => localStorage.getItem(key),
  set: (key, value) => localStorage.setItem(key, value),
};

let client = null;

function renderRecordEntry(entry) {
  if (entry.kind === 'event') {
    const {type, data} = entry.event;
    return `<span class="tag ${entry.origin}">${entry.origin}</span>#${entry.id} ${type} ${escapeHtml(JSON.stringify(data))}`;
  }
  if (entry.kind === 'duplicate') {
    return `<span class="tag duplicate">重复</span>#${entry.id} 已应用过,跳过`;
  }
  if (entry.kind === 'gap') {
    return `<span class="tag gap">缺口</span>期望 #${entry.expected} 却收到 #${entry.id},触发全量同步`;
  }
  if (entry.marker === 'full-sync') {
    return `<span class="tag marker">全量同步</span>${escapeHtml(entry.reason)}:游标 ${entry.fromCursor} → ${entry.toCursor},本地记录已替换为服务端快照`;
  }
  if (entry.marker === 'manual-cursor') {
    return `<span class="tag marker">强制游标</span>以游标 ${entry.cursor} 重新连接`;
  }
  return `<span class="tag marker">标记</span>${escapeHtml(entry.reason ?? '')}`;
}

function escapeHtml(text) {
  return String(text).replace(/[&<>"]/g, ch => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'}[ch]));
}

function render(view) {
  const [label, tone] = stateLabels[view.state] ?? [view.state, 'wait'];
  $('state').textContent = label;
  $('state').className = tone;
  $('cursor').textContent = view.cursor;
  $('shared').textContent = view.sharedCursor;
  $('dups').textContent = view.duplicates;
  $('gaps').textContent = view.gaps;
  $('record').innerHTML = view.record.map(entry => `<li>${renderRecordEntry(entry)}</li>`).join('');
  $('record').scrollTop = $('record').scrollHeight;
  $('transitions').innerHTML = client.transitions
    .map(t => `<li>${t.from} → ${t.to} (${escapeHtml(t.reason)})</li>`)
    .join('');
}

function ensureClient() {
  const user = $('user').value || 'demo';
  if (!client || client.user !== user) {
    client = createRecoveryClient({user, storage: sharedStorage, onChange: render});
  }
  return client;
}

$('connect').onclick = () => ensureClient().connect();
$('disconnect').onclick = () => client?.disconnect();
$('reconnect-stale').onclick = () => ensureClient().reconnectWithCursor(Number($('forced').value || 0));
$('publish').onclick = async () => {
  await fetch('/api/publish', {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify({type: 'note', data: {text: $('text').value, by: $('user').value}}),
  });
};

// Legacy cursor-less flow, unchanged.
$('read').onclick = async () => {
  const response = await fetch('/api/events?cursor=0');
  $('events').textContent = JSON.stringify(await response.json(), null, 2);
};
