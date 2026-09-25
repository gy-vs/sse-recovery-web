import {after, canResume, firstAvailable, latestCursor} from './event-log.mjs';

export function writeSseFrame(res, {id, event, data, retry} = {}) {
  let frame = '';
  if (retry !== undefined) frame += `retry: ${retry}\n`;
  if (id !== undefined) frame += `id: ${id}\n`;
  if (event !== undefined) frame += `event: ${event}\n`;
  const text = typeof data === 'string' ? data : JSON.stringify(data ?? {});
  for (const line of text.split('\n')) frame += `data: ${line}\n`;
  frame += '\n';
  res.write(frame);
}

// Live subscriber connections. broadcast() is best-effort per socket:
// a half-dead connection is dropped instead of breaking the others.
export function createHub() {
  const subscribers = new Set();
  const forget = res => subscribers.delete(res);
  return {
    get size() { return subscribers.size; },
    subscribe(res) {
      subscribers.add(res);
      res.on('close', () => forget(res));
      res.on('error', () => forget(res));
      return () => forget(res);
    },
    broadcast(event) {
      for (const res of [...subscribers]) {
        try {
          writeSseFrame(res, {id: event.id, data: event});
        } catch {
          forget(res);
        }
      }
    },
    heartbeat(comment = 'ping') {
      for (const res of [...subscribers]) {
        try {
          res.write(`: ${comment}\n\n`);
        } catch {
          forget(res);
        }
      }
    },
    // Test/ops aid: sever every live stream as if the network dropped.
    dropAll() {
      for (const res of [...subscribers]) {
        forget(res);
        res.destroy();
      }
    },
  };
}

// Attach a response to the stream at `cursor`. Runs synchronously so no
// publish can slip between reading the replay window and subscribing:
// replayed events, the sync marker and later live events stay ordered.
export function attachStream({res, log, cursor, hub}) {
  const latest = latestCursor(log);
  const first = firstAvailable(log);
  const numeric = Number(cursor ?? 0);
  const normalised = Number.isFinite(numeric) && numeric > 0 ? Math.floor(numeric) : 0;
  if (!canResume(log, normalised) || normalised > latest) {
    writeSseFrame(res, {
      event: 'resync-required',
      data: {
        reason: normalised > latest ? 'cursor-ahead-of-log' : 'cursor-expired',
        cursor: latest,
        firstAvailable: first,
      },
    });
    res.end();
    return {resumed: false, replayed: 0};
  }
  hub.subscribe(res);
  const missed = after(log, normalised);
  for (const event of missed) writeSseFrame(res, {id: event.id, data: event});
  writeSseFrame(res, {event: 'sync', data: {cursor: latest, firstAvailable: first}});
  return {resumed: true, replayed: missed.length};
}
