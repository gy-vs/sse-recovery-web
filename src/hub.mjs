import {after, canResume} from './event-log.mjs';

// Subscription hub. Connections replay missed events first; events
// broadcast while a connection is still replaying are buffered and
// flushed afterwards, so a reconnecting client never sees duplicates
// or gaps at the replay/live boundary.

export function createHub() {
  return {connections: new Set()};
}

// Returns the connection, or null when the cursor fell out of the
// retention window and the caller must ask the client to resync.
export function subscribe(hub, log, cursor, send) {
  if (!canResume(log, cursor)) return null;
  const missed = after(log, cursor);
  const conn = {pending: [], live: false, send};
  hub.connections.add(conn);
  for (const event of missed) send(event);
  conn.live = true;
  for (const event of conn.pending) send(event);
  conn.pending = null;
  return conn;
}

export function broadcast(hub, event) {
  for (const conn of hub.connections) {
    if (conn.live) conn.send(event);
    else conn.pending.push(event);
  }
}

export function unsubscribe(hub, conn) {
  hub.connections.delete(conn);
}
