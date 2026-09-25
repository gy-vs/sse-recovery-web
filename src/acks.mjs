// Per-user acknowledgement registry. Several browser tabs may confirm
// events for the same user; the stored cursor only ever moves forward,
// so a late (stale) ack from one tab can never push another tab's
// confirmed position backwards.

export function createAckRegistry() {
  return {cursors: new Map()};
}

// Returns {cursor, advanced}: the stored cursor after the ack and
// whether this ack actually moved it.
export function acknowledge(registry, user, cursor) {
  const key = String(user);
  const value = Number(cursor);
  if (!Number.isFinite(value) || value < 0) {
    throw new TypeError(`invalid ack cursor: ${cursor}`);
  }
  const current = registry.cursors.get(key) ?? 0;
  if (value <= current) return {cursor: current, advanced: false};
  registry.cursors.set(key, value);
  return {cursor: value, advanced: true};
}

export function ackedCursor(registry, user) {
  return registry.cursors.get(String(user)) ?? 0;
}

export function ackSnapshot(registry) {
  return Object.fromEntries(registry.cursors);
}
