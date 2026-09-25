// Append-only event log with a fixed retention window.
// The log is immutable: every operation returns a new value.

export function createEventLog(limit = 100) {
  return {nextId: 1, limit, events: []};
}

export function publish(log, type, data) {
  const next = structuredClone(log);
  next.events.push({id: next.nextId++, type, data: structuredClone(data)});
  if (next.events.length > next.limit) next.events.shift();
  return next;
}

// Events strictly after `cursor`, in id order.
export function after(log, cursor) {
  return log.events.filter(event => event.id > Number(cursor || 0));
}

// A cursor is resumable when every event after it is still retained,
// i.e. the cursor reaches at least the id just before the oldest retained event.
export function canResume(log, cursor) {
  return log.events.length === 0 || Number(cursor || 0) >= log.events[0].id - 1;
}

// Id of the newest event, 0 when the log is empty.
export function tip(log) {
  return log.events.at(-1)?.id ?? 0;
}

// Id of the oldest retained event, null when the log is empty.
export function firstRetained(log) {
  return log.events[0]?.id ?? null;
}

// Domain fold: derives the current state snapshot from an event.
// Unknown types are still logged, they just do not touch the state.
export function reduceEvent(state, event) {
  const next = {...state};
  if (event.type === 'set') next[event.data.key] = event.data.value;
  else if (event.type === 'delete') delete next[event.data.key];
  return next;
}
