export function createEventLog(limit = 100) { return {nextId: 1, limit, events: []}; }
export function publish(log, type, data) { const next = structuredClone(log); next.events.push({id: next.nextId++, type, data: structuredClone(data)}); if (next.events.length > next.limit) next.events.shift(); return next; }
export function after(log, cursor) { return log.events.filter(event => event.id > Number(cursor || 0)); }
export function canResume(log, cursor) { return log.events.length === 0 || Number(cursor || 0) >= log.events[0].id - 1; }
export function latestCursor(log) { return log.events.at(-1)?.id ?? 0; }
export function firstAvailable(log) { return log.events[0]?.id ?? 0; }
