import {mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import {dirname} from 'node:path';

// Per-user acknowledged cursor. Acks only ever move the boundary forward:
// a late ack from a slow tab must never drag another tab backwards.
export function createCursorStore({load, save} = {}) {
  let cursors = {...(load?.() ?? {})};
  return {
    ack(user, cursor) {
      const key = String(user ?? 'default');
      const value = Number(cursor);
      if (!Number.isFinite(value) || value < 0) return cursors[key] ?? 0;
      const next = Math.max(cursors[key] ?? 0, Math.floor(value));
      if (next !== cursors[key]) {
        cursors[key] = next;
        save?.({...cursors});
      }
      return next;
    },
    get(user) { return cursors[String(user ?? 'default')] ?? 0; },
    snapshot() { return {...cursors}; },
  };
}

// File-backed persistence so the ack boundary survives a server restart.
export function createFileCursorPersistence(file) {
  return {
    load() {
      try {
        const parsed = JSON.parse(readFileSync(file, 'utf8'));
        return parsed && typeof parsed === 'object' ? parsed : {};
      } catch {
        return {};
      }
    },
    save(cursors) {
      mkdirSync(dirname(file), {recursive: true});
      writeFileSync(file, JSON.stringify(cursors, null, 2));
    },
  };
}
