# sse-recovery-web

SSE 断线恢复演示:浏览器带着最后确认的事件游标重连,服务端从保留窗口补发缺失事件;补发不了时明确通知客户端全量同步。

## 运行

```bash
npm start          # http://localhost:4180 (PORT/EVENT_LIMIT/CURSOR_FILE 可覆盖)
npm test           # node --test
```

## 服务端

- `src/event-log.mjs` — 事件序列与保留窗口(纯函数,`publish`/`after`/`canResume` 不变)。
- `src/cursor-store.mjs` — 每用户确认游标,**单调递增**:迟到确认不会回退边界;经 `data/cursors.json` 持久化,重启后边界仍在。
- `src/sse.mjs` — 订阅中心(hub)与可恢复挂载:重放 → `sync` 标记 → 实时事件,挂载过程同步执行,发布不会插进重放与订阅之间。

### API

| 路由 | 说明 |
| --- | --- |
| `POST /api/publish` | 发布事件并广播给在线订阅者(不变) |
| `GET /api/events?cursor=` | 旧 JSON 端点,行为不变(游标过期返回 409) |
| `GET /api/events/stream?cursor=` | 可恢复 SSE;也认 `Last-Event-ID` 头。游标过期或领先于日志时先发 `event: resync-required` 再关闭 |
| `GET /api/state` | 全量同步快照:保留窗口内事件 + 当前游标,永远 200 |
| `POST /api/cursor/ack` | 确认游标,返回权威边界(单调) |
| `GET /api/cursor?user=` | 读取共享确认边界 |

## 前端

`public/client.mjs` 是 DOM 无关的恢复客户端状态机:`connecting → catching-up → live`,异常时 `resyncing`(需要全量同步)/ `reconnecting`。`public/app.js` 把它接到页面,每一步都有可观察状态:

- 重放/实时/快照来源分别标记;重复事件计数并跳过;顺序缺口触发全量同步。
- 全量同步不是悄悄重置:记录里留下 `full-sync` 标记行(原因、游标从哪到哪),再接上快照事件。
- 多标签页共享 localStorage 游标与服务端确认边界,两者都只升不降;每个标签页按**自己**的应用位置重连,不会被别的标签页推到错误位置。
- 新标签页发现共享游标领先时,先全量同步再进入实时,而不是跳过历史。

页面底部保留旧客户端入口(无游标的 `GET /api/events?cursor=0`)。

## 手动验证旅程

1. 打开页面点「连接」→ 状态 `已实时`;点「发布事件」几条。
2. 点「断开」,再发布几条,点「连接」→ 经历 `追赶中`,缺失事件以 `replay` 标记补齐,无重复无跳过。
3. 把「强制游标」填一个很小的值点「以旧游标重连」→ 保留窗口已滑过时会看到 `需要全量同步`,记录出现 `full-sync` 标记行,随后恢复 `已实时`。
4. 开第二个标签页(同一用户)→ 它先全量同步再实时;服务端 `GET /api/cursor?user=demo` 的确认边界与页面游标一致,且任何迟到确认都不会让它回退。
