# nullchat

An anonymous global chatroom that forgets you. One temporary handle per visit,
no accounts, no message history, no logs. Close the tab and the identity is
gone.

Built for native deployment on Vercel Functions: Express + `ws` for the socket,
Upstash Redis Pub/Sub to keep every serverless instance in sync.

---

## Directory structure

```
nullchat/
├── api/
│   └── server.js        Express + ws server, Redis Pub/Sub, presence, sanitisation
├── public/
│   └── index.html       Entire frontend: markup, styles, client logic
├── .claude/
│   └── launch.json      Local dev-server config (editor tooling only)
├── package.json
├── vercel.json          Routing + security headers
├── .env.example
└── README.md
```

Two source files do the whole job. `api/server.js` exports a raw `http.Server`,
which is exactly what Vercel's WebSocket support expects; `public/` is served
straight off the CDN.

---

## Run it locally

```bash
npm install
```

```bash
npm run dev
```

Open <http://localhost:3000>. Open a second window to talk to yourself.

Without `REDIS_URL` the server starts in **single-instance mode**: an
in-process event bus stands in for Redis Pub/Sub so the app runs with zero
setup. It logs a warning when it does this. Everything works, but only within
one process, which is fine locally and wrong in production.

> Use `npm run dev`, not `vercel dev`, for local work. `vercel dev` does not
> emulate the WebSocket upgrade path.

---

## Deploy to Vercel

### 1. Provision Upstash Redis

Easiest through the Vercel Marketplace, which wires the env var up for you:

```bash
vercel link
```

Then in the Vercel dashboard: **Storage → Create Database → Redis (Upstash) →
Connect to Project**.

Prefer doing it by hand? Create a database at
[console.upstash.com](https://console.upstash.com), copy the **ioredis**
connection string (`rediss://default:…@….upstash.io:6379`), and add it:

```bash
vercel env add REDIS_URL
```

Add it to Production, Preview, and Development when prompted. If the
marketplace integration named the variable something else (commonly
`REDIS_URL` or `KV_URL`), either rename it or set `REDIS_URL` to the same
value. The server also accepts `UPSTASH_REDIS_URL`.

### 2. Confirm Fluid compute is on

WebSockets require it. It has been the default for projects created since
23 April 2025; for older projects turn it on under **Settings → Functions →
Fluid compute**.

### 3. Ship

```bash
vercel --prod
```

Verify the deployment is healthy:

```bash
curl https://your-deployment.vercel.app/healthz
```

That returns the instance id, the active bus (`redis` or `local`), and the
current online count. **If it reports `"bus":"local"` in production, `REDIS_URL`
did not reach the function** and instances are not talking to each other.

---

## How it works

### Cross-instance sync

A WebSocket is pinned to one function instance, but a busy room spreads across
many. Nothing may live in module scope and be trusted, so everything crosses
Redis:

- **Messages, system logs, typing** are published to the `nullchat:global`
  Pub/Sub channel. Each instance subscribes and fans out to its own sockets.
  The sender's own message arrives the same way, so there is no local echo to
  deduplicate.
- **Online count** uses a sorted set (`nullchat:presence`) keyed by an opaque
  connection id with the heartbeat timestamp as score. Every instance refreshes
  its own members every 15s and evicts anything older than 45s before counting.
  An instance killed without cleanup ages out instead of inflating the counter
  forever.

The presence set stores connection ids, never usernames.

### Reconnects

Vercel caps a function at **5 minutes by default**, so every client's socket
drops roughly that often. That is normal, not an error, and it is handled:

- The client reconnects with exponential backoff (1s → 15s) and re-joins with
  `resume: true`, which suppresses the join announcement.
- On disconnect the presence entry is removed immediately, so the counter is
  always accurate, but the "left the room" line is held for 10 seconds. A
  reconnect within that window cancels it via a short-lived Redis key, so a
  duration-cap reconnect produces no join/leave noise at all. Someone who
  actually leaves is announced once the window closes.

Because the username lives only in a JavaScript variable, a reconnect keeps it
while a refresh discards it, which is the intended behaviour in both cases.

### One session per device

Two tabs on one device used to mean two users and a doubled online count.
A tab now claims a [Web Lock](https://developer.mozilla.org/en-US/docs/Web/API/Web_Locks_API)
named `nullchat-session` and holds it for as long as it is open. A second tab
finds the lock taken and shows an "Already open" notice instead of connecting,
so it never opens a socket and never reaches the presence set.

A lock is used rather than a `localStorage` flag for two reasons: the browser
releases it automatically when the tab closes or crashes, so there is no stale
claim to expire, and it persists nothing, which keeps the no-storage promise
intact.

The blocked tab queues for the lock rather than polling, which makes two things
work by themselves:

- Close the active tab and the waiting one takes over immediately.
- Press **Use this tab** and it broadcasts on a `BroadcastChannel`; the holder
  closes its socket, clears its copy of the room, and releases. Locks are
  granted in request order, so the tab that asked first wins and the two cannot
  fight over it.

Taking over gives you a **new** handle, because identity lives only in the
tab's memory. That is the same rule as refreshing the page, applied
consistently.

**What this does not do:** it is not an identity control. A second browser, a
private window, or another device each get their own lock and their own user.
Nothing available to a web page can prevent that. This stops the accidental
duplicate tab, which is what it is for. If browsers without the Web Locks API
show up, the check degrades to open rather than locking someone out on a guess.

### Sanitisation

Two independent layers:

- **Backend** strips control characters (including bidi overrides), removes
  `<` and `>`, collapses whitespace, and caps length. This runs before anything
  is published, so a hand-rolled WebSocket client that skips the browser
  entirely still cannot inject markup.
- **Frontend** builds every node with `textContent`. No user string ever
  reaches `innerHTML`.

Angle brackets are *removed* rather than entity-escaped, because escaping
server-side would double-encode against `textContent` and users would read a
literal `&lt;`. The trade-off is that `a < b` loses its bracket. To allow them,
drop the `.replace(/[<>]/g, '')` from `sanitizeMessage` in
[api/server.js](api/server.js). `textContent` rendering keeps it safe.

Also enforced: empty and whitespace-only messages are rejected on both sides,
usernames are restricted to `[a-zA-Z0-9_-]{2,20}`, messages are capped at 500
characters, the socket payload limit is 16 KB, and each connection is limited
to 12 messages per 10 seconds.

---

## Tuning

Constants at the top of [api/server.js](api/server.js):

| Constant | Default | Meaning |
| --- | --- | --- |
| `PRESENCE_TTL_MS` | 45s | Presence entries older than this are evicted |
| `HEARTBEAT_MS` | 15s | Ping clients, refresh presence |
| `TYPING_TTL_MS` | 5s | Typing state auto-clears after this |
| `LEAVE_GRACE_MS` | 10s | Window a reconnect has to cancel a leave notice |
| `RATE_MAX_MESSAGES` | 12 / 10s | Per-connection message rate limit |

To raise the 5-minute connection cap to 30 minutes (Pro/Enterprise, in beta),
add to `vercel.json`:

```json
{ "functions": { "api/server.js": { "maxDuration": 1800 } } }
```

---

## Deliberate omissions

- **No message history.** A new arrival sees an empty room. Persisting a
  rolling buffer in Redis would be ~10 lines, but it contradicts the premise.
- **No username uniqueness.** Two people can hold the same handle at once.
  Enforcing it would mean a registry of live names in Redis.
- **No moderation, no private rooms, no reactions.** One global room only.
