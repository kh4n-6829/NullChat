/**
 * nullchat: an anonymous global chatroom
 * ---------------------------------------------------------------------------
 * Express + `ws` WebSocket server, exported as a Vercel Function.
 *
 * Vercel pins a WebSocket connection to a single function instance for the
 * lifetime of that connection, but a busy room will be spread across many
 * instances. Nothing may live in module scope and be trusted as global truth,
 * so all cross-instance state moves through Redis:
 *
 *   - Chat, system logs and typing events -> Redis Pub/Sub channel.
 *   - Online count -> Redis sorted set keyed by heartbeat timestamp, so
 *     instances that die without running their cleanup handlers get evicted
 *     instead of inflating the counter forever.
 *
 * Nothing about a user is persisted. Usernames live in this instance's memory
 * for exactly as long as the socket is open, and the presence set stores an
 * opaque connection id, never a name.
 */

import { createServer } from 'node:http';
import { EventEmitter } from 'node:events';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import express from 'express';
import { WebSocketServer } from 'ws';
import Redis from 'ioredis';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const CHANNEL = 'nullchat:global';
const PRESENCE_KEY = 'nullchat:presence';
const LEAVING_PREFIX = 'nullchat:leaving:';

// A socket that drops because the function hit its duration cap is not someone
// leaving. Hold the "left the room" line briefly so the client's reconnect can
// cancel it; a real departure is announced after the window closes.
const LEAVE_GRACE_MS = 10_000;
const LEAVE_KEY_TTL_S = 20;

const PRESENCE_TTL_MS = 45_000; // presence entries older than this are stale
const HEARTBEAT_MS = 15_000; // ping clients + refresh presence
const TYPING_SWEEP_MS = 1_000; // expire stale typing states
const TYPING_TTL_MS = 5_000; // a typing state auto-clears after this
const TYPING_THROTTLE_MS = 900; // min gap between typing publishes per socket

const MAX_MESSAGE_LEN = 500;
const MAX_USERNAME_LEN = 20;
const MIN_USERNAME_LEN = 2;

const RATE_WINDOW_MS = 10_000;
const RATE_MAX_MESSAGES = 12;

const INSTANCE_ID = randomUUID().slice(0, 8);
const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Sanitisation
//
// Rendering on the client is done exclusively with textContent, which is
// already injection-proof. This layer is the second half of that defence: it
// runs before anything is published, so a hand-rolled WebSocket client that
// skips the browser entirely still cannot put markup into the stream.
//
// Angle brackets are dropped rather than entity-escaped. Escaping here would
// double-encode against textContent and users would literally read "&lt;".
// ---------------------------------------------------------------------------

/** Strip C0/C1 control characters, including the RTL/LTR override tricks. */
function stripControl(value) {
  return value.replace(/[\u0000-\u001F\u007F-\u009F\u200B-\u200F\u202A-\u202E\u2066-\u2069]/g, '');
}

function sanitizeMessage(raw) {
  if (typeof raw !== 'string') return '';
  return stripControl(raw)
    .replace(/[<>]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, MAX_MESSAGE_LEN);
}

function sanitizeUsername(raw) {
  if (typeof raw !== 'string') return '';
  const cleaned = stripControl(raw)
    .replace(/[^a-zA-Z0-9_-]/g, '')
    .slice(0, MAX_USERNAME_LEN);
  return cleaned.length >= MIN_USERNAME_LEN ? cleaned : '';
}

const ADJECTIVES = [
  'quiet', 'hollow', 'amber', 'static', 'velvet', 'copper', 'silent', 'faded',
  'neon', 'lunar', 'drifting', 'glass', 'ashen', 'violet', 'hidden', 'slow',
];
const NOUNS = [
  'fox', 'ember', 'signal', 'moth', 'harbor', 'cipher', 'wolf', 'echo',
  'orbit', 'ghost', 'kite', 'raven', 'vector', 'lantern', 'reef', 'nomad',
];

function randomUsername() {
  const a = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const n = NOUNS[Math.floor(Math.random() * NOUNS.length)];
  return `${a}_${n}_${Math.floor(Math.random() * 90) + 10}`;
}

// ---------------------------------------------------------------------------
// Event bus
//
// Two shapes behind one interface. The Redis bus is what runs on Vercel; the
// local bus keeps `npm run dev` working with no database so the app is
// testable out of the box.
// ---------------------------------------------------------------------------

function createRedisBus(url) {
  // A subscriber connection must never give up mid-room, hence the null retry
  // cap; ioredis would otherwise start failing commands after 20 attempts.
  const options = {
    maxRetriesPerRequest: null,
    retryStrategy: (times) => Math.min(times * 200, 5_000),
  };

  const pub = new Redis(url, options);
  const sub = new Redis(url, options);
  const events = new EventEmitter();

  pub.on('error', (err) => console.error('[redis:pub]', err.message));
  sub.on('error', (err) => console.error('[redis:sub]', err.message));

  sub.subscribe(CHANNEL).catch((err) => {
    console.error('[redis:sub] subscribe failed:', err.message);
  });

  sub.on('message', (channel, raw) => {
    if (channel !== CHANNEL) return;
    try {
      events.emit('event', JSON.parse(raw));
    } catch {
      /* a malformed payload from the wire is not worth crashing over */
    }
  });

  return {
    mode: 'redis',
    onEvent(handler) {
      events.on('event', handler);
    },
    async publish(event) {
      await pub.publish(CHANNEL, JSON.stringify(event));
    },
    async presenceAdd(member) {
      await pub.zadd(PRESENCE_KEY, Date.now(), member);
    },
    async presenceRemove(member) {
      await pub.zrem(PRESENCE_KEY, member);
    },
    async presenceTouch(members) {
      if (members.length === 0) return;
      const now = Date.now();
      const pipeline = pub.pipeline();
      for (const member of members) pipeline.zadd(PRESENCE_KEY, now, member);
      await pipeline.exec();
    },
    async presenceCount() {
      await pub.zremrangebyscore(PRESENCE_KEY, '-inf', Date.now() - PRESENCE_TTL_MS);
      return await pub.zcard(PRESENCE_KEY);
    },
    async markLeaving(username, token) {
      await pub.set(LEAVING_PREFIX + username, token, 'EX', LEAVE_KEY_TTL_S);
    },
    async cancelLeaving(username) {
      await pub.del(LEAVING_PREFIX + username);
    },
    async claimLeaving(username, token) {
      const key = LEAVING_PREFIX + username;
      if ((await pub.get(key)) !== token) return false;
      await pub.del(key);
      return true;
    },
    async close() {
      await Promise.allSettled([pub.quit(), sub.quit()]);
    },
  };
}

function createLocalBus() {
  const events = new EventEmitter();
  const presence = new Map(); // member -> last seen timestamp
  const leaving = new Map(); // username -> { token, expiresAt }

  return {
    mode: 'local',
    onEvent(handler) {
      events.on('event', handler);
    },
    async publish(event) {
      // setImmediate keeps the delivery asynchronous, matching Redis semantics
      // so behaviour does not subtly differ between the two modes.
      setImmediate(() => events.emit('event', event));
    },
    async presenceAdd(member) {
      presence.set(member, Date.now());
    },
    async presenceRemove(member) {
      presence.delete(member);
    },
    async presenceTouch(members) {
      const now = Date.now();
      for (const member of members) presence.set(member, now);
    },
    async presenceCount() {
      const cutoff = Date.now() - PRESENCE_TTL_MS;
      for (const [member, seen] of presence) {
        if (seen < cutoff) presence.delete(member);
      }
      return presence.size;
    },
    async markLeaving(username, token) {
      leaving.set(username, { token, expiresAt: Date.now() + LEAVE_KEY_TTL_S * 1000 });
    },
    async cancelLeaving(username) {
      leaving.delete(username);
    },
    async claimLeaving(username, token) {
      const entry = leaving.get(username);
      if (!entry || entry.token !== token || entry.expiresAt < Date.now()) return false;
      leaving.delete(username);
      return true;
    },
    async close() {
      presence.clear();
      leaving.clear();
    },
  };
}

const REDIS_URL = process.env.REDIS_URL || process.env.UPSTASH_REDIS_URL || '';
const bus = REDIS_URL ? createRedisBus(REDIS_URL) : createLocalBus();

if (bus.mode === 'local') {
  console.warn(
    '[nullchat] No REDIS_URL set. Running in single-instance mode. ' +
      'Messages will NOT sync across Vercel function instances.',
  );
}

// ---------------------------------------------------------------------------
// HTTP + WebSocket server
// ---------------------------------------------------------------------------

const app = express();
app.disable('x-powered-by');

// On Vercel the CDN serves /public directly and these routes are never hit.
// Locally they are what serves the app.
app.use(express.static(path.join(__dirname, '..', 'public'), { extensions: ['html'] }));

// Answers on every path that can reach this function, because a rewrite may
// hand Express either the original URL or the destination one depending on how
// the platform resolves it. A plain GET to the socket path lands here too,
// which makes it a usable smoke test.
app.get(['/healthz', '/api/healthz', '/api/server', '/api/ws'], (_req, res) => {
  res.json({ ok: true, instance: INSTANCE_ID, bus: bus.mode, online: lastCount });
});

const server = createServer(app);

// Accepting on any path keeps this working through the /api/ws -> /api/server
// rewrite, under `vercel dev`, and on a bare local port without three
// different path checks that each have to stay in sync.
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

/** ws -> connection state */
const clients = new Map();
/** username -> timestamp at which their typing state expires */
const typingUsers = new Map();

let lastCount = 0;

function send(ws, payload) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(payload));
}

function broadcastLocal(payload) {
  const frame = JSON.stringify(payload);
  for (const ws of clients.keys()) {
    if (ws.readyState === ws.OPEN) ws.send(frame);
  }
}

/**
 * Push the typing roster to local clients, with each recipient filtered out of
 * their own list. Only sends when a client's view actually changed. Otherwise
 * every keystroke in a busy room would fan out to every socket.
 */
function pushTyping() {
  const now = Date.now();
  for (const [name, expiresAt] of typingUsers) {
    if (expiresAt <= now) typingUsers.delete(name);
  }

  const names = [...typingUsers.keys()];
  for (const [ws, state] of clients) {
    if (ws.readyState !== ws.OPEN || !state.username) continue;
    const others = names.filter((name) => name !== state.username);
    const fingerprint = others.join(' ');
    if (state.lastTypingSent === fingerprint) continue;
    state.lastTypingSent = fingerprint;
    send(ws, { t: 'typing', users: others });
  }
}

async function refreshPresence({ force = false } = {}) {
  try {
    const count = await bus.presenceCount();
    if (force || count !== lastCount) {
      lastCount = count;
      await bus.publish({ t: 'presence', count });
    }
  } catch (err) {
    console.error('[presence]', err.message);
  }
}

// Fan events in from Redis (or the local bus) out to this instance's sockets.
bus.onEvent((event) => {
  switch (event.t) {
    case 'msg':
    case 'sys':
      broadcastLocal(event);
      break;

    case 'presence':
      lastCount = event.count;
      broadcastLocal(event);
      break;

    case 'typing':
      if (event.state) {
        typingUsers.set(event.user, Date.now() + TYPING_TTL_MS);
      } else {
        typingUsers.delete(event.user);
      }
      pushTyping();
      break;

    default:
      break;
  }
});

wss.on('connection', (ws) => {
  const id = randomUUID();
  const state = {
    id,
    member: `${INSTANCE_ID}:${id}`,
    username: null,
    alive: true,
    lastTypingSent: null,
    lastTypingPublish: 0,
    rateWindowStart: Date.now(),
    rateCount: 0,
  };
  clients.set(ws, state);

  send(ws, { t: 'hello', ts: Date.now(), online: lastCount, mode: bus.mode });

  ws.on('pong', () => {
    state.alive = true;
  });

  ws.on('message', async (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!data || typeof data.t !== 'string') return;

    try {
      switch (data.t) {
        case 'join': {
          if (state.username) break; // already joined; ignore replays
          const username = sanitizeUsername(data.username) || randomUsername();
          state.username = username;

          await bus.presenceAdd(state.member);
          send(ws, { t: 'welcome', username, ts: Date.now() });

          // Cancels the pending "left the room" from the socket this one is
          // replacing, wherever in the fleet that socket lived.
          await bus.cancelLeaving(username);

          // A reconnect after the function's duration cap is not a new
          // arrival; announcing it would fill the room with join/leave churn
          // every five minutes.
          if (!data.resume) {
            await bus.publish({
              t: 'sys',
              kind: 'join',
              text: `${username} joined the room`,
              ts: Date.now(),
            });
          }
          await refreshPresence({ force: true });
          break;
        }

        case 'nick': {
          if (!state.username) break;
          const next = sanitizeUsername(data.username);
          if (!next) {
            send(ws, {
              t: 'error',
              text: `Username must be ${MIN_USERNAME_LEN}-${MAX_USERNAME_LEN} characters: letters, numbers, _ or -`,
            });
            break;
          }
          if (next === state.username) break;

          const previous = state.username;
          state.username = next;
          typingUsers.delete(previous);

          send(ws, { t: 'welcome', username: next, ts: Date.now() });
          await bus.publish({ t: 'typing', user: previous, state: false });
          await bus.publish({
            t: 'sys',
            kind: 'nick',
            text: `${previous} is now ${next}`,
            ts: Date.now(),
          });
          break;
        }

        case 'msg': {
          if (!state.username) break;

          const now = Date.now();
          if (now - state.rateWindowStart > RATE_WINDOW_MS) {
            state.rateWindowStart = now;
            state.rateCount = 0;
          }
          if (++state.rateCount > RATE_MAX_MESSAGES) {
            send(ws, { t: 'error', text: 'Slow down a moment.' });
            break;
          }

          const text = sanitizeMessage(data.text);
          if (!text) break; // empty or whitespace-only submissions are dropped

          typingUsers.delete(state.username);
          await bus.publish({ t: 'typing', user: state.username, state: false });
          await bus.publish({ t: 'msg', user: state.username, text, ts: now });
          break;
        }

        case 'typing': {
          if (!state.username) break;
          const now = Date.now();
          const wantsTyping = Boolean(data.state);
          // Throttle only the "still typing" pings; a stop must always land or
          // the indicator sticks until its TTL expires.
          if (wantsTyping && now - state.lastTypingPublish < TYPING_THROTTLE_MS) break;
          state.lastTypingPublish = now;
          await bus.publish({ t: 'typing', user: state.username, state: wantsTyping });
          break;
        }

        default:
          break;
      }
    } catch (err) {
      console.error('[message]', err.message);
    }
  });

  ws.on('error', (err) => {
    console.error('[socket]', err.message);
  });

  ws.on('close', async () => {
    clients.delete(ws);
    if (!state.username) return; // never joined, so nothing was ever announced

    const username = state.username;
    typingUsers.delete(username);

    try {
      // Presence drops immediately so the counter stays honest; only the
      // announcement waits to see whether this was a reconnect.
      await bus.presenceRemove(state.member);
      await bus.publish({ t: 'typing', user: username, state: false });
      await refreshPresence({ force: true });

      const token = state.id;
      await bus.markLeaving(username, token);

      setTimeout(async () => {
        try {
          if (!(await bus.claimLeaving(username, token))) return; // they came back
          await bus.publish({
            t: 'sys',
            kind: 'leave',
            text: `${username} left the room`,
            ts: Date.now(),
          });
        } catch (err) {
          console.error('[leave]', err.message);
        }
      }, LEAVE_GRACE_MS).unref?.();
    } catch (err) {
      console.error('[disconnect]', err.message);
    }
  });
});

const heartbeat = setInterval(async () => {
  for (const [ws, state] of clients) {
    if (!state.alive) {
      ws.terminate();
      continue;
    }
    state.alive = false;
    try {
      ws.ping();
    } catch {
      /* socket is already gone; the close handler will clean it up */
    }
  }

  const members = [...clients.values()].filter((s) => s.username).map((s) => s.member);
  try {
    await bus.presenceTouch(members);
  } catch (err) {
    console.error('[heartbeat]', err.message);
  }
  await refreshPresence();
}, HEARTBEAT_MS);

const typingSweep = setInterval(pushTyping, TYPING_SWEEP_MS);

heartbeat.unref?.();
typingSweep.unref?.();

// ---------------------------------------------------------------------------
// Shutdown
//
// Vercel recycles instances constantly. Dropping our presence members on the
// way out keeps the counter honest without waiting for the 45s TTL sweep.
// ---------------------------------------------------------------------------

let shuttingDown = false;

async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;

  clearInterval(heartbeat);
  clearInterval(typingSweep);

  const members = [...clients.values()].filter((s) => s.username).map((s) => s.member);
  try {
    await Promise.allSettled(members.map((member) => bus.presenceRemove(member)));
    await bus.close();
  } catch {
    /* best effort: the TTL sweep is the backstop */
  }
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', async () => {
  await shutdown();
  process.exit(0);
});

// Local development only. On Vercel the platform owns the listener and just
// needs the server instance handed back to it.
if (!process.env.VERCEL) {
  const port = Number(process.env.PORT) || 3000;
  server.listen(port, () => {
    console.log(`[nullchat] listening on http://localhost:${port}  (bus: ${bus.mode})`);
  });
}

export default server;
