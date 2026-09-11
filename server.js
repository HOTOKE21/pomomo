/**
 * Pomomo Study/Music Room Server
 *
 * A single-file WebSocket server implementing the Metrolist Listen Together
 * protocol (protobuf over WebSocket) plus a study-session extension
 * (pomodoro / timer / stopwatch) so music sync and study sync live in one room.
 *
 * Deploy (Render):
 *   - New Web Service, point it at this repo, runtime "Node".
 *   - Build command:  npm install
 *   - Start command:  node server.js
 *   - Add a Disk if you want rooms to survive restarts: mount at /var/data
 *     and set PERSIST_PATH=/var/data/rooms.json
 *
 * Env vars:
 *   PORT          - listen port (Render sets this)
 *   PERSIST_PATH  - optional file to persist rooms across restarts
 *   ROOM_TTL_H    - hours an empty room lingers before cleanup (default 24)
 *
 * The app connects with the same protobuf envelope protocol used by the
 * public Metrolist servers; set this server's wss:// URL in
 * Settings -> Listen Together -> Server URL.
 */

const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const protobuf = require('protobufjs');

const PORT = process.env.PORT || 8080;
const PERSIST_PATH = process.env.PERSIST_PATH || '';
const ROOM_TTL_MS = (parseInt(process.env.ROOM_TTL_H, 10) || 24) * 3600 * 1000;

// ---------------------------------------------------------------------------
// Protobuf
// ---------------------------------------------------------------------------

const root = new protobuf.Root();
root.resolvePath = (origin, target) => {
  if (target.startsWith('protobuf/')) {
    // protobufjs field definitions don't reference external files here
    return target;
  }
  return path.join(__dirname, 'proto', target);
};
root.loadSync('listentogether.proto', { keepCase: false });
root.resolveAll();

const Envelope = root.lookupType('listentogether.Envelope');
const RoomState = root.lookupType('listentogether.RoomState');
const StudyState = root.lookupType('listentogether.StudyState');
const TrackInfo = root.lookupType('listentogether.TrackInfo');
const UserInfo = root.lookupType('listentogether.UserInfo');

const CompressionThreshold = 100;

function encodeMessage(type, payloadObj) {
  const MessageType = messageTypeFor(type);
  let payloadBytes = payloadObj && MessageType ? MessageType.encode(payloadObj).finish() : Buffer.alloc(0);
  let compressed = false;
  if (payloadBytes.length > CompressionThreshold) {
    const zlib = require('zlib');
    const gz = zlib.gzipSync(payloadBytes);
    if (gz.length < payloadBytes.length) {
      payloadBytes = gz;
      compressed = true;
    }
  }
  return Envelope.encode(Envelope.create({ type, payload: payloadBytes, compressed })).finish();
}

function decodeEnvelope(data) {
  const env = Envelope.decode(data);
  let payloadBytes = env.payload ? Buffer.from(env.payload) : Buffer.alloc(0);
  if (env.compressed) payloadBytes = require('zlib').gunzipSync(payloadBytes);
  let payload = null;
  if (payloadBytes.length > 0) {
    const MessageType = messageTypeFor(env.type);
    if (MessageType) payload = MessageType.decode(payloadBytes);
  }
  return { type: env.type, payload };
}

function messageTypeFor(type) {
  // All payload messages are named <PascalCase(type)>Payload, with one
  // exception: the playback broadcast reuses the client's payload type.
  const special = { sync_playback: 'PlaybackActionPayload' };
  const name = special[type] || PascalCase(type) + 'Payload';
  try {
    return root.lookupType('listentogether.' + name);
  } catch (e) {
    return null;
  }
}

/** int64 fields arrive as long.js objects; normalize everything to a JS number. */
function toNum(v) {
  if (v == null) return 0;
  if (typeof v === 'number') return v;
  if (typeof v.toNumber === 'function') return v.toNumber();
  return Number(v);
}

function PascalCase(s) {
  return s.split('_').map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join('');
}

// ---------------------------------------------------------------------------
// Room model
// ---------------------------------------------------------------------------

const rooms = new Map(); // roomCode -> Room
const sessionTokens = new Map(); // token -> { roomCode, userId }

function makeRoomCode() {
  const alphabet = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
  for (;;) {
    let code = '';
    for (let i = 0; i < 6; i++) code += alphabet[crypto.randomInt(alphabet.length)];
    if (!rooms.has(code)) return code;
  }
}

function makeUserId() {
  return 'user_' + crypto.randomBytes(8).toString('hex');
}

function defaultStudy() {
  return {
    mode: 'pomodoro',
    phase: 'focus',
    isRunning: false,
    durationMs: 25 * 60 * 1000,
    baseRemainingMs: 25 * 60 * 1000,
    anchorServerMs: 0,
    updatedAt: 0,
    completedFocus: 0,
    revision: 0,
    finished: false,
    focusMs: 25 * 60 * 1000,
    breakMs: 5 * 60 * 1000,
    longBreakMs: 15 * 60 * 1000,
    longBreakAfter: 4,
  };
}

function createRoom() {
  const code = makeRoomCode();
  const room = {
    code,
    hostId: null,
    createdAt: Date.now(),
    lastEmptyAt: Date.now(),
    users: new Map(), // userId -> { userId, username, sockets:Set, sessionToken, isHost, isGuest }
    joinQueue: [],    // pending join requests awaiting host approval
    pendingSuggestions: new Map(), // suggestionId -> { trackInfo, fromUserId, fromUsername }
    // Music sync state (host-authoritative)
    currentTrack: null,
    isPlaying: false,
    position: 0,
    lastUpdate: Date.now(),
    volume: 1,
    queue: [],
    revision: 0n,
    // Study session state (host-authoritative)
    study: defaultStudy(),
  };
  rooms.set(code, room);
  return room;
}

function studyRemainingMs(room) {
  const s = room.study;
  if (!s.isRunning || !s.anchorServerMs) return s.baseRemainingMs;
  const elapsed = Date.now() - s.anchorServerMs;
  return Math.max(0, s.baseRemainingMs - elapsed);
}

function buildRoomState(room) {
  const state = {
    roomCode: room.code,
    hostId: room.hostId || '',
    users: [...room.users.values()].map((u) =>
      UserInfo.create({ userId: u.userId, username: u.username, isHost: u.userId === room.hostId, isConnected: true })
    ),
    isPlaying: room.isPlaying,
    position: room.position,
    lastUpdate: room.lastUpdate,
    volume: room.volume,
    queue: room.queue,
    revision: room.revision,
    study: StudyState.create({
      mode: room.study.mode,
      phase: room.study.phase,
      isRunning: room.study.isRunning,
      durationMs: room.study.durationMs,
      baseRemainingMs: room.study.baseRemainingMs,
      anchorServerMs: room.study.anchorServerMs,
      updatedAt: room.study.updatedAt,
      completedFocus: room.study.completedFocus,
      revision: room.study.revision,
      finished: !!room.study.finished,
      focusMs: room.study.focusMs,
      breakMs: room.study.breakMs,
      longBreakMs: room.study.longBreakMs,
      longBreakAfter: room.study.longBreakAfter,
    }),
  };
  if (room.currentTrack) state.currentTrack = room.currentTrack;
  return RoomState.create(state);
}

// ---------------------------------------------------------------------------
// Persistence (optional)
// ---------------------------------------------------------------------------

function persistRooms() {
  if (!PERSIST_PATH) return;
  try {
    const data = [];
    for (const room of rooms.values()) {
      data.push({
        code: room.code,
        hostId: room.hostId,
        createdAt: room.createdAt,
        lastEmptyAt: room.lastEmptyAt,
        users: [],
        joinQueue: [],
        currentTrack: room.currentTrack,
        isPlaying: false,
        position: room.position,
        lastUpdate: room.lastUpdate,
        volume: room.volume,
        queue: room.queue,
        revision: String(room.revision),
        study: { ...room.study, revision: String(room.study.revision) },
      });
    }
    fs.mkdirSync(path.dirname(PERSIST_PATH), { recursive: true });
    fs.writeFileSync(PERSIST_PATH, JSON.stringify(data));
  } catch (e) {
    console.error('[persist] failed:', e.message);
  }
}

function loadPersistedRooms() {
  if (!PERSIST_PATH) return;
  try {
    if (!fs.existsSync(PERSIST_PATH)) return;
    const data = JSON.parse(fs.readFileSync(PERSIST_PATH, 'utf8'));
    for (const r of data) {
      const room = {
        code: r.code,
        hostId: r.hostId,
        createdAt: r.createdAt,
        lastEmptyAt: r.lastEmptyAt,
        users: new Map(),
        joinQueue: [],
        pendingSuggestions: new Map(),
        currentTrack: r.currentTrack || null,
        isPlaying: false,
        position: r.position || 0,
        lastUpdate: r.lastUpdate || Date.now(),
        volume: r.volume ?? 1,
        queue: r.queue || [],
        revision: Number(r.revision || '0'),
        study: { ...defaultStudy(), ...(r.study || {}), revision: Number(r.study?.revision || '0') },
      };
      rooms.set(room.code, room);
    }
    if (rooms.size) console.log(`[persist] restored ${rooms.size} room(s)`);
  } catch (e) {
    console.error('[persist] load failed:', e.message);
  }
}

// ---------------------------------------------------------------------------
// WebSocket server
// ---------------------------------------------------------------------------

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, uptime: process.uptime() }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const { WebSocketServer } = require('ws');
const wss = new WebSocketServer({ server, path: '/ws' });

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function send(socket, type, payloadObj) {
  if (socket.readyState !== 1) return;
  try {
    socket.send(encodeMessage(type, payloadObj));
  } catch (e) {
    log('send error', type, e.message);
  }
}

function broadcast(room, type, payloadObj, exceptUserId) {
  for (const u of room.users.values()) {
    if (exceptUserId && u.userId === exceptUserId) continue;
    for (const s of u.sockets) send(s, type, payloadObj);
  }
}

function nextRevision(room) {
  room.revision = (typeof room.revision === 'bigint' ? Number(room.revision) : room.revision) + 1;
  return room.revision;
}

function nextStudyRevision(room) {
  room.study.revision = (typeof room.study.revision === 'bigint' ? Number(room.study.revision) : room.study.revision) + 1;
  return room.study.revision;
}

function handleCreateRoom(socket, payload) {
  const room = createRoom();
  const userId = makeUserId();
  const username = (payload && payload.username) || 'Host';
  const token = 'sess_' + crypto.randomBytes(16).toString('hex');
  const user = {
    userId,
    username,
    sockets: new Set([socket]),
    sessionToken: token,
  };
  room.users.set(userId, user);
  room.hostId = userId;
  sessionTokens.set(token, { roomCode: room.code, userId });
  socket.roomCode = room.code;
  socket.userId = userId;

  send(socket, 'room_created', { roomCode: room.code, userId, sessionToken: token });
  broadcastStudyState(room);
  log(`room ${room.code} created by ${username} (${userId})`);
}

function handleJoinRoom(socket, payload) {
  const code = (payload.roomCode || '').toUpperCase().trim();
  const room = rooms.get(code);
  if (!room) {
    send(socket, 'join_rejected', { reason: 'invalid room code' });
    return;
  }
  const userId = makeUserId();
  const username = (payload && payload.username) || 'Guest';

  room.joinQueue.push({ socket, userId, username, at: Date.now() });
  if (room.hostId && room.users.has(room.hostId)) {
    // Ask the host for approval
    const host = room.users.get(room.hostId);
    for (const s of host.sockets) send(s, 'join_request', { userId, username });
  } else {
    // No host online (fresh empty room, restored room, or everyone left):
    // auto-approve and restore host role so the room is controllable again.
    const entry = room.joinQueue.find((j) => j.userId === userId);
    approveJoin(socket, room, entry);
    if (entry && (!room.hostId || !room.users.has(room.hostId))) {
      room.hostId = entry.userId;
      broadcast(room, 'host_changed', { newHostId: entry.userId, newHostName: entry.username });
      log(`host of ${room.code} restored to ${entry.username}`);
    }
  }
}

function approveJoin(requesterSocket, room, entry) {
  if (!entry) return;
  room.joinQueue = room.joinQueue.filter((j) => j.userId !== entry.userId);
  const token = 'sess_' + crypto.randomBytes(16).toString('hex');
  const user = { userId: entry.userId, username: entry.username, sockets: new Set([entry.socket]), sessionToken: token };
  room.users.set(entry.userId, user);
  room.lastEmptyAt = null;
  sessionTokens.set(token, { roomCode: room.code, userId: entry.userId });
  entry.socket.roomCode = room.code;
  entry.socket.userId = entry.userId;

  send(entry.socket, 'join_approved', {
    roomCode: room.code,
    userId: entry.userId,
    sessionToken: token,
    state: buildRoomState(room),
  });
  broadcast(room, 'user_joined', { userId: entry.userId, username: entry.username }, entry.userId);
  log(`${entry.username} joined room ${room.code}`);
}

function handleLeaveRoom(socket) {
  const room = rooms.get(socket.roomCode);
  if (!room) return;
  const user = room.users.get(socket.userId);
  if (!user) return;
  user.sockets.delete(socket);
  if (user.sockets.size === 0) {
    room.users.delete(socket.userId);
    if (room.users.size === 0) {
      room.lastEmptyAt = Date.now();
    } else {
      broadcast(room, 'user_left', { userId: socket.userId, username: user.username });
      if (room.hostId === socket.userId) {
        const next = [...room.users.values()][0];
        room.hostId = next.userId;
        broadcast(room, 'host_changed', { newHostId: next.userId, newHostName: next.username });
        log(`host of ${room.code} transferred to ${next.username}`);
      }
    }
  }
  socket.roomCode = null;
  socket.userId = null;
}

function handlePlaybackAction(socket, room, payload) {
  if (!payload) return;
  // Party mode: anyone can drive playback. Last tap wins; the server is the
  // single serializer so simultaneous taps resolve deterministically.
  const serverTime = Date.now();
  const revision = nextRevision(room);

  const actionPos = toNum(payload.position);
  const capturedAt = toNum(payload.capturedAtServerTime);

  switch (payload.action) {
    case 'play':
      room.isPlaying = true;
      if (payload.position != null) room.position = actionPos;
      room.lastUpdate = capturedAt > 0 ? capturedAt : serverTime;
      break;
    case 'pause':
      room.isPlaying = false;
      if (payload.position != null) room.position = actionPos;
      room.lastUpdate = capturedAt > 0 ? capturedAt : serverTime;
      break;
    case 'seek':
      if (payload.position != null) room.position = actionPos;
      room.lastUpdate = capturedAt > 0 ? capturedAt : serverTime;
      break;
    case 'skip_next':
    case 'skip_prev':
    case 'change_track': {
      if (payload.trackInfo) {
        room.currentTrack = TrackInfo.create(payload.trackInfo);
        room.position = 0;
        room.isPlaying = false;
        room.lastUpdate = serverTime;
      }
      if (payload.queue && payload.queue.length > 0) {
        room.queue = payload.queue.map((t) => TrackInfo.create(t));
      }
      break;
    }
    case 'queue_add': {
      if (payload.trackInfo) {
        const t = TrackInfo.create(payload.trackInfo);
        if (payload.queue && payload.queue.length > 0) {
          room.queue = payload.queue.map((x) => TrackInfo.create(x));
        } else if (payload.insertNext) {
          room.queue.unshift(t);
        } else {
          room.queue.push(t);
        }
      }
      break;
    }
    case 'queue_remove': {
      const id = payload.trackId;
      if (payload.queue && payload.queue.length > 0) {
        room.queue = payload.queue.map((x) => TrackInfo.create(x));
      } else if (id) {
        room.queue = room.queue.filter((t) => t.id !== id);
      }
      break;
    }
    case 'queue_clear':
      room.queue = [];
      break;
    case 'sync_queue':
      if (payload.queue) room.queue = payload.queue.map((t) => TrackInfo.create(t));
      break;
    case 'set_volume':
      if (payload.volume != null) room.volume = payload.volume;
      break;
    default:
      break;
  }

  const out = {
    action: payload.action,
    trackId: payload.trackId || '',
    position: actionPos,
    insertNext: !!payload.insertNext,
    queueTitle: payload.queueTitle || '',
    volume: payload.volume != null ? payload.volume : 1,
    serverTime,
    revision,
    capturedAtServerTime: room.lastUpdate,
  };
  if (payload.trackInfo) out.trackInfo = payload.trackInfo;
  if (payload.queue && payload.queue.length > 0) out.queue = room.queue;
  broadcast(room, 'sync_playback', out, socket.userId);
}

function handleStudyAction(socket, room, payload) {
  if (!payload || !payload.action) return;
  // Party mode: anyone can control the shared study session.
  const now = Date.now();
  const s = room.study;
  const remaining = studyRemainingMs(room);

  switch (payload.action) {
    case 'set_pomodoro': {
      // Per-phase pomodoro config: focus length, break length, long-break
      // length and sessions-per-long-break. Only while not running.
      if (!s.isRunning) {
        const focusMs = toNum(payload.focusMs);
        const breakMs = toNum(payload.breakMs);
        const longBreakMs = toNum(payload.longBreakMs);
        const longBreakAfter = toNum(payload.longBreakAfter);
        if (focusMs > 0) s.focusMs = focusMs;
        if (breakMs > 0) s.breakMs = breakMs;
        if (longBreakMs > 0) s.longBreakMs = longBreakMs;
        if (longBreakAfter > 0) s.longBreakAfter = longBreakAfter;
        if (s.phase === 'focus') {
          s.durationMs = s.focusMs;
          s.baseRemainingMs = s.focusMs;
        } else if (s.phase === 'long_break') {
          s.durationMs = s.longBreakMs;
          s.baseRemainingMs = s.longBreakMs;
        } else {
          s.durationMs = s.breakMs;
          s.baseRemainingMs = s.breakMs;
        }
        s.anchorServerMs = 0;
      }
      break;
    }
    case 'set_mode':
      if (payload.mode && ['pomodoro', 'timer', 'stopwatch'].includes(payload.mode)) {
        s.mode = payload.mode;
        s.isRunning = false;
        s.anchorServerMs = 0;
        s.phase = 'focus';
        s.completedFocus = 0;
        if (payload.mode === 'stopwatch') {
          s.durationMs = 0;
          s.baseRemainingMs = 0;
        } else if (payload.mode === 'timer') {
          s.durationMs = 5 * 60 * 1000;
          s.baseRemainingMs = s.durationMs;
        } else {
          s.durationMs = s.focusMs;
          s.baseRemainingMs = s.durationMs;
        }
      }
      break;
    case 'set_duration': {
      const dur = toNum(payload.durationMs);
      if (dur > 0 && !s.isRunning) {
        s.durationMs = dur;
        s.baseRemainingMs = dur;
        s.anchorServerMs = 0;
      }
      break;
    }
    case 'start':
      if (!s.isRunning) {
        if (s.mode !== 'stopwatch' && s.baseRemainingMs <= 0) {
          s.baseRemainingMs = s.durationMs;
        }
        s.anchorServerMs = now;
        s.isRunning = true;
      }
      break;
    case 'pause':
      if (s.isRunning) {
        if (s.mode === 'stopwatch') {
          // Elapsed time is stored negated (shared anchor formula:
          // display = −baseRemaining while not running). Must come from the
          // raw unclamped delta — the clamped helper would freeze it at 0.
          s.baseRemainingMs = s.anchorServerMs > 0
            ? Math.min(0, s.baseRemainingMs - (now - s.anchorServerMs))
            : s.baseRemainingMs;
        } else {
          s.baseRemainingMs = remaining;
        }
        s.isRunning = false;
        s.anchorServerMs = 0;
      }
      break;
    case 'reset':
      s.isRunning = false;
      s.anchorServerMs = 0;
      if (s.mode === 'stopwatch') {
        s.baseRemainingMs = 0;
      } else {
        s.baseRemainingMs = s.durationMs;
        s.phase = 'focus';
      }
      break;
    case 'advance':
      s.isRunning = false;
      s.anchorServerMs = 0;
      if (s.mode === 'pomodoro') {
        if (s.phase === 'focus') {
          s.completedFocus += 1;
          s.phase = s.completedFocus % (s.longBreakAfter || 4) === 0 ? 'long_break' : 'break';
          s.durationMs = s.phase === 'long_break' ? s.longBreakMs : s.breakMs;
        } else {
          s.phase = 'focus';
          s.durationMs = s.focusMs;
        }
        s.baseRemainingMs = s.durationMs;
      } else if (s.mode === 'timer') {
        s.baseRemainingMs = s.durationMs;
      } else {
        s.baseRemainingMs = 0;
      }
      break;
    default:
      return;
  }

  s.updatedAt = now;
  s.finished = false; // any manual action silences the end-of-phase alert
  broadcastStudyState(room);
  log(`room ${room.code} study ${payload.action} (mode=${s.mode} running=${s.isRunning})`);
}

function broadcastStudyState(room) {
  const revision = nextStudyRevision(room);
  const payload = {
    state: StudyState.create({
      mode: room.study.mode,
      phase: room.study.phase,
      isRunning: room.study.isRunning,
      durationMs: room.study.durationMs,
      baseRemainingMs: room.study.baseRemainingMs,
      anchorServerMs: room.study.anchorServerMs,
      updatedAt: room.study.updatedAt,
      completedFocus: room.study.completedFocus,
      revision,
      finished: !!room.study.finished,
    }),
    serverTime: Date.now(),
    revision,
  };
  // Sent to everyone INCLUDING the host: the host applied an optimistic local
  // echo for instant feedback and reconciles with this authoritative state.
  broadcast(room, 'sync_study', payload);
}

function handleReconnect(socket, payload) {
  if (!payload || !payload.sessionToken) {
    send(socket, 'error', { code: 'session_not_found', message: 'Missing session token' });
    return;
  }
  const sess = sessionTokens.get(payload.sessionToken);
  if (!sess) {
    send(socket, 'error', { code: 'session_not_found', message: 'Session expired' });
    return;
  }
  const room = rooms.get(sess.roomCode);
  const user = room && room.users.get(sess.userId);
  if (!room || !user) {
    sessionTokens.delete(payload.sessionToken);
    send(socket, 'error', { code: 'session_not_found', message: 'Room no longer exists' });
    return;
  }
  user.sockets.add(socket);
  socket.roomCode = room.code;
  socket.userId = user.userId;
  send(socket, 'reconnected', {
    roomCode: room.code,
    userId: user.userId,
    state: buildRoomState(room),
    isHost: room.hostId === user.userId,
  });
  broadcast(room, 'user_reconnected', { userId: user.userId, username: user.username }, user.userId);
  log(`${user.username} reconnected to ${room.code}`);
}

function handleBufferReady(socket, room, payload) {
  // Buffer-sync protocol: with a host-authoritative model the server simply
  // acknowledges; the app's host never blocks on guest buffering unless the
  // server sends buffer_wait. Keep it minimal: skip waiting entirely.
  send(socket, 'buffer_complete', { trackId: payload ? payload.trackId : '' });
}

wss.on('connection', (socket) => {
  socket.isAlive = true;
  socket.on('pong', () => { socket.isAlive = true; });

  socket.on('message', (data) => {
    let msg;
    try {
      msg = decodeEnvelope(Buffer.from(data));
    } catch (e) {
      log('bad envelope:', e.message);
      return;
    }

    const room = socket.roomCode ? rooms.get(socket.roomCode) : null;

    try {
      switch (msg.type) {
        case 'create_room':
          handleCreateRoom(socket, msg.payload);
          break;
        case 'join_room':
          handleJoinRoom(socket, msg.payload);
          break;
        case 'approve_join': {
          if (!room) break;
          const entry = room.joinQueue.find((j) => j.userId === (msg.payload && msg.payload.userId));
          if (entry) approveJoin(socket, room, entry);
          else send(socket, 'error', { code: 'unknown_user', message: 'No such join request' });
          break;
        }
        case 'reject_join': {
          if (!room) break;
          const entry = room.joinQueue.find((j) => j.userId === (msg.payload && msg.payload.userId));
          room.joinQueue = room.joinQueue.filter((j) => j !== entry);
          if (entry && entry.socket.readyState === 1) {
            send(entry.socket, 'join_rejected', { reason: (msg.payload && msg.payload.reason) || '' });
          }
          break;
        }
        case 'leave_room':
          handleLeaveRoom(socket);
          break;
        case 'playback_action':
          if (room) handlePlaybackAction(socket, room, msg.payload);
          break;
        case 'study_action':
          if (room) handleStudyAction(socket, room, msg.payload);
          break;
        case 'buffer_ready':
          if (room) handleBufferReady(socket, room, msg.payload);
          break;
        case 'ping': {
          const now = Date.now();
          send(socket, 'pong', {
            clientTime: toNum(msg.payload && msg.payload.clientTime),
            serverReceiveTime: now,
            serverSendTime: now,
            sequence: toNum(msg.payload && msg.payload.sequence),
          });
          break;
        }
        case 'request_sync':
          if (room) send(socket, 'sync_state', syncStatePayload(room));
          break;
        case 'reconnect':
          handleReconnect(socket, msg.payload);
          break;
        case 'kick_user': {
          if (!room || room.hostId !== socket.userId) break;
          const target = room.users.get(msg.payload && msg.payload.userId);
          if (!target) break;
          for (const s of target.sockets) send(s, 'kicked', { reason: (msg.payload && msg.payload.reason) || '' });
          for (const s of target.sockets) { s.roomCode = null; s.userId = null; }
          room.users.delete(target.userId);
          broadcast(room, 'user_left', { userId: target.userId, username: target.username });
          break;
        }
        case 'transfer_host': {
          if (!room || room.hostId !== socket.userId) break;
          const target = room.users.get(msg.payload && msg.payload.newHostId);
          if (!target) break;
          room.hostId = target.userId;
          broadcast(room, 'host_changed', { newHostId: target.userId, newHostName: target.username });
          break;
        }
        case 'suggest_track': {
          if (!room || !msg.payload || !msg.payload.trackInfo) break;
          if (socket.userId === room.hostId) break; // host doesn't suggest
          const fromUser = room.users.get(socket.userId);
          const suggestionId = 'sug_' + crypto.randomBytes(6).toString('hex');
          room.pendingSuggestions.set(suggestionId, {
            trackInfo: msg.payload.trackInfo,
            fromUserId: socket.userId,
            fromUsername: (fromUser && fromUser.username) || 'Guest',
          });
          const host = room.users.get(room.hostId);
          if (host) {
            for (const s of host.sockets) {
              send(s, 'suggestion_received', {
                suggestionId,
                fromUserId: socket.userId,
                fromUsername: (fromUser && fromUser.username) || 'Guest',
                trackInfo: msg.payload.trackInfo,
              });
            }
          }
          log(`room ${room.code} suggestion from ${(fromUser && fromUser.username) || '?'}`);
          break;
        }
        case 'approve_suggestion': {
          if (!room || room.hostId !== socket.userId) break;
          const sugId = msg.payload && msg.payload.suggestionId;
          const sug = room.pendingSuggestions.get(sugId);
          if (!sug) break;
          room.pendingSuggestions.delete(sugId);

          // Insert next after the current track and broadcast the new queue.
          room.queue.unshift(TrackInfo.create(sug.trackInfo));
          nextRevision(room);
          broadcast(room, 'sync_playback', {
            action: 'queue_add',
            trackInfo: sug.trackInfo,
            insertNext: true,
            queue: room.queue,
            serverTime: Date.now(),
            revision: room.revision,
          });

          // Tell everyone (clients use this to dismiss their UI/notification).
          broadcast(room, 'suggestion_approved', { suggestionId: sugId, trackInfo: sug.trackInfo });
          log(`room ${room.code} suggestion approved -> queue`);
          break;
        }
        case 'reject_suggestion': {
          if (!room || room.hostId !== socket.userId) break;
          const rejId = msg.payload && msg.payload.suggestionId;
          const sugInfo = room.pendingSuggestions.get(rejId);
          if (!sugInfo) break;
          room.pendingSuggestions.delete(rejId);
          broadcast(room, 'suggestion_rejected', {
            suggestionId: rejId,
            reason: (msg.payload && msg.payload.reason) || '',
          });
          break;
        }
        default:
          // chat etc: unknown types are ignored
          break;
      }
    } catch (e) {
      log('handler error', msg.type, e.message);
    }
  });

  socket.on('close', () => {
    if (socket.roomCode) handleLeaveRoom(socket);
  });
});

function syncStatePayload(room) {
  const payload = {
    isPlaying: room.isPlaying,
    position: room.position,
    lastUpdate: room.lastUpdate,
    volume: room.volume,
    revision: room.revision,
    queue: room.queue,
  };
  if (room.currentTrack) payload.currentTrack = room.currentTrack;
  return payload;
}

// Heartbeat: drop dead sockets
setInterval(() => {
  for (const socket of wss.clients) {
    if (!socket.isAlive) { socket.terminate(); continue; }
    socket.isAlive = false;
    try { socket.ping(); } catch (e) { /* ignore */ }
  }
}, 30000);

// Room cleanup + study ticking / completion
setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    const s = room.study;
    if (s.isRunning && s.anchorServerMs) {
      const remaining = studyRemainingMs(room);
      if (s.mode !== 'stopwatch' && remaining <= 0) {
        // Phase finished: stop, flag it (clients play the end sound) and
        // auto-advance the pomodoro to the next phase, paused and ready.
        s.baseRemainingMs = 0;
        s.isRunning = false;
        s.anchorServerMs = 0;
        s.updatedAt = now;
        s.finished = true;
        if (s.mode === 'pomodoro') {
          if (s.phase === 'focus') {
            s.completedFocus += 1;
            s.phase = s.completedFocus % (s.longBreakAfter || 4) === 0 ? 'long_break' : 'break';
            s.durationMs = s.phase === 'long_break' ? s.longBreakMs : s.breakMs;
          } else {
            s.phase = 'focus';
            s.durationMs = s.focusMs;
          }
          s.baseRemainingMs = s.durationMs;
        }
        broadcastStudyState(room);
        log(`room ${room.code} study phase finished (${s.mode}/${s.phase})`);
      } else if (!s.lastTickBroadcast || now - s.lastTickBroadcast >= 1000) {
        // Keep the countdown live: push a fresh anchor every second so every
        // client recomputes from server time without user interaction.
        s.lastTickBroadcast = now;
        broadcastStudyState(room);
      }
    }
    if (room.users.size === 0 && room.lastEmptyAt && now - room.lastEmptyAt > ROOM_TTL_MS) {
      rooms.delete(room.code);
      log(`room ${room.code} expired`);
    }
  }
  persistRooms();
}, 1000);

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

loadPersistedRooms();
server.listen(PORT, () => {
  log(`Pomomo room server listening on port ${PORT} (ws path: /ws)`);
});
