'use strict';
/* =====================================================================
   WILDERNESS - multiplayer server
   Node.js + Express (static files) + Socket.IO (real time).

   Server-authoritative:  rooms, player registry, world clock + weather,
   resource claims (a tree/rock/bush/log can only be taken once), building /
   structure placement, chest locking + contents, dropped items, wildlife
   ownership + kills.  Everything the clients send is validated, clamped and
   rate limited; rooms never see each other's traffic.
===================================================================== */
const path = require('path');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { Server } = require('socket.io');

/* ------------------------------ config ------------------------------ */
const PORT = parseInt(process.env.PORT, 10) || 3000;
const MAX_PLAYERS = clampInt(process.env.MAX_PLAYERS, 2, 16, 8);
const MAX_ROOMS = clampInt(process.env.MAX_ROOMS, 1, 5000, 300);
const RECONNECT_GRACE_MS = 20000;          // a dropped player may come back within this time
const EMPTY_ROOM_TTL_MS = 120000;          // empty rooms are deleted after this
const TICK_HZ = 15;                        // player / wildlife broadcast rate
const DAY_LEN = 600;                       // real seconds per in-game day (matches the game)
const CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';   // no 0/O/1/I
const COLORS = ['#e5483e', '#3e8fe5', '#e5b93e', '#5bc24a', '#b05be5', '#e57f3e', '#3ee5c9', '#e53ea0', '#9aa5b1', '#8be53e'];
const CORS_ORIGIN = (process.env.CORS_ORIGIN || '*').split(',').map(s => s.trim()).filter(Boolean);

function clampInt(v, lo, hi, d) { v = parseInt(v, 10); return Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : d; }

/* ------------------------------ http ------------------------------ */
const app = express();
app.disable('x-powered-by');
app.set('trust proxy', 1);
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: CORS_ORIGIN.includes('*') ? '*' : CORS_ORIGIN, methods: ['GET', 'POST'] },
  maxHttpBufferSize: 2e6,
  pingInterval: 10000,
  pingTimeout: 15000,
  transports: ['websocket', 'polling']
});

const rooms = new Map();           // code -> Room
app.get('/health', (req, res) => res.json({ ok: true, rooms: rooms.size, uptime: Math.round(process.uptime()) }));
app.get('/api/room/:code', (req, res) => {
  const r = rooms.get(String(req.params.code || '').toUpperCase());
  res.json(r ? { exists: true, players: onlineCount(r), max: MAX_PLAYERS } : { exists: false });
});
app.use(express.static(path.join(__dirname, 'public'), { extensions: ['html'], maxAge: '1h', setHeaders: (res, p) => { if (p.endsWith('.html')) res.setHeader('Cache-Control', 'no-cache'); } }));

/* ------------------------------ helpers ------------------------------ */
const now = () => Date.now();
const isNum = v => typeof v === 'number' && Number.isFinite(v);
const num = (v, lim, d) => (isNum(v) ? Math.max(-lim, Math.min(lim, v)) : (d === undefined ? 0 : d));
const round = (v, n) => { const m = Math.pow(10, n); return Math.round(v * m) / m; };
const str = (v, max) => (typeof v === 'string' ? v.slice(0, max) : '');
const KEY_RE = /^[a-z]:-?\d{1,8},-?\d{1,8}$/;
const PKEY_RE = /^[A-Za-z0-9:.,_\-]{1,48}$/;
const ID_RE = /^[a-z0-9_]{1,40}$/;
const NID_RE = /^[A-Za-z0-9.\-_]{1,40}$/;
const DIM_RE = /^[a-z0-9:_\-]{0,20}$/;
const PIECE_KINDS = new Set(['found', 'wall', 'door', 'floor', 'roof', 'chest']);
const MACHINE_IDS = new Set(['sawmill', 'rain_barrel', 'turret', 'jump_pad']);
const ANIMAL_KINDS = new Set(['deer', 'wolf', 'gorilla', 'cyclops']);
const FX_KINDS = new Set(['burst', 'arrow', 'spell', 'sound']);
const MAX_PIECES = 4000, MAX_STRUCTS = 600, MAX_DROPS = 400, MAX_ANIMALS = 700;
const TRACKED = { tree: 1, rock: 1, bush: 1, log: 1 };
const ROCK_MAX_HP = 3;

function sanitizeName(s) {
  s = String(s == null ? '' : s).normalize('NFKC').replace(/[^\w .\-]/g, '').replace(/\s+/g, ' ').trim().slice(0, 16);
  return s;
}
function sanitizeChat(s) {
  return String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, 160);
}
function genCode() {
  for (let tries = 0; tries < 200; tries++) {
    let c = '';
    const b = crypto.randomBytes(5);
    for (let i = 0; i < 5; i++) c += CODE_ALPHABET[b[i] % CODE_ALPHABET.length];
    if (!rooms.has(c)) return c;
  }
  return null;
}
function onlineCount(room) { let n = 0; room.players.forEach(p => { if (p.connected) n++; }); return n; }
function dist2(ax, az, bx, bz) { const dx = ax - bx, dz = az - bz; return dx * dx + dz * dz; }

/* simple token bucket per socket + category */
function allow(sock, cat, perSec, burst) {
  const b = sock.data.buckets || (sock.data.buckets = {});
  const t = now();
  let k = b[cat];
  if (!k) k = b[cat] = { tokens: burst, t };
  k.tokens = Math.min(burst, k.tokens + (t - k.t) / 1000 * perSec); k.t = t;
  if (k.tokens < 1) return false;
  k.tokens -= 1; return true;
}

/* ------------------------------ rooms ------------------------------ */
function newRoom(code) {
  return {
    code, createdAt: now(), emptySince: null, players: new Map(), nextColor: 0,
    clock: { hour: 8, day: 1, t0: now() },
    weather: { rain: 0, until: now() + 90000 + Math.random() * 90000 },
    trees: new Set(), logs: new Set(),
    rocks: new Map(),         // key -> { hp, next }
    bushes: new Map(),        // key -> regrowAt
    pieces: new Map(),        // key -> piece
    structs: new Map(),       // id -> struct (fire / table / machine)
    chests: new Map(),        // key -> { slots, ver, lock }
    drops: new Map(),         // did -> drop
    nextId: 1,
    animals: new Map(),       // nid -> animal record
    gens: {},                 // camp slot respawn counters
    deadL: new Set(),         // world-generated animals that were killed
    animalOut: new Map()      // nid -> pending compact record to broadcast
  };
}
function roomTime(room) {
  const el = (now() - room.clock.t0) / 1000;
  let h = room.clock.hour + el * 24 / DAY_LEN, d = room.clock.day;
  while (h >= 24) { h -= 24; d++; }
  return { hour: round(h, 3), day: d };
}
function pubPlayer(p) { return { id: p.pid, name: p.name, color: p.color, x: p.x, y: p.y, z: p.z, yaw: p.yaw, held: p.held, dim: p.dim, hp: p.hp }; }
function snapshot(room, forPid) {
  const me = room.players.get(forPid);
  const animals = [];
  room.animals.forEach(a => animals.push([a.nid, a.k, a.v, round(a.x, 2), round(a.y, 2), round(a.z, 2), round(a.yaw, 2), Math.round(a.hp), a.flags | 0, a.dead ? 1 : 0, a.owner || '']));
  return {
    time: roomTime(room), rain: room.weather.rain,
    trees: [...room.trees], logs: [...room.logs],
    rocks: [...room.rocks].map(([k, v]) => [k, v.hp]),
    bushes: [...room.bushes.keys()],
    pieces: [...room.pieces.values(), ...[...room.chests.values()].map(c => c.piece)],
    structs: [...room.structs.values()],
    chests: [...room.chests].map(([k, c]) => [k, c.ver]),
    drops: [...room.drops.values()],
    animals, gens: room.gens, deadL: [...room.deadL],
    players: [...room.players.values()].filter(p => p.pid !== forPid && p.connected).map(pubPlayer),
    you: me ? pubPlayer(me) : null
  };
}
function broadcast(room, event, payload, exceptPid) {
  room.players.forEach(p => { if (p.connected && p.socket && p.pid !== exceptPid) p.socket.emit(event, payload); });
}
function nearPlayers(room, x, z, r, exceptPid, fn) {
  const r2 = r * r;
  room.players.forEach(p => { if (p.connected && p.pid !== exceptPid && dist2(p.x, p.z, x, z) <= r2) fn(p); });
}
function uniqueName(room, base) {
  base = base || ('Player' + Math.floor(100 + Math.random() * 900));
  const taken = new Set([...room.players.values()].map(p => p.name.toLowerCase()));
  let n = base, i = 2;
  while (taken.has(n.toLowerCase())) { const suf = ' ' + i++; n = base.slice(0, 16 - suf.length) + suf; }
  return n;
}

function addPlayer(room, socket, name, token) {
  const pid = crypto.randomBytes(4).toString('hex');
  const p = {
    pid, token: token || crypto.randomBytes(12).toString('hex'), name: uniqueName(room, sanitizeName(name)),
    color: COLORS[room.nextColor++ % COLORS.length], socket, connected: true, dropTimer: null,
    x: 0, y: 0, z: 0, yaw: 0, pitch: 0, anim: 0, held: '', hp: 100, dim: '', dirty: true, lastMove: now()
  };
  room.players.set(pid, p);
  room.emptySince = null;
  attach(socket, room, p);
  return p;
}
function attach(socket, room, p) {
  p.socket = socket; p.connected = true;
  socket.data.room = room.code; socket.data.pid = p.pid;
  socket.join('room:' + room.code);
}

/* remove a player for good (leave or grace period over) */
function removePlayer(room, p, reason) {
  if (!room.players.has(p.pid)) return;
  clearTimeout(p.dropTimer);
  room.players.delete(p.pid);
  room.chests.forEach(c => { if (c.lock === p.pid) c.lock = null; });
  broadcast(room, 'player:leave', { id: p.pid, name: p.name, reason: reason || 'left', count: onlineCount(room) });
  reassignAnimals(room, true);
  if (room.players.size === 0) room.emptySince = now();
}
function dropConnection(room, p) {          // unexpected disconnect: keep the slot for a short grace period
  p.connected = false; p.socket = null;
  room.chests.forEach(c => { if (c.lock === p.pid) c.lock = null; });
  broadcast(room, 'player:offline', { id: p.pid, name: p.name, count: onlineCount(room) });
  reassignAnimals(room, true);
  clearTimeout(p.dropTimer);
  p.dropTimer = setTimeout(() => removePlayer(room, p, 'disconnected'), RECONNECT_GRACE_MS);
  if (onlineCount(room) === 0 && room.players.size === 0) room.emptySince = now();
}

/* ------------------------------ wildlife ownership ------------------------------ */
/* Every active animal is simulated by exactly one client (its owner = the nearest player). */
function reassignAnimals(room, force) {
  const live = [...room.players.values()].filter(p => p.connected);
  const changes = [];
  room.animals.forEach(a => {
    if (a.dead) return;
    let best = null, bd = Infinity, cur = null, cd = Infinity;
    for (const p of live) {
      const d = Math.sqrt(dist2(p.x, p.z, a.x, a.z));
      if (d < bd) { bd = d; best = p; }
      if (p.pid === a.owner) { cur = p; cd = d; }
    }
    let owner = a.owner || '';
    if (cur && cd <= 150 && (best === cur || cd - bd < 30)) owner = cur.pid;          // keep (hysteresis)
    else if (best && bd <= 140) owner = best.pid;
    else owner = '';                                                                  // dormant: nobody close
    if (owner !== (a.owner || '')) { a.owner = owner; changes.push([a.nid, owner]); }
  });
  if (changes.length) broadcast(room, 'an:own', changes);
}

/* ------------------------------ socket handlers ------------------------------ */
io.on('connection', socket => {
  socket.data.buckets = {};
  const getCtx = () => {
    const room = rooms.get(socket.data.room), p = room && room.players.get(socket.data.pid);
    return room && p && p.socket === socket ? { room, p } : null;
  };

  socket.on('room:peek', (msg, ack) => {
    if (typeof ack !== 'function') return;
    if (!allow(socket, 'peek', 4, 8)) return ack({ ok: false, error: 'Slow down.' });
    const code = str(msg && msg.code, 8).toUpperCase().replace(/[^A-Z0-9]/g, '');
    const r = rooms.get(code);
    ack({ ok: true, exists: !!r, players: r ? onlineCount(r) : 0, max: MAX_PLAYERS });
  });

  socket.on('room:create', (msg, ack) => {
    if (typeof ack !== 'function') return;
    if (!allow(socket, 'room', 1, 3)) return ack({ ok: false, error: 'Slow down.' });
    if (getCtx()) return ack({ ok: false, error: 'You are already in a room.' });
    if (rooms.size >= MAX_ROOMS) return ack({ ok: false, error: 'The server is full. Try again later.' });
    const code = genCode();
    if (!code) return ack({ ok: false, error: 'Could not create a room.' });
    const room = newRoom(code); rooms.set(code, room);
    const p = addPlayer(room, socket, msg && msg.name);
    ack({ ok: true, code, token: p.token, max: MAX_PLAYERS, snap: snapshot(room, p.pid), count: 1 });
  });

  socket.on('room:join', (msg, ack) => {
    if (typeof ack !== 'function') return;
    if (!allow(socket, 'room', 1, 4)) return ack({ ok: false, error: 'Slow down.' });
    if (getCtx()) return ack({ ok: false, error: 'You are already in a room.' });
    const code = str(msg && msg.code, 8).toUpperCase().replace(/[^A-Z0-9]/g, '');
    if (code.length < 4) return ack({ ok: false, error: 'Enter a room code.' });
    const room = rooms.get(code);
    if (!room) return ack({ ok: false, error: 'No room with that code.' });
    if (onlineCount(room) >= MAX_PLAYERS) return ack({ ok: false, error: 'That room is full (' + MAX_PLAYERS + ' players).' });
    const p = addPlayer(room, socket, msg && msg.name);
    ack({ ok: true, code, token: p.token, max: MAX_PLAYERS, snap: snapshot(room, p.pid), count: onlineCount(room) });
    socket.to('room:' + code).emit('player:join', { player: pubPlayer(p), count: onlineCount(room) });
  });

  /* come back after a dropped connection (same browser tab keeps its token) */
  socket.on('room:rejoin', (msg, ack) => {
    if (typeof ack !== 'function') return;
    if (!allow(socket, 'room', 1, 4)) return ack({ ok: false, error: 'Slow down.' });
    const code = str(msg && msg.code, 8).toUpperCase(), token = str(msg && msg.token, 40);
    const room = rooms.get(code);
    const p = room && [...room.players.values()].find(q => q.token === token);
    if (!p) return ack({ ok: false, error: 'Your seat was released.' });
    if (p.socket && p.socket !== socket) { try { p.socket.disconnect(true); } catch (e) {} }
    clearTimeout(p.dropTimer);
    const wasOffline = !p.connected;
    attach(socket, room, p);
    ack({ ok: true, code, token: p.token, max: MAX_PLAYERS, snap: snapshot(room, p.pid), count: onlineCount(room), rejoin: true });
    if (wasOffline) socket.to('room:' + code).emit('player:join', { player: pubPlayer(p), count: onlineCount(room), back: true });
  });

  socket.on('room:leave', (msg, ack) => {
    const c = getCtx();
    if (c) { socket.leave('room:' + c.room.code); socket.data.room = null; socket.data.pid = null; removePlayer(c.room, c.p, 'left'); }
    if (typeof ack === 'function') ack({ ok: true });
  });

  socket.on('disconnect', () => {
    const c = getCtx();
    if (c) dropConnection(c.room, c.p);
  });

  /* ---------- player state (high frequency) ---------- */
  socket.on('p:u', m => {
    const c = getCtx(); if (!c || !m || typeof m !== 'object') return;
    if (!allow(socket, 'pu', 40, 60)) return;
    const p = c.p;
    if (!isNum(m.x) || !isNum(m.y) || !isNum(m.z)) return;
    p.x = num(m.x, 1e6); p.y = num(m.y, 5000); p.z = num(m.z, 1e6);
    p.yaw = num(m.yaw, 100); p.pitch = num(m.pitch, 4);
    p.anim = (m.anim | 0) & 0xff;
    p.held = ID_RE.test(m.held || '') ? m.held : '';
    p.hp = Math.max(0, Math.min(100, Math.round(num(m.hp, 1000, 100))));
    p.dim = DIM_RE.test(m.dim || '') ? m.dim : '';
    p.dirty = true; p.lastMove = now();
  });

  /* ---------- chat ---------- */
  socket.on('chat', m => {
    const c = getCtx(); if (!c) return;
    if (!allow(socket, 'chat', 1, 4)) return socket.emit('chat:err', 'You are sending messages too fast.');
    const text = sanitizeChat(m && m.text); if (!text) return;
    broadcast(c.room, 'chat', { id: c.p.pid, name: c.p.name, color: c.p.color, text, t: now() });
  });

  /* ---------- cosmetic effects (relayed to nearby players only) ---------- */
  socket.on('fx', m => {
    const c = getCtx(); if (!c || !m) return;
    if (!allow(socket, 'fx', 20, 30)) return;
    const k = str(m.k, 10); if (!FX_KINDS.has(k)) return;
    const out = { k, id: c.p.pid, x: round(num(m.x, 1e6), 2), y: round(num(m.y, 5000), 2), z: round(num(m.z, 1e6), 2), a: str(m.a, 16), n: Math.max(0, Math.min(40, m.n | 0)),
      dx: round(num(m.dx, 200), 2), dy: round(num(m.dy, 200), 2), dz: round(num(m.dz, 200), 2), w: ID_RE.test(m.w || '') ? m.w : '' };
    nearPlayers(c.room, out.x, out.z, 180, c.p.pid, q => q.socket.emit('fx', out));
  });

  /* ---------- resource claims: the first valid claim wins ---------- */
  socket.on('claim', (m, ack) => {
    if (typeof ack !== 'function') return;
    const c = getCtx(); if (!c || !m) return ack({ ok: false });
    if (!allow(socket, 'claim', 8, 16)) return ack({ ok: false, reason: 'rate' });
    const { room, p } = c;
    const t = str(m.t, 6), k = str(m.k, 24);
    if (!TRACKED[t] || !KEY_RE.test(k) || k[0] !== t[0]) return ack({ ok: false, reason: 'bad' });
    const [kx, kz] = k.slice(2).split(',').map(Number);
    if (dist2(p.x, p.z, kx / 10, kz / 10) > 30 * 30) return ack({ ok: false, reason: 'far' });   // reach is ~4 m; allow lag slack
    const ev = { t, k, by: p.pid, px: round(p.x, 1), pz: round(p.z, 1) };
    if (t === 'tree') {
      if (room.trees.has(k)) return ack({ ok: false, reason: 'gone' });
      room.trees.add(k);
    } else if (t === 'log') {
      if (room.logs.has(k)) return ack({ ok: false, reason: 'gone' });
      room.logs.add(k);
    } else if (t === 'bush') {
      if (room.bushes.has(k)) return ack({ ok: false, reason: 'gone' });
      room.bushes.set(k, now() + 80000);
    } else {
      let r = room.rocks.get(k);
      if (!r) r = { hp: ROCK_MAX_HP, next: 0 };
      if (r.hp <= 0) return ack({ ok: false, reason: 'gone' });
      r.hp -= 1; r.next = now() + (r.hp <= 0 ? 55000 : 30000);
      room.rocks.set(k, r); ev.hp = r.hp;
      socket.to('room:' + room.code).emit('w:ev', ev);
      return ack({ ok: true, hpBefore: r.hp + 1, hp: r.hp });
    }
    socket.to('room:' + room.code).emit('w:ev', ev);
    ack({ ok: true });
  });

  /* ---------- placing things in the world ---------- */
  socket.on('place', (m, ack) => {
    if (typeof ack !== 'function') return;
    const c = getCtx(); if (!c || !m) return ack({ ok: false });
    if (!allow(socket, 'place', 4, 10)) return ack({ ok: false, reason: 'rate' });
    const { room, p } = c;
    const type = str(m.type, 8);
    const x = num(m.x, 1e6), z = num(m.z, 1e6), y = num(m.y, 5000);
    if (dist2(p.x, p.z, x, z) > 40 * 40) return ack({ ok: false, reason: 'far' });
    if (type === 'piece') {
      if (!PIECE_KINDS.has(m.kind) || room.pieces.size + room.chests.size >= MAX_PIECES) return ack({ ok: false, reason: 'bad' });
      const o = m.o === 'x' || m.o === 'z' ? m.o : '';
      const key = m.kind === 'chest' ? 'C:' + Math.round(x * 10) + ',' + Math.round(z * 10) + ',' + Math.round(y * 10) : str(m.key, 48);
      if (!PKEY_RE.test(key)) return ack({ ok: false, reason: 'bad' });
      if (room.pieces.has(key) || room.chests.has(key)) return ack({ ok: false, reason: 'occupied' });
      const opt = m.opt && typeof m.opt === 'object' ? { depth: num(m.opt.depth, 100), sg: m.opt.sg === -1 ? -1 : 1 } : {};
      const piece = { type: 'piece', kind: m.kind, o, x: round(x, 3), y: round(y, 3), z: round(z, 3), ci: m.ci | 0, cj: m.cj | 0, L: m.L | 0, f0: round(num(m.f0, 5000), 3), key, opt, by: p.pid };
      if (m.kind === 'chest') room.chests.set(key, { slots: new Array(27).fill(null), ver: 0, lock: null, piece });
      else room.pieces.set(key, piece);
      socket.to('room:' + room.code).emit('w:place', piece);
      return ack({ ok: true, key });
    }
    if (room.structs.size >= MAX_STRUCTS) return ack({ ok: false, reason: 'limit' });
    let id = null;
    for (const s of room.structs.values()) if (dist2(s.x, s.z, x, z) < 0.8 * 0.8) return ack({ ok: false, reason: 'occupied' });
    const yaw = round(num(m.yaw, 100), 3);
    if (type === 'fire' || type === 'table') id = type[0] + (room.nextId++);
    else if (type === 'machine' && MACHINE_IDS.has(m.id)) id = 'm' + (room.nextId++);
    else return ack({ ok: false, reason: 'bad' });
    const s = { type, id, mid: type === 'machine' ? m.id : '', x: round(x, 3), z: round(z, 3), y: round(y, 3), yaw, by: p.pid };
    room.structs.set(id, s);
    socket.to('room:' + room.code).emit('w:place', s);
    ack({ ok: true, id });
  });

  /* ---------- chests: one person at a time, server keeps the contents ---------- */
  socket.on('chest:open', (m, ack) => {
    if (typeof ack !== 'function') return;
    const c = getCtx(); if (!c) return ack({ ok: false });
    if (!allow(socket, 'chest', 6, 10)) return ack({ ok: false, reason: 'rate' });
    const ch = c.room.chests.get(str(m && m.k, 48));
    if (!ch) return ack({ ok: false, reason: 'gone' });
    if (ch.lock && ch.lock !== c.p.pid && c.room.players.get(ch.lock) && c.room.players.get(ch.lock).connected) {
      return ack({ ok: false, reason: 'busy', by: c.room.players.get(ch.lock).name });
    }
    ch.lock = c.p.pid;
    ack({ ok: true, slots: ch.slots, ver: ch.ver });
  });
  socket.on('chest:close', (m, ack) => {
    const c = getCtx(); if (!c || !m) return;
    const ch = c.room.chests.get(str(m.k, 48));
    if (!ch || ch.lock !== c.p.pid) { if (typeof ack === 'function') ack({ ok: false }); return; }
    if (Array.isArray(m.slots) && m.slots.length === 27) {
      const clean = m.slots.map(s => (s && ID_RE.test(s.id || '') && Number.isInteger(s.n) && s.n > 0 && s.n <= 9999 ? { id: s.id, n: s.n } : null));
      ch.slots = clean; ch.ver++;
    }
    ch.lock = null;
    if (typeof ack === 'function') ack({ ok: true, ver: ch.ver });
  });

  /* ---------- items lying on the ground ---------- */
  socket.on('drop:new', (m, ack) => {
    if (typeof ack !== 'function') return;
    const c = getCtx(); if (!c || !m) return ack({ ok: false });
    if (!allow(socket, 'drop', 6, 20)) return ack({ ok: false });
    const { room, p } = c;
    if (!ID_RE.test(m.id || '') || !Number.isInteger(m.n) || m.n < 1 || m.n > 9999 || room.drops.size >= MAX_DROPS) return ack({ ok: false });
    const x = num(m.x, 1e6), z = num(m.z, 1e6);
    if (dist2(p.x, p.z, x, z) > 30 * 30) return ack({ ok: false });
    const did = 'd' + (room.nextId++);
    const d = { did, id: m.id, n: m.n, x: round(x, 2), z: round(z, 2), t: now() };
    room.drops.set(did, d);
    socket.to('room:' + room.code).emit('drop:add', d);
    ack({ ok: true, did });
  });
  socket.on('drop:take', (m, ack) => {
    if (typeof ack !== 'function') return;
    const c = getCtx(); if (!c || !m) return ack({ ok: false });
    if (!allow(socket, 'take', 12, 24)) return ack({ ok: false });
    const { room, p } = c;
    const d = room.drops.get(str(m.did, 12));
    if (!d) return ack({ ok: false });
    if (dist2(p.x, p.z, d.x, d.z) > 12 * 12) return ack({ ok: false });
    const take = Math.max(1, Math.min(d.n, m.n | 0));
    d.n -= take;
    if (d.n <= 0) { room.drops.delete(d.did); broadcast(room, 'drop:rm', { did: d.did }, p.pid); }
    else broadcast(room, 'drop:upd', { did: d.did, n: d.n }, p.pid);
    ack({ ok: true, n: take, left: d.n });
  });

  /* ---------- wildlife ---------- */
  const validAnimal = a => Array.isArray(a) && NID_RE.test(a[0] || '') && ANIMAL_KINDS.has(a[1]);
  /* a client announces animals it created (world-generation ones are deterministic, so ids agree between clients) */
  socket.on('an:reg', list => {
    const c = getCtx(); if (!c || !Array.isArray(list)) return;
    if (!allow(socket, 'anreg', 4, 8)) return;
    const { room, p } = c;
    for (const a of list.slice(0, 400)) {
      if (!validAnimal(a) || room.animals.has(a[0]) || room.animals.size >= MAX_ANIMALS) continue;
      const x = num(a[3], 1e6), z = num(a[4], 1e6);
      if (room.deadL.has(a[0])) continue;                                                       // killed earlier in this room
      const gm = /^(g\d+\.\d+)\.(\d+)$/.exec(a[0]);
      if (gm && (room.gens[gm[1]] || 0) > parseInt(gm[2], 10)) continue;                         // an older camp generation
      room.animals.set(a[0], { nid: a[0], k: a[1], v: str(a[2], 12), x, y: 0, z, yaw: 0, hp: 1, flags: 0, dead: false, owner: /^L|^g/.test(a[0]) ? '' : p.pid });
    }
    reassignAnimals(room);
  });
  /* owner streams the state of the animals it simulates */
  socket.on('an:u', list => {
    const c = getCtx(); if (!c || !Array.isArray(list)) return;
    if (!allow(socket, 'anu', 20, 30)) return;
    const { room, p } = c;
    for (const r of list.slice(0, 120)) {
      if (!Array.isArray(r) || !NID_RE.test(r[0] || '')) continue;
      let a = room.animals.get(r[0]);
      if (!a) {
        if (!ANIMAL_KINDS.has(r[1]) || room.animals.size >= MAX_ANIMALS) continue;
        a = { nid: r[0], k: r[1], v: str(r[2], 12), x: 0, y: 0, z: 0, yaw: 0, hp: 1, flags: 0, dead: false, owner: p.pid };
        room.animals.set(a.nid, a);
      }
      if (a.owner !== p.pid || a.dead) continue;
      a.x = num(r[3], 1e6); a.y = num(r[4], 5000); a.z = num(r[5], 1e6); a.yaw = num(r[6], 100); a.hp = num(r[7], 5000); a.flags = r[8] | 0;
      room.animalOut.set(a.nid, [a.nid, a.k, a.v, round(a.x, 2), round(a.y, 2), round(a.z, 2), round(a.yaw, 2), Math.round(a.hp), a.flags, round(num(r[9], 50), 1)]);
    }
  });
  /* a hit on an animal is forwarded to the client that simulates it */
  socket.on('an:hit', m => {
    const c = getCtx(); if (!c || !m) return;
    if (!allow(socket, 'anhit', 10, 20)) return;
    const a = c.room.animals.get(str(m.nid, 40));
    if (!a || a.dead || !isNum(m.dmg)) return;
    const owner = a.owner && c.room.players.get(a.owner);
    if (dist2(c.p.x, c.p.z, a.x, a.z) > 80 * 80) return;
    const dmg = Math.max(0, Math.min(400, m.dmg));
    if (owner && owner.connected && owner.pid !== c.p.pid) owner.socket.emit('an:hit', { nid: a.nid, dmg, by: c.p.pid });
    else if (!owner || !owner.connected) {                   // dormant animal: the server applies it
      a.hp -= dmg;
      if (a.hp <= 0) killAnimal(c.room, a, c.p.pid);
    }
  });
  const killAnimal = (room, a, by) => {
    if (a.dead) return false;
    a.dead = true; a.hp = 0; room.animalOut.delete(a.nid);
    const mm = /^(g\d+\.\d+)\.(\d+)$/.exec(a.nid);
    if (/^L/.test(a.nid)) room.deadL.add(a.nid);
    if (mm) room.gens[mm[1]] = Math.max(room.gens[mm[1]] || 0, parseInt(mm[2], 10) + 1);
    broadcast(room, 'an:dead', { nid: a.nid, by, gen: mm ? room.gens[mm[1]] : 0, base: mm ? mm[1] : '' });
    setTimeout(() => { if (room.animals.get(a.nid) === a) room.animals.delete(a.nid); }, 8000);
    return true;
  };
  socket.on('an:kill', m => {
    const c = getCtx(); if (!c || !m) return;
    if (!allow(socket, 'ankill', 10, 20)) return;
    const a = c.room.animals.get(str(m.nid, 40));
    if (!a || a.dead) return;
    if (m.by && c.room.players.has(m.by)) killAnimal(c.room, a, m.by); else killAnimal(c.room, a, c.p.pid);
  });
  socket.on('an:rm', m => {            // owner despawned a runtime animal
    const c = getCtx(); if (!c || !m) return;
    const a = c.room.animals.get(str(m.nid, 40));
    if (!a || a.owner !== c.p.pid || /^L|^g/.test(a.nid)) return;
    c.room.animals.delete(a.nid); c.room.animalOut.delete(a.nid);
    broadcast(c.room, 'an:rm', { nid: a.nid }, c.p.pid);
  });

  socket.on('ping:t', (t, ack) => { if (typeof ack === 'function') ack(t); });
});

/* ------------------------------ world tick ------------------------------ */
let tickN = 0;
setInterval(() => {
  tickN++;
  rooms.forEach(room => {
    if (!room.players.size) return;
    // players
    const changed = [];
    room.players.forEach(p => {
      if (p.dirty && p.connected) { p.dirty = false; changed.push([p.pid, round(p.x, 2), round(p.y, 2), round(p.z, 2), round(p.yaw, 2), round(p.pitch, 2), p.anim, p.held, p.hp, p.dim]); }
    });
    if (changed.length) room.players.forEach(q => {
      if (!q.connected || !q.socket) return;
      const list = changed.filter(r => r[0] !== q.pid);
      if (list.length) q.socket.volatile.emit('p:s', list);
    });
    // wildlife: only send what is near the receiver and not simulated by it
    if (room.animalOut.size) {
      const recs = [...room.animalOut.values()]; room.animalOut.clear();
      room.players.forEach(q => {
        if (!q.connected || !q.socket) return;
        const list = recs.filter(r => { const a = room.animals.get(r[0]); return a && a.owner !== q.pid && dist2(q.x, q.z, r[3], r[5]) < 190 * 190; });
        if (list.length) q.socket.volatile.emit('an:s', list);
      });
    }
  });
}, Math.round(1000 / TICK_HZ));

/* slow housekeeping: regrowth, clock, weather, animal ownership, empty rooms */
setInterval(() => {
  const t = now();
  rooms.forEach((room, code) => {
    if (!room.players.size) { if (room.emptySince && t - room.emptySince > EMPTY_ROOM_TTL_MS) rooms.delete(code); return; }
    // rocks grow back, bushes regrow (the server owns the timers, so everybody sees the same thing)
    room.rocks.forEach((r, k) => {
      if (r.hp >= ROCK_MAX_HP || t < r.next) return;
      r.hp += 1; r.next = t + 28000;
      if (r.hp >= ROCK_MAX_HP) room.rocks.delete(k);
      broadcast(room, 'w:ev', { t: 'rock', k, hp: r.hp, regrow: true });
    });
    room.bushes.forEach((at, k) => { if (t >= at) { room.bushes.delete(k); broadcast(room, 'w:ev', { t: 'bush', k, regrow: true }); } });
    room.drops.forEach((d, did) => { if (t - d.t > 600000) { room.drops.delete(did); broadcast(room, 'drop:rm', { did }); } });
    // weather
    if (t >= room.weather.until) {
      if (room.weather.rain === 0) { if (Math.random() < 0.6) { room.weather.rain = 1; room.weather.until = t + (70 + Math.random() * 100) * 1000; } else room.weather.until = t + (60 + Math.random() * 90) * 1000; }
      else { room.weather.rain = 0; room.weather.until = t + (180 + Math.random() * 240) * 1000; }
      broadcast(room, 'w:weather', { rain: room.weather.rain });
    }
    // wildlife ownership follows the players around
    if (tickN % 15 === 0) reassignAnimals(room);
  });
}, 1000);
setInterval(() => rooms.forEach(room => { if (room.players.size) broadcast(room, 'w:time', roomTime(room)); }), 5000);

server.listen(PORT, () => console.log('WILDERNESS multiplayer server listening on port ' + PORT + ' (max ' + MAX_PLAYERS + ' players per room)'));
process.on('SIGTERM', () => { io.close(); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000).unref(); });
module.exports = { app, server, io, rooms };
