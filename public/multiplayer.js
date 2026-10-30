/* =====================================================================
   WILDERNESS - multiplayer client
   Menu (singleplayer / create room / join room), Socket.IO networking, remote player avatars,
   and the glue between the server and the game bridge (window.__WildBridge, see index.html).
===================================================================== */
(function () {
'use strict';
const $ = id => document.getElementById(id);
const SERVER_URL = (window.WILDERNESS_SERVER_URL || new URLSearchParams(location.search).get('server') || '').replace(/\/$/, '');
const LS = k => { try { return localStorage.getItem(k) || ''; } catch (e) { return ''; } };
const LSset = (k, v) => { try { localStorage.setItem(k, v); } catch (e) {} };

let socket = null, B = null, me = null, room = null, snap0 = null, token = '', ping = 0, ready = false;
const remotes = new Map();            // pid -> avatar
const netPieces = new Map(), netStructs = new Map(), netDrops = new Map(), byNid = new Map();
let gens = {}, nidN = 0, regQ = [];
const hooks = { spawnedByCamp: false };
const MPobj = { hooks, attach: b => attach(b) };

/* ------------------------------ menu ------------------------------ */
const menu = document.createElement('div');
menu.id = 'mpMenu';
menu.innerHTML = '<div class="box"><h1>WILDERNESS</h1><div class="sub">A FOREST SURVIVAL BEGINNING</div>' +
  '<div id="mpMain"><button class="btn" id="mpSolo">SINGLEPLAYER</button><button class="btn" id="mpMulti">MULTIPLAYER</button></div>' +
  '<div id="mpOnline" style="display:none"><div class="lbl">YOUR NAME</div><input id="mpName" maxlength="16" placeholder="Player name" autocomplete="off">' +
  '<button class="btn" id="mpCreate">CREATE ROOM</button><div class="or">— OR —</div>' +
  '<div class="lbl">ENTER ROOM CODE</div><input id="mpCode" class="code" maxlength="6" placeholder="XXXXX" autocomplete="off">' +
  '<div class="msg ok" id="mpPeek"></div><button class="btn" id="mpJoin">JOIN ROOM</button>' +
  '<div class="msg" id="mpMsg"></div><button class="link" id="mpBack">← back</button></div></div>';
document.body.appendChild(menu);
window.__WildMenu = true;
const msg = (t, ok) => { const m = $('mpMsg'); m.textContent = t || ''; m.className = 'msg' + (ok ? ' ok' : ''); };
$('mpName').value = LS('wild_name') || '';
const urlRoom = (new URLSearchParams(location.search).get('room') || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6);
if (urlRoom) $('mpCode').value = urlRoom;

function boot(mp) {
  menu.style.display = 'none'; $('loading').style.display = 'flex';
  requestAnimationFrame(() => requestAnimationFrame(() => setTimeout(() => {
    try { window.__startWilderness(mp); } catch (e) { console.error(e); $('loading').textContent = 'Error: ' + e.message; }
  }, 40)));
}
$('mpSolo').onclick = () => boot(null);
$('mpBack').onclick = () => { $('mpOnline').style.display = 'none'; $('mpMain').style.display = ''; };
$('mpMulti').onclick = () => {
  $('mpMain').style.display = 'none'; $('mpOnline').style.display = '';
  msg('Connecting…', true); setBusy(true);
  connect(err => { setBusy(false); if (err) msg('Cannot reach the multiplayer server. Start it with "npm start" (singleplayer still works).'); else { msg(''); peek(); } });
};
if (urlRoom) setTimeout(() => $('mpMulti').click(), 50);
const setBusy = b => ['mpCreate', 'mpJoin'].forEach(i => { $(i).disabled = b; });
function name() { const n = $('mpName').value.trim(); LSset('wild_name', n); return n; }
$('mpCode').addEventListener('input', () => { $('mpCode').value = $('mpCode').value.toUpperCase().replace(/[^A-Z0-9]/g, ''); peek(); });
let peekT = 0;
function peek() {
  clearTimeout(peekT);
  const c = $('mpCode').value; $('mpPeek').textContent = '';
  if (c.length < 4 || !socket || !socket.connected) return;
  peekT = setTimeout(() => socket.emit('room:peek', { code: c }, r => {
    if (!r || !r.ok) return;
    $('mpPeek').textContent = r.exists ? 'Room ' + c + ': ' + r.players + ' / ' + r.max + ' players' : 'No room with that code';
  }), 250);
}
$('mpCreate').onclick = () => { setBusy(true); msg('Creating room…', true); socket.emit('room:create', { name: name() }, r => entered(r)); };
$('mpJoin').onclick = () => { setBusy(true); msg('Joining…', true); socket.emit('room:join', { code: $('mpCode').value, name: name() }, r => entered(r)); };
['mpName', 'mpCode'].forEach(i => $(i).addEventListener('keydown', e => { e.stopPropagation(); if (e.key === 'Enter') (i === 'mpCode' ? $('mpJoin') : $('mpCreate')).click(); }));

function entered(r) {
  setBusy(false);
  if (!r || !r.ok) { msg((r && r.error) || 'Could not enter the room.'); return; }
  room = { code: r.code, max: r.max, count: r.count }; token = r.token; snap0 = r.snap; me = r.snap.you;
  history.replaceState(null, '', location.pathname + '?room=' + r.code);
  boot(MPobj);
}

/* ------------------------------ connection ------------------------------ */
function loadClient(cb) {
  if (typeof io !== 'undefined') return cb(true);
  if (!SERVER_URL) return cb(false);
  const s = document.createElement('script'); s.src = SERVER_URL + '/socket.io/socket.io.js';
  s.onload = () => cb(typeof io !== 'undefined'); s.onerror = () => cb(false); document.head.appendChild(s);
}
function connect(done) {
  loadClient(ok => {
    if (!ok) return done(true);
    if (socket) return done(false);
    socket = SERVER_URL ? io(SERVER_URL, { reconnectionDelay: 500 }) : io({ reconnectionDelay: 500 });   // same origin; wss:// on https
    let first = true;
    socket.on('connect', () => {
      if (first) { first = false; done(false); }
      else if (room && token) {                                   // dropped and came back: reclaim our seat
        socket.emit('room:rejoin', { code: room.code, token }, r => {
          if (r && r.ok) { me = r.snap.you; room.count = r.count; if (ready) { applySnapshot(r.snap); statusLine(); toast('Reconnected'); } }
          else endScreen('Your seat in the room was released.');
        });
      }
    });
    socket.on('connect_error', () => { if (first) { first = false; done(true); } });
    socket.on('disconnect', () => { if (ready) toast('Connection lost — reconnecting…'); });
    wire();
  });
}
const toast = t => { if (B) B.toast(t, 2200); };
const feed = (t, c) => { if (B) B.feed(t, c || '#cfe8a0'); };
function endScreen(text) {
  let e = $('mpEnd'); if (!e) { e = document.createElement('div'); e.id = 'mpEnd'; document.body.appendChild(e); }
  e.innerHTML = '<div><h2>' + text + '</h2><button id="mpEndB">BACK TO MENU</button></div>'; e.style.display = 'flex';
  $('mpEndB').onclick = () => { location.href = location.pathname; };
  try { document.exitPointerLock(); } catch (x) {}
}
function leave() { try { socket.emit('room:leave'); socket.disconnect(); } catch (e) {} location.href = location.pathname; }
window.addEventListener('pagehide', () => { try { if (socket && ready) socket.emit('room:leave'); } catch (e) {} });

/* ------------------------------ socket events ------------------------------ */
function wire() {
  socket.on('player:join', m => { if (!ready) return; room.count = m.count; addRemote(m.player); feed(m.player.name + (m.back ? ' is back' : ' joined'), '#9be08a'); statusLine(); });
  socket.on('player:leave', m => { room.count = m.count; dropRemote(m.id); if (ready) { feed(m.name + ' left', '#e0a08a'); statusLine(); } });
  socket.on('player:offline', m => { room.count = m.count; const r = remotes.get(m.id); if (r) r.g.visible = false; if (ready) { feed(m.name + ' lost connection', '#e0c08a'); statusLine(); } });
  socket.on('p:s', list => { if (!ready) return; for (const r of list) { const a = remotes.get(r[0]); if (a) pushState(a, r); } });
  socket.on('chat', m => addChat(m));
  socket.on('chat:err', t => toast(t));
  socket.on('w:time', t => { if (ready) B.setTime(t.hour, t.day); });
  socket.on('w:weather', w => { if (ready) { B.setRain(w.rain); toast(w.rain ? 'Rain begins to fall...' : 'The rain is easing.'); } });
  socket.on('w:ev', e => {
    if (!ready) return;
    if (e.t === 'tree') B.applyTree(e.k, e.px, e.pz);
    else if (e.t === 'rock') B.applyRock(e.k, e.hp, true);
    else if (e.t === 'bush') B.applyBush(e.k, !e.regrow);
    else if (e.t === 'log') B.applyLog(e.k);
  });
  socket.on('w:place', d => { if (ready) applyPlaced(d); });
  socket.on('drop:add', d => { if (ready) addNetDrop(d); });
  socket.on('drop:rm', m => { const d = netDrops.get(m.did); if (d) { B.removeDrop(d); netDrops.delete(m.did); } });
  socket.on('drop:upd', m => { const d = netDrops.get(m.did); if (d) d.n = m.n; });
  socket.on('fx', f => { if (ready && f.k === 'burst' && Math.hypot(B.player.x - f.x, B.player.z - f.z) < 45) { B.hitPoint.set(f.x, f.y, f.z); B.burst(B.hitPoint, f.a || 'dust', Math.min(f.n || 3, 6), 3); if (Math.hypot(B.player.x - f.x, B.player.z - f.z) < 14) (f.a === 'stone' ? B.AudioSys.mine : B.AudioSys.chop).call(B.AudioSys); } });
  socket.on('an:s', list => { if (ready) for (const r of list) animalState(r); });
  socket.on('an:own', list => { if (ready) for (const [nid, owner] of list) setOwner(nid, owner); });
  socket.on('an:hit', m => { const a = getA(m.nid); if (a && !a.remote && !a.dead) B.remoteHit(a, m.dmg, m.by); });
  socket.on('an:dead', m => {
    if (m.base) gens[m.base] = m.gen;
    const a = getA(m.nid); if (!a || a.dead) return;
    if (m.by === me.id && a.remote) B.lootAnimal(a); else B.ghostDie(a);
  });
  socket.on('an:rm', m => { const a = getA(m.nid); if (a) { byNid.delete(m.nid); B.removeAnimal(a); } });
}

/* ------------------------------ attach: the game is built, hook everything up ------------------------------ */
function attach(bridge) {
  B = bridge;
  const sx = ((parseInt(me.id.slice(0, 2), 16) % 7) - 3) * 1.5, sz = ((parseInt(me.id.slice(2, 4), 16) % 7) - 3) * 1.5;
  B.player.x = sx; B.player.z = sz; B.player.y = B.terrainH(sx, sz);
  // world-generated animals: deterministic ids; camp gorillas get camp ids so respawns stay in sync
  B.animals.forEach(a => { if (a.nid) byNid.set(a.nid, a); });
  B.gorillaSlots().forEach((slots, ci) => slots.forEach((s, si) => { if (s.a && s.a.nid) { byNid.delete(s.a.nid); s.a.nid = 'g' + ci + '.' + si + '.0'; byNid.set(s.a.nid, s.a); } }));
  applySnapshot(snap0);
  B.worldReady();
  const list = []; byNid.forEach(a => { if (a.mesh.parent) list.push([a.nid, a.kind, a.variant || '', Math.round(a.x * 10) / 10, Math.round(a.z * 10) / 10]); });
  for (let i = 0; i < list.length; i += 150) socket.emit('an:reg', list.slice(i, i + 150));
  buildHud(); buildOverlay(); ready = true; statusLine();
  setInterval(sendPlayer, 66); setInterval(sendAnimals, 125); setInterval(pingTick, 3000);
  setInterval(flushReg, 250);
  feed('Room ' + room.code + ' — share the code with friends!', '#ffd36b');
}

function applySnapshot(s) {
  B.setTime(s.time.hour, s.time.day); B.setRain(s.rain);
  s.trees.forEach(k => B.applyTree(k)); s.logs.forEach(k => B.applyLog(k));
  s.rocks.forEach(([k, hp]) => B.applyRock(k, hp, false)); s.bushes.forEach(k => B.applyBush(k, true));
  s.pieces.forEach(applyPlaced); s.structs.forEach(applyPlaced); s.drops.forEach(addNetDrop);
  gens = s.gens || {};
  (s.deadL || []).forEach(nid => { const a = getA(nid); if (a) { byNid.delete(nid); B.removeAnimal(a); } });
  s.animals.forEach(r => {
    const [nid, kind, v, x, y, z, yaw, hp, flags, dead, owner] = r;
    let a = getA(nid);
    if (dead) { if (a) { byNid.delete(nid); B.removeAnimal(a); } return; }
    if (!a) { if (!y && !x && !z) return; a = B.newGhost(kind, v, x, z); a.nid = nid; byNid.set(nid, a); }
    a.hp = hp || a.hp; a.net = { x, y: y || a.y, z, yaw, spd: 0, flags, t: performance.now() };
    if (y) { a.x = x; a.z = z; a.y = y; a.yaw = yaw; }
    if (owner === me.id) a.remote = false;
  });
  s.players.forEach(addRemote);
}
function applyPlaced(d) {
  if (d.type === 'piece') {
    if (netPieces.has(d.key)) return; netPieces.set(d.key, B.placeRemotePiece(d));
  } else { if (netStructs.has(d.id)) return; netStructs.set(d.id, B.placeRemoteStruct(d)); }
}
function addNetDrop(d) {
  if (netDrops.has(d.did)) return;
  const n0 = B.drops.length; window.__spawnDropMP = true;
  B.spawnDropNet(d.id, d.n, d.x, d.z, d.did);
  netDrops.set(d.did, B.drops[B.drops.length - 1]); if (B.drops.length === n0) netDrops.delete(d.did);
}

/* ------------------------------ hooks called from the game ------------------------------ */
const ackOr = (cb) => r => cb(r || { ok: false });
hooks.myId = () => me.id;
hooks.claim = (kind, key, cb) => socket.emit('claim', { t: kind, k: key }, ackOr(cb));
hooks.localPiece = (d, cb) => socket.emit('place', d, r => { r = r || { ok: false }; if (r.ok) netPieces.set(r.key || d.key, true); cb(r); });
hooks.localStruct = (d, cb) => socket.emit('place', d.type === 'machine' ? Object.assign({}, d, { id: d.id }) : d, r => { r = r || { ok: false }; if (r.ok) netStructs.set(r.id, true); cb(r); });
hooks.chestOpen = (k, cb) => socket.emit('chest:open', { k }, ackOr(cb));
hooks.chestClose = (k, slots) => socket.emit('chest:close', { k, slots });
hooks.dropNew = d => socket.emit('drop:new', { id: d.id, n: d.n, x: d.x, z: d.z }, r => {
  d.pendingNet = false; if (r && r.ok) { d.did = r.did; netDrops.set(r.did, d); }
});
hooks.dropTake = (d, n) => socket.emit('drop:take', { did: d.did, n }, r => { B.dropTaken(d, r && r.ok ? r.n : 0); if (!r || !r.ok || d.n <= 0) netDrops.delete(d.did); });
hooks.fxHit = (kind, x, y, z) => { if (!socket || !ready) return; const a = kind === 'rock' ? 'stone' : kind === 'bush' ? 'leaf' : 'wood'; socket.emit('fx', { k: 'burst', a, n: 3, x, y, z }); };
hooks.newNid = () => me.id + '-' + (nidN++);
hooks.localAnimal = a => { byNid.set(a.nid, a); regQ.push([a.nid, a.kind, a.variant || '', Math.round(a.x * 10) / 10, Math.round(a.z * 10) / 10]); };
hooks.campRespawn = (g, ci, si) => {
  const base = 'g' + ci + '.' + si, nid = base + '.' + (gens[base] || 0);
  if (g.nid && byNid.get(g.nid) === g) byNid.delete(g.nid);
  const ex = byNid.get(nid);
  g.nid = nid; g.remote = true;
  if (ex && ex !== g && ex.mesh.parent) { g.net = ex.net; g.hp = ex.hp; if (!ex.remote) g.remote = false; B.removeAnimal(ex); }
  byNid.set(nid, g); regQ.push([nid, g.kind, g.variant || '', g.x, g.z]);
};
hooks.animalRemoved = a => { byNid.delete(a.nid); socket.emit('an:rm', { nid: a.nid }); };
hooks.hitRemote = (a, dmg) => socket.emit('an:hit', { nid: a.nid, dmg });
hooks.animalKilled = (a, by) => socket.emit('an:kill', { nid: a.nid, by });
function flushReg() { if (regQ.length) { socket.emit('an:reg', regQ.splice(0, 100)); } }

/* ------------------------------ wildlife replication ------------------------------ */
function getA(nid) { const a = byNid.get(nid); return a && a.mesh && a.mesh.parent ? a : null; }
const ST = { idle: 0, chase: 1, flee: 2 }, STN = ['idle', 'chase', 'flee'];
function sendAnimals() {
  if (!ready || !socket.connected) return;
  const out = [], t = performance.now(), p = B.player;
  for (const a of B.animals) {
    if (!a.nid || a.remote || a.dead || a.cv || !a.mesh.parent) continue;
    if (Math.hypot(a.x - p.x, a.z - p.z) > 150) continue;
    const L = a._ls;
    if (L && Math.abs(L.x - a.x) < 0.05 && Math.abs(L.z - a.z) < 0.05 && Math.abs(L.yaw - a.yaw) < 0.05 && L.hp === a.hp && t - L.t < 2000) continue;
    a._ls = { x: a.x, z: a.z, yaw: a.yaw, hp: a.hp, t };
    out.push([a.nid, a.kind, a.variant || '', a.x, a.y, a.z, a.yaw, a.hp, (a.moving ? 1 : 0) | ((ST[a.state] || 0) << 1) | (a.swing > 0 ? 8 : 0), a.spd || 0]);
  }
  if (out.length) socket.volatile.emit('an:u', out);
}
function animalState(r) {
  const [nid, kind, v, x, y, z, yaw, hp, flags, spd] = r;
  let a = getA(nid);
  if (!a) { a = B.newGhost(kind, v, x, z); a.nid = nid; byNid.set(nid, a); a.x = x; a.z = z; a.y = y; a.yaw = yaw; }
  if (!a.remote || a.dead) return;
  a.hp = hp; a.net = { x, y, z, yaw, spd, flags, t: performance.now() };
}
function setOwner(nid, owner) {
  const a = getA(nid); if (!a) return;
  if (owner === me.id && a.remote) {
    a.remote = false; const n = a.net;
    if (n) { a.x = n.x; a.z = n.z; a.y = n.y; a.yaw = n.yaw; a.state = STN[(n.flags >> 1) & 3] || 'idle'; a.moving = !!(n.flags & 1); }
    a.t = 2; a._ls = null;
  } else if (owner !== me.id && !a.remote) {
    a.remote = true; a.net = { x: a.x, y: a.y, z: a.z, yaw: a.yaw, spd: 0, flags: 0, t: performance.now() };
  }
}
function updateGhosts(dt) {
  const k = 1 - Math.exp(-10 * dt), p = B.player;
  for (const a of B.animals) {
    if (!a.nid || !a.remote || a.dead || !a.net || a.cv || !a.mesh.parent) continue;
    const n = a.net, ox = a.x, oz = a.z;
    a.x += (n.x - a.x) * k; a.z += (n.z - a.z) * k; a.y += (n.y - a.y) * k;
    let d = n.yaw - a.yaw; d = Math.atan2(Math.sin(d), Math.cos(d)); a.yaw += d * k;
    const dx = p.x - a.x, dz = p.z - a.z;
    a.mesh.visible = dx * dx + dz * dz < 28900;
    const sp = Math.hypot(a.x - ox, a.z - oz) / Math.max(dt, 1e-3);
    a.spd += (Math.min(sp, 10) - a.spd) * Math.min(1, 8 * dt);
    a.phase += a.spd * dt * 1.6;
    const amp = Math.min(1, a.spd / 5) * 0.75, sw = Math.sin(a.phase * 2);
    if (a.lf) { a.lf.rotation.x = sw * amp; a.lb.rotation.x = -sw * amp; }
    if (n.flags & 8) a.swing = Math.max(a.swing || 0, 0.6);
    a.mesh.position.set(a.x, a.y + Math.abs(sw) * 0.03 * amp, a.z);
    a.mesh.rotation.set(a.swing > 0 ? a.swing * 0.4 : 0, a.yaw, 0); if (a.swing > 0) a.swing = Math.max(0, a.swing - dt * 2.5);
    const fx = Math.sin(a.yaw) * 0.4, fz = Math.cos(a.yaw) * 0.4, hy = a.y + (a.kind === 'deer' ? 0.85 : 0.6);
    a.seg[0] = a.x - fx; a.seg[1] = hy; a.seg[2] = a.z - fz; a.seg[3] = a.x + fx; a.seg[4] = hy; a.seg[5] = a.z + fz; a.seg[6] = a.kind === 'deer' ? 0.6 : 0.5;
    if (a.brute) { a.seg[0] = a.x; a.seg[1] = a.y + 0.5; a.seg[2] = a.z; a.seg[3] = a.x; a.seg[4] = a.y + a.brute.h; a.seg[5] = a.z; a.seg[6] = a.brute.sr; }
  }
}

/* ------------------------------ players: send ------------------------------ */
let lastSent = null, lastSentT = 0;
function sendPlayer() {
  if (!ready || !socket.connected) return;
  const p = B.player, t = performance.now();
  const sp = Math.hypot(p.vx, p.vz);
  const anim = (sp > 0.5 ? 1 : 0) | (sp > 7 ? 2 : 0) | (p.onGround ? 0 : 4) | (B.swinging() ? 8 : 0) | (B.gathering() ? 16 : 0);
  const m = { x: p.x, y: p.y, z: p.z, yaw: p.yaw, pitch: p.pitch, anim, held: B.heldId(), hp: p.health, dim: B.dim() };
  const L = lastSent;
  if (L && t - lastSentT < 1000 && Math.abs(L.x - m.x) < 0.02 && Math.abs(L.y - m.y) < 0.02 && Math.abs(L.z - m.z) < 0.02 && Math.abs(L.yaw - m.yaw) < 0.01 && Math.abs(L.pitch - m.pitch) < 0.01 && L.anim === anim && L.held === m.held) return;
  lastSent = m; lastSentT = t; socket.volatile.emit('p:u', m);
}
function pingTick() { if (!socket.connected) return; const t = Date.now(); socket.emit('ping:t', t, r => { ping = Date.now() - r; statusLine(); }); }

/* ------------------------------ players: avatars ------------------------------ */
function tagSprite(name, color) {
  const T = B.THREE, c = document.createElement('canvas'); c.width = 256; c.height = 64;
  const x = c.getContext('2d'); x.font = '700 30px Segoe UI,Arial'; x.textAlign = 'center'; x.textBaseline = 'middle';
  const w = Math.min(246, x.measureText(name).width + 24);
  x.fillStyle = 'rgba(0,0,0,.55)'; x.beginPath(); x.roundRect ? x.roundRect(128 - w / 2, 8, w, 48, 14) : x.rect(128 - w / 2, 8, w, 48); x.fill();
  x.lineWidth = 4; x.strokeStyle = 'rgba(0,0,0,.8)'; x.strokeText(name, 128, 33); x.fillStyle = color; x.fillText(name, 128, 33);
  const tex = new T.CanvasTexture(c);
  const sp = new T.Sprite(new T.SpriteMaterial({ map: tex, transparent: true, depthTest: false, depthWrite: false, fog: false }));
  sp.scale.set(2.2, 0.55, 1); sp.renderOrder = 999; return sp;
}
function addRemote(pl) {
  if (pl.id === me.id || remotes.has(pl.id)) { const r = remotes.get(pl.id); if (r) r.g.visible = true; return; }
  const T = B.THREE, g = new T.Group();
  const col = new T.Color(pl.color), skin = new T.MeshLambertMaterial({ color: 0xe0b48a }), cloth = new T.MeshLambertMaterial({ color: col }), pants = new T.MeshLambertMaterial({ color: 0x3b4a66 });
  const box = (w, h, d, m, x, y, z, parent) => { const q = new T.Mesh(new T.BoxGeometry(w, h, d), m); q.position.set(x, y, z); q.castShadow = true; (parent || g).add(q); return q; };
  const limb = (w, h, d, m, x, y, z) => { const piv = new T.Group(); piv.position.set(x, y, z); const q = new T.Mesh(new T.BoxGeometry(w, h, d), m); q.position.y = -h / 2; q.castShadow = true; piv.add(q); g.add(piv); return piv; };
  box(0.5, 0.7, 0.28, cloth, 0, 1.15, 0);
  const head = new T.Group(); head.position.set(0, 1.62, 0); g.add(head); box(0.34, 0.34, 0.34, skin, 0, 0.17, 0, head); box(0.36, 0.1, 0.36, new T.MeshLambertMaterial({ color: 0x4a3320 }), 0, 0.37, -0.01, head);
  box(0.06, 0.05, 0.02, new T.MeshBasicMaterial({ color: 0x222222 }), -0.08, 0.2, 0.175, head); box(0.06, 0.05, 0.02, new T.MeshBasicMaterial({ color: 0x222222 }), 0.08, 0.2, 0.175, head);
  const armL = limb(0.16, 0.65, 0.16, cloth, -0.34, 1.48, 0), armR = limb(0.16, 0.65, 0.16, cloth, 0.34, 1.48, 0);
  const legL = limb(0.2, 0.8, 0.2, pants, -0.13, 0.8, 0), legR = limb(0.2, 0.8, 0.2, pants, 0.13, 0.8, 0);
  const held = new T.Mesh(new T.BoxGeometry(0.14, 0.14, 0.5), new T.MeshLambertMaterial({ color: 0xaaaaaa })); held.position.set(0, -0.62, 0.22); held.visible = false; armR.add(held);
  const tag = tagSprite(pl.name, pl.color); tag.position.set(0, 2.25, 0); g.add(tag);
  B.scene.add(g);
  const a = { id: pl.id, name: pl.name, g, head, armL, armR, legL, legR, held, tag, phase: 0, swingT: 0, t: { x: pl.x, y: pl.y, z: pl.z, yaw: pl.yaw, pitch: 0 }, anim: 0, dim: pl.dim || '', heldId: pl.held || '', spd: 0, init: false };
  remotes.set(pl.id, a);
}
function dropRemote(id) { const a = remotes.get(id); if (!a) return; B.scene.remove(a.g); remotes.delete(id); }
function pushState(a, r) {
  a.t.x = r[1]; a.t.y = r[2]; a.t.z = r[3]; a.t.yaw = r[4]; a.t.pitch = r[5]; a.anim = r[6]; a.hp = r[8]; a.dim = r[9] || '';
  if (r[7] !== a.heldId) { a.heldId = r[7]; a.held.visible = !!r[7]; if (r[7]) { const it = B.ITEMS[r[7]]; a.held.material.color.setHex(it && it.col ? it.col : 0x9a7a4a); } }
  a.g.visible = true;
}
function updateRemotes(dt, time) {
  const k = 1 - Math.exp(-14 * dt), p = B.player, myDim = B.dim();
  remotes.forEach(a => {
    const t = a.t, g = a.g;
    if (!a.init) { g.position.set(t.x, t.y, t.z); g.rotation.y = t.yaw + Math.PI; a.init = true; }
    const dx = t.x - g.position.x, dz = t.z - g.position.z;
    if (dx * dx + dz * dz > 900) g.position.set(t.x, t.y, t.z);                       // teleport (portal, cave...)
    const ox = g.position.x, oz = g.position.z;
    g.position.x += dx * k; g.position.z += dz * k; g.position.y += (t.y - g.position.y) * k;
    let d = t.yaw + Math.PI - g.rotation.y; d = Math.atan2(Math.sin(d), Math.cos(d)); g.rotation.y += d * k;
    const sp = Math.hypot(g.position.x - ox, g.position.z - oz) / Math.max(dt, 1e-3);
    a.spd += (Math.min(sp, 12) - a.spd) * Math.min(1, 10 * dt);
    const air = a.anim & 4, moving = a.spd > 0.6;
    a.phase += a.spd * dt * 1.5;
    const amp = air ? 0.5 : Math.min(1, a.spd / 5) * 0.9, sw = Math.sin(a.phase * 2) * amp;
    a.legL.rotation.x = air ? -0.5 : sw; a.legR.rotation.x = air ? 0.5 : -sw; a.armL.rotation.x = -sw * 0.8;
    const act = (a.anim & 8) || (a.anim & 16);
    a.armR.rotation.x = act ? -1.2 + Math.sin(time * (a.anim & 8 ? 16 : 11)) * 0.7 : sw * 0.8;
    a.head.rotation.x = -t.pitch * 0.7;
    const near = (g.position.x - p.x) ** 2 + (g.position.z - p.z) ** 2;
    g.visible = a.dim === myDim && near < 250 * 250;
    a.tag.visible = near < 120 * 120;
    a.tag.material.opacity = near < 60 * 60 ? 1 : Math.max(0.25, 1 - (Math.sqrt(near) - 60) / 60);
  });
}

/* ------------------------------ HUD, chat, pause panel ------------------------------ */
let hud, chatBox, chatIn;
function buildHud() {
  hud = document.createElement('div'); hud.id = 'mpHud'; document.body.appendChild(hud);
  chatBox = document.createElement('div'); chatBox.id = 'mpChat';
  chatBox.innerHTML = '<div id="mpLog"></div><input id="mpChatIn" maxlength="160" placeholder="Say something… (Enter to send, Esc to cancel)">';
  document.body.appendChild(chatBox); chatIn = $('mpChatIn');
  window.addEventListener('keydown', e => {
    if (e.target === chatIn) return;
    if (e.code === 'Enter' && B.state.started && !B.state.invOpen) { e.preventDefault(); chatIn.style.display = 'block'; chatIn.focus(); for (const k in B.keys) B.keys[k] = false; }
  });
  chatIn.addEventListener('keydown', e => {
    e.stopPropagation();
    if (e.key === 'Enter') { const t = chatIn.value.trim(); if (t) socket.emit('chat', { text: t }); chatIn.value = ''; chatIn.style.display = 'none'; chatIn.blur(); }
    else if (e.key === 'Escape') { chatIn.value = ''; chatIn.style.display = 'none'; chatIn.blur(); }
  });
  chatIn.addEventListener('keyup', e => e.stopPropagation());
}
function addChat(m) {
  if (!chatBox) return;
  const d = document.createElement('div'); d.className = 'm';
  const n = document.createElement('b'); n.style.color = m.color; n.textContent = m.name + ': ';
  d.appendChild(n); d.appendChild(document.createTextNode(m.text));
  $('mpLog').appendChild(d);
  while ($('mpLog').children.length > 8) $('mpLog').removeChild($('mpLog').firstChild);
  setTimeout(() => { d.style.opacity = 0; setTimeout(() => d.remove(), 1000); }, 12000);
}
function statusLine() {
  if (!hud) return;
  let h = '<div class="room">ROOM ' + room.code + ' · ' + room.count + '/' + room.max + ' · ' + (socket.connected ? ping + ' ms' : 'reconnecting…') + '</div>';
  h += '<div class="pl"><span class="dot" style="background:' + me.color + '"></span>' + esc(me.name) + ' (you)</div>';
  remotes.forEach(a => { h += '<div class="pl"><span class="dot" style="background:' + a.g.children[0].material.color.getStyle() + '"></span>' + esc(a.name) + '</div>'; });
  hud.innerHTML = h;
  const c = $('mpOvCount'); if (c) c.textContent = room.count + ' / ' + room.max + ' players online';
}
const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
function buildOverlay() {
  const panel = document.querySelector('#overlay .panel'), box = document.createElement('div');
  box.id = 'mpOv';
  box.innerHTML = '<div style="font-size:11px;letter-spacing:3px;color:#9fb88a">ROOM CODE</div><div class="code">' + room.code + '</div><div id="mpOvCount" style="font-size:12px;color:#c9d6bb"></div>' +
    '<button id="mpCopy">COPY INVITE LINK</button><button id="mpLeave" class="red">LEAVE ROOM</button>';
  box.addEventListener('click', e => e.stopPropagation());
  panel.insertBefore(box, panel.querySelector('.ctl'));
  $('mpCopy').onclick = () => { const u = location.origin + location.pathname + '?room=' + room.code; (navigator.clipboard ? navigator.clipboard.writeText(u) : Promise.reject()).then(() => { $('mpCopy').textContent = 'COPIED!'; }, () => { prompt('Invite link:', u); }); };
  $('mpLeave').onclick = leave;
}

/* ------------------------------ per-frame ------------------------------ */
hooks.frame = (dt, time) => {
  if (!ready) return;
  B.tickSwing(dt); updateRemotes(dt, time); updateGhosts(dt);
};
})();
