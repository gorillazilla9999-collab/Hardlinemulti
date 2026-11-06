// HARDLINE multiplayer server: serves the game and relays messages between players in a room.
// No dependencies. Run:  node server.js   (PORT env var is respected, default 8080)
// Put the game file next to this file as index.html.
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 8080;
const GAME = path.join(__dirname, 'index.html');
const MAX_PLAYERS = 8;
const rooms = new Map(); // code -> {code, host, players: Map(id -> player), started}
let nextId = 1;

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (req.url === '/' || req.url.startsWith('/?') || req.url === '/index.html') {
    fs.readFile(GAME, (err, data) => {
      if (err) { res.writeHead(500); return res.end('index.html not found next to server.js'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
    return;
  }
  res.writeHead(404); res.end('not found');
});

// ---- minimal WebSocket (RFC 6455): text frames, ping/pong, close
function wsSend(sock, str, op = 1) {
  if (sock.destroyed) return;
  const payload = Buffer.from(str), n = payload.length;
  let head;
  if (n < 126) head = Buffer.from([0x80 | op, n]);
  else if (n < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(n, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(n), 2); }
  try { sock.write(Buffer.concat([head, payload])); } catch (e) {}
}

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = { id: nextId++, sock, room: null, name: 'Player', team: 0, alive: true, buf: Buffer.alloc(0), frag: [] };
  sock.on('data', d => {
    c.buf = Buffer.concat([c.buf, d]);
    if (c.buf.length > 1 << 20) return sock.destroy();
    for (;;) {
      const b = c.buf;
      if (b.length < 2) return;
      const fin = b[0] & 0x80, op = b[0] & 15, masked = b[1] & 0x80;
      let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 65536) return sock.destroy();
      if (b.length < off + (masked ? 4 : 0) + len) return;
      let payload;
      if (masked) {
        const mask = b.slice(off, off + 4); off += 4;
        payload = Buffer.from(b.slice(off, off + len));
        for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
      } else payload = b.slice(off, off + len);
      c.buf = b.slice(off + len);
      if (op === 8) { sock.end(); return; }
      if (op === 9) { wsSend(sock, payload.toString(), 10); continue; }
      if (op === 10) continue;
      if (op === 0 || op === 1) {
        c.frag.push(payload);
        if (fin) { const s = Buffer.concat(c.frag).toString(); c.frag = []; try { onMsg(c, JSON.parse(s)); } catch (e) {} }
      }
    }
  });
  const gone = () => { if (c.alive) { c.alive = false; leave(c); } };
  sock.on('close', gone); sock.on('error', gone); sock.on('end', gone);
  c.ping = setInterval(() => wsSend(sock, '', 9), 25000);
  sock.on('close', () => clearInterval(c.ping));
});

const send = (c, o) => wsSend(c.sock, JSON.stringify(o));
const roster = r => [...r.players.values()].map(p => ({ id: p.id, name: p.name, team: p.team }));
const broadcastRoom = r => { const m = { t: 'room', code: r.code, host: r.host, started: r.started, players: roster(r) }; r.players.forEach(p => send(p, m)); };
const clean = s => String(s || 'Player').replace(/[^\w \-.]/g, '').slice(0, 16) || 'Player';

function newCode() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
  for (;;) { let s = ''; for (let i = 0; i < 4; i++) s += A[crypto.randomInt(A.length)]; if (!rooms.has(s)) return s; }
}

function join(c, r) {
  c.room = r;
  const n = [0, 0]; r.players.forEach(p => n[p.team]++);
  c.team = n[0] <= n[1] ? 0 : 1;
  r.players.set(c.id, c);
  send(c, { t: 'hello', id: c.id });
  broadcastRoom(r);
}

function leave(c) {
  const r = c.room; if (!r) return;
  c.room = null; r.players.delete(c.id);
  if (c.id === r.host || r.players.size === 0) {
    r.players.forEach(p => { send(p, { t: 'closed', m: 'The host left, so the room was closed.' }); p.room = null; });
    rooms.delete(r.code);
  } else {
    r.players.forEach(p => send(p, { t: 'left', id: c.id }));
    broadcastRoom(r);
  }
}

function onMsg(c, m) {
  if (m.t === 'create' && !c.room) {
    const r = { code: newCode(), host: c.id, players: new Map(), started: false };
    rooms.set(r.code, r); c.name = clean(m.name); join(c, r);
  } else if (m.t === 'join' && !c.room) {
    const r = rooms.get(String(m.code || '').toUpperCase().trim());
    if (!r) return send(c, { t: 'err', m: 'No room with that code.' });
    if (r.started) return send(c, { t: 'err', m: 'That match has already started.' });
    if (r.players.size >= MAX_PLAYERS) return send(c, { t: 'err', m: 'That room is full (8 players).' });
    c.name = clean(m.name); join(c, r);
  } else if (m.t === 'team' && c.room && !c.room.started) {
    c.team = m.team ? 1 : 0; broadcastRoom(c.room);
  } else if (m.t === 'started' && c.room && c.id === c.room.host) {
    c.room.started = !!m.v; broadcastRoom(c.room);
  } else if (m.t === 'rel' && c.room) {
    const out = JSON.stringify({ t: 'm', from: c.id, d: m.d });
    if (m.to) { const p = c.room.players.get(m.to); if (p) wsSend(p.sock, out); }
    else c.room.players.forEach(p => { if (p !== c) wsSend(p.sock, out); });
  }
}

server.listen(PORT, () => console.log('HARDLINE server on port ' + PORT));
