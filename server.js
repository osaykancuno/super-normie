const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = 3456;
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

// --- Configuration ---
const MAX_ROOMS = 100;
const MAX_BODY_SIZE = 1e5; // 100KB
const API_RATE_LIMIT = 30; // requests per minute per IP
const WS_MSG_RATE_LIMIT = 20; // messages per second per connection
const WS_MAX_PAYLOAD = 1024; // max WebSocket message size in bytes

// --- Rate Limiting ---
const apiRates = new Map(); // ip -> { count, resetAt }

function checkApiRate(ip) {
  const now = Date.now();
  let entry = apiRates.get(ip);
  if (!entry || now > entry.resetAt) {
    entry = { count: 0, resetAt: now + 60000 };
    apiRates.set(ip, entry);
  }
  entry.count++;
  return entry.count <= API_RATE_LIMIT;
}

// Clean up stale rate limit entries every 5 minutes
setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of apiRates) {
    if (now > entry.resetAt) apiRates.delete(ip);
  }
}, 300000);

// --- Static File Server + REST API ---
const MIME = {
  '.html': 'text/html', '.js': 'application/javascript',
  '.css': 'text/css', '.png': 'image/png', '.json': 'application/json'
};

function sendJSON(res, code, data) {
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  });
  res.end(JSON.stringify(data));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    let rejected = false;
    req.on('data', c => {
      if (rejected) return;
      body += c;
      if (body.length > MAX_BODY_SIZE) {
        rejected = true;
        req.destroy();
        reject('Too large');
      }
    });
    req.on('end', () => {
      if (rejected) return;
      try { resolve(JSON.parse(body)); } catch (e) { reject('Bad JSON'); }
    });
  });
}

// --- Input Validation ---
function isInt(v, min, max) {
  return typeof v === 'number' && Number.isInteger(v) && v >= min && v <= max;
}

function sanitizeString(s, maxLen) {
  if (typeof s !== 'string') return '';
  return s.slice(0, maxLen).replace(/[<>&"']/g, '');
}

// --- Daily Leaderboard ---
function dailyKey() {
  const d = new Date();
  return `${d.getFullYear()}${String(d.getMonth()+1).padStart(2,'0')}${String(d.getDate()).padStart(2,'0')}`;
}

function getDailyFile() {
  return path.join(DATA_DIR, `daily-${dailyKey()}.json`);
}

function loadDaily() {
  const f = getDailyFile();
  try { return JSON.parse(fs.readFileSync(f, 'utf8')); }
  catch (e) { return { seed: parseInt(dailyKey()), scores: [] }; }
}

function saveDaily(data) {
  fs.writeFileSync(getDailyFile(), JSON.stringify(data));
}

// --- HTTP Server ---
const server = http.createServer((req, res) => {
  // CORS preflight
  if (req.method === 'OPTIONS') { sendJSON(res, 200, {}); return; }

  const clientIp = req.socket.remoteAddress || 'unknown';

  // REST API endpoints
  if (req.url === '/api/daily' && req.method === 'GET') {
    if (!checkApiRate(clientIp)) { sendJSON(res, 429, { error: 'Rate limit exceeded' }); return; }
    const data = loadDaily();
    const top20 = data.scores.sort((a, b) => b.score - a.score || a.time - b.time).slice(0, 20);
    sendJSON(res, 200, { seed: data.seed, scores: top20 });
    return;
  }

  if (req.url === '/api/daily' && req.method === 'POST') {
    if (!checkApiRate(clientIp)) { sendJSON(res, 429, { error: 'Rate limit exceeded' }); return; }
    readBody(req).then(body => {
      const { normieId, score, time, name } = body;
      // Strict input validation
      if (!isInt(normieId, 0, 9999)) { sendJSON(res, 400, { error: 'Invalid normieId (must be 0-9999)' }); return; }
      if (!isInt(score, 0, 999999)) { sendJSON(res, 400, { error: 'Invalid score' }); return; }
      if (!isInt(time, 0, 9999999)) { sendJSON(res, 400, { error: 'Invalid time' }); return; }
      const safeName = sanitizeString(name, 50);

      const data = loadDaily();
      // Check if this normie already submitted today (keep best)
      const existing = data.scores.find(s => s.normieId === normieId);
      if (existing) {
        if (score > existing.score || (score === existing.score && time < existing.time)) {
          existing.score = score; existing.time = time; existing.name = safeName;
        }
      } else {
        data.scores.push({ normieId, score, time, name: safeName });
      }
      saveDaily(data);
      const top20 = data.scores.sort((a, b) => b.score - a.score || a.time - b.time).slice(0, 20);
      sendJSON(res, 200, { seed: data.seed, scores: top20 });
    }).catch(e => sendJSON(res, 400, { error: String(e) }));
    return;
  }

  // Static files — hardened path traversal protection
  let urlPath;
  try { urlPath = decodeURIComponent(req.url.split('?')[0]); }
  catch (e) { res.writeHead(400); res.end('Bad request'); return; }

  // Block null bytes and double-dot sequences before path resolution
  if (urlPath.includes('\0') || urlPath.includes('..')) {
    res.writeHead(403); res.end('Forbidden'); return;
  }

  const file = path.resolve(__dirname, urlPath === '/' ? 'index.html' : urlPath.replace(/^\//, ''));
  const normalizedBase = path.resolve(__dirname) + path.sep;

  // Ensure resolved path is within project directory
  if (!file.startsWith(normalizedBase) && file !== path.resolve(__dirname)) {
    res.writeHead(403); res.end('Forbidden'); return;
  }

  // Block access to data directory and hidden files
  if (file.startsWith(path.resolve(DATA_DIR)) || path.basename(file).startsWith('.')) {
    res.writeHead(403); res.end('Forbidden'); return;
  }

  const ext = path.extname(file);
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'text/plain' });
    res.end(data);
  });
});

// --- WebSocket Multiplayer ---
const wss = new WebSocketServer({ server, maxPayload: WS_MAX_PAYLOAD });
const rooms = new Map(); // code -> { players, level, started }

function genCode() {
  let code, attempts = 0;
  do {
    code = String(1000 + Math.floor(Math.random() * 9000));
    attempts++;
    if (attempts > 100) return null; // prevent infinite loop if all codes taken
  } while (rooms.has(code));
  return code;
}

function broadcast(room, msg, excludeWs) {
  const str = JSON.stringify(msg);
  room.players.forEach(p => { if (p.ws !== excludeWs && p.ws.readyState === 1) p.ws.send(str); });
}

function cleanRoom(code) {
  const room = rooms.get(code);
  if (!room) return;
  room.players = room.players.filter(p => p.ws.readyState === 1);
  if (room.players.length === 0) rooms.delete(code);
}

// Periodic stale room cleanup every 60 seconds
setInterval(() => {
  for (const [code, room] of rooms) {
    room.players = room.players.filter(p => p.ws.readyState === 1);
    if (room.players.length === 0) rooms.delete(code);
  }
}, 60000);

// --- WebSocket Validation Helpers ---
function validNormieId(id) { return isInt(id, 0, 9999); }
function validCoord(v) { return typeof v === 'number' && Number.isFinite(v); }
function validRoomCode(c) { return typeof c === 'string' && /^\d{4}$/.test(c); }

wss.on('connection', ws => {
  let myRoom = null, myCode = null;

  // Per-connection rate limiting
  let msgCount = 0, msgResetAt = Date.now() + 1000;

  ws.on('message', raw => {
    // Rate limit: max WS_MSG_RATE_LIMIT messages per second
    const now = Date.now();
    if (now > msgResetAt) { msgCount = 0; msgResetAt = now + 1000; }
    msgCount++;
    if (msgCount > WS_MSG_RATE_LIMIT) return; // silently drop excess messages

    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (!msg || typeof msg.type !== 'string') return;

    switch (msg.type) {
      case 'create': {
        if (myRoom) break; // already in a room
        if (rooms.size >= MAX_ROOMS) {
          ws.send(JSON.stringify({ type: 'error', msg: 'Server full' })); break;
        }
        const code = genCode();
        if (!code) { ws.send(JSON.stringify({ type: 'error', msg: 'Server full' })); break; }
        const nid = validNormieId(msg.normieId) ? msg.normieId : 0;
        const room = {
          players: [{ ws, normieId: nid, x: 0, y: 0, alive: true, ready: false }],
          level: 0, started: false
        };
        rooms.set(code, room);
        myRoom = room; myCode = code;
        ws.send(JSON.stringify({ type: 'created', code, slot: 0 }));
        break;
      }
      case 'join': {
        if (myRoom) break; // already in a room
        if (!validRoomCode(msg.code)) { ws.send(JSON.stringify({ type: 'error', msg: 'Invalid code' })); break; }
        const room = rooms.get(msg.code);
        if (!room) { ws.send(JSON.stringify({ type: 'error', msg: 'Room not found' })); break; }
        if (room.players.length >= 2) { ws.send(JSON.stringify({ type: 'error', msg: 'Room full' })); break; }
        if (room.started) { ws.send(JSON.stringify({ type: 'error', msg: 'Game in progress' })); break; }
        const nid = validNormieId(msg.normieId) ? msg.normieId : 0;
        room.players.push({ ws, normieId: nid, x: 0, y: 0, alive: true, ready: false });
        myRoom = room; myCode = msg.code;
        ws.send(JSON.stringify({ type: 'joined', code: msg.code, slot: 1 }));
        // Notify host
        broadcast(room, { type: 'opponent_joined', normieId: nid }, ws);
        // Auto-start countdown
        room.started = true;
        let count = 3;
        const iv = setInterval(() => {
          // Stop countdown if room was cleaned up
          if (!rooms.has(myCode)) { clearInterval(iv); return; }
          broadcast(room, { type: 'countdown', count });
          count--;
          if (count < 0) {
            clearInterval(iv);
            broadcast(room, { type: 'start', level: room.level });
          }
        }, 1000);
        break;
      }
      case 'update': {
        if (!myRoom) break;
        if (!validCoord(msg.x) || !validCoord(msg.y)) break;
        const me = myRoom.players.find(p => p.ws === ws);
        if (me) {
          me.x = msg.x; me.y = msg.y;
          me.alive = !!msg.alive;
          me.face = msg.face === -1 ? -1 : 1;
        }
        broadcast(myRoom, {
          type: 'ghost', x: msg.x, y: msg.y, alive: !!msg.alive,
          face: msg.face === -1 ? -1 : 1,
          normieId: me ? me.normieId : 0
        }, ws);
        break;
      }
      case 'flag': {
        if (!myRoom) break;
        const flagTime = isInt(msg.time, 0, 9999999) ? msg.time : 0;
        const flagScore = isInt(msg.score, 0, 999999) ? msg.score : 0;
        const flagNid = validNormieId(msg.normieId) ? msg.normieId : 0;
        broadcast(myRoom, { type: 'opponent_flag', time: flagTime, score: flagScore }, ws);
        broadcast(myRoom, { type: 'race_end', winner: flagNid, time: flagTime, score: flagScore });
        break;
      }
      case 'leave': {
        if (myRoom) {
          broadcast(myRoom, { type: 'opponent_left' }, ws);
          cleanRoom(myCode);
        }
        myRoom = null; myCode = null;
        break;
      }
      // Unknown message types are silently ignored
    }
  });

  ws.on('close', () => {
    if (myRoom) {
      broadcast(myRoom, { type: 'opponent_left' }, ws);
      cleanRoom(myCode);
    }
  });
});

server.listen(PORT, () => console.log(`Super Normie server running on http://localhost:${PORT}`));
