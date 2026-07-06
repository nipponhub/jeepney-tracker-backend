/**
 * Jeepney Live Tracker — Backend
 * -------------------------------
 * Ingests GPS pings from the Traccar Client mobile app (installed on drivers'
 * phones) and rebroadcasts them in real time to every connected passenger
 * browser via Socket.io.
 *
 * Traccar Client talks the "OsmAnd" protocol: it sends the device's location
 * as query-string parameters on a plain HTTP request (GET by default, but it
 * can also POST) to whatever "Server URL" you configure in the app. There is
 * no auth, no JSON body required — just query params. That's why /api/positions
 * accepts both GET and POST and reads from req.query first.
 *
 * Data is kept in memory only (a JS Map). Perfect for an MVP — restart the
 * server and positions reset, which is fine since drivers' phones will just
 * ping again within seconds.
 */

const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: '*' }, // MVP: allow any origin. Lock this down to your domain in production.
});

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ---------------------------------------------------------------------------
// In-memory state
// ---------------------------------------------------------------------------

/** deviceId -> { deviceId, routeId, lat, lon, speedKph, bearing, updatedAt } */
const vehiclePositions = new Map();

/**
 * deviceId -> routeId
 * Traccar Client has no concept of "route", it only reports a device id.
 * We map device ids to jeepney route ids here. For the MVP this is edited
 * via a tiny admin endpoint (or just hardcode your fleet below).
 * Example: driver's phone reports id "353629104123456" -> route "R-1 Fairview-Cubao"
 */
const deviceRouteMap = new Map([
  // ['353629104123456', 'R-1 Fairview-Cubao'],
]);

const DEFAULT_ROUTE_ID = 'Unassigned';
const STALE_MS = 10 * 60 * 1000; // consider a vehicle "offline" only after 10 min of silence —
// Android's background location throttling can cause normal gaps of 5-14 minutes,
// so a shorter cutoff would wrongly remove vehicles that are still actively tracking.

/**
 * Rolling log of every ping received, successful or not. Kept in memory,
 * capped at PING_HISTORY_LIMIT entries (oldest dropped first) so it can't
 * grow unbounded. Good enough for MVP-scale debugging; won't survive a
 * restart and isn't meant to scale past a handful of test devices.
 */
const PING_HISTORY_LIMIT = 300;
const pingHistory = [];

function recordPing(entry) {
  const fullEntry = { receivedAt: new Date().toISOString(), ...entry };
  pingHistory.push(fullEntry);
  if (pingHistory.length > PING_HISTORY_LIMIT) pingHistory.shift();
  sendToGoogleSheet(fullEntry);
}

/**
 * Forwards a ping entry to a Google Apps Script webhook, which appends it as
 * a row in a Google Sheet. This is "fire and forget" — we don't await it and
 * we swallow errors, so a slow or unreachable Sheet can NEVER block or break
 * the actual GPS ingestion response to Traccar Client.
 * Configure via the GOOGLE_SHEET_WEBHOOK_URL environment variable on Render.
 */
const GOOGLE_SHEET_WEBHOOK_URL = process.env.GOOGLE_SHEET_WEBHOOK_URL || '';
const SHEET_SHARED_SECRET = process.env.SHEET_SHARED_SECRET || '';

function sendToGoogleSheet(entry) {
  if (!GOOGLE_SHEET_WEBHOOK_URL) return; // not configured — skip silently
  fetch(GOOGLE_SHEET_WEBHOOK_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ...entry, secret: SHEET_SHARED_SECRET }),
  }).catch((err) => {
    console.log(`[SHEET-LOG-FAIL] could not reach Google Sheet webhook: ${err.message}`);
  });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function knotsToKph(knots) {
  const n = parseFloat(knots);
  return Number.isFinite(n) ? Math.round(n * 1.852 * 10) / 10 : 0;
}

function metersPerSecondToKph(mps) {
  const n = parseFloat(mps);
  if (!Number.isFinite(n) || n < 0) return 0; // Android reports -1 for "speed unknown"
  return Math.round(n * 3.6 * 10) / 10;
}

function normalizePayload(source) {
  if (!source) return null;

  // --- Shape 1: nested JSON body (seen from newer Traccar Client versions) ---
  // { device_id, location: { coords: { latitude, longitude, speed, heading, altitude }, battery: { level }, timestamp } }
  if (source.location && source.location.coords) {
    const { coords, timestamp, battery } = source.location;
    const deviceId = source.device_id;
    if (!deviceId || coords.latitude === undefined || coords.longitude === undefined) return null;

    return {
      deviceId: String(deviceId),
      routeId: deviceRouteMap.get(String(deviceId)) || DEFAULT_ROUTE_ID,
      lat: parseFloat(coords.latitude),
      lon: parseFloat(coords.longitude),
      speedKph: metersPerSecondToKph(coords.speed),
      bearing: coords.heading !== undefined ? parseFloat(coords.heading) : 0,
      altitude: coords.altitude !== undefined ? parseFloat(coords.altitude) : null,
      battery: battery && battery.level !== undefined ? Math.round(battery.level * 100) : null,
      updatedAt: timestamp ? new Date(timestamp).getTime() : Date.now(),
    };
  }

  // --- Shape 2: classic flat OsmAnd-protocol query params ---
  // ?id=...&lat=...&lon=...&timestamp=...&speed=...&bearing=...
  const { id, lat, lon, timestamp, speed, bearing, altitude, batt } = source;
  if (!id || lat === undefined || lon === undefined) return null;

  const latitude = parseFloat(lat);
  const longitude = parseFloat(lon);
  if (Number.isNaN(latitude) || Number.isNaN(longitude)) return null;

  return {
    deviceId: String(id),
    routeId: deviceRouteMap.get(String(id)) || DEFAULT_ROUTE_ID,
    lat: latitude,
    lon: longitude,
    speedKph: knotsToKph(speed),
    bearing: bearing !== undefined ? parseFloat(bearing) : 0,
    altitude: altitude !== undefined ? parseFloat(altitude) : null,
    battery: batt !== undefined ? parseFloat(batt) : null,
    updatedAt: timestamp ? Number(timestamp) * (String(timestamp).length <= 10 ? 1000 : 1) : Date.now(),
  };
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// Traccar Client hits the bare Server URL on every GPS update — it does not
// support a custom path, so this MUST be the root route. GET is the default;
// POST is supported too in case you configure Traccar Client for "POST" requests.
app.all('/', (req, res) => {
  const source = Object.keys(req.query).length ? req.query : req.body;
  const position = normalizePayload(source || {});

  if (!position) {
    // No GPS params on this request — could be a real bad payload from a
    // device, or just someone opening the bare URL in a browser. Log it as a
    // failed ping either way so it shows up in /api/ping-history, then reply
    // with something friendly instead of an error.
    const rawPreview = JSON.stringify(source).slice(0, 300);
    recordPing({ success: false, reason: 'unrecognized or missing lat/lon', raw: rawPreview });
    console.log(`[PING-FAIL] unrecognized payload: ${rawPreview}`);
    return res.status(200).send('Jeepney Live Tracker backend is running.');
  }

  vehiclePositions.set(position.deviceId, position);
  io.emit('position', position);

  recordPing({
    success: true,
    deviceId: position.deviceId,
    routeId: position.routeId,
    lat: position.lat,
    lon: position.lon,
    speedKph: position.speedKph,
  });
  console.log(`[PING-OK] ${position.deviceId} @ ${position.lat},${position.lon} · ${position.speedKph} km/h`);

  // Traccar Client just needs a 200 OK — body content doesn't matter.
  res.status(200).json({ ok: true });
});

// Lets a freshly-loaded map (or a reconnecting client) get everyone's
// current position immediately, instead of waiting for the next GPS ping.
app.get('/api/positions/snapshot', (req, res) => {
  const now = Date.now();
  const active = Array.from(vehiclePositions.values()).filter(
    (v) => now - v.updatedAt < STALE_MS
  );
  res.json(active);
});

// Tiny admin helper to assign a route to a device id without restarting the
// server. In production, put this behind auth.
app.post('/api/devices/:deviceId/route', (req, res) => {
  const { deviceId } = req.params;
  const { routeId } = req.body;
  if (!routeId) return res.status(400).json({ error: 'routeId is required' });
  deviceRouteMap.set(deviceId, routeId);
  res.json({ deviceId, routeId });
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', activeVehicles: vehiclePositions.size, uptime: process.uptime() });
});

// View recent ping history — newest first. Optional ?deviceId=... filter for
// successful pings; failed pings (no recognized deviceId) always show, since
// they're the ones you're usually debugging.
app.get('/api/ping-history', (req, res) => {
  const { deviceId } = req.query;
  let entries = pingHistory;
  if (deviceId) {
    entries = entries.filter((e) => !e.success || e.deviceId === deviceId);
  }
  res.json(entries.slice().reverse());
});

// ---------------------------------------------------------------------------
// Socket.io
// ---------------------------------------------------------------------------

io.on('connection', (socket) => {
  console.log(`[socket] client connected: ${socket.id}`);

  socket.on('disconnect', () => {
    console.log(`[socket] client disconnected: ${socket.id}`);
  });
});

// Sweep stale vehicles every 30s and tell clients to remove them from the map
setInterval(() => {
  const now = Date.now();
  for (const [deviceId, pos] of vehiclePositions.entries()) {
    if (now - pos.updatedAt > STALE_MS) {
      vehiclePositions.delete(deviceId);
      io.emit('vehicle-offline', { deviceId });
    }
  }
}, 30000);

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚌 Jeepney Live Tracker backend running on port ${PORT}`);
  console.log(`   Webhook URL for Traccar Client: http://<your-host>:${PORT}/ (root path — no custom path supported)`);
});
