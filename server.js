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
const STALE_MS = 2 * 60 * 1000; // consider a vehicle "offline" after 2 min of silence

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function knotsToKph(knots) {
  const n = parseFloat(knots);
  return Number.isFinite(n) ? Math.round(n * 1.852 * 10) / 10 : 0;
}

function metersPerSecondToKph(mps) {
  const n = parseFloat(mps);
  return Number.isFinite(n) ? Math.round(n * 3.6 * 10) / 10 : 0;
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
    // No GPS params on this request — likely just someone opening the bare
    // URL in a browser. Reply with something friendly instead of an error.
    return res.status(200).send('Jeepney Live Tracker backend is running.');
  }

  vehiclePositions.set(position.deviceId, position);
  io.emit('position', position);

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
