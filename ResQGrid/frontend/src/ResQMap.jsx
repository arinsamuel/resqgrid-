import React, { useEffect, useRef } from 'react';
import { MapContainer, TileLayer, Marker, Popup, Polygon, CircleMarker, useMap } from 'react-leaflet';
import L from 'leaflet';

// Fix Leaflet default icon path issue with bundlers
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
});

// ── Sector geographic bounding boxes (lat/lng polygons) ──────────────
// Approximated over Dhaka metro area as a realistic demo city
const SECTOR_POLYGONS = {
  'Sector 1': [[23.820, 90.390], [23.820, 90.420], [23.800, 90.420], [23.800, 90.390]],
  'Sector 2': [[23.800, 90.390], [23.800, 90.420], [23.780, 90.420], [23.780, 90.390]],
  'Sector 3': [[23.820, 90.420], [23.820, 90.450], [23.800, 90.450], [23.800, 90.420]],
  'Sector 4': [[23.800, 90.420], [23.800, 90.450], [23.780, 90.450], [23.780, 90.420]],
  'Sector 5': [[23.780, 90.390], [23.780, 90.450], [23.760, 90.450], [23.760, 90.390]],
};

const SECTOR_CENTERS = {
  'Sector 1': [23.810, 90.405],
  'Sector 2': [23.790, 90.405],
  'Sector 3': [23.810, 90.435],
  'Sector 4': [23.790, 90.435],
  'Sector 5': [23.770, 90.420],
};

// ── Icon factories ────────────────────────────────────────────────────
const makeIcon = (color, symbol, size = 32) => L.divIcon({
  className: '',
  html: `<div style="
    width:${size}px;height:${size}px;border-radius:50%;
    background:${color};border:2px solid rgba(255,255,255,0.8);
    display:flex;align-items:center;justify-content:center;
    font-size:${size * 0.5}px;
    box-shadow:0 0 10px ${color},0 2px 6px rgba(0,0,0,0.4);
  ">${symbol}</div>`,
  iconSize: [size, size],
  iconAnchor: [size / 2, size / 2],
  popupAnchor: [0, -(size / 2)],
});

const INCIDENT_ICONS = {
  MEDICAL: makeIcon('#ef4444', '🚑', 34),
  FIRE:    makeIcon('#f97316', '🔥', 34),
  FLOOD:   makeIcon('#3b82f6', '🌊', 34),
};
const CRITICAL_ICON = makeIcon('#dc2626', '🆘', 38);

const RESOURCE_ICONS = {
  MEDICAL: makeIcon('#60a5fa', '🚐', 28),
  FIRE:    makeIcon('#fb923c', '🚒', 28),
  WATER:   makeIcon('#38bdf8', '⛵', 28),
  AIR:     makeIcon('#a78bfa', '🚁', 28),
};

const HOSPITAL_ICON_OK   = makeIcon('#10b981', '🏥', 30);
const HOSPITAL_ICON_LOW  = makeIcon('#f59e0b', '🏥', 30);
const HOSPITAL_ICON_FULL = makeIcon('#ef4444', '🏥', 30);

// ── Helper to pick incident icon ──────────────────────────────────────
function incidentIcon(inc) {
  if (inc.severity >= 5) return CRITICAL_ICON;
  return INCIDENT_ICONS[inc.type] || makeIcon('#ef4444', '⚠️', 32);
}

// ── Map bounds updater component ──────────────────────────────────────
function BoundsUpdater() {
  const map = useMap();
  useEffect(() => {
    const bounds = [
      [23.755, 90.382],
      [23.828, 90.458],
    ];
    map.fitBounds(bounds, { padding: [20, 20] });
  }, [map]);
  return null;
}

// ── Main Map Component ────────────────────────────────────────────────
export default function ResQMap({ incidents, resources, hospitals, roadClosures }) {
  const blockedSectors = new Set(
    (roadClosures || []).filter(c => c.status === 'blocked').map(c => c.location)
  );

  const activeIncidentLocations = new Set((incidents || []).map(i => i.location));

  return (
    <MapContainer
      center={[23.793, 90.420]}
      zoom={13}
      style={{ height: '100%', width: '100%', borderRadius: '12px' }}
      zoomControl={true}
      attributionControl={true}
    >
      <BoundsUpdater />

      {/* Dark tile layer – Carto Dark Matter */}
      <TileLayer
        url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        attribution='&copy; <a href="https://carto.com/">CARTO</a> &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        subdomains="abcd"
        maxZoom={19}
      />

      {/* ── Sector Polygons ── */}
      {Object.entries(SECTOR_POLYGONS).map(([sector, coords]) => {
        const isBlocked = blockedSectors.has(sector);
        const hasIncident = activeIncidentLocations.has(sector);
        const color = isBlocked ? '#ef4444' : hasIncident ? '#f97316' : '#3b82f6';
        const fillOpacity = isBlocked ? 0.20 : hasIncident ? 0.12 : 0.06;

        return (
          <Polygon
            key={sector}
            positions={coords}
            pathOptions={{
              color,
              fillColor: color,
              fillOpacity,
              weight: isBlocked ? 2 : 1,
              dashArray: isBlocked ? '6 4' : undefined,
            }}
          >
            <Popup>
              <div style={{ fontFamily: 'Inter, sans-serif', minWidth: 160 }}>
                <strong style={{ fontSize: '0.95rem' }}>{sector}</strong>
                <br />
                <span style={{ color: isBlocked ? '#ef4444' : '#10b981', fontWeight: 600 }}>
                  {isBlocked ? '⚠️ ROUTE BLOCKED' : '✅ Route Clear'}
                </span>
              </div>
            </Popup>
          </Polygon>
        );
      })}

      {/* ── Sector labels (circle markers at center) ── */}
      {Object.entries(SECTOR_CENTERS).map(([sector, center]) => (
        <CircleMarker
          key={`label-${sector}`}
          center={center}
          radius={0}
          pathOptions={{ opacity: 0, fillOpacity: 0 }}
        >
          <Popup>{sector}</Popup>
        </CircleMarker>
      ))}

      {/* ── Active Incident Markers ── */}
      {(incidents || []).map(inc => {
        const center = SECTOR_CENTERS[inc.location];
        if (!center) return null;
        // Jitter slightly so overlapping incidents don't stack
        const jitter = [
          center[0] + (Math.random() - 0.5) * 0.002,
          center[1] + (Math.random() - 0.5) * 0.002,
        ];
        return (
          <Marker key={inc.id} position={jitter} icon={incidentIcon(inc)}>
            <Popup>
              <div style={{ fontFamily: 'Inter, sans-serif', minWidth: 200 }}>
                <strong style={{ color: '#ef4444' }}>{inc.type} — SEV {inc.severity}</strong>
                <br /><strong>Location:</strong> {inc.location}
                <br /><strong>Time:</strong> {new Date(inc.created_at).toLocaleTimeString()}
                {inc.solution && (
                  <>
                    <br /><strong>ETA:</strong> {inc.solution.eta} min
                    <br /><strong>Units:</strong> {(inc.solution.selected_units || []).join(', ') || 'None'}
                    <br /><strong>Hospital:</strong> {inc.solution.hospital_bed}
                  </>
                )}
                {inc.description && <><br /><em style={{ color: '#9ca3af' }}>{inc.description}</em></>}
              </div>
            </Popup>
          </Marker>
        );
      })}

      {/* ── Resource / Vehicle Markers ── */}
      {(resources || []).map(res => {
        const center = SECTOR_CENTERS[res.location];
        if (!center) return null;
        const offset = [center[0] + 0.004, center[1] + 0.004];
        const icon = RESOURCE_ICONS[res.type] || makeIcon('#60a5fa', '🚗', 28);
        return (
          <Marker key={res.id} position={offset} icon={icon}>
            <Popup>
              <div style={{ fontFamily: 'Inter, sans-serif', minWidth: 180 }}>
                <strong>{res.id}</strong>
                <br /><strong>Type:</strong> {res.type}
                <br /><strong>Capacity:</strong> {res.capacity}
                <br /><strong>Location:</strong> {res.location}
                <br />
                <span style={{
                  fontWeight: 700,
                  color: res.status === 'busy' ? '#c084fc' : '#10b981'
                }}>
                  {res.status === 'busy' ? `🔒 LEASED (${res.lease_ttl}s)` : '🟢 AVAILABLE'}
                </span>
              </div>
            </Popup>
          </Marker>
        );
      })}

      {/* ── Hospital Markers ── */}
      {(hospitals || []).map(h => {
        const center = SECTOR_CENTERS[h.location];
        if (!center) return null;
        const offset = [center[0] - 0.004, center[1] - 0.004];
        const ratio = h.available_beds / Math.max(h.total_beds, 1);
        const icon = ratio === 0 ? HOSPITAL_ICON_FULL
          : ratio < 0.4 ? HOSPITAL_ICON_LOW
          : HOSPITAL_ICON_OK;
        return (
          <Marker key={h.id} position={offset} icon={icon}>
            <Popup>
              <div style={{ fontFamily: 'Inter, sans-serif', minWidth: 190 }}>
                <strong>🏥 {h.name}</strong>
                <br /><strong>Location:</strong> {h.location}
                <br />
                <span style={{
                  fontWeight: 700,
                  color: ratio === 0 ? '#ef4444' : ratio < 0.4 ? '#f59e0b' : '#10b981'
                }}>
                  Beds: {h.available_beds} / {h.total_beds}
                  {ratio === 0 ? ' — FULL' : ratio < 0.4 ? ' — LOW' : ' — OK'}
                </span>
              </div>
            </Popup>
          </Marker>
        );
      })}
    </MapContainer>
  );
}
