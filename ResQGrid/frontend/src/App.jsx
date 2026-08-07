import React, { useState, useEffect, useRef, Suspense } from 'react';
import './App.css';
const ResQMap = React.lazy(() => import('./ResQMap'));

const SECTORS = ["Sector 1", "Sector 2", "Sector 3", "Sector 4", "Sector 5"];
const INCIDENT_TYPES = ["MEDICAL", "FIRE", "FLOOD"];

function App() {
  // WebSockets & System State
  const [connected, setConnected]       = useState(false);
  const [incidents, setIncidents]       = useState([]);
  const [resources, setResources]       = useState([]);
  const [hospitals, setHospitals]       = useState([]);
  const [roadClosures, setRoadClosures] = useState([]);
  const [metrics, setMetrics]           = useState({
    ingestion_latency_p95_ms: 0,
    double_dispatch_rate: "0.00%",
    redis_connected: false,
    kafka_connected: false,
  });

  // UI States
  const [selectedIncident, setSelectedIncident] = useState(null);
  const [error, setError]         = useState(null);
  const [successMsg, setSuccessMsg] = useState(null);

  // Forms
  const [incidentForm, setIncidentForm] = useState({
    type: "MEDICAL",
    severity: 3,
    location: "Sector 4",
    description: "",
    idempotencyKey: generateKey(),
  });
  const [closureForm, setClosureForm] = useState({
    location: "Sector 4",
    status: "blocked",
  });

  const wsRef = useRef(null);

  function generateKey() {
    return 'key-' + Math.random().toString(36).substr(2, 9).toUpperCase();
  }

  const connectWS = () => {
    const wsUrl = `ws://${window.location.hostname}:8080/api/v1/ws`;
    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => { setConnected(true); setError(null); };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.event === 'state_update') {
          setIncidents(data.incidents || []);
          setRoadClosures(data.road_closures || []);
          setResources(data.resources || []);
          setHospitals(data.hospitals || []);
          setMetrics(data.metrics || {
            ingestion_latency_p95_ms: 0,
            double_dispatch_rate: "0.00%",
            redis_connected: false,
            kafka_connected: false,
          });
        }
      } catch (err) {
        console.error("Failed to parse WebSocket message", err);
      }
    };

    ws.onclose = () => { setConnected(false); setTimeout(connectWS, 3000); };
    ws.onerror = (err) => { console.error("WebSocket error:", err); ws.close(); };
  };

  useEffect(() => {
    connectWS();
    return () => { if (wsRef.current) wsRef.current.close(); };
  }, []);

  // ── Form handlers ──────────────────────────────────────────────────
  const handleIncidentSubmit = async (e) => {
    e.preventDefault();
    setError(null); setSuccessMsg(null);

    const payload = {
      idempotency_key: incidentForm.idempotencyKey,
      type: incidentForm.type,
      severity: parseInt(incidentForm.severity),
      location: incidentForm.location,
      description: incidentForm.description,
    };

    try {
      const response = await fetch(
        `http://${window.location.hostname}:8080/api/v1/incidents`,
        { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }
      );
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || `Server error ${response.status}`);
      }
      const result = await response.json();
      setSuccessMsg(`Incident ${result.id.substring(0, 8)} dispatched & hospital bed reserved!`);
      setIncidentForm({ type: "MEDICAL", severity: 3, location: "Sector 4", description: "", idempotencyKey: generateKey() });
      setTimeout(() => setSuccessMsg(null), 4000);
    } catch (err) {
      setError(err.message);
    }
  };

  const handleClosureSubmit = async (e) => {
    e.preventDefault();
    setError(null); setSuccessMsg(null);
    try {
      const response = await fetch(
        `http://${window.location.hostname}:8080/api/v1/road-closure`,
        { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(closureForm) }
      );
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || `Server error ${response.status}`);
      }
      setSuccessMsg(`Route in ${closureForm.location} set to ${closureForm.status.toUpperCase()}`);
      setTimeout(() => setSuccessMsg(null), 4000);
    } catch (err) {
      setError(err.message);
    }
  };

  const handleShowExplanation = async (inc) => {
    try {
      const response = await fetch(`http://${window.location.hostname}:8080/api/v1/explain/${inc.id}`);
      if (!response.ok) throw new Error("Failed to load explanation data.");
      const data = await response.json();
      setSelectedIncident({ ...inc, explanation: data });
    } catch (err) {
      setError(err.message);
    }
  };

  const activeClosures = roadClosures.filter(c => c.status === "blocked");

  // ── Reason descriptions ────────────────────────────────────────────
  const REASON_DESC = {
    UNIT_RESERVED:      'Unit currently dispatched and locked under active lease.',
    ROUTE_INFEASIBLE:   'Sector routes blocked by road hazard configurations.',
    SPECIALTY_MISMATCH: 'Resource specialty incompatible with incident type.',
    COVERAGE_FLOOR:     'Available but not selected — closer units satisfied capacity.',
    HOSPITAL_FULL:      'No available beds in this hospital at dispatch time.',
    HOSPITAL_UNREACHABLE: 'Hospital sector is blocked by active road closure.',
  };

  return (
    <div className="command-center">
      {/* ── Header ── */}
      <header className="control-header">
        <div className="title-group">
          <div className="live-indicator-dot blinking"></div>
          <h1>ResQGrid Command &amp; Dispatch</h1>
        </div>
        <div className="system-pills">
          <div className={`pill ${connected ? 'online' : 'offline'}`}>
            Gateway WS: {connected ? 'CONNECTED' : 'DISCONNECTED'}
          </div>
          <div className={`pill ${metrics.redis_connected ? 'online' : 'warn'}`}>
            Redis: {metrics.redis_connected ? 'LIVE' : 'FALLBACK'}
          </div>
          <div className={`pill ${metrics.kafka_connected ? 'online' : 'warn'}`}>
            Kafka: {metrics.kafka_connected ? 'STREAMING' : 'FALLBACK'}
          </div>
        </div>
      </header>

      {/* ── Metrics Row ── */}
      <section className="metrics-dashboard">
        <div className="metric-card">
          <span className="metric-title">Ingestion Latency (p95)</span>
          <span className="metric-value">{metrics.ingestion_latency_p95_ms.toFixed(1)} ms</span>
          <span className="metric-sub">Target SLA &lt; 300ms</span>
        </div>
        <div className="metric-card">
          <span className="metric-title">Double-Dispatch Lock Rate</span>
          <span className="metric-value">{metrics.double_dispatch_rate}</span>
          <span className="metric-sub">Strict Lock Violations Blocked</span>
        </div>
        <div className="metric-card">
          <span className="metric-title">Active Emergencies</span>
          <span className="metric-value">{incidents.length}</span>
          <span className="metric-sub">Real-Time Ingested Events</span>
        </div>
        <div className="metric-card">
          <span className="metric-title">Blocked Sectors</span>
          <span className="metric-value">{activeClosures.length}</span>
          <span className="metric-sub">Route Closures Active</span>
        </div>
        <div className="metric-card">
          <span className="metric-title">Hospital Beds Available</span>
          <span className="metric-value">
            {hospitals.reduce((sum, h) => sum + h.available_beds, 0)}
            <span style={{ fontSize: '1rem', color: '#9ca3af' }}>
              /{hospitals.reduce((sum, h) => sum + h.total_beds, 0)}
            </span>
          </span>
          <span className="metric-sub">Across {hospitals.length} Hospitals</span>
        </div>
      </section>

      {/* ── Live Map Row ── */}
      <div className="map-row">
        <div className="map-legend">
          <span className="legend-item"><span className="legend-dot" style={{background:'#ef4444'}}></span>Incident</span>
          <span className="legend-item"><span className="legend-dot" style={{background:'#60a5fa'}}></span>Vehicle</span>
          <span className="legend-item"><span className="legend-dot" style={{background:'#10b981'}}></span>Hospital OK</span>
          <span className="legend-item"><span className="legend-dot" style={{background:'#f59e0b'}}></span>Beds Low</span>
          <span className="legend-item"><span className="legend-dot" style={{background:'#ef4444', opacity:0.5}}></span>Blocked Sector</span>
        </div>
        <div className="map-container">
          <Suspense fallback={<div className="map-loading">🗺️ Loading geospatial map...</div>}>
            <ResQMap
              incidents={incidents}
              resources={resources}
              hospitals={hospitals}
              roadClosures={roadClosures}
            />
          </Suspense>
        </div>
      </div>

      {/* ── Main Dashboard Grid ── */}
      <div className="dashboard-grid">

        {/* Left Column: Command Entry */}
        <div className="grid-col form-col">
          {error    && <div className="alert error">{error}</div>}
          {successMsg && <div className="alert success">{successMsg}</div>}

          <div className="card glass-card">
            <h3>🚨 File Emergency Event</h3>
            <form onSubmit={handleIncidentSubmit}>
              <div className="form-group">
                <label>Emergency Type</label>
                <select value={incidentForm.type}
                  onChange={e => setIncidentForm({...incidentForm, type: e.target.value})}>
                  {INCIDENT_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
              <div className="form-group">
                <label>Severity Level (1-5)</label>
                <input type="range" min="1" max="5"
                  value={incidentForm.severity}
                  onChange={e => setIncidentForm({...incidentForm, severity: e.target.value})} />
                <span className="badge-value">Level {incidentForm.severity}</span>
              </div>
              <div className="form-group">
                <label>Dispatch Location</label>
                <select value={incidentForm.location}
                  onChange={e => setIncidentForm({...incidentForm, location: e.target.value})}>
                  {SECTORS.map(s => <option key={s} value={s}>{s}</option>)}
                </select>
              </div>
              <div className="form-group">
                <label>Situation Details</label>
                <textarea rows="2" placeholder="Describe the scene..."
                  value={incidentForm.description}
                  onChange={e => setIncidentForm({...incidentForm, description: e.target.value})} />
              </div>
              <div className="form-group">
                <label>Idempotency Key</label>
                <div className="input-with-button">
                  <input type="text" readOnly value={incidentForm.idempotencyKey} />
                  <button type="button" className="secondary-btn"
                    onClick={() => setIncidentForm({...incidentForm, idempotencyKey: generateKey()})}>
                    🔄
                  </button>
                </div>
              </div>
              <button type="submit" className="primary-btn pulse-hover">⚡ Dispatch Optimization</button>
            </form>
          </div>

          <div className="card glass-card">
            <h3>🚧 Configure Route Closures</h3>
            <form onSubmit={handleClosureSubmit}>
              <div className="form-row">
                <div className="form-group">
                  <label>Sector / Route</label>
                  <select value={closureForm.location}
                    onChange={e => setClosureForm({...closureForm, location: e.target.value})}>
                    {SECTORS.map(s => <option key={s} value={s}>{s}</option>)}
                  </select>
                </div>
                <div className="form-group" style={{ width: '70%' }}>
                  <label>Status</label>
                  <select value={closureForm.status}
                    onChange={e => setClosureForm({...closureForm, status: e.target.value})}>
                    <option value="blocked">⚠️ BLOCKED</option>
                    <option value="open">✅ OPEN</option>
                  </select>
                </div>
              </div>
              <button type="submit" className="warn-btn">Save Route Configuration</button>
            </form>
          </div>

          {/* Hospital Status Panel */}
          <div className="card glass-card">
            <h3>🏥 Hospital Bed Status</h3>
            <div className="hospital-list">
              {hospitals.map(h => {
                const ratio = h.available_beds / Math.max(h.total_beds, 1);
                const statusClass = ratio === 0 ? 'hosp-full' : ratio < 0.4 ? 'hosp-low' : 'hosp-ok';
                return (
                  <div key={h.id} className={`hospital-row ${statusClass}`}>
                    <div className="hosp-info">
                      <strong>{h.name}</strong>
                      <span className="hosp-loc">{h.location}</span>
                    </div>
                    <div className="hosp-beds">
                      <span className="beds-num">{h.available_beds}</span>
                      <span className="beds-total">/{h.total_beds} beds</span>
                      <div className="beds-bar">
                        <div className="beds-bar-fill" style={{ width: `${ratio * 100}%` }}></div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* Middle Column: Incident Feed */}
        <div className="grid-col feed-col">
          <div className="card fill-card">
            <h3>📡 Live Incident Feed</h3>
            <div className="list-container">
              {incidents.length === 0 ? (
                <div className="empty-state">No incidents filed yet. Use the command form to dispatch.</div>
              ) : (
                incidents.map(inc => (
                  <div key={inc.id} className="incident-row-card animate-slide-in">
                    <div className="incident-header">
                      <span className={`severity-badge sev-${inc.severity}`}>SEV-{inc.severity}</span>
                      <h4>{inc.type} — {inc.location}</h4>
                      <span className="time-badge">{new Date(inc.created_at).toLocaleTimeString()}</span>
                    </div>
                    <p className="desc">{inc.description || "No description provided."}</p>
                    {inc.solution && (
                      <div className="solution-block">
                        <div className="solution-summary">
                          <div>
                            <strong>Assigned Units:</strong>
                            <div className="unit-tags">
                              {inc.solution.selected_units && inc.solution.selected_units.length > 0 ? (
                                inc.solution.selected_units.map(unit => (
                                  <span key={unit} className="unit-tag">{unit}</span>
                                ))
                              ) : (
                                <span className="error-tag">INSUFFICIENT CAPACITY</span>
                              )}
                            </div>
                          </div>
                          <div className="solution-meta">
                            <div><strong>ETA:</strong> {inc.solution.eta} min</div>
                            <div><strong>Hospital:</strong> {inc.solution.hospital_bed}</div>
                          </div>
                        </div>
                        <button type="button" className="explain-btn"
                          onClick={() => handleShowExplanation(inc)}>
                          🔍 Explain CP-SAT Allocation
                        </button>
                      </div>
                    )}
                  </div>
                ))
              )}
            </div>
          </div>
        </div>

        {/* Right Column: Resources + Blockades */}
        <div className="grid-col resources-col">
          <div className="card fill-card">
            <h3>🛡️ Emergency Units &amp; Leases</h3>
            <div className="list-container">
              {resources.map(res => (
                <div key={res.id} className={`resource-row-card status-${res.status}`}>
                  <div className="res-info">
                    <strong>{res.id}</strong>
                    <span className="res-details">{res.type} | Cap: {res.capacity} | {res.location}</span>
                  </div>
                  <div className="res-status">
                    {res.status === "busy" ? (
                      <span className="status-badge busy">🔒 LEASED ({res.lease_ttl}s)</span>
                    ) : (
                      <span className="status-badge available">🟢 AVAILABLE</span>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="card glass-card">
            <h3>⚠️ Route Blockades</h3>
            <div className="blocked-list">
              {activeClosures.length === 0 ? (
                <div className="no-closures">All road routes currently clear.</div>
              ) : (
                activeClosures.map(closure => (
                  <div key={closure.location} className="closure-row alert-bg animate-pulse">
                    <span>Blocked: {closure.location}</span>
                    <span className="closed-time">Since {new Date(closure.configured_at).toLocaleTimeString()}</span>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      </div>

      {/* ── Explainability Modal ── */}
      {selectedIncident && (
        <div className="modal-overlay" onClick={() => setSelectedIncident(null)}>
          <div className="modal-content animate-scale-up" onClick={e => e.stopPropagation()}>
            <header className="modal-header">
              <h2>Decision Explanation — Incident {selectedIncident.id.substring(0, 8)}</h2>
              <button className="close-btn" onClick={() => setSelectedIncident(null)}>&times;</button>
            </header>
            <div className="modal-body">

              {/* Incident info grid */}
              <div className="modal-info-grid">
                <div><strong>Type:</strong> {selectedIncident.type}</div>
                <div><strong>Severity:</strong> Level {selectedIncident.severity}</div>
                <div><strong>Location:</strong> {selectedIncident.location}</div>
                <div><strong>Risk ETA:</strong> {selectedIncident.explanation.eta} min</div>
              </div>

              {/* Dispatched vehicles */}
              <h3>🎯 CP-SAT Optimal Vehicle Dispatch</h3>
              <div className="allocated-units">
                {selectedIncident.explanation.selected_units && selectedIncident.explanation.selected_units.length > 0 ? (
                  selectedIncident.explanation.selected_units.map(unit => (
                    <div key={unit} className="allocated-unit-card">
                      <strong>{unit}</strong>
                      <span className="status-badge available">Dispatched</span>
                    </div>
                  ))
                ) : (
                  <div className="alert error">SOLVER FAILURE: Insufficient capacity for SEV-{selectedIncident.severity}!</div>
                )}
              </div>

              {/* Hospital allocation */}
              <h3>🏥 Joint Hospital Bed Allocation</h3>
              <div className="hospital-allocation-block">
                {selectedIncident.explanation.selected_hospital_id ? (
                  <div className="allocated-hospital-card">
                    <div>
                      <strong>{selectedIncident.explanation.hospital_bed}</strong>
                      <span className="hosp-id-tag">{selectedIncident.explanation.selected_hospital_id}</span>
                    </div>
                    <span className="status-badge available">Bed Reserved</span>
                  </div>
                ) : (
                  <div className="no-closures">No bed reservation required for {selectedIncident.type} (or no beds available).</div>
                )}

                {/* Hospital rejection logs */}
                {selectedIncident.explanation.hospital_rejections && selectedIncident.explanation.hospital_rejections.length > 0 && (
                  <div className="rejected-analysis-table" style={{ marginTop: 10 }}>
                    <div className="table-header" style={{ gridTemplateColumns: '1fr 140px 1fr' }}>
                      <div>Hospital ID</div>
                      <div>Status</div>
                      <div>Rejection Reason</div>
                    </div>
                    {selectedIncident.explanation.hospital_rejections.map(rej => (
                      <div key={rej.hospital_id} className="table-row" style={{ gridTemplateColumns: '1fr 140px 1fr' }}>
                        <div className="rej-unit-id">{rej.hospital_id}</div>
                        <div>
                          <span className={`reason-pill ${rej.reason.toLowerCase()}`}>{rej.reason}</span>
                        </div>
                        <div className="reason-desc">{REASON_DESC[rej.reason] || rej.reason}</div>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Vehicle rejection logs */}
              <h3>🔍 CP-SAT Constraint Analysis (Rejected Units)</h3>
              <div className="rejected-analysis-table">
                <div className="table-header">
                  <div>Resource Unit</div>
                  <div>Status</div>
                  <div>Reason Code</div>
                </div>
                {selectedIncident.explanation.explainability && selectedIncident.explanation.explainability.length > 0 ? (
                  selectedIncident.explanation.explainability.map(rej => (
                    <div key={rej.unit_id} className="table-row">
                      <div className="rej-unit-id">{rej.unit_id}</div>
                      <div>
                        <span className={`reason-pill ${rej.reason.toLowerCase()}`}>{rej.reason}</span>
                      </div>
                      <div className="reason-desc">{REASON_DESC[rej.reason] || rej.reason}</div>
                    </div>
                  ))
                ) : (
                  <div className="no-rejections-row">No units were rejected during this solver run.</div>
                )}
              </div>
            </div>
            <footer className="modal-footer">
              <button className="primary-btn" onClick={() => setSelectedIncident(null)}>
                Acknowledge Decision Logs
              </button>
            </footer>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
