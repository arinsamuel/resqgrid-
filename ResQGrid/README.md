# ResQGrid: Resilient Real-Time Emergency Coordination Platform

ResQGrid is a resilient, real-time emergency dispatch and route coordination platform. It combines a high-performance **Go Ingestion Gateway**, a **Google OR-Tools CP-SAT Python Solver**, **Apache Kafka** event streaming, **Redis** state caching, and a real-time **React Command Dashboard**.

---

## 🏗️ Architecture Overview

1. **Go Gateway & Ingestion Service (`backend-go`)**:
   - Ingests emergency events (`POST /api/v1/incidents`).
   - Prevents double-dispatch using a locking system (Redis TTL or RWMutex in-memory fallback).
   - Serves real-time snapshots and event broadcasts via **WebSockets**.
   - Features **automated fallback integration**: if Redis or Kafka are disconnected, it seamlessly degrades to in-memory state tracking.
   - Measures p95 ingestion latency via a thread-safe sliding window.

2. **Python Solver Service (`solver-python`)**:
   - Built on FastAPI.
   - Formulates a **Constraint Programming (CP-SAT)** model using **Google OR-Tools** to compute optimal resource bundles.
   - Evaluates unit availability, specialty compatibility, and route viabilities (against dynamic road closures).
   - Generates an **Explainability Log** explaining why rejected units were excluded.

3. **Orchestration**:
   - Powered by Docker Compose, setting up Redis caches, Kafka Event Brokers, and a Vite-React hot-reloaded development server.

---

## 🚀 Quick Start

Ensure you have **Docker** and **Docker Compose** installed.

### 1. Build and Run containers
In the root directory, run:
```bash
docker compose up --build
```

### 2. Access the Command Center
- Open your browser to: **`http://localhost:5173`**
- The dashboard connects to the Go gateway via WebSockets automatically.

---

## 🛠️ API & Verification Guide

You can interact with the Go API directly using `curl` or Postman.

### 1. File a new emergency incident
Ingests an incident and triggers the optimization solver.
```bash
curl -X POST http://localhost:8080/api/v1/incidents \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "IDEMP-KEY-001",
    "type": "FIRE",
    "severity": 4,
    "location": "Sector 4",
    "description": "Commercial building fire hazard in sector 4. Heavy smoke reported."
  }'
```
*Note: Resending this request with the same `idempotency_key` returns the cached solution instantly.*

### 2. Trigger dynamic road closures
Simulate flooding or blockages that prevent routes from being planned through specific sectors.
```bash
curl -X POST http://localhost:8080/api/v1/road-closure \
  -H "Content-Type: application/json" \
  -d '{
    "location": "Sector 3",
    "status": "blocked"
  }'
```
*If a route closure is configured, the Python solver will automatically mark resources located in Sector 3 as `ROUTE_INFEASIBLE` and redirect dispatch to alternative sectors.*

### 3. Retrieve system status and metrics
Fetches live health checks and performance metrics:
```bash
curl http://localhost:8080/api/v1/status
```

### 4. Fetch CP-SAT Explainability Decision logs
Retrieves full optimization solver diagnostics for any incident UUID:
```bash
curl http://localhost:8080/api/v1/explain/<incident-uuid>
```

---

## 🛡️ Key Resilience Features & Design Highlights

- **Pure Go Kafka Client**: By utilizing `segmentio/kafka-go`, the Go Gateway compiles cleanly under alpine without needing bulky gcc compiler settings (CGO).
- **Graceful Connectivity Degradation**: The Go Gateway continuously tests Kafka and Redis connections. If they fail or take time to boot up, it uses a RWMutex-backed internal channel structure, guaranteeing that emergency dispatches never fail.
- **Double-Dispatch Prevention**: Expiring leases are locked inside Redis (TTL key) or the local ledger map, blocking concurrent attempts to book the same vehicle for 30 seconds.
