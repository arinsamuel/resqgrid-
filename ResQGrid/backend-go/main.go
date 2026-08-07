package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// --- CONFIGURATION ---
const (
	RedisAddr      = "redis:6379"
	KafkaAddr      = "kafka:9092"
	KafkaTopic     = "resqgrid-incidents"
	SolverURL      = "http://solver-python:8000/solve"
	LeaseDuration  = 30 * time.Second
	BedLeaseDur    = 30 * time.Second
	LatencyHistory = 100
)

// --- DATA STRUCTURES ---

type Incident struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"` // FIRE, FLOOD, MEDICAL
	Severity       int       `json:"severity"`
	Location       string    `json:"location"`
	Description    string    `json:"description"`
	IdempotencyKey string    `json:"idempotency_key"`
	Status         string    `json:"status"` // DISPATCHED, RESOLVED, FAILED
	CreatedAt      time.Time `json:"created_at"`
	Solution       *Solution `json:"solution,omitempty"`
}

type Resource struct {
	ID       string `json:"id"`
	Type     string `json:"type"` // MEDICAL, WATER, FIRE, AIR
	Capacity int    `json:"capacity"`
	Location string `json:"location"`
	Status   string `json:"status"` // available, busy
	LeaseTTL int    `json:"lease_ttl,omitempty"`
}

type Hospital struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Location      string `json:"location"`
	TotalBeds     int    `json:"total_beds"`
	AvailableBeds int    `json:"available_beds"`
}

type RoadClosure struct {
	Location     string    `json:"location"`
	Status       string    `json:"status"` // blocked, open
	ConfiguredAt time.Time `json:"configured_at"`
}

type RejectionLog struct {
	UnitID string `json:"unit_id"`
	Reason string `json:"reason"` // UNIT_RESERVED, ROUTE_INFEASIBLE, SPECIALTY_MISMATCH, COVERAGE_FLOOR
}

type HospitalRejectionLog struct {
	HospitalID string `json:"hospital_id"`
	Reason     string `json:"reason"` // HOSPITAL_FULL, HOSPITAL_UNREACHABLE
}

type Solution struct {
	IncidentID          string                 `json:"incident_id"`
	SelectedUnits       []string               `json:"selected_units"`
	ETA                 float64                `json:"eta"`
	HospitalBed         string                 `json:"hospital_bed"`
	SelectedHospitalID  string                 `json:"selected_hospital_id"`
	Explainability      []RejectionLog         `json:"explainability"`
	HospitalRejections  []HospitalRejectionLog `json:"hospital_rejections"`
}

type SolverRequest struct {
	Incident     Incident   `json:"incident"`
	RoadClosures []string   `json:"road_closures"`
	Resources    []Resource `json:"resources"`
	Hospitals    []Hospital `json:"hospitals"`
}

type SolverResponse struct {
	Status              string                 `json:"status"`
	SelectedUnits       []string               `json:"selected_units"`
	ETA                 float64                `json:"eta"`
	HospitalBed         string                 `json:"hospital_bed"`
	SelectedHospitalID  string                 `json:"selected_hospital_id"`
	Explainability      []RejectionLog         `json:"explainability"`
	HospitalRejections  []HospitalRejectionLog `json:"hospital_rejections"`
}

// --- STATE LEDGER ---

type StateLedger struct {
	mu           sync.RWMutex
	Incidents    map[string]Incident
	RoadClosures map[string]RoadClosure
	Resources    map[string]Resource
	Hospitals    map[string]Hospital
	Leases       map[string]time.Time // ResourceID -> Lease Expiry
	BedLeases    map[string]time.Time // HospitalID -> Bed Lease Expiry

	// Metrics
	latencyTimes         []float64
	latencyIndex         int
	dispatchAttempts     int64
	doubleDispatchBlocks int64
}

func NewStateLedger() *StateLedger {
	ledger := &StateLedger{
		Incidents:    make(map[string]Incident),
		RoadClosures: make(map[string]RoadClosure),
		Resources:    make(map[string]Resource),
		Hospitals:    make(map[string]Hospital),
		Leases:       make(map[string]time.Time),
		BedLeases:    make(map[string]time.Time),
		latencyTimes: make([]float64, 0, LatencyHistory),
	}
	ledger.bootstrapResources()
	ledger.bootstrapHospitals()
	return ledger
}

func (s *StateLedger) bootstrapResources() {
	units := []Resource{
		{ID: "Ambulance A10", Type: "MEDICAL", Capacity: 2, Location: "Sector 1", Status: "available"},
		{ID: "Ambulance A17", Type: "MEDICAL", Capacity: 3, Location: "Sector 3", Status: "available"},
		{ID: "Rescue Boat B02", Type: "WATER", Capacity: 4, Location: "Sector 5", Status: "available"},
		{ID: "Rescue Boat B11", Type: "WATER", Capacity: 2, Location: "Sector 4", Status: "available"},
		{ID: "Fire Truck F09", Type: "FIRE", Capacity: 5, Location: "Sector 2", Status: "available"},
		{ID: "Fire Truck F15", Type: "FIRE", Capacity: 4, Location: "Sector 4", Status: "available"},
		{ID: "Helicopter H01", Type: "AIR", Capacity: 10, Location: "Sector 1", Status: "available"},
	}
	for _, u := range units {
		s.Resources[u.ID] = u
	}
}

func (s *StateLedger) bootstrapHospitals() {
	hospitals := []Hospital{
		{ID: "hosp-s1", Name: "Mercy Trauma Center", Location: "Sector 1", TotalBeds: 10, AvailableBeds: 10},
		{ID: "hosp-s2", Name: "City Burn ICU", Location: "Sector 2", TotalBeds: 8, AvailableBeds: 8},
		{ID: "hosp-s3", Name: "St. Luke General", Location: "Sector 3", TotalBeds: 12, AvailableBeds: 12},
		{ID: "hosp-s4", Name: "County General Hospital", Location: "Sector 4", TotalBeds: 15, AvailableBeds: 15},
		{ID: "hosp-s5", Name: "Emergency Evacuation Shelter", Location: "Sector 5", TotalBeds: 6, AvailableBeds: 6},
	}
	for _, h := range hospitals {
		s.Hospitals[h.ID] = h
	}
}

func (s *StateLedger) GetResourcesList() []Resource {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list := make([]Resource, 0, len(s.Resources))
	now := time.Now()
	for _, res := range s.Resources {
		expiry, leased := s.Leases[res.ID]
		if leased && expiry.After(now) {
			res.Status = "busy"
			res.LeaseTTL = int(expiry.Sub(now).Seconds())
		} else {
			res.Status = "available"
			res.LeaseTTL = 0
		}
		list = append(list, res)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	return list
}

func (s *StateLedger) GetHospitalsList() []Hospital {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	list := make([]Hospital, 0, len(s.Hospitals))
	for _, h := range s.Hospitals {
		// Release expired bed leases dynamically
		if expiry, leased := s.BedLeases[h.ID]; leased && expiry.Before(now) {
			if h.AvailableBeds < h.TotalBeds {
				h.AvailableBeds++
			}
		}
		list = append(list, h)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	return list
}

func (s *StateLedger) AddRoadClosure(c RoadClosure) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RoadClosures[c.Location] = c
}

func (s *StateLedger) GetRoadClosuresList() []RoadClosure {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]RoadClosure, 0, len(s.RoadClosures))
	for _, rc := range s.RoadClosures {
		list = append(list, rc)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Location < list[j].Location })
	return list
}

func (s *StateLedger) GetActiveRoadClosuresString() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var list []string
	for loc, rc := range s.RoadClosures {
		if rc.Status == "blocked" {
			list = append(list, loc)
		}
	}
	return list
}

func (s *StateLedger) RecordLatency(dur time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ms := float64(dur.Nanoseconds()) / 1e6
	if len(s.latencyTimes) < LatencyHistory {
		s.latencyTimes = append(s.latencyTimes, ms)
	} else {
		s.latencyTimes[s.latencyIndex] = ms
		s.latencyIndex = (s.latencyIndex + 1) % LatencyHistory
	}
}

func (s *StateLedger) GetP95Latency() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.latencyTimes) == 0 {
		return 0.0
	}
	temp := make([]float64, len(s.latencyTimes))
	copy(temp, s.latencyTimes)
	sort.Float64s(temp)
	idx := int(math.Ceil(float64(len(temp))*0.95)) - 1
	if idx < 0 {
		idx = 0
	}
	return temp[idx]
}

// --- WEBSOCKET MANAGER ---

type WSConnection struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

type WSManager struct {
	mu        sync.Mutex
	conns     map[*WSConnection]bool
	broadcast chan []byte
}

func NewWSManager() *WSManager {
	mgr := &WSManager{
		conns:     make(map[*WSConnection]bool),
		broadcast: make(chan []byte, 100),
	}
	go mgr.startBroadcasting()
	return mgr
}

func (w *WSManager) Register(conn *websocket.Conn) *WSConnection {
	w.mu.Lock()
	defer w.mu.Unlock()
	wsConn := &WSConnection{conn: conn}
	w.conns[wsConn] = true
	return wsConn
}

func (w *WSManager) Unregister(wsConn *WSConnection) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.conns[wsConn]; ok {
		delete(w.conns, wsConn)
		wsConn.conn.Close()
	}
}

func (w *WSManager) Broadcast(data interface{}) {
	payload, err := json.Marshal(data)
	if err != nil {
		log.Printf("WS broadcast serialization error: %v", err)
		return
	}
	w.broadcast <- payload
}

func (w *WSManager) startBroadcasting() {
	for payload := range w.broadcast {
		w.mu.Lock()
		for wsConn := range w.conns {
			go func(wc *WSConnection, msg []byte) {
				wc.mu.Lock()
				defer wc.mu.Unlock()
				wc.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := wc.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					log.Printf("WS write error: %v", err)
					w.Unregister(wc)
				}
			}(wsConn, payload)
		}
		w.mu.Unlock()
	}
}

// --- GLOBAL STATE ---

var (
	ledger    = NewStateLedger()
	wsManager = NewWSManager()
	upgrader  = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	redisClient *redis.Client
	kafkaWriter *kafka.Writer
	kafkaReader *kafka.Reader

	redisConnected = false
	kafkaConnected = false
)

func initExternalServices() {
	rClient := redis.NewClient(&redis.Options{
		Addr:         RedisAddr,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rClient.Ping(ctx).Err(); err != nil {
		log.Printf("WARN: Redis not reachable (%v). Falling back to in-memory state store.", err)
	} else {
		redisClient = rClient
		redisConnected = true
		log.Println("SUCCESS: Connected to Redis Cache.")
	}

	kWriter := &kafka.Writer{
		Addr:     kafka.TCP(KafkaAddr),
		Topic:    KafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	conn, err := kafka.DialLeader(context.Background(), "tcp", KafkaAddr, KafkaTopic, 0)
	if err != nil {
		log.Printf("WARN: Kafka not reachable (%v). Falling back to in-memory event channels.", err)
	} else {
		conn.Close()
		kafkaWriter = kWriter
		kafkaConnected = true
		log.Println("SUCCESS: Connected to Apache Kafka Broker.")
		go startKafkaConsumer()
	}
}

func startKafkaConsumer() {
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{KafkaAddr},
		Topic:    KafkaTopic,
		GroupID:  "resqgrid-go-gateway",
		MinBytes: 10e3,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,
	})
	defer kafkaReader.Close()

	log.Println("Kafka Background Consumer Started.")
	for {
		m, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Kafka consumer read error: %v", err)
			break
		}
		var incident Incident
		if err := json.Unmarshal(m.Value, &incident); err == nil {
			log.Printf("Kafka Event Consumed: Incident %s status updated to %s", incident.ID, incident.Status)
		}
	}
}

func broadcastSystemState() {
	type StateSnapshot struct {
		Event        string        `json:"event"`
		Incidents    []Incident    `json:"incidents"`
		RoadClosures []RoadClosure `json:"road_closures"`
		Resources    []Resource    `json:"resources"`
		Hospitals    []Hospital    `json:"hospitals"`
		Metrics      interface{}   `json:"metrics"`
	}

	ledger.mu.RLock()
	incList := make([]Incident, 0, len(ledger.Incidents))
	for _, inc := range ledger.Incidents {
		incList = append(incList, inc)
	}
	sort.Slice(incList, func(i, j int) bool { return incList[i].CreatedAt.After(incList[j].CreatedAt) })
	ledger.mu.RUnlock()

	p95 := ledger.GetP95Latency()
	doubleDispatchRate := 0.0
	ledger.mu.RLock()
	if ledger.dispatchAttempts > 0 {
		doubleDispatchRate = float64(ledger.doubleDispatchBlocks) / float64(ledger.dispatchAttempts) * 100.0
	}
	ledger.mu.RUnlock()

	metrics := map[string]interface{}{
		"ingestion_latency_p95_ms": p95,
		"double_dispatch_rate":     fmt.Sprintf("%.2f%%", doubleDispatchRate),
		"redis_connected":          redisConnected,
		"kafka_connected":          kafkaConnected,
	}

	wsManager.Broadcast(StateSnapshot{
		Event:        "state_update",
		Incidents:    incList,
		RoadClosures: ledger.GetRoadClosuresList(),
		Resources:    ledger.GetResourcesList(),
		Hospitals:    ledger.GetHospitalsList(),
		Metrics:      metrics,
	})
}

// --- HANDLERS ---

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WS upgrade error: %v", err)
		return
	}
	wsConn := wsManager.Register(conn)
	log.Printf("WS client connected: %s", conn.RemoteAddr().String())

	broadcastSystemState()

	defer func() {
		wsManager.Unregister(wsConn)
		log.Printf("WS client disconnected")
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	p95 := ledger.GetP95Latency()
	doubleDispatchRate := 0.0
	ledger.mu.RLock()
	if ledger.dispatchAttempts > 0 {
		doubleDispatchRate = float64(ledger.doubleDispatchBlocks) / float64(ledger.dispatchAttempts) * 100.0
	}
	ledger.mu.RUnlock()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"ingestion_latency_p95_ms": p95,
		"double_dispatch_rate":     fmt.Sprintf("%.2f%%", doubleDispatchRate),
		"redis_status":             redisConnected,
		"kafka_status":             kafkaConnected,
		"active_incidents_count":   len(ledger.Incidents),
		"active_road_closures":     ledger.GetActiveRoadClosuresString(),
	})
}

func explainHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	incidentID := r.URL.Path[len("/api/v1/explain/"):]
	if incidentID == "" {
		http.Error(w, `{"error":"Missing incident ID"}`, http.StatusBadRequest)
		return
	}

	ledger.mu.RLock()
	inc, ok := ledger.Incidents[incidentID]
	ledger.mu.RUnlock()

	if !ok || inc.Solution == nil {
		http.Error(w, `{"error":"Explanation not found for this incident"}`, http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(inc.Solution)
}

func roadClosureHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var rc RoadClosure
	if err := json.NewDecoder(r.Body).Decode(&rc); err != nil {
		http.Error(w, `{"error":"Invalid request payload"}`, http.StatusBadRequest)
		return
	}

	if rc.Location == "" || rc.Status == "" {
		http.Error(w, `{"error":"Location and Status are required"}`, http.StatusBadRequest)
		return
	}

	rc.ConfiguredAt = time.Now()
	ledger.AddRoadClosure(rc)

	log.Printf("Road Closure Updated: %s set to %s", rc.Location, rc.Status)
	broadcastSystemState()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "success",
		"location": rc.Location,
		"state":    rc.Status,
	})
}

func incidentHandler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		IdempotencyKey string `json:"idempotency_key"`
		Type           string `json:"type"`
		Severity       int    `json:"severity"`
		Location       string `json:"location"`
		Description    string `json:"description"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, `{"error":"Invalid request payload"}`, http.StatusBadRequest)
		return
	}

	if payload.Type == "" || payload.Location == "" || payload.Severity <= 0 {
		http.Error(w, `{"error":"Type, Location, and Severity are required parameters"}`, http.StatusBadRequest)
		return
	}

	// Idempotency check
	if payload.IdempotencyKey != "" {
		if redisConnected {
			val, err := redisClient.Get(context.Background(), "idemp:"+payload.IdempotencyKey).Result()
			if err == nil {
				log.Printf("SUCCESS: Idempotency cache hit for key %s", payload.IdempotencyKey)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(val))
				return
			}
		}

		ledger.mu.RLock()
		for _, inc := range ledger.Incidents {
			if inc.IdempotencyKey == payload.IdempotencyKey {
				ledger.mu.RUnlock()
				log.Printf("SUCCESS: Local state ledger hit for idempotency key %s", payload.IdempotencyKey)
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(inc)
				return
			}
		}
		ledger.mu.RUnlock()
	}

	incidentID := uuid.New().String()
	inc := Incident{
		ID:             incidentID,
		Type:           payload.Type,
		Severity:       payload.Severity,
		Location:       payload.Location,
		Description:    payload.Description,
		IdempotencyKey: payload.IdempotencyKey,
		Status:         "DISPATCHED",
		CreatedAt:      time.Now(),
	}

	ledger.mu.Lock()
	ledger.dispatchAttempts++
	ledger.mu.Unlock()

	activeClosures := ledger.GetActiveRoadClosuresString()
	resources := ledger.GetResourcesList()
	hospitals := ledger.GetHospitalsList()

	solverRequest := SolverRequest{
		Incident:     inc,
		RoadClosures: activeClosures,
		Resources:    resources,
		Hospitals:    hospitals,
	}

	solution, err := callSolver(solverRequest)
	if err != nil {
		log.Printf("ERROR: Solver call failed: %v. Reverting to fallback static solver.", err)
		solution = runFallbackSolver(solverRequest)
	}

	// Double-dispatch protection on vehicles
	now := time.Now()
	ledger.mu.Lock()
	doubleDispatchDetected := false
	for _, unitID := range solution.SelectedUnits {
		expiry, leased := ledger.Leases[unitID]
		if leased && expiry.After(now) {
			doubleDispatchDetected = true
			ledger.doubleDispatchBlocks++
			log.Printf("CRITICAL BLOCK: Double dispatch prevented on unit %s", unitID)
			break
		}
	}

	if doubleDispatchDetected {
		ledger.mu.Unlock()
		http.Error(w, `{"error":"Double-dispatch threat detected. Resource booking conflicts locked. Please try again."}`, http.StatusConflict)
		return
	}

	// Lock vehicle leases
	for _, unitID := range solution.SelectedUnits {
		expiryTime := now.Add(LeaseDuration)
		ledger.Leases[unitID] = expiryTime
		if redisConnected {
			redisClient.Set(context.Background(), "lease:"+unitID, expiryTime.Unix(), LeaseDuration)
		}
	}

	// Lock hospital bed lease
	if solution.SelectedHospitalID != "" {
		if h, ok := ledger.Hospitals[solution.SelectedHospitalID]; ok {
			if h.AvailableBeds > 0 {
				h.AvailableBeds--
				ledger.Hospitals[solution.SelectedHospitalID] = h
				expiryTime := now.Add(BedLeaseDur)
				ledger.BedLeases[solution.SelectedHospitalID] = expiryTime
				if redisConnected {
					redisClient.Set(context.Background(), "hbed:"+solution.SelectedHospitalID, expiryTime.Unix(), BedLeaseDur)
				}
				log.Printf("Hospital bed reserved: %s (beds remaining: %d)", solution.SelectedHospitalID, h.AvailableBeds)
			}
		}
	}

	solution.IncidentID = incidentID
	inc.Solution = solution
	ledger.Incidents[incidentID] = inc
	ledger.mu.Unlock()

	// Publish to Kafka
	eventBytes, err := json.Marshal(inc)
	if err == nil {
		if kafkaConnected {
			err = kafkaWriter.WriteMessages(context.Background(), kafka.Message{
				Key:   []byte(incidentID),
				Value: eventBytes,
			})
			if err != nil {
				log.Printf("WARN: Failed publishing event to Kafka stream (%v)", err)
			}
		}
	}

	// Cache idempotency response
	if redisConnected && payload.IdempotencyKey != "" {
		redisClient.Set(context.Background(), "idemp:"+payload.IdempotencyKey, string(eventBytes), 10*time.Minute)
	}

	duration := time.Since(startTime)
	ledger.RecordLatency(duration)

	log.Printf("Incident Dispatched: %s -> Units %v, Hospital %s in %s",
		incidentID, solution.SelectedUnits, solution.SelectedHospitalID, duration)
	broadcastSystemState()

	w.WriteHeader(http.StatusCreated)
	w.Write(eventBytes)
}

func callSolver(req SolverRequest) (*Solution, error) {
	payloadBytes, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, "POST", SolverURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("solver returned status %d: %s", resp.StatusCode, string(body))
	}

	var solverResp SolverResponse
	if err := json.NewDecoder(resp.Body).Decode(&solverResp); err != nil {
		return nil, err
	}

	return &Solution{
		SelectedUnits:      solverResp.SelectedUnits,
		ETA:                solverResp.ETA,
		HospitalBed:        solverResp.HospitalBed,
		SelectedHospitalID: solverResp.SelectedHospitalID,
		Explainability:     solverResp.Explainability,
		HospitalRejections: solverResp.HospitalRejections,
	}, nil
}

func runFallbackSolver(req SolverRequest) *Solution {
	var selected []string
	var rejections []RejectionLog
	incidentType := req.Incident.Type

	for _, u := range req.Resources {
		if u.Status == "busy" {
			rejections = append(rejections, RejectionLog{UnitID: u.ID, Reason: "UNIT_RESERVED"})
			continue
		}

		isBlocked := false
		for _, bc := range req.RoadClosures {
			if bc == u.Location {
				isBlocked = true
				break
			}
		}
		if isBlocked {
			rejections = append(rejections, RejectionLog{UnitID: u.ID, Reason: "ROUTE_INFEASIBLE"})
			continue
		}

		typeMatch := false
		if (incidentType == "MEDICAL" && u.Type == "MEDICAL") ||
			(incidentType == "FIRE" && u.Type == "FIRE") ||
			(incidentType == "FLOOD" && u.Type == "WATER") ||
			(u.Type == "AIR") {
			typeMatch = true
		}
		if !typeMatch {
			rejections = append(rejections, RejectionLog{UnitID: u.ID, Reason: "SPECIALTY_MISMATCH"})
			continue
		}

		if len(selected) < 2 {
			selected = append(selected, u.ID)
		} else {
			rejections = append(rejections, RejectionLog{UnitID: u.ID, Reason: "COVERAGE_FLOOR"})
		}
	}

	eta := 15.0
	if len(selected) > 0 {
		eta = 8.5
	}

	// Pick fallback hospital
	fallbackHospital := "County General - Fallback Wing"
	fallbackHospitalID := ""
	for _, h := range req.Hospitals {
		if h.AvailableBeds > 0 {
			fallbackHospital = h.Name + " (Fallback)"
			fallbackHospitalID = h.ID
			break
		}
	}

	return &Solution{
		SelectedUnits:      selected,
		ETA:                eta,
		HospitalBed:        fallbackHospital,
		SelectedHospitalID: fallbackHospitalID,
		Explainability:     rejections,
		HospitalRejections: []HospitalRejectionLog{},
	}
}

// --- MAIN FUNCTION ---

func main() {
	go initExternalServices()

	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/incidents", incidentHandler)
	mux.HandleFunc("/api/v1/road-closure", roadClosureHandler)
	mux.HandleFunc("/api/v1/status", statusHandler)
	mux.HandleFunc("/api/v1/explain/", explainHandler)
	mux.HandleFunc("/api/v1/ws", wsHandler)

	corsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, Idempotency-Key")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		mux.ServeHTTP(w, r)
	})

	log.Println("ResQGrid Ingestion Service running on port 8080...")
	if err := http.ListenAndServe(":8080", corsHandler); err != nil {
		log.Fatalf("Server launch failed: %v", err)
	}
}
