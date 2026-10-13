package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Health struct {
	AgentID        string    `json:"agentId"`
	Timestamp      time.Time `json:"timestamp"`
	CPUPercent     float64   `json:"cpuPercent"`
	RAMUsedGB      float64   `json:"ramUsedGB"`
	RAMTotalGB     float64   `json:"ramTotalGB"`
	RAMAvailableGB float64   `json:"ramAvailableGB"`
	RAMPercent     float64   `json:"ramPercent"`
	DiskFreeGB     float64   `json:"diskFreeGB"`
	DiskTotalGB    float64   `json:"diskTotalGB"`
	Status         string    `json:"status"`
}
type Worker struct {
	Health   Health    `json:"health"`
	LastSeen time.Time `json:"lastSeen"`
	Online   bool      `json:"online"`
	Endpoint string    `json:"endpoint,omitempty"`
}
type Server struct {
	mu      sync.RWMutex
	workers map[string]Worker
	token   string
}

func main() {
	token := os.Getenv("CLOUD_TOKEN")
	if len(token) < 24 {
		log.Fatal("CLOUD_TOKEN is required and must be at least 24 characters")
	}
	 port := os.Getenv("PORT")
     if port == "" {
    port = "8080"
      }

     addr := "0.0.0.0:" + port
	s := &Server{workers: map[string]Worker{}, token: token}
	m := http.NewServeMux()
	m.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	m.HandleFunc("/api/workers/health", s.auth(s.health))
	m.HandleFunc("/api/workers/endpoint", s.auth(s.endpoint))
	m.HandleFunc("/api/workers", s.auth(s.list))
	m.HandleFunc("/api/rendezvous", s.auth(s.rendezvous))
	log.Println("Coordinator listening on", addr)
	log.Fatal(http.ListenAndServe(addr, m))
}
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+s.token {
			writeJSON(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
}
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}
	var h Health
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&h) != nil || strings.TrimSpace(h.AgentID) == "" {
		writeJSON(w, 400, map[string]string{"error": "invalid health report"})
		return
	}
	now := time.Now().UTC()
	s.mu.Lock()
	v := s.workers[h.AgentID]
	v.Health = h
	v.LastSeen = now
	v.Online = true
	s.workers[h.AgentID] = v
	s.mu.Unlock()
	writeJSON(w, 200, map[string]string{"status": "updated"})
}
func (s *Server) endpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}
	var in struct {
		AgentID  string `json:"agentId"`
		Endpoint string `json:"endpoint"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&in) != nil || in.AgentID == "" {
		writeJSON(w, 400, map[string]string{"error": "invalid request"})
		return
	}
	host, port, err := net.SplitHostPort(in.Endpoint)
	if err != nil || host == "" || port == "" {
		writeJSON(w, 400, map[string]string{"error": "endpoint must be host:port"})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.workers[in.AgentID]
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "worker must report health first"})
		return
	}
	v.Endpoint = in.Endpoint
	s.workers[in.AgentID] = v
	writeJSON(w, 200, map[string]string{"status": "endpoint registered"})
}
func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeJSON(w, 405, map[string]string{"error": "GET required"})
		return
	}
	now := time.Now()
	s.mu.Lock()
	out := make([]Worker, 0, len(s.workers))
	for id, v := range s.workers {
		v.Online = now.Sub(v.LastSeen) <= 35*time.Second
		s.workers[id] = v
		out = append(out, v)
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Health.CPUPercent == out[j].Health.CPUPercent {
			return out[i].Health.RAMAvailableGB > out[j].Health.RAMAvailableGB
		}
		return out[i].Health.CPUPercent < out[j].Health.CPUPercent
	})
	writeJSON(w, 200, map[string]any{"workers": out})
}
func (s *Server) rendezvous(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}
	var in struct {
		WorkerID string `json:"workerId"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&in) != nil || in.WorkerID == "" {
		writeJSON(w, 400, map[string]string{"error": "workerId required"})
		return
	}
	s.mu.RLock()
	v, ok := s.workers[in.WorkerID]
	s.mu.RUnlock()
	if !ok || !v.Online || v.Endpoint == "" {
		writeJSON(w, 409, map[string]string{"error": "worker offline or UDP endpoint not registered"})
		return
	}
	writeJSON(w, 200, map[string]any{"workerId": in.WorkerID, "endpoint": v.Endpoint})
}
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
