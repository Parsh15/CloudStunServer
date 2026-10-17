package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type AgentInfo struct {
	ID         string    `json:"id"`
	PublicAddr string    `json:"public_addr"`
	CPUUsage   float64   `json:"cpu_usage"`
	RAMUsage   float64   `json:"ram_usage"`
	LastSeen   time.Time `json:"last_seen"`
}

var (
	agentsLock sync.RWMutex
	agents     = make(map[string]*AgentInfo)
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "10000"
	}

	// Health check
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		agentsLock.RLock()
		defer agentsLock.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":        "online",
			"active_agents": len(agents),
		})
	})

	// Agent registers its public IP:Port + Metrics via HTTP POST
	http.HandleFunc("/api/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var a AgentInfo
		if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		a.LastSeen = time.Now()

		agentsLock.Lock()
		agents[a.ID] = &a
		agentsLock.Unlock()

		log.Printf("Agent registered: %s at %s (CPU: %.1f%%, RAM: %.1fMB)", a.ID, a.PublicAddr, a.CPUUsage, a.RAMUsage)
		w.WriteHeader(http.StatusOK)
	})

	// User requests the least-loaded agent via HTTP GET
	http.HandleFunc("/api/best-agent", func(w http.ResponseWriter, r *http.Request) {
		agentsLock.RLock()
		defer agentsLock.RUnlock()

		var best *AgentInfo
		minScore := 99999.0
		now := time.Now()

		for _, a := range agents {
			// Ignore agents inactive for > 90 seconds
			if now.Sub(a.LastSeen) > 90*time.Second {
				continue
			}
			score := (a.CPUUsage * 0.6) + (a.RAMUsage * 0.4)
			if score < minScore {
				minScore = score
				best = a
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if best == nil {
			http.Error(w, `{"error":"No agents available"}`, http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(best)
	})

	log.Printf("Signaling server running on port %s", port)
	log.Fatal(http.ListenAndServe("0.0.0.0:"+port, nil))
}
