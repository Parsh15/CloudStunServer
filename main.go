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

	// Holds punch targets for agents: agentID -> userPublicAddr
	punchLock    sync.Mutex
	punchTargets = make(map[string]string)
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "10000"
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		agentsLock.RLock()
		defer agentsLock.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":        "online",
			"active_agents": len(agents),
		})
	})

	// Agent registers and polls for any pending user punch targets
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

		// Check if a user requested to connect to this agent
		punchLock.Lock()
		targetUser, hasTarget := punchTargets[a.ID]
		if hasTarget {
			delete(punchTargets, a.ID) // Consume target
		}
		punchLock.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "ok",
			"target_user": targetUser, // Tells agent to punch toward user
		})
	})

	// User requests the best agent AND provides its own public UDP address
	http.HandleFunc("/api/best-agent", func(w http.ResponseWriter, r *http.Request) {
		userAddr := r.URL.Query().Get("user_addr")
		if userAddr == "" {
			http.Error(w, `{"error":"user_addr parameter is required"}`, http.StatusBadRequest)
			return
		}

		agentsLock.RLock()
		defer agentsLock.RUnlock()

		var best *AgentInfo
		minScore := 999999.0
		now := time.Now()

		for _, a := range agents {
			if now.Sub(a.LastSeen) > 90*time.Second {
				continue
			}
			score := (a.CPUUsage * 0.6) + (a.RAMUsage * 0.4)
			if score < minScore {
				minScore = score
				best = a
			}
		}

		if best == nil {
			http.Error(w, `{"error":"No agents available"}`, http.StatusNotFound)
			return
		}

		// Save the user's public address for the agent to punch back
		punchLock.Lock()
		punchTargets[best.ID] = userAddr
		punchLock.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(best)
	})

	log.Printf("Signaling server listening on port %s", port)
	log.Fatal(http.ListenAndServe("0.0.0.0:"+port, nil))
}
