package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
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

type Packet struct {
	Type     string     `json:"type"` // "agent_register", "agent_heartbeat", "user_request", "assigned_agent"
	AgentID  string     `json:"agent_id,omitempty"`
	CPUUsage float64    `json:"cpu_usage,omitempty"`
	RAMUsage float64    `json:"ram_usage,omitempty"`
	Agent    *AgentInfo `json:"agent,omitempty"`
}

var (
	agentsLock sync.RWMutex
	agents     = make(map[string]*AgentInfo)
)

func main() {
	// 1. Start HTTP Health Check Server for Render
	httpPort := os.Getenv("PORT")
	if httpPort == "" {
		httpPort = "10000" // Render default
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

	go func() {
		log.Printf("Starting HTTP health-check listener on 0.0.0.0:%s for Render", httpPort)
		if err := http.ListenAndServe("0.0.0.0:"+httpPort, nil); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// 2. Start UDP Coordinator Server
	udpPort := 9000
	if envUDP := os.Getenv("UDP_PORT"); envUDP != "" {
		if p, err := strconv.Atoi(envUDP); err == nil {
			udpPort = p
		}
	}

	udpAddr := net.UDPAddr{Port: udpPort, IP: net.ParseIP("0.0.0.0")}
	conn, err := net.ListenUDP("udp", &udpAddr)
	if err != nil {
		log.Fatalf("Failed to bind UDP: %v", err)
	}
	defer conn.Close()
	log.Printf("STUN / Coordinator listening on 0.0.0.0:%d UDP...", udpPort)

	// Housekeeper to purge inactive agents (>2 mins without heartbeat)
	go func() {
		for range time.Tick(30 * time.Second) {
			agentsLock.Lock()
			now := time.Now()
			for id, agent := range agents {
				if now.Sub(agent.LastSeen) > 2*time.Minute {
					delete(agents, id)
					log.Printf("Agent %s expired", id)
				}
			}
			agentsLock.Unlock()
		}
	}()

	buf := make([]byte, 4096)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("UDP Read error: %v", err)
			continue
		}

		var pkt Packet
		if err := json.Unmarshal(buf[:n], &pkt); err != nil {
			continue
		}

		switch pkt.Type {
		case "agent_register", "agent_heartbeat":
			agentsLock.Lock()
			agents[pkt.AgentID] = &AgentInfo{
				ID:         pkt.AgentID,
				PublicAddr: remoteAddr.String(),
				CPUUsage:   pkt.CPUUsage,
				RAMUsage:   pkt.RAMUsage,
				LastSeen:   time.Now(),
			}
			agentsLock.Unlock()
			log.Printf("[%s] %s registered from %s (CPU: %.1f%%, RAM: %.1f%%)",
				pkt.Type, pkt.AgentID, remoteAddr.String(), pkt.CPUUsage, pkt.RAMUsage)

		case "user_request":
			agentsLock.RLock()
			var bestAgent *AgentInfo
			minScore := 9999.0
			for _, a := range agents {
				score := (a.CPUUsage * 0.6) + (a.RAMUsage * 0.4)
				if score < minScore {
					minScore = score
					bestAgent = a
				}
			}
			agentsLock.RUnlock()

			resp := Packet{Type: "assigned_agent"}
			if bestAgent != nil {
				resp.Agent = bestAgent
				log.Printf("Assigned Agent %s (%s) to User %s", bestAgent.ID, bestAgent.PublicAddr, remoteAddr.String())
			}

			data, _ := json.Marshal(resp)
			conn.WriteToUDP(data, remoteAddr)

			if bestAgent != nil {
				agentUDPAddr, err := net.ResolveUDPAddr("udp", bestAgent.PublicAddr)
				if err == nil {
					punchMsg, _ := json.Marshal(map[string]string{
						"type":      "punch_peer",
						"peer_addr": remoteAddr.String(),
					})
					conn.WriteToUDP(punchMsg, agentUDPAddr)
				}
			}
		}
	}
}
