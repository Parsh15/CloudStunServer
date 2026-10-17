package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type AgentInfo struct {
	ID        string    `json:"id"`
	PublicAddr string   `json:"public_addr"`
	CPUUsage  float64   `json:"cpu_usage"`
	RAMUsage  float64   `json:"ram_usage"`
	LastSeen  time.Time `json:"last_seen"`
}

type Packet struct {
	Type     string    `json:"type"` // "agent_register", "agent_heartbeat", "user_request", "assigned_agent"
	AgentID  string    `json:"agent_id,omitempty"`
	CPUUsage float64   `json:"cpu_usage,omitempty"`
	RAMUsage float64   `json:"ram_usage,omitempty"`
	Agent    *AgentInfo `json:"agent,omitempty"`
}

var (
	agentsLock sync.RWMutex
	agents     = make(map[string]*AgentInfo)
)

func main() {
	addr := net.UDPAddr{Port: 9000, IP: net.ParseIP("0.0.0.0")}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		log.Fatalf("Failed to bind UDP: %v", err)
	}
	defer conn.Close()
	log.Println("STUN / Coordinator listening on :9000 UDP...")

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
			// Find least-loaded agent based on combined CPU + RAM load score
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

			// Signal agent to start sending punch packets to this user
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
