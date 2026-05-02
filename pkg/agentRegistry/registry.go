package agentregistry

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

const evictionTimeout = 90 * time.Second

type Agent struct {
	ID       string
	URL      string
	ADNLPort string
	LastSeen time.Time
}

type Registry struct {
	mu     sync.RWMutex
	agents map[string]*Agent
}

func New() *Registry {
	return &Registry{
		agents: make(map[string]*Agent),
	}
}

func (r *Registry) Register(url, adnlPort string) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := uuid.NewString()
	r.agents[id] = &Agent{
		ID:       id,
		URL:      url,
		ADNLPort: adnlPort,
		LastSeen: time.Now(),
	}
	return id
}

// Heartbeat refreshes the agent's last-seen timestamp. Returns false if the agent is unknown.
func (r *Registry) Heartbeat(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	a, ok := r.agents[id]
	if !ok {
		return false
	}
	a.LastSeen = time.Now()
	return true
}

// Active returns all agents that have sent a heartbeat within evictionTimeout.
func (r *Registry) Active() []*Agent {
	r.mu.RLock()
	defer r.mu.RUnlock()

	agents := make([]*Agent, 0, len(r.agents))
	for _, a := range r.agents {
		if time.Since(a.LastSeen) < evictionTimeout {
			agents = append(agents, a)
		}
	}
	return agents
}

// Evict removes agents that have not sent a heartbeat within evictionTimeout.
func (r *Registry) Evict() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, a := range r.agents {
		if time.Since(a.LastSeen) >= evictionTimeout {
			delete(r.agents, id)
		}
	}
}
