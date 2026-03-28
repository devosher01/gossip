package membership

import (
	"sync"
	"time"
)

// MemberStatus represents the health state of a cluster member.
// Follows the SWIM protocol state machine:
//
//	Alive -> Suspect -> Dead -> Removed
//
// Reference: "SWIM: Scalable Weakly consistent Infection-style Process Group Membership Protocol"
// Das et al. (2002)
type MemberStatus int

const (
	// Alive indicates the member is responding to heartbeats.
	Alive MemberStatus = iota
	// Suspect indicates the member has missed heartbeats and may have failed.
	// Other nodes should attempt indirect probing before marking as Dead.
	Suspect
	// Dead The dead indicates the member is confirmed failed and will be removed.
	Dead
)

// String returns the string representation of the status.
func (s MemberStatus) String() string {
	switch s {
	case Alive:
		return "alive"
	case Suspect:
		return "suspect"
	case Dead:
		return "dead"
	default:
		return "unknown"
	}
}

// Member represents a node in the cluster with its health metadata.
type Member struct {
	Addr       string
	Status     MemberStatus
	Heartbeat  uint64
	LastUpdate time.Time
}

// List maintains the membership view of the cluster.
// Thread-safe for concurrent access.
type List struct {
	mu      sync.RWMutex
	members map[string]*Member
	self    string
}

// NewList creates a new membership list with the local node as the initial member.
func NewList(selfAddr string) *List {
	l := &List{
		members: make(map[string]*Member),
		self:    selfAddr,
	}
	l.members[selfAddr] = &Member{
		Addr:       selfAddr,
		Status:     Alive,
		Heartbeat:  0,
		LastUpdate: time.Now(),
	}
	return l
}

// UpdateHeartbeat processes a heartbeat from a member.
// If the heartbeat is newer, the member is marked as Alive.
func (l *List) UpdateHeartbeat(addr string, heartbeat uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	m, ok := l.members[addr]
	if !ok {
		l.members[addr] = &Member{
			Addr:       addr,
			Status:     Alive,
			Heartbeat:  heartbeat,
			LastUpdate: time.Now(),
		}
		return
	}

	if heartbeat > m.Heartbeat {
		m.Heartbeat = heartbeat
		m.Status = Alive
		m.LastUpdate = time.Now()
	}
}

// GetMembers returns a snapshot of all members in the cluster.
// Only includes Live and Suspect members, excludes Dead.
func (l *List) GetMembers() []Member {
	l.mu.RLock()
	defer l.mu.RUnlock()
	res := make([]Member, 0, len(l.members))
	for _, m := range l.members {
		if m.Status != Dead {
			res = append(res, *m)
		}
	}
	return res
}

// GetAllMembers returns all members including Dead ones.
// Useful for debugging and observability.
func (l *List) GetAllMembers() []Member {
	l.mu.RLock()
	defer l.mu.RUnlock()
	res := make([]Member, 0, len(l.members))
	for _, m := range l.members {
		res = append(res, *m)
	}
	return res
}

// MarkSuspect transitions a member from Alive to Suspect.
// Returns true if the transition occurred.
func (l *List) MarkSuspect(addr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	m, ok := l.members[addr]
	if !ok || m.Status != Alive {
		return false
	}

	m.Status = Suspect
	m.LastUpdate = time.Now()
	return true
}

// MarkDead transitions a member from Suspect to Dead.
// Returns true if the transition occurred.
func (l *List) MarkDead(addr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	m, ok := l.members[addr]
	if !ok || m.Status != Suspect {
		return false
	}

	m.Status = Dead
	m.LastUpdate = time.Now()
	return true
}

// UpdateMemberStatus performs state machine transitions based on timeouts.
// Implements SWIM-style failure detection:
//   - Alive -> Suspect after suspectTimeout
//   - Suspect -> Dead after deadTimeout
//   - Dead members are removed after removeTimeout
func (l *List) UpdateMemberStatus(suspectTimeout, deadTimeout, removeTimeout time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	for addr, m := range l.members {
		if addr == l.self {
			continue
		}

		timeSinceUpdate := now.Sub(m.LastUpdate)

		switch m.Status {
		case Alive:
			if timeSinceUpdate > suspectTimeout {
				m.Status = Suspect
				m.LastUpdate = now
			}
		case Suspect:
			if timeSinceUpdate > deadTimeout {
				m.Status = Dead
				m.LastUpdate = now
			}
		case Dead:
			if timeSinceUpdate > removeTimeout {
				delete(l.members, addr)
			}
		}
	}
}

// RemoveDead immediately removes all members that have exceeded the timeout.
// This is a simplified version for backward compatibility.
func (l *List) RemoveDead(timeout time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for addr, m := range l.members {
		if addr == l.self {
			continue
		}
		if time.Since(m.LastUpdate) > timeout {
			delete(l.members, addr)
		}
	}
}

// Count returns the number of members in each state.
func (l *List) Count() (alive, suspect, dead int) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, m := range l.members {
		switch m.Status {
		case Alive:
			alive++
		case Suspect:
			suspect++
		case Dead:
			dead++
		}
	}
	return
}
