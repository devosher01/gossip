package membership

import (
	"iter"
	"sync"
	"time"
)

type Status int

const (
	Alive Status = iota
	Suspect
	Dead
)

func (s Status) String() string {
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

type Member struct {
	Addr       string
	Status     Status
	Heartbeat  uint64
	LastUpdate time.Time
}

type List struct {
	mu      sync.RWMutex
	members map[string]*Member
	self    string
}

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

// UpdateHeartbeat uses monotonic counter-comparison, not wall clock.
// NTP adjustments can move wall clocks backwards; a counter only moves forward.
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

func (l *List) All() iter.Seq[Member] {
	return func(yield func(Member) bool) {
		l.mu.RLock()
		defer l.mu.RUnlock()
		for _, m := range l.members {
			if m.Status == Dead {
				continue
			}
			if !yield(*m) {
				return
			}
		}
	}
}

func (l *List) Snapshot() []Member {
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

// Tick State transitions use relative durations, not absolute timestamps,
// so the system is independent of the clock skew between nodes.
func (l *List) Tick(suspectTimeout, deadTimeout, removeTimeout time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	for addr, m := range l.members {
		if addr == l.self {
			continue
		}

		elapsed := now.Sub(m.LastUpdate)

		switch m.Status {
		case Alive:
			if elapsed > suspectTimeout {
				m.Status = Suspect
				m.LastUpdate = now
			}
		case Suspect:
			if elapsed > deadTimeout {
				m.Status = Dead
				m.LastUpdate = now
			}
		case Dead:
			if elapsed > removeTimeout {
				delete(l.members, addr)
			}
		}
	}
}

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
