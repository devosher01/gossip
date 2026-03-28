package data

import (
	"fmt"
	"iter"
	"maps"
	"sync"
)

type Ordering int

const (
	Concurrent Ordering = iota
	Before
	After
	Equal
)

func (o Ordering) String() string {
	switch o {
	case Concurrent:
		return "concurrent"
	case Before:
		return "before"
	case After:
		return "after"
	case Equal:
		return "equal"
	default:
		return "unknown"
	}
}

// VectorClock tracks causality across replicas using Lamport's vector clocks.
// Without a global clock (impossible in distributed systems — Lamport 1978, Kleppmann ch.8),
// this is the only way to establish a happens-before partial order between events.
type VectorClock struct {
	mu     sync.RWMutex
	clocks map[string]uint64
}

func NewVectorClock() *VectorClock {
	return &VectorClock{
		clocks: make(map[string]uint64),
	}
}

func (vc *VectorClock) Increment(id string) error {
	if id == "" {
		return ErrInvalidNodeID
	}

	vc.mu.Lock()
	defer vc.mu.Unlock()

	current := vc.clocks[id]
	if current == ^uint64(0) {
		return fmt.Errorf("%w: node %s", ErrOverflow, id)
	}

	vc.clocks[id]++
	return nil
}

func (vc *VectorClock) Merge(other map[string]uint64) error {
	if other == nil {
		return ErrNilState
	}

	vc.mu.Lock()
	defer vc.mu.Unlock()

	for id, val := range other {
		if val > vc.clocks[id] {
			vc.clocks[id] = val
		}
	}
	return nil
}

func (vc *VectorClock) MergeIter(seq iter.Seq2[string, uint64]) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	for id, val := range seq {
		if val > vc.clocks[id] {
			vc.clocks[id] = val
		}
	}
}

func (vc *VectorClock) GetClocks() map[string]uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return maps.Clone(vc.clocks)
}

func (vc *VectorClock) Get(id string) uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.clocks[id]
}

func (vc *VectorClock) Set(id string, value uint64) error {
	if id == "" {
		return ErrInvalidNodeID
	}

	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clocks[id] = value
	return nil
}

func (vc *VectorClock) Copy() *VectorClock {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	return &VectorClock{
		clocks: maps.Clone(vc.clocks),
	}
}

// Compare Before: all components <= and at least one <.
// Concurrent: causally independent — neither could have influenced the other.
func (vc *VectorClock) Compare(other *VectorClock) Ordering {
	if other == nil {
		return Concurrent
	}

	vc.mu.RLock()
	defer vc.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	return compareClocks(vc.clocks, other.clocks)
}

func (vc *VectorClock) HappensBefore(other *VectorClock) bool {
	return vc.Compare(other) == Before
}

func (vc *VectorClock) HappensAfter(other *VectorClock) bool {
	return vc.Compare(other) == After
}

func (vc *VectorClock) IsConcurrentWith(other *VectorClock) bool {
	return vc.Compare(other) == Concurrent
}

func CompareRaw(vc1, vc2 map[string]uint64) Ordering {
	return compareClocks(vc1, vc2)
}

func compareClocks(vc1, vc2 map[string]uint64) Ordering {
	if vc1 == nil && vc2 == nil {
		return Equal
	}
	if vc1 == nil || vc2 == nil {
		return Concurrent
	}

	lessOrEqual := true
	greaterOrEqual := true

	allKeys := make(map[string]struct{}, len(vc1)+len(vc2))
	for k := range vc1 {
		allKeys[k] = struct{}{}
	}
	for k := range vc2 {
		allKeys[k] = struct{}{}
	}

	for k := range allKeys {
		v1 := vc1[k]
		v2 := vc2[k]

		if v1 > v2 {
			lessOrEqual = false
		}
		if v1 < v2 {
			greaterOrEqual = false
		}
	}

	switch {
	case lessOrEqual && greaterOrEqual:
		return Equal
	case lessOrEqual:
		return Before
	case greaterOrEqual:
		return After
	default:
		return Concurrent
	}
}
