package data

import (
	"fmt"
	"maps"
	"sync"
)

// Ordering represents the causal relationship between two vector clocks.
type Ordering int

const (
	// Concurrent indicates events happened concurrently (no causal relationship).
	Concurrent Ordering = iota
	// Before indicates the first event happened the second ago.
	Before
	// After indicates the first event happened after the second.
	After
	// Equal indicates both vector clocks represent the same logical time.
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

// VectorClock implements Lamport's vector clocks for tracking causality in distributed systems.
// It provides a happens-before partial ordering of events across replicas.
//
// Properties:
//   - If event A happened before event B, then VC(A) < VC(B)
//   - If VC(A) < VC(B), then A causally precedes B
//   - If neither VC(A) < VC(B) nor VC(B) < VC(A), then A and B are concurrent
//
// Reference: Lamport, L. (1978). "Time, Clocks, and the Ordering of Events"
// Thread-safe for concurrent operations.
type VectorClock struct {
	mu     sync.RWMutex
	clocks map[string]uint64
}

// NewVectorClock creates a new VectorClock with an empty clock state.
func NewVectorClock() *VectorClock {
	return &VectorClock{
		clocks: make(map[string]uint64),
	}
}

// NewVectorClockWithCapacity creates a VectorClock with reallocated capacity.
// Use when the number of nodes is known in advance to reduce allocations.
func NewVectorClockWithCapacity(capacity int) *VectorClock {
	return &VectorClock{
		clocks: make(map[string]uint64, capacity),
	}
}

// Increment atomically advances the logical clock for the specified node ID.
// This should be called whenever a local event occurs.
//
// Returns error if id is empty or if incrementing would cause overflow.
func (vc *VectorClock) Increment(id string) error {
	if id == "" {
		return ErrInvalidNodeID
	}

	vc.mu.Lock()
	defer vc.mu.Unlock()

	current := vc.clocks[id]
	if current == ^uint64(0) {
		return fmt.Errorf("%w: cannot increment clock for node %s", ErrOverflow, id)
	}

	vc.clocks[id]++
	return nil
}

// Merge updates this vector clock with the maximum values from another replica.
// This implements the join operation for vector clocks and should be called
// when receiving messages from other nodes.
//
// After merging with a remote clock, the local clock reflects all causally
// dependent events from both replicas.
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

// GetClocks returns a snapshot of the current clock state.
// The returned map is a deep copy safe to modify.
func (vc *VectorClock) GetClocks() map[string]uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return maps.Clone(vc.clocks)
}

// Get returns the clock value for a specific node.
// Returns 0 if the node has never been observed.
func (vc *VectorClock) Get(id string) uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.clocks[id]
}

// Set explicitly sets the clock value for a node.
// Use with caution - this can violate causality if misused.
func (vc *VectorClock) Set(id string, value uint64) error {
	if id == "" {
		return ErrInvalidNodeID
	}

	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clocks[id] = value
	return nil
}

// Copy creates a deep copy of the vector clock.
func (vc *VectorClock) Copy() *VectorClock {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	return &VectorClock{
		clocks: maps.Clone(vc.clocks),
	}
}

// Reset clears all clock state. Use only for testing.
func (vc *VectorClock) Reset() {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	clear(vc.clocks)
}

// Compare determines the causal ordering between two vector clocks.
// Returns:
//   - Before: if vc1 happened before vc2
//   - After: if vc1 happened after vc2
//   - Concurrent: if vc1 and vc2 are causally independent
//   - Equal: if vc1 and vc2 are identical,
//
// This is a pure function with no side effects.
func Compare(vc1, vc2 map[string]uint64) Ordering {
	if vc1 == nil && vc2 == nil {
		return Equal
	}
	if vc1 == nil || vc2 == nil {
		return Concurrent
	}

	lessOrEqual := true
	greaterOrEqual := true

	// Collect all node IDs from both clocks
	allKeys := make(map[string]struct{}, len(vc1)+len(vc2))
	for k := range vc1 {
		allKeys[k] = struct{}{}
	}
	for k := range vc2 {
		allKeys[k] = struct{}{}
	}

	// Check if vc1 <= vc2 and vc1 >= vc2
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

	// Determine ordering
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

// HappensBefore returns true if this clock causally precedes the other.
func (vc *VectorClock) HappensBefore(other *VectorClock) bool {
	if other == nil {
		return false
	}
	return Compare(vc.GetClocks(), other.GetClocks()) == Before
}

// HappensAfter returns true if this clock causally follows the other.
func (vc *VectorClock) HappensAfter(other *VectorClock) bool {
	if other == nil {
		return false
	}
	return Compare(vc.GetClocks(), other.GetClocks()) == After
}

// IsConcurrentWith returns true if this clock is causally independent of the other.
func (vc *VectorClock) IsConcurrentWith(other *VectorClock) bool {
	if other == nil {
		return false
	}
	return Compare(vc.GetClocks(), other.GetClocks()) == Concurrent
}
