package data

import (
	"errors"
	"fmt"
	"maps"
	"sync"
)

var (
	// ErrInvalidNodeID is returned when a node ID is empty or invalid.
	ErrInvalidNodeID = errors.New("node ID cannot be empty")
	// ErrInvalidDelta is returned when attempting to increment by zero.
	ErrInvalidDelta = errors.New("delta must be greater than zero")
	// ErrNilState is returned when attempting to merge with nil state.
	ErrNilState = errors.New("cannot merge with nil state")
	// ErrOverflow is returned when an operation would cause numeric overflow.
	ErrOverflow = errors.New("operation would cause numeric overflow")
)

// GCounter implements a Grow-only Counter CRDT as defined in Shapiro et al. (2011).
// It guarantees strong eventual consistency: all replicas that have received
// the same updates will converge to the same state, regardless of message ordering.
//
// Properties:
//   - Commutative: merge(A, B) == merge(B, A)
//   - Associative: merge(merge(A, B), C) == merge(A, merge(B, C))
//   - Idempotent: merge(A, A) == A
//
// Thread-safe for concurrent operations.
type GCounter struct {
	mu     sync.RWMutex
	counts map[string]uint64
}

// NewGCounter creates a new GCounter instance with an empty state.
// The returned counter is ready for concurrent use.
func NewGCounter() *GCounter {
	return &GCounter{
		counts: make(map[string]uint64),
	}
}

// NewGCounterWithCapacity creates a new GCounter with reallocated capacity.
// Use this when you know the approximate number of nodes in advance to reduce allocations.
func NewGCounterWithCapacity(capacity int) *GCounter {
	return &GCounter{
		counts: make(map[string]uint64, capacity),
	}
}

// Increment atomically increases the counter for the specified node by delta.
// Returns an error if nodeID is empty or delta is zero.
//
// This operation is local and does not require coordination with other replicas.
// The increment is guaranteed to be monotonic for each node.
func (c *GCounter) Increment(nodeID string, delta uint64) error {
	if nodeID == "" {
		return ErrInvalidNodeID
	}
	if delta == 0 {
		return ErrInvalidDelta
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for overflow before incrementing
	current := c.counts[nodeID]
	if delta > (^uint64(0) - current) {
		return fmt.Errorf("%w: current=%d, delta=%d", ErrOverflow, current, delta)
	}

	c.counts[nodeID] += delta
	return nil
}

// Value returns the total sum of all node counters.
// This operation is linearizable with respect to other operations on the same GCounter.
//
// The returned value represents the global state as observed by this replica.
func (c *GCounter) Value() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var total uint64
	for _, v := range c.counts {
		total += v
	}
	return total
}

// Merge combines the state from another GCounter replica using the max function.
// This implements the join semilattice operation for CRDTs.
//
// After merge, this counter will have observed all increments from both replicas.
// Returns error if the input state is nil.
func (c *GCounter) Merge(other map[string]uint64) error {
	if other == nil {
		return ErrNilState
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for node, val := range other {
		if val > c.counts[node] {
			c.counts[node] = val
		}
	}
	return nil
}

// GetState returns a snapshot of the current counter-state.
// The returned map is a deep copy and safe to modify.
//
// Use this for serialization and transmission to other replicas.
func (c *GCounter) GetState() map[string]uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Use maps.Clone for efficient deep copy (Go 1.21+)
	return maps.Clone(c.counts)
}

// Reset clears all counter-state. Use with extreme caution.
// This violates CRDT monotonicity and should only be used for testing
// or when starting a new epoch with coordination.
func (c *GCounter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.counts) // Go 1.21+ builtin
}

// NodeCount returns the number of unique nodes that have incremented this counter.
func (c *GCounter) NodeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.counts)
}

// GetNodeValue returns the counter-value for a specific node.
// Returns 0 if the node has never incremented.
func (c *GCounter) GetNodeValue(nodeID string) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.counts[nodeID]
}
