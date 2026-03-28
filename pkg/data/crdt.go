package data

import (
	"errors"
	"fmt"
	"iter"
	"maps"
	"sync"
)

var (
	ErrInvalidNodeID = errors.New("node ID cannot be empty")
	ErrInvalidDelta  = errors.New("delta must be greater than zero")
	ErrNilState      = errors.New("cannot merge with nil state")
	ErrOverflow      = errors.New("operation would cause numeric overflow")
)

// GCounter is a Grow-only Counter CRDT (Shapiro et al., 2011).
// Convergence is guaranteed by the join semilattice: merge uses max() per node,
// which is commutative, associative, and idempotent by construction.
type GCounter struct {
	mu     sync.RWMutex
	counts map[string]uint64
}

func NewGCounter() *GCounter {
	return &GCounter{
		counts: make(map[string]uint64),
	}
}

func (c *GCounter) Increment(nodeID string, delta uint64) error {
	if nodeID == "" {
		return ErrInvalidNodeID
	}
	if delta == 0 {
		return ErrInvalidDelta
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.counts[nodeID]
	if delta > (^uint64(0) - current) {
		return fmt.Errorf("%w: current=%d delta=%d", ErrOverflow, current, delta)
	}

	c.counts[nodeID] += delta
	return nil
}

func (c *GCounter) Value() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var total uint64
	for _, v := range c.counts {
		total += v
	}
	return total
}

// Merge applies the join semilattice operation: max(local[k], remote[k]) for each node k.
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

// MergeIter merges from an iterator without requiring the caller to allocate a map snapshot.
func (c *GCounter) MergeIter(seq iter.Seq2[string, uint64]) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for node, val := range seq {
		if val > c.counts[node] {
			c.counts[node] = val
		}
	}
}

func (c *GCounter) GetState() map[string]uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return maps.Clone(c.counts)
}

func (c *GCounter) NodeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.counts)
}

func (c *GCounter) GetNodeValue(nodeID string) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.counts[nodeID]
}
