package data

import (
	"errors"
	"math"
	"sync"
	"testing"
)

func TestGCounter_Increment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		nodeID  string
		delta   uint64
		wantErr error
	}{
		{
			name:    "valid increment",
			nodeID:  "node1",
			delta:   5,
			wantErr: nil,
		},
		{
			name:    "empty node ID",
			nodeID:  "",
			delta:   1,
			wantErr: ErrInvalidNodeID,
		},
		{
			name:    "zero delta",
			nodeID:  "node1",
			delta:   0,
			wantErr: ErrInvalidDelta,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := NewGCounter()
			err := c.Increment(tt.nodeID, tt.delta)

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Increment() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil && c.GetNodeValue(tt.nodeID) != tt.delta {
				t.Errorf("Increment() value = %d, want %d", c.GetNodeValue(tt.nodeID), tt.delta)
			}
		})
	}
}

func TestGCounter_Overflow(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	if err := c.Increment("node1", math.MaxUint64); err != nil {
		t.Fatalf("first increment failed: %v", err)
	}

	err := c.Increment("node1", 1)
	if !errors.Is(err, ErrOverflow) {
		t.Errorf("expected overflow error, got %v", err)
	}
}

func TestGCounter_Value(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	_ = c.Increment("node1", 10)
	_ = c.Increment("node2", 20)
	_ = c.Increment("node3", 5)

	if got := c.Value(); got != 35 {
		t.Errorf("Value() = %d, want %d", got, 35)
	}
}

func TestGCounter_Merge_Commutativity(t *testing.T) {
	t.Parallel()

	// Create two counters with different states
	c1 := NewGCounter()
	_ = c1.Increment("node1", 10)
	_ = c1.Increment("node2", 5)

	c2 := NewGCounter()
	_ = c2.Increment("node1", 8)
	_ = c2.Increment("node3", 15)

	// Merge c2 into c1
	c1Copy := NewGCounter()
	_ = c1Copy.Merge(c1.GetState())
	_ = c1Copy.Merge(c2.GetState())

	// Merge c1 into c2 (reverse order)
	c2Copy := NewGCounter()
	_ = c2Copy.Merge(c2.GetState())
	_ = c2Copy.Merge(c1.GetState())

	// Both should converge to the same value
	if c1Copy.Value() != c2Copy.Value() {
		t.Errorf("merge not commutative: c1Copy=%d, c2Copy=%d", c1Copy.Value(), c2Copy.Value())
	}

	// Verify expected state: max(10,8) + max(5,0) + max(0,15) = 10 + 5 + 15 = 30
	if got := c1Copy.Value(); got != 30 {
		t.Errorf("merged value = %d, want %d", got, 30)
	}
}

func TestGCounter_Merge_Idempotency(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	_ = c.Increment("node1", 10)

	state := c.GetState()

	// Merge the same state multiple times
	_ = c.Merge(state)
	val1 := c.Value()

	_ = c.Merge(state)
	val2 := c.Value()

	if val1 != val2 {
		t.Errorf("merge not idempotent: first=%d, second=%d", val1, val2)
	}
}

func TestGCounter_Merge_NilState(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	err := c.Merge(nil)

	if !errors.Is(err, ErrNilState) {
		t.Errorf("expected ErrNilState, got %v", err)
	}
}

func TestGCounter_ConcurrentIncrements(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	const (
		numGoroutines          = 100
		incrementsPerGoroutine = 100
	)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			nodeID := "node1"
			for j := 0; j < incrementsPerGoroutine; j++ {
				_ = c.Increment(nodeID, 1)
			}
		}(i)
	}

	wg.Wait()

	expected := uint64(numGoroutines * incrementsPerGoroutine)
	if got := c.Value(); got != expected {
		t.Errorf("concurrent increments: got %d, want %d", got, expected)
	}
}

func TestGCounter_GetState_IsolationCopy(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	_ = c.Increment("node1", 10)

	state := c.GetState()
	state["node1"] = 999 // Mutate returned state

	// Original counter should not be affected
	if c.GetNodeValue("node1") != 10 {
		t.Errorf("GetState() did not return isolated copy")
	}
}

func TestGCounter_NodeCount(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	_ = c.Increment("node1", 1)
	_ = c.Increment("node2", 1)
	_ = c.Increment("node3", 1)

	if got := c.NodeCount(); got != 3 {
		t.Errorf("NodeCount() = %d, want %d", got, 3)
	}
}

func TestGCounter_Reset(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	_ = c.Increment("node1", 10)
	_ = c.Increment("node2", 20)

	c.Reset()

	if got := c.Value(); got != 0 {
		t.Errorf("Reset() failed: Value() = %d, want 0", got)
	}

	if got := c.NodeCount(); got != 0 {
		t.Errorf("Reset() failed: NodeCount() = %d, want 0", got)
	}
}

func TestNewGCounterWithCapacity(t *testing.T) {
	t.Parallel()

	c := NewGCounterWithCapacity(100)
	if c == nil {
		t.Fatal("NewGCounterWithCapacity returned nil")
	}

	// Verify it works normally
	_ = c.Increment("node1", 5)
	if c.Value() != 5 {
		t.Errorf("Value() = %d, want 5", c.Value())
	}
}

// Benchmark tests
func BenchmarkGCounter_Increment(b *testing.B) {
	c := NewGCounter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Increment("node1", 1)
	}
}

func BenchmarkGCounter_Value(b *testing.B) {
	c := NewGCounter()
	for i := 0; i < 100; i++ {
		_ = c.Increment("node1", 1)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Value()
	}
}

func BenchmarkGCounter_Merge(b *testing.B) {
	c1 := NewGCounter()
	c2 := NewGCounter()

	for i := 0; i < 100; i++ {
		_ = c2.Increment("node1", 1)
	}

	state := c2.GetState()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = c1.Merge(state)
	}
}

func BenchmarkGCounter_ConcurrentIncrement(b *testing.B) {
	c := NewGCounter()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = c.Increment("node1", 1)
		}
	})
}
