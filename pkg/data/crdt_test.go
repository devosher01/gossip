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
		{"valid increment", "node1", 5, nil},
		{"empty node ID", "", 1, ErrInvalidNodeID},
		{"zero delta", "node1", 0, ErrInvalidDelta},
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
		t.Errorf("Value() = %d, want 35", got)
	}
}

func TestGCounter_Merge_Commutativity(t *testing.T) {
	t.Parallel()

	c1 := NewGCounter()
	_ = c1.Increment("node1", 10)
	_ = c1.Increment("node2", 5)

	c2 := NewGCounter()
	_ = c2.Increment("node1", 8)
	_ = c2.Increment("node3", 15)

	// merge(c1, c2)
	forward := NewGCounter()
	_ = forward.Merge(c1.GetState())
	_ = forward.Merge(c2.GetState())

	// merge(c2, c1)
	reverse := NewGCounter()
	_ = reverse.Merge(c2.GetState())
	_ = reverse.Merge(c1.GetState())

	if forward.Value() != reverse.Value() {
		t.Errorf("commutativity violated: %d != %d", forward.Value(), reverse.Value())
	}

	// max(10,8) + max(5,0) + max(0,15) = 30
	if got := forward.Value(); got != 30 {
		t.Errorf("merged value = %d, want 30", got)
	}
}

func TestGCounter_Merge_Associativity(t *testing.T) {
	t.Parallel()

	a := NewGCounter()
	_ = a.Increment("n1", 3)

	b := NewGCounter()
	_ = b.Increment("n2", 7)

	c := NewGCounter()
	_ = c.Increment("n3", 11)

	// (a merge b) merge c
	left := NewGCounter()
	_ = left.Merge(a.GetState())
	_ = left.Merge(b.GetState())
	_ = left.Merge(c.GetState())

	// a merge (b merge c)
	right := NewGCounter()
	bc := NewGCounter()
	_ = bc.Merge(b.GetState())
	_ = bc.Merge(c.GetState())
	_ = right.Merge(a.GetState())
	_ = right.Merge(bc.GetState())

	if left.Value() != right.Value() {
		t.Errorf("associativity violated: %d != %d", left.Value(), right.Value())
	}
}

func TestGCounter_Merge_Idempotency(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	_ = c.Increment("node1", 10)

	state := c.GetState()

	_ = c.Merge(state)
	val1 := c.Value()

	_ = c.Merge(state)
	val2 := c.Value()

	if val1 != val2 {
		t.Errorf("idempotency violated: %d != %d", val1, val2)
	}
}

func TestGCounter_Merge_NilState(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	if err := c.Merge(nil); !errors.Is(err, ErrNilState) {
		t.Errorf("expected ErrNilState, got %v", err)
	}
}

func TestGCounter_ConcurrentIncrements(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	const goroutines = 100
	const ops = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range ops {
				_ = c.Increment("node1", 1)
			}
		}()
	}

	wg.Wait()

	expected := uint64(goroutines * ops)
	if got := c.Value(); got != expected {
		t.Errorf("concurrent increments: got %d, want %d", got, expected)
	}
}

func TestGCounter_GetState_IsolationCopy(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	_ = c.Increment("node1", 10)

	state := c.GetState()
	state["node1"] = 999

	if c.GetNodeValue("node1") != 10 {
		t.Error("GetState() did not return isolated copy")
	}
}

func TestGCounter_NodeCount(t *testing.T) {
	t.Parallel()

	c := NewGCounter()
	_ = c.Increment("node1", 1)
	_ = c.Increment("node2", 1)
	_ = c.Increment("node3", 1)

	if got := c.NodeCount(); got != 3 {
		t.Errorf("NodeCount() = %d, want 3", got)
	}
}

func BenchmarkGCounter_Increment(b *testing.B) {
	c := NewGCounter()
	for b.Loop() {
		_ = c.Increment("node1", 1)
	}
}

func BenchmarkGCounter_Value(b *testing.B) {
	c := NewGCounter()
	for range 100 {
		_ = c.Increment("node1", 1)
	}
	for b.Loop() {
		_ = c.Value()
	}
}

func BenchmarkGCounter_Merge(b *testing.B) {
	c := NewGCounter()
	other := NewGCounter()
	for range 100 {
		_ = other.Increment("node1", 1)
	}
	state := other.GetState()
	for b.Loop() {
		_ = c.Merge(state)
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
