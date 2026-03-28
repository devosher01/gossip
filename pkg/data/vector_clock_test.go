package data

import (
	"errors"
	"math"
	"testing"
)

func TestVectorClock_Increment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      string
		wantErr error
	}{
		{"valid increment", "node1", nil},
		{"empty id", "", ErrInvalidNodeID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			vc := NewVectorClock()
			err := vc.Increment(tt.id)

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Increment() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil && vc.Get(tt.id) != 1 {
				t.Errorf("Increment() value = %d, want 1", vc.Get(tt.id))
			}
		})
	}
}

func TestVectorClock_IncrementOverflow(t *testing.T) {
	t.Parallel()

	vc := NewVectorClock()
	_ = vc.Set("node1", math.MaxUint64)

	err := vc.Increment("node1")
	if !errors.Is(err, ErrOverflow) {
		t.Errorf("expected overflow error, got %v", err)
	}
}

func TestVectorClock_Merge(t *testing.T) {
	t.Parallel()

	vc := NewVectorClock()
	_ = vc.Increment("node1")
	_ = vc.Increment("node1")

	other := map[string]uint64{"node1": 1, "node2": 5}

	if err := vc.Merge(other); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	if vc.Get("node1") != 2 {
		t.Errorf("Merge() node1 = %d, want 2", vc.Get("node1"))
	}
	if vc.Get("node2") != 5 {
		t.Errorf("Merge() node2 = %d, want 5", vc.Get("node2"))
	}
}

func TestVectorClock_MergeNil(t *testing.T) {
	t.Parallel()

	vc := NewVectorClock()
	if err := vc.Merge(nil); !errors.Is(err, ErrNilState) {
		t.Errorf("expected ErrNilState, got %v", err)
	}
}

func TestVectorClock_Compare_Ordering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		vc1  map[string]uint64
		vc2  map[string]uint64
		want Ordering
	}{
		{"equal", map[string]uint64{"n1": 5, "n2": 3}, map[string]uint64{"n1": 5, "n2": 3}, Equal},
		{"before", map[string]uint64{"n1": 3, "n2": 2}, map[string]uint64{"n1": 5, "n2": 4}, Before},
		{"after", map[string]uint64{"n1": 5, "n2": 4}, map[string]uint64{"n1": 3, "n2": 2}, After},
		{"concurrent", map[string]uint64{"n1": 5, "n2": 2}, map[string]uint64{"n1": 3, "n2": 4}, Concurrent},
		{"both nil", nil, nil, Equal},
		{"vc1 nil", nil, map[string]uint64{"n1": 1}, Concurrent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := CompareRaw(tt.vc1, tt.vc2)
			if got != tt.want {
				t.Errorf("CompareRaw() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVectorClock_HappensBefore(t *testing.T) {
	t.Parallel()

	vc1 := NewVectorClock()
	_ = vc1.Increment("node1")

	vc2 := NewVectorClock()
	_ = vc2.Increment("node1")
	_ = vc2.Increment("node1")

	if !vc1.HappensBefore(vc2) {
		t.Error("expected vc1 to happen before vc2")
	}
	if vc2.HappensBefore(vc1) {
		t.Error("vc2 should not happen before vc1")
	}
}

func TestVectorClock_HappensAfter(t *testing.T) {
	t.Parallel()

	vc1 := NewVectorClock()
	_ = vc1.Increment("node1")
	_ = vc1.Increment("node1")

	vc2 := NewVectorClock()
	_ = vc2.Increment("node1")

	if !vc1.HappensAfter(vc2) {
		t.Error("expected vc1 to happen after vc2")
	}
}

func TestVectorClock_IsConcurrentWith(t *testing.T) {
	t.Parallel()

	vc1 := NewVectorClock()
	_ = vc1.Increment("node1")

	vc2 := NewVectorClock()
	_ = vc2.Increment("node2")

	if !vc1.IsConcurrentWith(vc2) {
		t.Error("expected vc1 and vc2 to be concurrent")
	}
}

func TestVectorClock_Copy(t *testing.T) {
	t.Parallel()

	vc := NewVectorClock()
	_ = vc.Increment("node1")

	copied := vc.Copy()
	_ = copied.Increment("node1")

	if vc.Get("node1") == copied.Get("node1") {
		t.Error("Copy() did not create independent copy")
	}
}

func TestVectorClock_GetClocks_IsolationCopy(t *testing.T) {
	t.Parallel()

	vc := NewVectorClock()
	_ = vc.Increment("node1")

	clocks := vc.GetClocks()
	clocks["node1"] = 999

	if vc.Get("node1") != 1 {
		t.Error("GetClocks() did not return isolated copy")
	}
}

func TestOrdering_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ordering Ordering
		want     string
	}{
		{Concurrent, "concurrent"},
		{Before, "before"},
		{After, "after"},
		{Equal, "equal"},
		{Ordering(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.ordering.String(); got != tt.want {
			t.Errorf("String() = %v, want %v", got, tt.want)
		}
	}
}

func BenchmarkVectorClock_Increment(b *testing.B) {
	vc := NewVectorClock()
	for b.Loop() {
		_ = vc.Increment("node1")
	}
}

func BenchmarkVectorClock_Merge(b *testing.B) {
	vc := NewVectorClock()
	other := map[string]uint64{"n1": 10, "n2": 20, "n3": 30}
	for b.Loop() {
		_ = vc.Merge(other)
	}
}

func BenchmarkCompareRaw(b *testing.B) {
	vc1 := map[string]uint64{"n1": 5, "n2": 3, "n3": 8}
	vc2 := map[string]uint64{"n1": 3, "n2": 7, "n3": 2}
	for b.Loop() {
		_ = CompareRaw(vc1, vc2)
	}
}
