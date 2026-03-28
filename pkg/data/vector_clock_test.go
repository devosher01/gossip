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
	if err := vc.Set("node1", math.MaxUint64); err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

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

	other := map[string]uint64{
		"node1": 1,
		"node2": 5,
	}

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
	err := vc.Merge(nil)

	if !errors.Is(err, ErrNilState) {
		t.Errorf("expected ErrNilState, got %v", err)
	}
}

func TestCompare_Ordering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		vc1  map[string]uint64
		vc2  map[string]uint64
		want Ordering
	}{
		{
			name: "equal clocks",
			vc1:  map[string]uint64{"node1": 5, "node2": 3},
			vc2:  map[string]uint64{"node1": 5, "node2": 3},
			want: Equal,
		},
		{
			name: "vc1 before vc2",
			vc1:  map[string]uint64{"node1": 3, "node2": 2},
			vc2:  map[string]uint64{"node1": 5, "node2": 4},
			want: Before,
		},
		{
			name: "vc1 after vc2",
			vc1:  map[string]uint64{"node1": 5, "node2": 4},
			vc2:  map[string]uint64{"node1": 3, "node2": 2},
			want: After,
		},
		{
			name: "concurrent events",
			vc1:  map[string]uint64{"node1": 5, "node2": 2},
			vc2:  map[string]uint64{"node1": 3, "node2": 4},
			want: Concurrent,
		},
		{
			name: "both nil",
			vc1:  nil,
			vc2:  nil,
			want: Equal,
		},
		{
			name: "vc1 nil",
			vc1:  nil,
			vc2:  map[string]uint64{"node1": 1},
			want: Concurrent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Compare(tt.vc1, tt.vc2)
			if got != tt.want {
				t.Errorf("Compare() = %v, want %v", got, tt.want)
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

func TestVectorClock_Reset(t *testing.T) {
	t.Parallel()

	vc := NewVectorClock()
	_ = vc.Increment("node1")
	_ = vc.Increment("node2")

	vc.Reset()

	if vc.Get("node1") != 0 || vc.Get("node2") != 0 {
		t.Error("Reset() did not clear all clocks")
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

func TestNewVectorClockWithCapacity(t *testing.T) {
	t.Parallel()

	vc := NewVectorClockWithCapacity(10)
	if vc == nil {
		t.Fatal("NewVectorClockWithCapacity() returned nil")
	}

	_ = vc.Increment("node1")
	if vc.Get("node1") != 1 {
		t.Error("NewVectorClockWithCapacity() did not work correctly")
	}
}

// Benchmark tests
func BenchmarkVectorClock_Increment(b *testing.B) {
	vc := NewVectorClock()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vc.Increment("node1")
	}
}

func BenchmarkVectorClock_Merge(b *testing.B) {
	vc := NewVectorClock()
	other := map[string]uint64{
		"node1": 10,
		"node2": 20,
		"node3": 30,
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = vc.Merge(other)
	}
}

func BenchmarkCompare(b *testing.B) {
	vc1 := map[string]uint64{"node1": 5, "node2": 3, "node3": 8}
	vc2 := map[string]uint64{"node1": 3, "node2": 7, "node3": 2}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = Compare(vc1, vc2)
	}
}
