package membership

import (
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func TestNewList(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	members := l.Snapshot()
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if members[0].Addr != "127.0.0.1:8000" {
		t.Errorf("expected self addr, got %s", members[0].Addr)
	}
	if members[0].Status != Alive {
		t.Errorf("expected Alive, got %v", members[0].Status)
	}
}

func TestUpdateHeartbeat_NewMember(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)

	if len(l.Snapshot()) != 2 {
		t.Errorf("expected 2 members, got %d", len(l.Snapshot()))
	}
}

func TestUpdateHeartbeat_UpdateExisting(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)
	l.UpdateHeartbeat("127.0.0.1:8001", 2)

	members := l.Snapshot()
	idx := slices.IndexFunc(members, func(m Member) bool { return m.Addr == "127.0.0.1:8001" })
	if idx == -1 {
		t.Fatal("member not found")
	}
	if members[idx].Heartbeat != 2 {
		t.Errorf("expected heartbeat 2, got %d", members[idx].Heartbeat)
	}
	if members[idx].Status != Alive {
		t.Errorf("expected Alive, got %v", members[idx].Status)
	}
}

func TestMarkSuspect(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)

	if !l.MarkSuspect("127.0.0.1:8001") {
		t.Error("MarkSuspect returned false")
	}
	if l.MarkSuspect("127.0.0.1:9999") {
		t.Error("MarkSuspect should return false for non-existent member")
	}
}

func TestMarkDead_RequiresSuspect(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)

	if l.MarkDead("127.0.0.1:8001") {
		t.Error("MarkDead should require Suspect state")
	}

	l.MarkSuspect("127.0.0.1:8001")
	if !l.MarkDead("127.0.0.1:8001") {
		t.Error("MarkDead returned false after MarkSuspect")
	}
}

func TestSnapshot_ExcludesDead(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)
	l.MarkSuspect("127.0.0.1:8001")
	l.MarkDead("127.0.0.1:8001")

	for _, m := range l.Snapshot() {
		if m.Status == Dead {
			t.Error("Snapshot should exclude Dead members")
		}
	}
}

func TestAll_Iterator(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)
	l.UpdateHeartbeat("127.0.0.1:8002", 1)

	count := 0
	for range l.All() {
		count++
	}
	if count != 3 {
		t.Errorf("All() yielded %d members, want 3", count)
	}
}

func TestAll_ExcludesDead(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)
	l.MarkSuspect("127.0.0.1:8001")
	l.MarkDead("127.0.0.1:8001")

	for m := range l.All() {
		if m.Status == Dead {
			t.Error("All() should exclude Dead members")
		}
	}
}

// Tick tests use synctest to avoid time.Sleep and remove flakiness.
// Synctest provides a fake clock that only advances when all goroutines are blocked,
// making time-dependent tests deterministic.
func TestTick_StateMachine(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewList("127.0.0.1:8000")
		l.UpdateHeartbeat("127.0.0.1:8001", 1)

		// Advance past suspect timeout
		time.Sleep(6 * time.Second)
		l.Tick(5*time.Second, 10*time.Second, 30*time.Second)

		_, suspect, _ := l.Count()
		if suspect != 1 {
			t.Errorf("expected 1 suspect, got %d", suspect)
		}

		// Advance past dead timeout
		time.Sleep(11 * time.Second)
		l.Tick(5*time.Second, 10*time.Second, 30*time.Second)

		_, _, dead := l.Count()
		if dead != 1 {
			t.Errorf("expected 1 dead, got %d", dead)
		}

		// Advance past remove timeout
		time.Sleep(31 * time.Second)
		l.Tick(5*time.Second, 10*time.Second, 30*time.Second)

		alive, suspect, dead := l.Count()
		if alive != 1 || suspect != 0 || dead != 0 {
			t.Errorf("expected only self remaining: alive=%d suspect=%d dead=%d", alive, suspect, dead)
		}
	})
}

func TestTick_HeartbeatRevivesSuspect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewList("127.0.0.1:8000")
		l.UpdateHeartbeat("127.0.0.1:8001", 1)

		time.Sleep(6 * time.Second)
		l.Tick(5*time.Second, 10*time.Second, 30*time.Second)

		_, suspect, _ := l.Count()
		if suspect != 1 {
			t.Fatalf("expected 1 suspect, got %d", suspect)
		}

		// A new heartbeat should revive the member
		l.UpdateHeartbeat("127.0.0.1:8001", 2)

		alive, suspect, _ := l.Count()
		if alive != 2 || suspect != 0 {
			t.Errorf("heartbeat should revive: alive=%d suspect=%d", alive, suspect)
		}
	})
}

func TestCount(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)
	l.UpdateHeartbeat("127.0.0.1:8002", 1)
	l.UpdateHeartbeat("127.0.0.1:8003", 1)

	l.MarkSuspect("127.0.0.1:8001")
	l.MarkSuspect("127.0.0.1:8002")
	l.MarkDead("127.0.0.1:8002")

	alive, suspect, dead := l.Count()
	if alive != 2 {
		t.Errorf("expected 2 alive, got %d", alive)
	}
	if suspect != 1 {
		t.Errorf("expected 1 suspect, got %d", suspect)
	}
	if dead != 1 {
		t.Errorf("expected 1 dead, got %d", dead)
	}
}

func TestStatus_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status Status
		want   string
	}{
		{Alive, "alive"},
		{Suspect, "suspect"},
		{Dead, "dead"},
		{Status(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.status.String(); got != tt.want {
			t.Errorf("String() = %v, want %v", got, tt.want)
		}
	}
}

func BenchmarkUpdateHeartbeat(b *testing.B) {
	l := NewList("127.0.0.1:8000")
	var i uint64
	for b.Loop() {
		i++
		l.UpdateHeartbeat("127.0.0.1:8001", i)
	}
}

func BenchmarkSnapshot(b *testing.B) {
	l := NewList("127.0.0.1:8000")
	for i := range 100 {
		l.UpdateHeartbeat("127.0.0.1:800"+string(rune(i)), 1)
	}
	for b.Loop() {
		_ = l.Snapshot()
	}
}
