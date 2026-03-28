package membership

import (
	"testing"
	"time"
)

func TestNewList(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	if l == nil {
		t.Fatal("NewList returned nil")
	}

	members := l.GetMembers()
	if len(members) != 1 {
		t.Errorf("expected 1 member, got %d", len(members))
	}

	if members[0].Addr != "127.0.0.1:8000" {
		t.Errorf("expected self addr, got %s", members[0].Addr)
	}

	if members[0].Status != Alive {
		t.Errorf("expected Alive status, got %v", members[0].Status)
	}
}

func TestUpdateHeartbeat_NewMember(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)

	members := l.GetMembers()
	if len(members) != 2 {
		t.Errorf("expected 2 members, got %d", len(members))
	}
}

func TestUpdateHeartbeat_UpdateExisting(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)
	l.UpdateHeartbeat("127.0.0.1:8001", 2)

	members := l.GetMembers()
	found := false
	for _, m := range members {
		if m.Addr == "127.0.0.1:8001" {
			found = true
			if m.Heartbeat != 2 {
				t.Errorf("expected heartbeat 2, got %d", m.Heartbeat)
			}
			if m.Status != Alive {
				t.Errorf("expected Alive status, got %v", m.Status)
			}
		}
	}
	if !found {
		t.Error("member not found")
	}
}

func TestMarkSuspect(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)

	ok := l.MarkSuspect("127.0.0.1:8001")
	if !ok {
		t.Error("MarkSuspect returned false")
	}

	members := l.GetAllMembers()
	for _, m := range members {
		if m.Addr == "127.0.0.1:8001" && m.Status != Suspect {
			t.Errorf("expected Suspect status, got %v", m.Status)
		}
	}
}

func TestMarkSuspect_NonExistent(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	ok := l.MarkSuspect("127.0.0.1:9999")
	if ok {
		t.Error("MarkSuspect should return false for non-existent member")
	}
}

func TestMarkDead(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)
	l.MarkSuspect("127.0.0.1:8001")

	ok := l.MarkDead("127.0.0.1:8001")
	if !ok {
		t.Error("MarkDead returned false")
	}

	members := l.GetAllMembers()
	for _, m := range members {
		if m.Addr == "127.0.0.1:8001" && m.Status != Dead {
			t.Errorf("expected Dead status, got %v", m.Status)
		}
	}
}

func TestMarkDead_RequiresSuspect(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)

	// Try to mark as Dead without going through Suspect
	ok := l.MarkDead("127.0.0.1:8001")
	if ok {
		t.Error("MarkDead should require Suspect state first")
	}
}

func TestGetMembers_ExcludesDead(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)
	l.MarkSuspect("127.0.0.1:8001")
	l.MarkDead("127.0.0.1:8001")

	members := l.GetMembers()
	for _, m := range members {
		if m.Status == Dead {
			t.Error("GetMembers should exclude Dead members")
		}
	}
}

func TestGetAllMembers_IncludesDead(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)
	l.MarkSuspect("127.0.0.1:8001")
	l.MarkDead("127.0.0.1:8001")

	members := l.GetAllMembers()
	foundDead := false
	for _, m := range members {
		if m.Addr == "127.0.0.1:8001" && m.Status == Dead {
			foundDead = true
		}
	}
	if !foundDead {
		t.Error("GetAllMembers should include Dead members")
	}
}

func TestUpdateMemberStatus_StateMachine(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)

	// Simulate time passing - should transition to Suspect
	time.Sleep(10 * time.Millisecond)
	l.UpdateMemberStatus(5*time.Millisecond, 5*time.Millisecond, 5*time.Millisecond)

	alive, suspect, dead := l.Count()
	if suspect != 1 {
		t.Errorf("expected 1 suspect member, got %d", suspect)
	}

	// Simulate more time - should transition to Dead
	time.Sleep(10 * time.Millisecond)
	l.UpdateMemberStatus(5*time.Millisecond, 5*time.Millisecond, 5*time.Millisecond)

	alive, suspect, dead = l.Count()
	if dead != 1 {
		t.Errorf("expected 1 dead member, got %d (alive=%d, suspect=%d)", dead, alive, suspect)
	}

	// Simulate even more time - should be removed
	time.Sleep(10 * time.Millisecond)
	l.UpdateMemberStatus(5*time.Millisecond, 5*time.Millisecond, 5*time.Millisecond)

	members := l.GetAllMembers()
	if len(members) != 1 { // Only self should remain
		t.Errorf("expected 1 member (self), got %d", len(members))
	}
}

func TestRemoveDead_BackwardCompatibility(t *testing.T) {
	t.Parallel()

	l := NewList("127.0.0.1:8000")
	l.UpdateHeartbeat("127.0.0.1:8001", 1)

	time.Sleep(10 * time.Millisecond)
	l.RemoveDead(5 * time.Millisecond)

	members := l.GetMembers()
	if len(members) != 1 {
		t.Errorf("expected 1 member, got %d", len(members))
	}
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
	if alive != 2 { // self + 8003
		t.Errorf("expected 2 alive, got %d", alive)
	}
	if suspect != 1 { // 8001
		t.Errorf("expected 1 suspect, got %d", suspect)
	}
	if dead != 1 { // 8002
		t.Errorf("expected 1 dead, got %d", dead)
	}
}

func TestMemberStatus_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status MemberStatus
		want   string
	}{
		{Alive, "alive"},
		{Suspect, "suspect"},
		{Dead, "dead"},
		{MemberStatus(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.status.String(); got != tt.want {
			t.Errorf("String() = %v, want %v", got, tt.want)
		}
	}
}

// Benchmark tests
func BenchmarkUpdateHeartbeat(b *testing.B) {
	l := NewList("127.0.0.1:8000")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.UpdateHeartbeat("127.0.0.1:8001", uint64(i))
	}
}

func BenchmarkGetMembers(b *testing.B) {
	l := NewList("127.0.0.1:8000")
	for i := 0; i < 100; i++ {
		l.UpdateHeartbeat("127.0.0.1:800"+string(rune(i)), 1)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = l.GetMembers()
	}
}
