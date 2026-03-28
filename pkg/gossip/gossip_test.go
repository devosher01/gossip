package gossip_test

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"gossip/pkg/gossip"
)

func makeTestNode(t *testing.T, id string, port int) *gossip.Node {
	t.Helper()
	cfg := gossip.DefaultConfig(id, port)
	cfg.GossipInterval = 100 * time.Millisecond
	cfg.SuspectTimeout = 2 * time.Second
	cfg.DeadTimeout = 3 * time.Second
	cfg.RemoveTimeout = 5 * time.Second
	cfg.Logger = slog.Default()

	node, err := gossip.NewNode(cfg)
	if err != nil {
		t.Fatalf("create node %s: %v", id, err)
	}
	t.Cleanup(func() {
		err := node.Stop()
		if err != nil {
			return
		}
	})
	return node
}

func awaitConvergence(t *testing.T, nodes []*gossip.Node, expected uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			vals := make([]uint64, len(nodes))
			for i, n := range nodes {
				vals[i] = n.Counter.Value()
			}
			t.Fatalf("convergence timeout: expected %d, got %v", expected, vals)
		case <-ticker.C:
			converged := true
			for _, n := range nodes {
				if n.Counter.Value() != expected {
					converged = false
					break
				}
			}
			if converged {
				return
			}
		}
	}
}

func TestGossipConvergence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test with real UDP")
	}

	nodes := make([]*gossip.Node, 3)
	for i := range 3 {
		nodes[i] = makeTestNode(t, fmt.Sprintf("node%d", i+1), 9001+i)
	}

	// Chain topology: transitive discovery must propagate through node2
	nodes[0].Members.UpdateHeartbeat(nodes[1].Addr, 0)
	nodes[1].Members.UpdateHeartbeat(nodes[2].Addr, 0)

	ctx := t.Context()
	for _, n := range nodes {
		n.Start(ctx)
	}

	_ = nodes[0].Increment()
	_ = nodes[0].Increment()
	_ = nodes[1].Increment()
	_ = nodes[2].Increment()

	awaitConvergence(t, nodes, 4, 15*time.Second)

	for i, n := range nodes {
		snap := n.Stats()
		if snap.MessagesReceived == 0 {
			t.Errorf("node%d received 0 messages", i+1)
		}
		if snap.GossipRounds == 0 {
			t.Errorf("node%d performed 0 gossip rounds", i+1)
		}
	}
}

func TestPartitionHealing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test with real UDP")
	}

	// Two isolated clusters that will later be connected.
	// Validates that CRDT state converges after partition healed
	// without a central coordinator — the core gossip guarantee.
	nodeA := makeTestNode(t, "nodeA", 9010)
	nodeB := makeTestNode(t, "nodeB", 9011)
	nodeC := makeTestNode(t, "nodeC", 9012)

	// Phase 1: A <-> B connected, C isolated
	nodeA.Members.UpdateHeartbeat(nodeB.Addr, 0)
	nodeB.Members.UpdateHeartbeat(nodeA.Addr, 0)

	ctx := t.Context()
	nodeA.Start(ctx)
	nodeB.Start(ctx)
	nodeC.Start(ctx)

	_ = nodeA.Increment() // A=1
	_ = nodeA.Increment() // A=2
	_ = nodeB.Increment() // B=1
	_ = nodeC.Increment() // C=1
	_ = nodeC.Increment() // C=2
	_ = nodeC.Increment() // C=3

	awaitConvergence(t, []*gossip.Node{nodeA, nodeB}, 3, 10*time.Second)

	if v := nodeC.Counter.Value(); v != 3 {
		t.Fatalf("nodeC before healing: got %d, want 3", v)
	}

	// Phase 2: heal partition
	nodeB.Members.UpdateHeartbeat(nodeC.Addr, 0)
	nodeC.Members.UpdateHeartbeat(nodeB.Addr, 0)

	// A:2 + B:1 + C:3 = 6
	awaitConvergence(t, []*gossip.Node{nodeA, nodeB, nodeC}, 6, 15*time.Second)
}
