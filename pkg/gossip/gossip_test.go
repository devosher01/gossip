package gossip_test

import (
	"log/slog"
	"testing"
	"time"

	"gossip/pkg/gossip"
)

func TestGossipConvergence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test with real UDP")
	}

	log := slog.Default()

	nodes := make([]*gossip.Node, 3)
	for i := range 3 {
		cfg := gossip.DefaultConfig(
			"node"+string(rune('1'+i)),
			9001+i,
		)
		cfg.GossipInterval = 100 * time.Millisecond
		cfg.Logger = log

		var err error
		nodes[i], err = gossip.NewNode(cfg)
		if err != nil {
			t.Fatalf("create node %d: %v", i, err)
		}
		defer nodes[i].Stop()
	}

	// Chain topology: node1 -> node2 -> node3
	// Gossip protocol guarantees full mesh discovery through transitive propagation
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

	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("convergence timeout: node1=%d node2=%d node3=%d",
				nodes[0].Counter.Value(),
				nodes[1].Counter.Value(),
				nodes[2].Counter.Value(),
			)
		case <-ticker.C:
			v0 := nodes[0].Counter.Value()
			v1 := nodes[1].Counter.Value()
			v2 := nodes[2].Counter.Value()
			if v0 == 4 && v1 == 4 && v2 == 4 {
				return
			}
		}
	}
}
