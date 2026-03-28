package gossip_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gossip/pkg/gossip"
)

func TestGossipConvergence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create 3 nodes
	node1, _ := gossip.NewNode("node1", 8001)
	node2, _ := gossip.NewNode("node2", 8002)
	node3, _ := gossip.NewNode("node3", 8003)

	// Connect them in a chain: 1 -> 2 -> 3
	node1.Members.UpdateHeartbeat(node2.Addr, 0)
	node2.Members.UpdateHeartbeat(node3.Addr, 0)

	node1.Start(ctx)
	node2.Start(ctx)
	node3.Start(ctx)

	// Increment some values
	_ = node1.Increment()
	_ = node1.Increment()
	_ = node2.Increment()
	_ = node3.Increment()

	// Expected total = 4
	fmt.Println("Waiting for convergence...")
	time.Sleep(10 * time.Second)

	v1 := node1.Counter.Value()
	v2 := node2.Counter.Value()
	v3 := node3.Counter.Value()

	if v1 != 4 || v2 != 4 || v3 != 4 {
		t.Errorf("Nodes did not converge to 4. Node1: %d, Node2: %d, Node3: %d", v1, v2, v3)
	} else {
		fmt.Printf("Convergence achieved: All nodes have counter = %d\n", v1)
	}
}
