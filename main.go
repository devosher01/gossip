package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gossip/pkg/gossip"
)

func main() {
	id := flag.String("id", "node1", "Node ID")
	port := flag.Int("port", 8000, "UDP Port")
	peer := flag.String("peer", "", "Initial peer address")
	flag.Parse()

	node, err := gossip.NewNode(*id, *port)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}

	if *peer != "" {
		node.Members.UpdateHeartbeat(*peer, 0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node.Start(ctx)

	fmt.Printf("Node %s started at %s\n", *id, node.Addr)

	// Periodic status print
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			members := node.Members.GetMembers()
			fmt.Printf("\n--- Status Node %s ---\n", *id)
			fmt.Printf("Members: %d\n", len(members))
			fmt.Printf("Counter Value: %d\n", node.Counter.Value())
			fmt.Printf("Vector Clocks: %v\n", node.Clocks.GetClocks())
		}
	}()

	// Simulate work
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		for range ticker.C {
			_ = node.Increment()
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	fmt.Println("Shutting down...")
}
