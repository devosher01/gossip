package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gossip/pkg/gossip"
)

func main() {
	id := flag.String("id", "node1", "Node ID")
	port := flag.Int("port", 8000, "UDP port")
	bind := flag.String("bind", "127.0.0.1", "Bind address")
	peer := flag.String("peer", "", "Initial peer address to join the cluster")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	cfg := gossip.DefaultConfig(*id, *port)
	cfg.BindAddr = *bind
	cfg.Logger = log

	node, err := gossip.NewNode(cfg)
	if err != nil {
		log.Error("failed to create node", "error", err)
		os.Exit(1)
	}

	if *peer != "" {
		node.Members.UpdateHeartbeat(*peer, 0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node.Start(ctx)
	log.Info("node started", "id", *id, "addr", node.Addr)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			alive, suspect, dead := node.Members.Count()
			log.Info("status",
				"counter", node.Counter.Value(),
				"alive", alive,
				"suspect", suspect,
				"dead", dead,
				"clocks", node.Clocks.GetClocks(),
			)
		}
	}()

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			_ = node.Increment()
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("shutting down")
	cancel()
	if err := node.Stop(); err != nil {
		log.Error("shutdown error", "error", err)
	}
}
