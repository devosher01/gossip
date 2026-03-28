# gossip

Gossip protocol implementation in Go. No dependencies, no leader, no consensus.

Each node picks 3 random peers every second and exchanges state over UDP. Membership is tracked with SWIM-style failure detection (Alive → Suspect → Dead). Data convergence uses a G-Counter CRDT — nodes can increment independently and always arrive at the same total. Vector clocks track causal ordering between events.

## Run

```bash
# Node 1
go run main.go -id node1 -port 8001

# Node 2 joins via node 1
go run main.go -id node2 -port 8002 -peer 127.0.0.1:8001

# Node 3 joins via node 2
go run main.go -id node3 -port 8003 -peer 127.0.0.1:8002
```

Nodes discover each other transitively — node 3 will learn about node 1 through gossip without direct connection.

Use `-bind 0.0.0.0` for multi-host deployment.

## Test

```bash
go test ./... -race              # unit + integration
go test ./... -race -short       # unit only (no UDP)
go test ./... -race -bench=.     # benchmarks
```

## Structure

```text
pkg/network/      UDP transport (atomic.Bool close, sync.Once idempotency)
pkg/membership/   SWIM failure detection, monotonic heartbeat counters
pkg/data/         G-Counter CRDT, vector clocks
pkg/gossip/       Protocol orchestration, config, stats
```

## References

- Demers et al. (1987) — Epidemic Algorithms for Replicated Database Maintenance
- Shapiro et al. (2011) — Conflict-free Replicated Data Types
- Lamport (1978) — Time, Clocks, and the Ordering of Events in a Distributed System
- Das et al. (2002) — SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol
