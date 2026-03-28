package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"time"

	"gossip/pkg/data"
	"gossip/pkg/membership"
	"gossip/pkg/network"
)

type MessageType int

const (
	MembershipSync MessageType = iota
	DataSync
)

type Envelope struct {
	Type    MessageType     `json:"type"`
	Sender  string          `json:"sender"`
	Payload json.RawMessage `json:"payload"`
}

type MembershipPayload struct {
	Members []membership.Member `json:"members"`
}

type DataPayload struct {
	Clocks map[string]uint64 `json:"clocks"`
	State  map[string]uint64 `json:"state"`
}

type Config struct {
	ID             string
	BindAddr       string
	Port           int
	GossipInterval time.Duration
	SuspectTimeout time.Duration
	DeadTimeout    time.Duration
	RemoveTimeout  time.Duration
	FanOut         int
	Logger         *slog.Logger
}

func DefaultConfig(id string, port int) Config {
	return Config{
		ID:             id,
		BindAddr:       "127.0.0.1",
		Port:           port,
		GossipInterval: 1 * time.Second,
		SuspectTimeout: 5 * time.Second,
		DeadTimeout:    10 * time.Second,
		RemoveTimeout:  30 * time.Second,
		FanOut:         3,
		Logger:         slog.Default(),
	}
}

type Node struct {
	ID      string
	Addr    string
	Members *membership.List
	Counter *data.GCounter
	Clocks  *data.VectorClock

	transport network.Transport
	stats     Stats
	cfg       Config
	log       *slog.Logger
	cancel    context.CancelFunc
	mu        sync.Mutex

	// heartbeatSeq is a monotonic counter incremented each gossip tick.
	// Using a sequence number instead of wall clock avoids issues with
	// NTP adjustments moving time backwards.
	heartbeatSeq uint64
}

func NewNode(cfg Config) (*Node, error) {
	addr := fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.Port)
	transport, err := network.NewUDPTransport(cfg.Port)
	if err != nil {
		return nil, err
	}

	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	return &Node{
		ID:        cfg.ID,
		Addr:      addr,
		transport: transport,
		Members:   membership.NewList(addr),
		Counter:   data.NewGCounter(),
		Clocks:    data.NewVectorClock(),
		cfg:       cfg,
		log:       log.With("node", cfg.ID),
	}, nil
}

func (n *Node) Start(ctx context.Context) {
	ctx, n.cancel = context.WithCancel(ctx)
	go n.listen(ctx)
	go n.gossipLoop(ctx)
}

func (n *Node) Stop() error {
	if n.cancel != nil {
		n.cancel()
	}
	return n.transport.Close()
}

func (n *Node) Stats() StatsSnapshot {
	return n.stats.Snapshot()
}

func (n *Node) LocalAddr() net.Addr {
	return n.transport.LocalAddr()
}

func (n *Node) listen(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, buf, err := n.transport.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		n.stats.MessagesReceived.Add(1)
		n.stats.BytesReceived.Add(uint64(len(buf)))

		var env Envelope
		if err := json.Unmarshal(buf, &env); err != nil {
			n.stats.MalformedMessages.Add(1)
			n.log.Warn("malformed envelope", "error", err)
			continue
		}

		switch env.Type {
		case MembershipSync:
			n.handleMembership(env.Payload)
		case DataSync:
			n.handleData(env.Sender, env.Payload)
		default:
			n.stats.MalformedMessages.Add(1)
			n.log.Warn("unknown message type", "type", env.Type)
		}
	}
}

func (n *Node) handleMembership(payload json.RawMessage) {
	var p MembershipPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		n.stats.MalformedMessages.Add(1)
		n.log.Warn("malformed membership payload", "error", err)
		return
	}
	for _, m := range p.Members {
		n.Members.UpdateHeartbeat(m.Addr, m.Heartbeat)
	}
}

func (n *Node) handleData(sender string, payload json.RawMessage) {
	var p DataPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		n.stats.MalformedMessages.Add(1)
		n.log.Warn("malformed data payload", "error", err)
		return
	}

	// Compare causal ordering before merging.
	// This is the entire point of vector clocks in a leaderless system:
	// knowing whether remote state is causally ahead, behind, or concurrent
	// determines whether this is a simple update or a true concurrent modification.
	localClocks := n.Clocks.GetClocks()
	ordering := data.CompareRaw(localClocks, p.Clocks)
	n.log.Debug("merge",
		"from", sender,
		"ordering", ordering.String(),
		"remote_counter", sumValues(p.State),
		"local_counter", n.Counter.Value(),
	)

	_ = n.Clocks.Merge(p.Clocks)
	_ = n.Counter.Merge(p.State)
	n.stats.MergesPerformed.Add(1)
}

func (n *Node) gossipLoop(ctx context.Context) {
	ticker := time.NewTicker(n.cfg.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			n.heartbeatSeq++
			seq := n.heartbeatSeq
			n.mu.Unlock()

			n.Members.UpdateHeartbeat(n.Addr, seq)
			n.Members.Tick(n.cfg.SuspectTimeout, n.cfg.DeadTimeout, n.cfg.RemoveTimeout)
			n.gossip(ctx)
			n.stats.GossipRounds.Add(1)
		}
	}
}

func (n *Node) gossip(ctx context.Context) {
	members := n.Members.Snapshot()
	if len(members) <= 1 {
		return
	}

	targets := selectRandom(members, n.Addr, n.cfg.FanOut)

	memPayload, err := json.Marshal(MembershipPayload{Members: members})
	if err != nil {
		return
	}

	dataPayload, err := json.Marshal(DataPayload{
		Clocks: n.Clocks.GetClocks(),
		State:  n.Counter.GetState(),
	})
	if err != nil {
		return
	}

	for _, t := range targets {
		if err := n.send(ctx, t.Addr, MembershipSync, memPayload); err != nil {
			n.log.Debug("send membership failed", "peer", t.Addr, "error", err)
		}
		if err := n.send(ctx, t.Addr, DataSync, dataPayload); err != nil {
			n.log.Debug("send data failed", "peer", t.Addr, "error", err)
		}
	}
}

func (n *Node) send(ctx context.Context, addr string, typ MessageType, payload []byte) error {
	env := Envelope{
		Type:    typ,
		Sender:  n.Addr,
		Payload: payload,
	}
	encoded, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if err := n.transport.Send(ctx, addr, encoded); err != nil {
		n.stats.SendErrors.Add(1)
		return err
	}
	n.stats.MessagesSent.Add(1)
	n.stats.BytesSent.Add(uint64(len(encoded)))
	return nil
}

func (n *Node) Increment() error {
	if err := n.Clocks.Increment(n.ID); err != nil {
		return err
	}
	return n.Counter.Increment(n.ID, 1)
}

// selectRandom picks up to k random peers, excluding self.
// Random peer selection is fundamental to gossip protocols: it guarantees
// O(log N) convergence time with high probability (Demers et al., 1987).
func selectRandom(members []membership.Member, self string, k int) []membership.Member {
	var pool []membership.Member
	for _, m := range members {
		if m.Addr != self {
			pool = append(pool, m)
		}
	}

	if len(pool) <= k {
		return pool
	}

	rand.Shuffle(len(pool), func(i, j int) {
		pool[i], pool[j] = pool[j], pool[i]
	})
	return pool[:k]
}

func sumValues(m map[string]uint64) uint64 {
	var total uint64
	for _, v := range m {
		total += v
	}
	return total
}
