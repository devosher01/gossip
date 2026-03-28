package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
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
	State  map[string]uint64 `json:"state"` // G-Counter state
}

type Node struct {
	ID        string
	Addr      string
	Transport *network.UDPTransport
	Members   *membership.List
	Counter   *data.GCounter
	Clocks    *data.VectorClock

	mu sync.Mutex
}

func NewNode(id string, port int) (*Node, error) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	transport, err := network.NewUDPTransport(port)
	if err != nil {
		return nil, err
	}

	return &Node{
		ID:        id,
		Addr:      addr,
		Transport: transport,
		Members:   membership.NewList(addr),
		Counter:   data.NewGCounter(),
		Clocks:    data.NewVectorClock(),
	}, nil
}

func (n *Node) Start(ctx context.Context) {
	go n.listen(ctx)
	go n.gossipLoop(ctx)
}

func (n *Node) listen(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			addr, buf, err := n.Transport.Receive(ctx)
			if err != nil {
				continue
			}
			var env Envelope
			if err := json.Unmarshal(buf, &env); err != nil {
				continue
			}

			switch env.Type {
			case MembershipSync:
				var p MembershipPayload
				err := json.Unmarshal(env.Payload, &p)
				if err != nil {
					return
				}
				for _, m := range p.Members {
					n.Members.UpdateHeartbeat(m.Addr, m.Heartbeat)
				}
			case DataSync:
				var p DataPayload
				err := json.Unmarshal(env.Payload, &p)
				if err != nil {
					return
				}
				_ = n.Clocks.Merge(p.Clocks)
				_ = n.Counter.Merge(p.State)
			}
			_ = addr
		}
	}
}

func (n *Node) gossipLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.Members.UpdateHeartbeat(n.Addr, uint64(time.Now().Unix()))
			n.Members.RemoveDead(10 * time.Second)
			n.gossip(ctx)
		}
	}
}

func (n *Node) gossip(ctx context.Context) {
	members := n.Members.GetMembers()
	if len(members) <= 1 {
		return
	}

	// Select 3 random targets
	targets := selectRandom(members, n.Addr, 3)

	// Prepare payloads
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
		_ = n.send(ctx, t.Addr, MembershipSync, memPayload)
		_ = n.send(ctx, t.Addr, DataSync, dataPayload)
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
	return n.Transport.Send(ctx, addr, encoded)
}

func (n *Node) Increment() error {
	if err := n.Clocks.Increment(n.ID); err != nil {
		return err
	}
	return n.Counter.Increment(n.ID, 1)
}

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
