package gossip

import "sync/atomic"

type Stats struct {
	MessagesSent      atomic.Uint64
	MessagesReceived  atomic.Uint64
	SendErrors        atomic.Uint64
	MergesPerformed   atomic.Uint64
	MalformedMessages atomic.Uint64
	GossipRounds      atomic.Uint64
	BytesSent         atomic.Uint64
	BytesReceived     atomic.Uint64
}

type StatsSnapshot struct {
	MessagesSent      uint64
	MessagesReceived  uint64
	SendErrors        uint64
	MergesPerformed   uint64
	MalformedMessages uint64
	GossipRounds      uint64
	BytesSent         uint64
	BytesReceived     uint64
}

func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		MessagesSent:      s.MessagesSent.Load(),
		MessagesReceived:  s.MessagesReceived.Load(),
		SendErrors:        s.SendErrors.Load(),
		MergesPerformed:   s.MergesPerformed.Load(),
		MalformedMessages: s.MalformedMessages.Load(),
		GossipRounds:      s.GossipRounds.Load(),
		BytesSent:         s.BytesSent.Load(),
		BytesReceived:     s.BytesReceived.Load(),
	}
}
