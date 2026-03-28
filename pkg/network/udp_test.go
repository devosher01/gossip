package network

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// UDPTransport binds to 0.0.0.0 which on some systems resolves to [::],
// but sending to [::] fails when IPv6 routing is unavailable.
func localAddr(t *testing.T, tr *UDPTransport) string {
	t.Helper()
	_, port, _ := net.SplitHostPort(tr.LocalAddr().String())
	return "127.0.0.1:" + port
}

func newTestPair(t *testing.T) (*UDPTransport, *UDPTransport) {
	t.Helper()
	a, err := NewUDPTransport(0)
	if err != nil {
		t.Fatalf("create transport A: %v", err)
	}
	t.Cleanup(func() { a.Close() })

	b, err := NewUDPTransport(0)
	if err != nil {
		t.Fatalf("create transport B: %v", err)
	}
	t.Cleanup(func() { b.Close() })

	return a, b
}

func TestNewUDPTransport_InvalidPort(t *testing.T) {
	t.Parallel()

	for _, port := range []int{-1, 65536, 100000} {
		_, err := NewUDPTransport(port)
		if !errors.Is(err, ErrInvalidPort) {
			t.Errorf("port %d: expected ErrInvalidPort, got %v", port, err)
		}
	}
}

func TestNewUDPTransport_PortZero(t *testing.T) {
	t.Parallel()

	tr, err := NewUDPTransport(0)
	if err != nil {
		t.Fatalf("port 0 should auto-assign: %v", err)
	}
	defer tr.Close()

	addr := tr.LocalAddr().String()
	if addr == "" {
		t.Fatal("LocalAddr() returned empty string")
	}
}

func TestSendReceive(t *testing.T) {
	t.Parallel()

	sender, receiver := newTestPair(t)
	payload := []byte("hello gossip")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := sender.Send(ctx, localAddr(t, receiver), payload); err != nil {
		t.Fatalf("Send: %v", err)
	}

	addr, data, err := receiver.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}

	if string(data) != string(payload) {
		t.Errorf("received %q, want %q", data, payload)
	}

	if addr == "" {
		t.Error("sender address should not be empty")
	}
}

func TestSend_InvalidAddress(t *testing.T) {
	t.Parallel()

	tr, _ := newTestPair(t)
	ctx := context.Background()

	err := tr.Send(ctx, "not-a-valid-address", []byte("data"))
	if !errors.Is(err, ErrInvalidAddress) {
		t.Errorf("expected ErrInvalidAddress, got %v", err)
	}
}

func TestSend_MessageTooLarge(t *testing.T) {
	t.Parallel()

	tr, _ := newTestPair(t)
	ctx := context.Background()

	huge := make([]byte, MaxUDPPacketSize+1)
	err := tr.Send(ctx, "127.0.0.1:9999", huge)
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Errorf("expected ErrMessageTooLarge, got %v", err)
	}
}

func TestSend_AfterClose(t *testing.T) {
	t.Parallel()

	tr, _ := newTestPair(t)
	tr.Close()

	err := tr.Send(context.Background(), "127.0.0.1:9999", []byte("data"))
	if !errors.Is(err, ErrTransportClosed) {
		t.Errorf("expected ErrTransportClosed, got %v", err)
	}
}

func TestReceive_AfterClose(t *testing.T) {
	t.Parallel()

	_, tr := newTestPair(t)
	tr.Close()

	_, _, err := tr.Receive(context.Background())
	if !errors.Is(err, ErrTransportClosed) {
		t.Errorf("expected ErrTransportClosed, got %v", err)
	}
}

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()

	tr, err := NewUDPTransport(0)
	if err != nil {
		t.Fatal(err)
	}

	if err := tr.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close should succeed: %v", err)
	}
}

func TestReceive_ContextCanceled(t *testing.T) {
	t.Parallel()

	tr, err := NewUDPTransport(0)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, _, err = tr.Receive(ctx)
	if err == nil {
		t.Fatal("expected error on timeout")
	}
	if !strings.Contains(err.Error(), "timeout") && !strings.Contains(err.Error(), "canceled") {
		t.Errorf("expected timeout/canceled error, got: %v", err)
	}
}

func TestSend_ContextDeadline(t *testing.T) {
	t.Parallel()

	sender, receiver := newTestPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Sending with deadline should work on loopback
	err := sender.Send(ctx, localAddr(t, receiver), []byte("with-deadline"))
	if err != nil {
		t.Fatalf("Send with deadline: %v", err)
	}

	_, data, err := receiver.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if string(data) != "with-deadline" {
		t.Errorf("got %q, want %q", data, "with-deadline")
	}
}

func TestSendReceive_MultipleMessages(t *testing.T) {
	t.Parallel()

	sender, receiver := newTestPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	messages := []string{"msg-1", "msg-2", "msg-3", "msg-4", "msg-5"}

	for _, msg := range messages {
		if err := sender.Send(ctx, localAddr(t, receiver), []byte(msg)); err != nil {
			t.Fatalf("Send %q: %v", msg, err)
		}
	}

	received := make(map[string]bool)
	for range len(messages) {
		_, data, err := receiver.Receive(ctx)
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		received[string(data)] = true
	}

	for _, msg := range messages {
		if !received[msg] {
			t.Errorf("message %q not received", msg)
		}
	}
}

func TestReceive_DataIsolation(t *testing.T) {
	t.Parallel()

	sender, receiver := newTestPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	raddr := localAddr(t, receiver)
	original := []byte("immutable-data")

	if err := sender.Send(ctx, raddr, original); err != nil {
		t.Fatalf("first send: %v", err)
	}
	_, data, err := receiver.Receive(ctx)
	if err != nil {
		t.Fatalf("first receive: %v", err)
	}

	// Mutating returned data should not affect future receives
	data[0] = 'X'

	if err := sender.Send(ctx, raddr, original); err != nil {
		t.Fatalf("second send: %v", err)
	}
	_, data2, err := receiver.Receive(ctx)
	if err != nil {
		t.Fatalf("second receive: %v", err)
	}

	if data2[0] != 'i' {
		t.Error("Receive returned shared buffer — data isolation violated")
	}
}

func BenchmarkSendReceive(b *testing.B) {
	sender, err := NewUDPTransport(0)
	if err != nil {
		b.Fatal(err)
	}
	defer sender.Close()

	receiver, err := NewUDPTransport(0)
	if err != nil {
		b.Fatal(err)
	}
	defer receiver.Close()

	_, port, _ := net.SplitHostPort(receiver.LocalAddr().String())
	addr := "127.0.0.1:" + port
	payload := []byte("benchmark-payload-64-bytes-padded-for-realistic-gossip-message!!")
	ctx := context.Background()

	for b.Loop() {
		_ = sender.Send(ctx, addr, payload)
		_, _, _ = receiver.Receive(ctx)
	}
}
