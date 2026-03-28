package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"
)

var (
	// ErrInvalidPort is returned when a port number is invalid.
	ErrInvalidPort = errors.New("invalid port number")
	// ErrInvalidAddress is returned when an address cannot be parsed.
	ErrInvalidAddress = errors.New("invalid network address")
	// ErrMessageTooLarge is returned when attempting to send a message exceeding MTU.
	ErrMessageTooLarge = errors.New("message exceeds maximum transmission unit")
	// ErrTransportClosed is returned when operating on a closed transport.
	ErrTransportClosed = errors.New("transport is closed")
)

const (
	// MaxUDPPacketSize is the maximum size for UDP packets (64KB - 1).
	MaxUDPPacketSize = 65535
	// DefaultReadBufferSize is the default size for the UDP read buffer.
	DefaultReadBufferSize = 4 * 1024 * 1024 // 4MB
	// DefaultWriteBufferSize is the default size for the UDP write buffer.
	DefaultWriteBufferSize = 4 * 1024 * 1024 // 4MB
)

// Transport defines the interface for network communication.
// Implementations must be safe for concurrent use.
type Transport interface {
	// Send transmits data to the specified address.
	// Returns error if the address is invalid or transmission fails.
	Send(ctx context.Context, addr string, data []byte) error

	// Receive blocks until a message arrives or context is canceled.
	// Returns the sender address, message data, and any error.
	Receive(ctx context.Context) (string, []byte, error)

	// Close gracefully shuts down the transport.
	// After Close, all operations will return ErrTransportClosed.
	Close() error
}

// UDPTransport implements Transport over UDP.
// Optimized for low-latency message passing in distributed systems.
type UDPTransport struct {
	conn   *net.UDPConn
	closed bool
}

// UDPConfig holds configuration for UDP transport.
type UDPConfig struct {
	ReadBufferSize  int
	WriteBufferSize int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}

// DefaultUDPConfig returns a configuration with sensible defaults.
func DefaultUDPConfig() *UDPConfig {
	return &UDPConfig{
		ReadBufferSize:  DefaultReadBufferSize,
		WriteBufferSize: DefaultWriteBufferSize,
		ReadTimeout:     0, // No timeout by default
		WriteTimeout:    5 * time.Second,
	}
}

// NewUDPTransport creates a new UDP transport bound to the specified port.
// Returns error if the port is invalid or binding fails.
func NewUDPTransport(port int) (*UDPTransport, error) {
	return NewUDPTransportWithConfig(port, DefaultUDPConfig())
}

// NewUDPTransportWithConfig creates a UDP transport with custom configuration.
func NewUDPTransportWithConfig(port int, cfg *UDPConfig) (*UDPTransport, error) {
	if port < 0 || port > 65535 {
		return nil, fmt.Errorf("%w: port must be between 0 and 65535, got %d", ErrInvalidPort, port)
	}

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidAddress, err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to bind UDP socket: %w", err)
	}

	// Set buffer sizes to handle high-throughput scenarios
	if err := conn.SetReadBuffer(cfg.ReadBufferSize); err != nil {
		err := conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("failed to set read buffer: %w", err)
	}

	if err := conn.SetWriteBuffer(cfg.WriteBufferSize); err != nil {
		err := conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("failed to set write buffer: %w", err)
	}

	return &UDPTransport{
		conn:   conn,
		closed: false,
	}, nil
}

// Send transmits data to the specified address with context support.
// Returns error if the message is too large, the address is invalid, or transport is closed.
func (t *UDPTransport) Send(ctx context.Context, addr string, data []byte) error {
	if t.closed {
		return ErrTransportClosed
	}

	if len(data) > MaxUDPPacketSize {
		return fmt.Errorf("%w: size=%d, max=%d", ErrMessageTooLarge, len(data), MaxUDPPacketSize)
	}

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAddress, err)
	}

	// Respect context deadline if set
	if deadline, ok := ctx.Deadline(); ok {
		if err := t.conn.SetWriteDeadline(deadline); err != nil {
			return fmt.Errorf("failed to set write deadline: %w", err)
		}
		defer func(conn *net.UDPConn, t time.Time) {
			err := conn.SetWriteDeadline(t)
			if err != nil {

			}
		}(t.conn, time.Time{}) // Clear deadline
	}

	n, err := t.conn.WriteToUDP(data, udpAddr)
	if err != nil {
		// Check for context cancellation
		if ctx.Err() != nil {
			return fmt.Errorf("send canceled: %w", ctx.Err())
		}
		return fmt.Errorf("failed to write UDP packet: %w", err)
	}

	if n != len(data) {
		return fmt.Errorf("partial write: sent %d bytes of %d", n, len(data))
	}

	return nil
}

// Receive blocks until a message is available or context is canceled.
// Returns the sender address, message payload, and any error encountered.
func (t *UDPTransport) Receive(ctx context.Context) (string, []byte, error) {
	if t.closed {
		return "", nil, ErrTransportClosed
	}

	buf := make([]byte, MaxUDPPacketSize)

	// Set a read deadline based on context
	if deadline, ok := ctx.Deadline(); ok {
		if err := t.conn.SetReadDeadline(deadline); err != nil {
			return "", nil, fmt.Errorf("failed to set read deadline: %w", err)
		}
		defer func(conn *net.UDPConn, t time.Time) {
			err := conn.SetReadDeadline(t)
			if err != nil {

			}
		}(t.conn, time.Time{})
	}

	n, addr, err := t.conn.ReadFromUDP(buf)
	if err != nil {
		// Check if context was canceled
		if ctx.Err() != nil {
			return "", nil, fmt.Errorf("receive canceled: %w", ctx.Err())
		}

		// Handle timeout separately from other errors
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return "", nil, fmt.Errorf("read timeout: %w", err)
		}

		// Check if the connection was closed
		if errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.EBADF) {
			return "", nil, ErrTransportClosed
		}

		return "", nil, fmt.Errorf("failed to read UDP packet: %w", err)
	}

	// Return a copy of the buffer to prevent data races
	received := make([]byte, n)
	copy(received, buf[:n])

	return addr.String(), received, nil
}

// Close gracefully shuts down the transport and releases resources.
// It is safe to call Close multiple times.
func (t *UDPTransport) Close() error {
	if t.closed {
		return nil
	}

	t.closed = true
	if err := t.conn.Close(); err != nil {
		return fmt.Errorf("failed to close UDP connection: %w", err)
	}

	return nil
}

// LocalAddr returns the local network address.
func (t *UDPTransport) LocalAddr() net.Addr {
	return t.conn.LocalAddr()
}

// IsClosed returns true if the transport has been closed.
func (t *UDPTransport) IsClosed() bool {
	return t.closed
}
