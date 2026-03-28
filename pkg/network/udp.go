package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	ErrInvalidPort     = errors.New("invalid port number")
	ErrInvalidAddress  = errors.New("invalid network address")
	ErrMessageTooLarge = errors.New("message exceeds maximum transmission unit")
	ErrTransportClosed = errors.New("transport is closed")
)

const MaxUDPPacketSize = 65535

type Transport interface {
	Send(ctx context.Context, addr string, data []byte) error
	Receive(ctx context.Context) (string, []byte, error)
	Close() error
	LocalAddr() net.Addr
}

type UDPTransport struct {
	conn      *net.UDPConn
	closed    atomic.Bool
	closeOnce sync.Once
}

func NewUDPTransport(port int) (*UDPTransport, error) {
	if port < 0 || port > 65535 {
		return nil, fmt.Errorf("%w: %d", ErrInvalidPort, port)
	}

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidAddress, err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("bind UDP socket: %w", err)
	}

	// 4MB buffers reduce packet loss under burst traffic from multiple peers
	if bufErr := conn.SetReadBuffer(4 << 20); bufErr != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("set read buffer: %w", bufErr)
	}
	if bufErr := conn.SetWriteBuffer(4 << 20); bufErr != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("set write buffer: %w", bufErr)
	}

	return &UDPTransport{conn: conn}, nil
}

func (t *UDPTransport) Send(ctx context.Context, addr string, data []byte) error {
	if t.closed.Load() {
		return ErrTransportClosed
	}

	if len(data) > MaxUDPPacketSize {
		return fmt.Errorf("%w: size=%d max=%d", ErrMessageTooLarge, len(data), MaxUDPPacketSize)
	}

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAddress, err)
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := t.conn.SetWriteDeadline(deadline); err != nil {
			return fmt.Errorf("set write deadline: %w", err)
		}
		defer func(conn *net.UDPConn, t time.Time) {
			err := conn.SetWriteDeadline(t)
			if err != nil {

			}
		}(t.conn, time.Time{})
	}

	n, err := t.conn.WriteToUDP(data, udpAddr)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("send canceled: %w", ctx.Err())
		}
		return fmt.Errorf("write UDP: %w", err)
	}

	if n != len(data) {
		return fmt.Errorf("partial write: %d/%d bytes", n, len(data))
	}

	return nil
}

func (t *UDPTransport) Receive(ctx context.Context) (string, []byte, error) {
	if t.closed.Load() {
		return "", nil, ErrTransportClosed
	}

	buf := make([]byte, MaxUDPPacketSize)

	if deadline, ok := ctx.Deadline(); ok {
		if err := t.conn.SetReadDeadline(deadline); err != nil {
			return "", nil, fmt.Errorf("set read deadline: %w", err)
		}
		defer func(conn *net.UDPConn, t time.Time) {
			err := conn.SetReadDeadline(t)
			if err != nil {

			}
		}(t.conn, time.Time{})
	}

	n, addr, err := t.conn.ReadFromUDP(buf)
	if err != nil {
		if ctx.Err() != nil {
			return "", nil, fmt.Errorf("receive canceled: %w", ctx.Err())
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return "", nil, fmt.Errorf("read timeout: %w", err)
		}
		if errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.EBADF) {
			return "", nil, ErrTransportClosed
		}
		return "", nil, fmt.Errorf("read UDP: %w", err)
	}

	received := make([]byte, n)
	copy(received, buf[:n])

	return addr.String(), received, nil
}

func (t *UDPTransport) Close() error {
	var closeErr error
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		closeErr = t.conn.Close()
	})
	return closeErr
}

func (t *UDPTransport) LocalAddr() net.Addr {
	return t.conn.LocalAddr()
}
