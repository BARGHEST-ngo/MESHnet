package meshnet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
	"github.com/BARGHEST-ngo/MESHnet/internal/transport"
	"github.com/tailscale/tailcat"
)

var errServerClosed = errors.New("meshnet: server closed")

type ClientConfig struct {
	ServerAddr string
	PairingPIN string
	Name       string
	Logf       func(format string, args ...any)
	OnMessage  func(m Message)
}

type Client struct {
	mu     sync.Mutex // guards all to the line break
	tc     *tailcat.Client
	conn   net.Conn
	used   bool // Connect has been called, Clients are single-use
	writer *transport.Writer

	cfg ClientConfig
}

func NewClient(cfg ClientConfig) *Client {
	// nil Logf in tailcat pushes everything to log.Printf
	// if nil is sent give it an empty func to discard everything
	if cfg.Logf == nil {
		cfg.Logf = func(format string, args ...any) {}
	}

	if cfg.OnMessage == nil {
		cfg.OnMessage = func(m Message) {}
	}

	c := &Client{
		cfg: cfg,
	}
	return c
}

func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.used {
		c.mu.Unlock()
		return ErrClosed
	}

	c.used = true
	c.mu.Unlock()

	tc := tailcat.NewClient(tailcat.Addr(c.cfg.ServerAddr))
	tc.Logf = c.cfg.Logf

	conn, err := tc.DialTCPPort(ctx, protocol.ControlPort)
	if err != nil {
		tc.Close()
		return err
	}

	if err := c.handshake(ctx, conn, tc); err != nil {
		conn.Close()
		tc.Close()
		return err
	}

	w := transport.NewWriter(conn)

	c.mu.Lock()
	c.tc = tc
	c.conn = conn
	c.writer = w
	c.mu.Unlock()

	go c.readLoop(conn)

	return nil
}

func (c *Client) Disconnect() error {
	c.mu.Lock()
	tc := c.tc
	conn := c.conn
	w := c.writer

	if conn == nil && tc == nil {
		c.mu.Unlock()
		return nil
	}

	c.tc = nil
	c.conn = nil
	c.writer = nil
	c.mu.Unlock()

	var connErr error
	if conn != nil {
		connErr = conn.Close()
	}

	var tcErr error
	if tc != nil {
		tcErr = tc.Close()
	}

	if w != nil {
		w.Close()
	}

	if connErr != nil {
		return fmt.Errorf("client: disconnect: %w", connErr)
	}
	if tcErr != nil {
		return fmt.Errorf("client: disconnect: %w", tcErr)
	}
	return nil
}

func (c *Client) Send(m Message) error {
	c.mu.Lock()
	if c.writer == nil {
		c.mu.Unlock()
		return ErrNotConnected
	}
	w := c.writer
	c.mu.Unlock()

	payload, err := protocol.Msg{Topic: m.Topic, Data: m.Data}.MarshalBinary()
	if err != nil {
		return err
	}

	if err := w.Enqueue(protocol.Frame{Kind: protocol.KindMsg, Payload: payload}); err != nil {
		if errors.Is(err, transport.ErrFull) {
			return ErrSendQueueFull
		}

		if errors.Is(err, transport.ErrClosed) {
			return ErrNotConnected
		}

		return err
	}
	return nil
}

func (c *Client) ID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tc == nil {
		return ""
	}
	return c.tc.PublicKey().String()
}

func (c *Client) handshake(ctx context.Context, conn net.Conn, tc *tailcat.Client) error {
	deadline := time.Now().Add(10 * (time.Second))
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}

	if err := conn.SetReadDeadline(deadline); err != nil {
		return fmt.Errorf("client: handshake: %w", err)
	}

	if err := protocol.WritePayload(conn, protocol.KindHello, protocol.Hello{
		Version: protocol.Version,
		Key:     tc.PublicKey().String(),
		Name:    c.cfg.Name,
		PIN:     c.cfg.PairingPIN,
	}); err != nil {
		return fmt.Errorf("client: handshake: %w", err)
	}

	f, err := protocol.ReadFrame(conn)
	if err != nil {
		return fmt.Errorf("client: handshake: %w", err)
	}

	switch f.Kind {
	case protocol.KindHelloAck:
		var ack protocol.HelloAck
		if err := ack.UnmarshalBinary(f.Payload); err != nil {
			return fmt.Errorf("client: handshake: %w", err)
		}

		if ack.ClientID != tc.PublicKey().String() {
			return fmt.Errorf("unexpected clientID received")
		}

		conn.SetReadDeadline(time.Time{})
		return nil
	case protocol.KindClose:
		var closeMsg protocol.Close
		if err := closeMsg.UnmarshalBinary(f.Payload); err != nil {
			return fmt.Errorf("client: handshake: %w", err)
		}
		return fmt.Errorf("client: handshake rejected by server: %v", closeMsg.Err)
	default:
		return fmt.Errorf("client: unexpected %v frame during handshake", f.Kind)
	}
}

func (c *Client) readLoop(conn net.Conn) {
	defer c.Disconnect()

	for {
		f, err := protocol.ReadFrame(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.cfg.Logf("meshnet: server closed the connection")
			} else {
				c.cfg.Logf("meshnet: connection lost: %v", err)
			}
			return
		}

		if err := c.handleFrame(f); err != nil {
			if errors.Is(err, errServerClosed) {
				c.cfg.Logf("server closed: %v", err)
			} else {
				c.cfg.Logf("handle frame: %v", err)
			}
			return
		}
	}
}

func (c *Client) handleFrame(f protocol.Frame) error {
	switch f.Kind {
	case protocol.KindMsg:
		return c.handleMsg(f)
	case protocol.KindClose:
		return c.handleClose(f)
	default:
		return fmt.Errorf("unexpected %v frame after handshake", f.Kind)
	}
}

func (c *Client) handleMsg(f protocol.Frame) error {
	var m protocol.Msg
	if err := m.UnmarshalBinary(f.Payload); err != nil {
		return err
	}

	c.cfg.OnMessage(Message{Topic: m.Topic, Data: m.Data})
	return nil
}

func (c *Client) handleClose(f protocol.Frame) error {
	var cl protocol.Close
	if err := cl.UnmarshalBinary(f.Payload); err != nil {
		return err
	}
	return fmt.Errorf("%w: %v", errServerClosed, cl.Err)
}
