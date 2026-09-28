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
	"github.com/tailscale/tailcat"
)

type ClientConfig struct {
	ServerAddr string
	PairingPIN string
	Name       string
	Logf       func(format string, args ...any)
}

type Client struct {
	mu   sync.Mutex // guards all to the line break
	tc   *tailcat.Client
	conn net.Conn
	used bool // Connect has been called, Clients are single-use

	cfg       ClientConfig
	onMessage func(m Message)
	writeMu   sync.Mutex
}

func NewClient(cfg ClientConfig) *Client {
	// nil Logf in tailcat pushes everything to log.Printf
	// if nil is sent give it an empty func to discard everything
	if cfg.Logf == nil {
		cfg.Logf = func(format string, args ...any) {}
	}
	c := &Client{
		cfg:       cfg,
		onMessage: func(m Message) {},
	}
	return c
}

func (c *Client) OnMessage(fn func(m Message)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.used {
		panic("meshnet: OnMessage called after start")
	}

	if fn == nil {
		fn = func(m Message) {}
	}
	c.onMessage = fn
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

	c.mu.Lock()
	c.tc = tc
	c.conn = conn
	c.mu.Unlock()

	go c.readLoop(conn)

	return nil
}

func (c *Client) Disconnect() error {
	c.mu.Lock()
	tc := c.tc
	conn := c.conn

	if conn == nil {
		c.mu.Unlock()
		return nil
	}

	c.tc = nil
	c.conn = nil
	c.mu.Unlock()

	connErr := conn.Close()
	tcErr := tc.Close()

	if connErr != nil {
		return fmt.Errorf("client: disconnect: %w", connErr)
	}
	if tcErr != nil {
		return fmt.Errorf("client: disconnect: %w", tcErr)
	}
	return nil
}

func (c *Client) Send(m Message) error {
	//TODO
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
			c.cfg.Logf("meshnet: %v", err)
			return
		}
	}
}

func (c *Client) handleFrame(f protocol.Frame) error { return nil }
