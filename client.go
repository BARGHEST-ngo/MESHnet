package meshnet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
	"github.com/BARGHEST-ngo/MESHnet/internal/transport"
	"github.com/tailscale/tailcat"
	"tailscale.com/types/key"
)

type ClientConfig struct {
	ServerAddr    string
	PairingPIN    string
	Name          string
	Logf          func(format string, args ...any)
	OnMessage     func(m Message)
	OnStateChange func(s State, e error)
}

type Client struct {
	mu       sync.Mutex // guards all to the line break
	tc       tailcatClient
	conn     net.Conn
	used     bool // Connect has been called, Clients are single-use
	writer   *transport.Writer
	state    State
	finished bool // final state has been reported, set once

	cfg             ClientConfig
	newTailcat      func(cfg ClientConfig) tailcatClient
	pingInterval    time.Duration
	livenessTimeout time.Duration
}

type tailcatClient interface {
	DialTCPPort(ctx context.Context, port uint16) (net.Conn, error)
	PublicKey() key.NodePublic
	Close() error
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

	if cfg.OnStateChange == nil {
		cfg.OnStateChange = func(s State, e error) {}
	}

	c := &Client{
		cfg:             cfg,
		pingInterval:    pingInterval,
		livenessTimeout: defaultLivenessTimeout,
	}
	c.newTailcat = c.buildTailcat
	return c
}

func (c *Client) buildTailcat(cfg ClientConfig) tailcatClient {
	tc := tailcat.NewClient(tailcat.Addr(cfg.ServerAddr))
	tc.Logf = cfg.Logf
	return tc
}

func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.used {
		c.mu.Unlock()
		return ErrClosed
	}

	c.used = true
	c.mu.Unlock()

	c.setState(StateConnecting, nil)

	tc := c.newTailcat(c.cfg)

	conn, err := tc.DialTCPPort(ctx, protocol.ControlPort)
	if err != nil {
		tc.Close()
		c.finish(StateErrored, err)
		return err
	}

	if err := c.handshake(ctx, conn, tc.PublicKey().String()); err != nil {
		conn.Close()
		tc.Close()
		c.finish(StateErrored, err)
		return err
	}

	c.mu.Lock()
	if c.finished { // Disconnect called whilst Connecting
		c.mu.Unlock()
		conn.Close()
		tc.Close()
		return ErrClosed
	}
	c.tc = tc
	c.conn = conn
	w := transport.NewWriter(conn)
	c.writer = w
	c.mu.Unlock()

	c.setState(StateConnected, nil)
	go c.readLoop(conn)
	go c.pingLoop(w)
	return nil
}

func (c *Client) Disconnect() error {
	c.mu.Lock()
	if !c.used || c.finished {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	c.setState(StateDisconnecting, nil)
	c.finish(StateDisconnected, nil)
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

func (c *Client) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *Client) handshake(ctx context.Context, conn net.Conn, key string) error {
	deadline := time.Now().Add(10 * (time.Second))
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}

	if err := conn.SetReadDeadline(deadline); err != nil {
		return fmt.Errorf("client: handshake: %w", err)
	}

	if err := protocol.WritePayload(conn, protocol.KindHello, protocol.Hello{
		Version: protocol.Version,
		Key:     key,
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

		if ack.ClientID != key {
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
	cause := c.serve(conn)

	if errors.Is(cause, ErrServerClosed) {
		c.cfg.Logf("meshnet: %v", cause)
		c.finish(StateDisconnected, cause)
		return
	}
	c.cfg.Logf("meshnet: connection lost: %v", cause)
	c.finish(StateErrored, cause)
}

func (c *Client) pingLoop(w *transport.Writer) {
	t := time.NewTicker(c.pingInterval)
	defer t.Stop()
	for {
		select {
		case <-w.Done():
			return
		case <-t.C:
			err := w.Enqueue(protocol.Frame{Kind: protocol.KindPing})
			if err != nil && !errors.Is(err, transport.ErrFull) {
				return
			}
		}
	}
}

func (c *Client) serve(conn net.Conn) error {
	r := deadlineReader{
		conn:    conn,
		timeout: c.livenessTimeout,
	}
	for {
		f, err := protocol.ReadFrame(r)
		if err != nil {
			return timeoutErr(err) // EOF, read error, timeout, or our own Disconnect
		}
		if err := c.handleFrame(f); err != nil {
			return err // server Close, malformed or unexpected frame
		}
	}
}
func (c *Client) handleFrame(f protocol.Frame) error {
	switch f.Kind {
	case protocol.KindMsg:
		return c.handleMsg(f)
	case protocol.KindClose:
		return c.handleClose(f)
	case protocol.KindPong:
		return c.handlePong(f)
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
	return fmt.Errorf("%w: %v", ErrServerClosed, cl.Err)
}

func (c *Client) handlePong(f protocol.Frame) error {
	if len(f.Payload) != 0 {
		return fmt.Errorf("pong: %w", protocol.ErrMalformedPayload)
	}
	return nil
}

func (c *Client) setState(s State, err error) {
	c.mu.Lock()
	c.state = s
	c.mu.Unlock()
	c.cfg.OnStateChange(s, err)
}

func (c *Client) finish(s State, err error) {
	c.mu.Lock()
	if c.finished {
		c.mu.Unlock()
		return
	}
	c.finished = true

	conn := c.conn
	w := c.writer
	tc := c.tc

	c.conn = nil
	c.writer = nil
	c.tc = nil
	c.mu.Unlock()

	if conn != nil {
		conn.Close()
	}

	if w != nil {
		w.Close()
	}

	if tc != nil {
		tc.Close()
	}

	c.setState(s, err)
}
