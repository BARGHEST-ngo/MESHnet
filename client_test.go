package meshnet

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
	"github.com/BARGHEST-ngo/MESHnet/internal/transport"
	"tailscale.com/types/key"
)

type fakeTailcatClient struct {
	key     key.NodePublic
	conn    net.Conn
	dialErr error
	closed  atomic.Bool
}

func (f *fakeTailcatClient) DialTCPPort(context.Context, uint16) (net.Conn, error) {
	if f.dialErr != nil {
		return nil, f.dialErr
	}
	return f.conn, nil
}

func (f *fakeTailcatClient) PublicKey() key.NodePublic {
	return f.key
}

func (f *fakeTailcatClient) Close() error {
	f.closed.Store(true)
	return nil
}

type stateEvent struct {
	state State
	err   error
}

// fakeConnect builds a Client whose tailcat client is a fake dialling one end of a pipe.
// The test plays the server on the returned end.
func fakeConnect(t *testing.T, setup func(cfg *ClientConfig)) (c *Client, fake *fakeTailcatClient, server net.Conn, states chan stateEvent) {
	t.Helper()
	serverEnd, clientEnd := net.Pipe()
	deadline := time.Now().Add(2 * time.Second)
	serverEnd.SetDeadline(deadline)
	clientEnd.SetDeadline(deadline)
	t.Cleanup(func() { serverEnd.Close() })

	states = make(chan stateEvent, 8)
	// A valid Hello needs a 6-digit PIN; the fake server doesn't check it.
	cfg := &ClientConfig{PairingPIN: "123456", Name: "test"}
	if setup != nil {
		setup(cfg)
	}
	appState := cfg.OnStateChange
	cfg.OnStateChange = func(s State, err error) {
		if appState != nil {
			appState(s, err)
		}
		states <- stateEvent{s, err}
	}

	fake = &fakeTailcatClient{key: key.NewNode().Public(), conn: clientEnd}
	c = NewClient(*cfg)
	c.newTailcat = func(ClientConfig) tailcatClient { return fake }
	t.Cleanup(func() { c.Disconnect() })
	return c, fake, serverEnd, states
}

// acceptHello plays the server's side of the handshake: reads the Hello and replies HelloAck.
// Returns Hello so tests can inspect it.
func acceptHello(t *testing.T, server net.Conn) protocol.Hello {
	t.Helper()
	f, err := protocol.ReadFrame(server)
	if err != nil || f.Kind != protocol.KindHello {
		t.Errorf("read Hello: kind %v, err %v", f.Kind, err)
		return protocol.Hello{}
	}
	var h protocol.Hello
	if err := h.UnmarshalBinary(f.Payload); err != nil {
		t.Errorf("decode Hello: %v", err)
		return h
	}
	if err := protocol.WritePayload(server, protocol.KindHelloAck, protocol.HelloAck{Version: protocol.Version, ClientID: h.Key}); err != nil {
		t.Errorf("write HelloAck: %v", err)
	}
	return h
}

// goServer runs the fake server's side in a goroutine.
// At clean-up it closes the server and waits for the goroutine to return so it can never report to a test that has already finished.
func goServer(t *testing.T, server net.Conn, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	t.Cleanup(func() {
		server.Close()
		<-done
	})
}

// connected runs fakeConnect, completes the handshake and consumes the Connecting and Connected states leaving a connected client.
func connected(t *testing.T, setup func(cfg *ClientConfig)) (*Client, *fakeTailcatClient, net.Conn, chan stateEvent) {
	t.Helper()
	c, fake, server, states := fakeConnect(t, setup)
	goServer(t, server, func() { acceptHello(t, server) })
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	wantStates(t, states, StateConnecting, StateConnected)
	return c, fake, server, states
}

// wantStates checks state events in order.
func wantStates(t *testing.T, states <-chan stateEvent, want ...State) []stateEvent {
	t.Helper()
	got := make([]stateEvent, 0, len(want))
	for _, w := range want {
		ev := next(t, states)
		if ev.state != w {
			t.Fatalf("state %v, want %v (so far %+v)", ev.state, w, got)
		}
		got = append(got, ev)
	}
	return got
}

// Returns a Client on a pipe as if Connect has succeeded with the server on the other end.
// done is closed when the Client readloop exits
// register callbacks on Client via setup
func pipeClient(t *testing.T, setup func(cfg *ClientConfig)) (c *Client, server net.Conn, done <-chan struct{}) {
	t.Helper()
	serverEnd, clientEnd := net.Pipe()
	deadline := time.Now().Add(2 * time.Second)
	serverEnd.SetDeadline(deadline)
	clientEnd.SetDeadline(deadline)

	cfg := &ClientConfig{}
	if setup != nil {
		setup(cfg)
	}
	c = NewClient(*cfg)

	c.mu.Lock()
	c.used = true
	c.conn = clientEnd
	c.writer = transport.NewWriter(clientEnd)
	c.mu.Unlock()

	finished := make(chan struct{})
	go func() {
		c.readLoop(clientEnd)
		close(finished)
	}()

	t.Cleanup(func() {
		serverEnd.Close()
		<-finished
	})

	return c, serverEnd, finished
}

func TestClientSendMessage(t *testing.T) {
	c, server, _ := pipeClient(t, nil)
	want := Message{Topic: "status", Data: []byte("ok")}
	if err := c.Send(want); err != nil {
		t.Fatalf("Send: %v", err)
	}
	f, err := protocol.ReadFrame(server)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	var got protocol.Msg
	if f.Kind != protocol.KindMsg || got.UnmarshalBinary(f.Payload) != nil {
		t.Fatalf("got kind %v, payload % x", f.Kind, f.Payload)
	}
	if got.Topic != want.Topic || !bytes.Equal(got.Data, want.Data) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestClientReceivesMessage(t *testing.T) {
	got := make(chan Message, 1) // buffered: the callback runs on readLoop's goroutine
	_, server, _ := pipeClient(t, func(cfg *ClientConfig) {
		cfg.OnMessage = func(m Message) { got <- m }
	})

	sent := protocol.Msg{Topic: "cmd", Data: []byte{0x00, 0xff}}
	if err := protocol.WritePayload(server, protocol.KindMsg, sent); err != nil {
		t.Fatalf("write Msg: %v", err)
	}
	select {
	case m := <-got:
		if m.Topic != sent.Topic || !bytes.Equal(m.Data, sent.Data) {
			t.Errorf("got %+v, want %+v", m, sent)
		}
	case <-time.After(time.Second):
		t.Fatal("OnMessage not called")
	}
}

func TestClientSendErrors(t *testing.T) {
	t.Run("not connected", func(t *testing.T) {
		c := NewClient(ClientConfig{})
		if err := c.Send(Message{Topic: "t"}); !errors.Is(err, ErrNotConnected) {
			t.Fatalf("err = %v, want ErrNotConnected", err)
		}
	})
	t.Run("invalid message", func(t *testing.T) {
		c, _, _ := pipeClient(t, nil)
		if err := c.Send(Message{Topic: ""}); !errors.Is(err, protocol.ErrMalformedPayload) {
			t.Fatalf("err = %v, want ErrMalformedPayload", err)
		}
	})
}

func TestClientSendQueueFull(t *testing.T) {
	c, _, _ := pipeClient(t, nil)

	accepted := 0
	var err error
	for accepted <= transport.DefaultQueueSize+1 {
		if err = c.Send(Message{Topic: "Darth", Data: []byte("Vader")}); err != nil {
			break
		}
		accepted++
	}

	if !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("after %d sends err: %v, want %v", accepted, err, ErrSendQueueFull)
	}
	if accepted < transport.DefaultQueueSize {
		t.Fatalf("queue full after only %d sends, want at least %d", accepted, transport.DefaultQueueSize)
	}
}

func TestClientOnMessageCanSend(t *testing.T) {
	var c *Client
	c, server, _ := pipeClient(t, func(cfg *ClientConfig) {
		cfg.OnMessage = func(m Message) {
			if err := c.Send(m); err != nil {
				t.Errorf("send from callback: %v", err)
			}
		}
	})

	want := protocol.Msg{Topic: "Darth", Data: []byte("Vader")}
	if err := protocol.WritePayload(server, protocol.KindMsg, want); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	f, err := protocol.ReadFrame(server)
	if err != nil {
		t.Fatalf("readframe: %v", err)
	}

	if f.Kind != protocol.KindMsg {
		t.Fatalf("unexpected frame kind %v", f.Kind)
	}

	var got protocol.Msg
	if err := got.UnmarshalBinary(f.Payload); err != nil {
		t.Fatalf("decode message: %v", err)
	}

	if got.Topic != want.Topic || !bytes.Equal(got.Data, want.Data) {
		t.Errorf("got %+v, wanted %+v", got, want)
	}
}

func TestClientDisconnectTwice(t *testing.T) {
	c, _, done := pipeClient(t, nil)
	if err := c.Disconnect(); err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	if err := c.Disconnect(); err != nil {
		t.Errorf("unexpected error on second disconnect: %v", err)
	}

	assertClosed(t, done)
}

func TestClientServerCloseEndsConnection(t *testing.T) {
	c, server, done := pipeClient(t, nil)
	if err := protocol.WritePayload(server, protocol.KindClose, protocol.Close{}); err != nil {
		t.Fatalf("write Close: %v", err)
	}
	assertClosed(t, done)

	if err := c.Send(Message{Topic: "t"}); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Send after server Close: %v, want ErrNotConnected", err)
	}
}

func TestClientMalformedFrameEndsConnection(t *testing.T) {
	cases := []struct {
		name  string
		frame protocol.Frame
	}{
		{"malformed msg", protocol.Frame{Kind: protocol.KindMsg, Payload: []byte{0x05, 'h'}}},
		{"malformed close", protocol.Frame{Kind: protocol.KindClose, Payload: []byte{0x02}}},
		{"hello ack after handshake", protocol.Frame{Kind: protocol.KindHelloAck}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var called atomic.Bool
			_, server, done := pipeClient(t, func(cfg *ClientConfig) {
				cfg.OnMessage = func(Message) { called.Store(true) }
			})

			if err := protocol.WriteFrame(server, tc.frame); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			assertClosed(t, done)

			if called.Load() {
				t.Error("OnMessage called for a malformed frame")
			}
		})
	}
}

func TestClientServerGoneEndsConnection(t *testing.T) {
	c, server, done := pipeClient(t, nil)

	server.Close()
	assertClosed(t, done)

	if err := c.Send(Message{Topic: "t"}); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Send after server went away: %v, want ErrNotConnected", err)
	}
}

func TestClientOnMessageNil(t *testing.T) {
	_, server, done := pipeClient(t, func(cfg *ClientConfig) { cfg.OnMessage = nil })

	if err := protocol.WritePayload(server, protocol.KindMsg, protocol.Msg{Topic: "hello"}); err != nil {
		t.Fatalf("write msg: %v", err)
	}

	if err := protocol.WritePayload(server, protocol.KindClose, protocol.Close{}); err != nil {
		t.Fatalf("write close: %v", err)
	}

	assertClosed(t, done)
}

func TestClientConnectStates(t *testing.T) {
	c, _, server, states := fakeConnect(t, nil)
	goServer(t, server, func() { acceptHello(t, server) })

	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	wantStates(t, states, StateConnecting, StateConnected)
}

func TestClientConnectRejected(t *testing.T) {
	c, fake, server, states := fakeConnect(t, nil)
	goServer(t, server, func() {
		protocol.ReadFrame(server) // the Hello
		protocol.WritePayload(server, protocol.KindClose, protocol.Close{Err: protocol.CloseErrWrongPIN})
	})

	err := c.Connect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "wrong_pin") {
		t.Fatalf("Connect err = %v, want wrong_pin", err)
	}
	evs := wantStates(t, states, StateConnecting, StateErrored)
	if evs[1].err == nil {
		t.Error("Errored with nil error")
	}
	if !fake.closed.Load() {
		t.Error("tailcat client not closed after failed Connect")
	}
}

func TestClientDisconnectDuringConnect(t *testing.T) {
	c, _, server, states := fakeConnect(t, nil)
	helloRead := make(chan struct{})
	goServer(t, server, func() {
		protocol.ReadFrame(server) // read the Hello, but never reply
		close(helloRead)
	})

	result := make(chan error, 1)
	go func() { result <- c.Connect(context.Background()) }()

	<-helloRead
	c.Disconnect() // while Connect waits for the ack
	// Disconnect's finish closes nothing (conn isn't stored yet), so release
	// Connect by failing its read: close the server end.
	server.Close()

	if err := <-result; err == nil {
		t.Fatal("Connect succeeded after Disconnect")
	}
	for _, ev := range drain(states) {
		if ev.state == StateConnected {
			t.Fatal("reported Connected after Disconnect")
		}
	}
}

func TestClientConnectDialFails(t *testing.T) {
	c, fake, _, states := fakeConnect(t, nil)
	dialErr := errors.New("no route")
	fake.dialErr = dialErr

	if err := c.Connect(context.Background()); !errors.Is(err, dialErr) {
		t.Fatalf("Connect err = %v, want %v", err, dialErr)
	}
	evs := wantStates(t, states, StateConnecting, StateErrored)
	if !errors.Is(evs[1].err, dialErr) {
		t.Errorf("Errored with %v, want %v", evs[1].err, dialErr)
	}
	if !fake.closed.Load() {
		t.Error("tailcat client not closed after failed dial")
	}
}

func TestClientDisconnectStates(t *testing.T) {
	c, fake, _, states := connected(t, nil)

	c.Disconnect()
	evs := wantStates(t, states, StateDisconnecting, StateDisconnected)
	if evs[1].err != nil {
		t.Errorf("Disconnected with %v, want nil", evs[1].err)
	}
	none(t, states)
	if !fake.closed.Load() {
		t.Error("tailcat client not closed after Disconnect")
	}
}

func TestClientServerCloseState(t *testing.T) {
	_, _, server, states := connected(t, nil)

	if err := protocol.WritePayload(server, protocol.KindClose, protocol.Close{}); err != nil {
		t.Fatalf("write Close: %v", err)
	}
	evs := wantStates(t, states, StateDisconnected)
	if !errors.Is(evs[0].err, ErrServerClosed) {
		t.Errorf("Disconnected with %v, want ErrServerClosed", evs[0].err)
	}
	none(t, states)
}

func TestClientConnectionLostState(t *testing.T) {
	_, _, server, states := connected(t, nil)

	server.Close() // no Close frame: the connection just drops
	evs := wantStates(t, states, StateErrored)
	if evs[0].err == nil {
		t.Error("Errored with nil error")
	}
	none(t, states)
}

func TestClientDisconnectRacesDrop(t *testing.T) {
	c, _, server, states := connected(t, nil)

	go server.Close()
	c.Disconnect()

	final := 0
	for _, ev := range drain(states) {
		if ev.state == StateDisconnected || ev.state == StateErrored {
			final++
		}
	}
	if final != 1 {
		t.Fatalf("got %d final states, want exactly 1", final)
	}
}

func TestClientSendOnConnected(t *testing.T) {
	// c is assigned from fakeConnect before Connect runs
	// so the callback sees it when Connected is reported.
	var c *Client
	c, _, server, states := fakeConnect(t, func(cfg *ClientConfig) {
		cfg.OnStateChange = func(s State, _ error) {
			if s == StateConnected {
				if err := c.Send(Message{Topic: "hello"}); err != nil {
					t.Errorf("Send on Connected: %v", err)
				}
			}
		}
	})
	goServer(t, server, func() { acceptHello(t, server) })
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	wantStates(t, states, StateConnecting, StateConnected)

	f, err := protocol.ReadFrame(server)
	if err != nil || f.Kind != protocol.KindMsg {
		t.Fatalf("read: kind %v, err %v", f.Kind, err)
	}
	var m protocol.Msg
	if err := m.UnmarshalBinary(f.Payload); err != nil || m.Topic != "hello" {
		t.Fatalf("message %+v, err %v", m, err)
	}
}
