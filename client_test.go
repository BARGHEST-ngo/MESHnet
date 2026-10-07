package meshnet

import (
	"bytes"
	"context"
	"errors"
	"io"
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
	key           key.NodePublic
	conn          net.Conn
	dialErr       error
	closed        atomic.Bool
	streams       chan net.Conn
	streamDialErr error
}

func (f *fakeTailcatClient) DialTCPPort(_ context.Context, port uint16) (net.Conn, error) {
	if port == protocol.StreamPort {
		if f.streamDialErr != nil {
			return nil, f.streamDialErr
		}
		server, client := net.Pipe()
		f.streams <- server
		return client, nil
	}
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

	fake = &fakeTailcatClient{
		key:     key.NewNode().Public(),
		conn:    clientEnd,
		streams: make(chan net.Conn, 8),
	}
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
	// Disconnect while Connect waits for the ack, then the handshake fails:
	// Connect returns through the handshake error path
	t.Run("server then closes", func(t *testing.T) {
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
	})

	// Disconnect while Connect waits for the ack and then the handshake succeeds:
	// Connect must notice finished before storing the connection.
	t.Run("server then accepts", func(t *testing.T) {
		c, fake, server, states := fakeConnect(t, nil)
		helloRead := make(chan struct{})
		disconnected := make(chan struct{})
		goServer(t, server, func() {
			f, err := protocol.ReadFrame(server)
			if err != nil {
				t.Errorf("read Hello: %v", err)
				return
			}
			var h protocol.Hello
			if err := h.UnmarshalBinary(f.Payload); err != nil {
				t.Errorf("unmarshal Hello: %v", err)
				return
			}
			close(helloRead)
			<-disconnected // reply only after the client has been told to disconnect
			protocol.WritePayload(server, protocol.KindHelloAck, protocol.HelloAck{Version: protocol.Version, ClientID: h.Key})
		})

		result := make(chan error, 1)
		go func() { result <- c.Connect(context.Background()) }()

		<-helloRead
		c.Disconnect()
		close(disconnected)

		if err := <-result; !errors.Is(err, ErrClosed) {
			t.Fatalf("Connect err = %v, want ErrClosed", err)
		}

		if !fake.closed.Load() {
			t.Error("tailcat client not closed")
		}

		for _, ev := range drain(states) {
			if ev.state == StateConnected {
				t.Fatal("reported Connected after Disconnect")
			}
		}
		if err := c.Send(Message{Topic: "t"}); !errors.Is(err, ErrNotConnected) {
			t.Errorf("Send after Disconnect: %v, want ErrNotConnected", err)
		}
	})
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

type streamCall struct {
	target string
	stream net.Conn
}

func requestStream(t *testing.T, fake *fakeTailcatClient, server net.Conn, id uint64, target string) net.Conn {
	t.Helper()
	if err := protocol.WritePayload(
		server,
		protocol.KindStreamRequest,
		protocol.StreamRequest{ID: id, Target: target},
	); err != nil {
		t.Fatalf("write StreamRequest: %v", err)
	}

	stream := next(t, fake.streams)
	t.Cleanup(func() { stream.Close() })
	stream.SetDeadline(time.Now().Add(2 * time.Second)) // a stuck stream fails, not hangs

	got, err := protocol.ReadStreamID(stream)
	if err != nil {
		t.Fatalf("read stream ID: %v", err)
	}
	if got != id {
		t.Fatalf("stream ID %d, want %d", got, id)
	}
	return stream
}

func TestClientStreamRequestOpensStream(t *testing.T) {
	_, fake, server, _ := connected(t, nil)
	requestStream(t, fake, server, 4, "5555")
}

func TestClientStreamRequestHandler(t *testing.T) {
	calls := make(chan streamCall, 1)
	_, fake, server, _ := connected(t, func(cfg *ClientConfig) {
		cfg.OnStreamRequest = func(target string, stream net.Conn) {
			calls <- streamCall{target, stream}
		}
	})

	serverEnd := requestStream(t, fake, server, 4, "5555")
	call := next(t, calls)
	t.Cleanup(func() { call.stream.Close() })
	if call.target != "5555" {
		t.Fatalf("handler target %q, want 5555", call.target)
	}

	deadline := time.Now().Add(2 * time.Second)
	call.stream.SetDeadline(deadline)

	for _, dir := range []struct {
		name     string
		from, to net.Conn
	}{
		{name: "client to server", from: call.stream, to: serverEnd},
		{name: "server to client", from: serverEnd, to: call.stream},
	} {
		want := []byte(dir.name)
		wrote := make(chan error, 1)
		go func() {
			_, err := dir.from.Write(want)
			wrote <- err
		}()

		got := make([]byte, len(want))
		if _, err := io.ReadFull(dir.to, got); err != nil {
			t.Fatalf("%s: read: %v", dir.name, err)
		}
		if err := <-wrote; err != nil {
			t.Fatalf("%s: write: %v", dir.name, err)
		}
		if string(got) != string(want) {
			t.Fatalf("%s: got %q, want %q", dir.name, want, got)
		}
	}
}

func TestClientStreamRequestDefaultRefuses(t *testing.T) {
	_, fake, server, states := connected(t, nil)

	stream := requestStream(t, fake, server, 7, "5555")

	if _, err := stream.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read after the ID: %v, want EOF", err)
	}
	none(t, states) // refusing a stream does not end the session
}

func TestClientStreamRequestMalformed(t *testing.T) {
	_, fake, server, states := connected(t, nil)

	if err := protocol.WriteFrame(
		server,
		protocol.Frame{Kind: protocol.KindStreamRequest, Payload: []byte{0x01}},
	); err != nil {
		t.Fatalf("write frame: %v", err)
	}

	events := wantStates(t, states, StateErrored)
	if !errors.Is(events[0].err, protocol.ErrMalformedPayload) {
		t.Fatalf("Errored with %v, want ErrMalformedPayload", events[0].err)
	}
	none(t, fake.streams) // nothing was dialled
}

func TestClientStreamRequestDialFails(t *testing.T) {
	calls := make(chan streamCall, 1)
	c, fake, server, states := connected(t, func(cfg *ClientConfig) {
		cfg.OnStreamRequest = func(target string, stream net.Conn) {
			calls <- streamCall{target, stream}
		}
	})
	fake.streamDialErr = errors.New("no route")

	if err := protocol.WritePayload(
		server,
		protocol.KindStreamRequest, protocol.StreamRequest{ID: 7, Target: "5555"},
	); err != nil {
		t.Fatalf("write StreamRequest: %v", err)
	}

	none(t, calls)  // no stream so the handler is not called
	none(t, states) // and the session carries on
	if got := c.State(); got != StateConnected {
		t.Fatalf("state %v, want Connected", got)
	}
}

func TestClientStreamsCloseWithSession(t *testing.T) {
	calls := make(chan streamCall, 1)
	c, fake, server, _ := connected(t, func(cfg *ClientConfig) {
		cfg.OnStreamRequest = func(target string, stream net.Conn) {
			calls <- streamCall{target, stream}
		}
	})

	serverEnd := requestStream(t, fake, server, 7, "5555")
	call := next(t, calls)

	if err := c.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	if _, err := call.stream.Read(make([]byte, 1)); err == nil {
		t.Fatal("handler's stream still readable after Disconnect")
	}
	if _, err := serverEnd.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("server's end: %v, want EOF", err)
	}
}

// Stream handlers might block for the lifetime of a stream.
// The read loop must keep going or messages and ping/pong responses stop and end the session
func TestClientStreamHandlerDoesNotBlockReadLoop(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	msgs := make(chan Message, 1)
	_, fake, server, _ := connected(t, func(cfg *ClientConfig) {
		cfg.OnStreamRequest = func(_ string, stream net.Conn) {
			close(entered)
			<-release
			stream.Close()
		}
		cfg.OnMessage = func(m Message) { msgs <- m }
	})
	t.Cleanup(func() { close(release) })

	requestStream(t, fake, server, 7, "5555")
	next(t, entered) // the handler is now blocked

	if err := protocol.WritePayload(
		server,
		protocol.KindMsg, protocol.Msg{Topic: "status", Data: []byte("ok")},
	); err != nil {
		t.Fatalf("write Msg: %v", err)
	}
	if m := next(t, msgs); m.Topic != "status" {
		t.Fatalf("got topic %q, want status", m.Topic)
	}
}

func TestClientStreamRequestsAreIndependent(t *testing.T) {
	calls := make(chan streamCall, 2)
	_, fake, server, _ := connected(t, func(cfg *ClientConfig) {
		cfg.OnStreamRequest = func(target string, stream net.Conn) {
			calls <- streamCall{target, stream}
		}
	})

	// Each request gets its own stream, carrying its own ID.
	requestStream(t, fake, server, 7, "5555")
	first := next(t, calls)
	t.Cleanup(func() { first.stream.Close() })

	requestStream(t, fake, server, 9, "capture")
	second := next(t, calls)
	t.Cleanup(func() { second.stream.Close() })

	if first.target != "5555" || second.target != "capture" {
		t.Fatalf("targets %q then %q, want 5555 then capture", first.target, second.target)
	}

	if first.stream == second.stream {
		t.Fatal("both requests were given the same stream")
	}
}
