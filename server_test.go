package meshnet

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
	"github.com/BARGHEST-ngo/MESHnet/internal/transport"
	"github.com/tailscale/tailcat"
	"tailscale.com/types/key"
)

type fakeTailcat struct {
	closeCalls atomic.Int32
}

const fakeAddress = "fake-address"

func (f *fakeTailcat) TailcatAddr() tailcat.Addr {
	return fakeAddress
}

func (f *fakeTailcat) Start() error {
	return nil
}

func (f *fakeTailcat) Close() error {
	f.closeCalls.Add(1)
	return nil
}

func (f *fakeTailcat) AddAllowedClient(k key.NodePublic) {}

func newTestServer(t *testing.T, setup func(cfg *ServerConfig)) (*Server, *fakeTailcat) {
	t.Helper()
	fake := &fakeTailcat{}
	cfg := &ServerConfig{}
	if setup != nil {
		setup(cfg)
	}
	s := NewServer(*cfg)
	s.newTailcat = func([]key.NodePublic) tailcatServer {
		return fake
	}
	return s, fake
}

func assertClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("channel not closed")
	}
}

func assertOpen(t *testing.T, ch <-chan struct{}) {
	select {
	case <-ch:
		t.Fatal("channel closed, want open")
	default:
	}
}

func TestServerStartStop(t *testing.T) {
	t.Run("basic start stop", func(t *testing.T) {
		s, fake := newTestServer(t, nil)
		addr, err := s.Start()
		if err != nil {
			t.Fatalf("Start: %v", err)
		}

		if addr != fakeAddress {
			t.Errorf("addr = %q, want %q", addr, fakeAddress)
		}

		if err := s.Stop(); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		if n := fake.closeCalls.Load(); n != 1 {
			t.Errorf("Close called %d times, want 1", n)
		}
	})

	t.Run("done before start", func(t *testing.T) {
		s, _ := newTestServer(t, nil)
		assertClosed(t, s.Done())

		if !errors.Is(s.Err(), ErrClosed) {
			t.Errorf("expected %v, got %v", ErrClosed, s.Err())
		}
	})

	t.Run("done while running", func(t *testing.T) {
		s, _ := newTestServer(t, nil)
		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}

		assertOpen(t, s.Done())
	})

	t.Run("done after stop", func(t *testing.T) {
		s, _ := newTestServer(t, nil)
		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if err := s.Stop(); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		assertClosed(t, s.Done())
		if !errors.Is(s.Err(), ErrClosed) {
			t.Errorf("wanted %v, got %v", ErrClosed, s.Err())
		}
	})

	t.Run("start twice", func(t *testing.T) {
		s, _ := newTestServer(t, nil)
		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if _, err := s.Start(); !errors.Is(err, ErrAlreadyStarted) {
			t.Errorf("wanted %v, got %v", ErrAlreadyStarted, err)
		}
		assertOpen(t, s.Done())
	})

	t.Run("stop twice", func(t *testing.T) {
		s, fake := newTestServer(t, nil)
		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if err := s.Stop(); err != nil {
			t.Fatalf("stop: %v", err)
		}

		if err := s.Stop(); err != nil {
			t.Errorf("wanted %v, got %v", nil, err)
		}
		assertClosed(t, s.Done())
		if n := fake.closeCalls.Load(); n != 1 {
			t.Errorf("Close called %d times, want 1", n)
		}
	})

	t.Run("stop before start", func(t *testing.T) {
		s, fake := newTestServer(t, nil)
		if err := s.Stop(); err != nil {
			t.Fatalf("stop: %v", err)
		}

		if n := fake.closeCalls.Load(); n != 0 {
			t.Errorf("Close called %d times, want 0", n)
		}
	})

	t.Run("restart", func(t *testing.T) {
		s, fake := newTestServer(t, nil)
		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		first := s.Done()

		if err := s.Stop(); err != nil {
			t.Fatalf("stop: %v", err)
		}

		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		second := s.Done()

		assertClosed(t, first)
		assertOpen(t, second)

		if n := fake.closeCalls.Load(); n != 1 {
			t.Errorf("Close called %d times, want 1", n)
		}
	})

	t.Run("concurrent stop", func(t *testing.T) {
		s, fake := newTestServer(t, nil)
		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 10 {
			wg.Go(func() {
				<-start
				if err := s.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			})
		}
		close(start)
		wg.Wait()
		if n := fake.closeCalls.Load(); n != 1 {
			t.Errorf("Close called %d times, want 1", n)
		}
		assertClosed(t, s.Done())
		if !errors.Is(s.Err(), ErrClosed) {
			t.Errorf("wanted %v, got %v", ErrClosed, s.Err())
		}
	})
}

func TestServerParseAllowedClients(t *testing.T) {
	k1 := key.NewNode().Public().String()
	k2 := key.NewNode().Public().String()
	k3 := key.NewNode().Public().String()
	tests := []struct {
		name    string
		in      []string
		wantLen int
		wantErr bool
	}{
		{name: "nil input", in: nil, wantLen: 0, wantErr: false},
		{name: "empty slice", in: []string{}, wantLen: 0, wantErr: false},
		{name: "one valid", in: []string{k1}, wantLen: 1, wantErr: false},
		{name: "several valid", in: []string{k1, k2, k3}, wantLen: 3, wantErr: false},
		{name: "bad entry", in: []string{"rubbish"}, wantLen: 0, wantErr: true},
		{name: "mixed entry", in: []string{k1, "rubbish", k2}, wantLen: 0, wantErr: true},
		{name: "missing prefix", in: []string{strings.TrimPrefix(k1, "nodekey:")}, wantLen: 0, wantErr: true},
		{name: "whitespace", in: []string{" " + k1 + " "}, wantLen: 1, wantErr: false},
		{name: "empty key", in: []string{""}, wantLen: 0, wantErr: true},
		{name: "zero key", in: []string{key.NodePublic{}.String()}, wantLen: 0, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseAllowedClients(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.wantErr {
				return
			}

			if len(got) != tc.wantLen {
				t.Errorf("expected %d, got %d", tc.wantLen, len(got))
			}

			for i := range got {
				if got[i].String() != strings.TrimSpace(tc.in[i]) {
					t.Errorf("unexpected key found")
				}
			}
		})
	}
}

const testClientName = "test"

// Run handleControl on one end of the pipe and perform handshake from the other side (as the client would)
func connectClient(t *testing.T, s *Server) (conn net.Conn, clientID string, done <-chan struct{}) {
	t.Helper()
	server, client := net.Pipe()
	deadline := time.Now().Add(2 * time.Second)
	server.SetDeadline(deadline)
	client.SetDeadline(deadline)

	finished := make(chan struct{})
	go func() {
		s.handleControl(server)
		close(finished)
	}()

	t.Cleanup(func() {
		client.Close()
		<-finished
	})

	clientID = key.NewNode().Public().String()
	hello := protocol.Hello{Version: protocol.Version, Key: clientID, Name: testClientName, PIN: s.PairingPIN()}
	if err := protocol.WritePayload(client, protocol.KindHello, hello); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	f, err := protocol.ReadFrame(client)
	if err != nil || f.Kind != protocol.KindHelloAck {
		t.Fatalf("handshake: kind %v, err %v", f.Kind, err)
	}

	waitWriter(t, s)
	return client, clientID, finished
}

// During tests the server attaches a writer immediately after HelloAck is sent
// An immediate send could correctly return ErrNotConnected
// Will be redundant after adding ClientConnected / Disconnected events etc
func waitWriter(t *testing.T, s *Server) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		ready := s.sess != nil && s.sess.writer != nil
		s.mu.Unlock()
		if ready {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("session writer was never created")
}

func startNewTestServer(t *testing.T) *Server {
	t.Helper()
	s, _ := newTestServer(t, nil)
	if _, err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	return s
}

func TestServerReceiveMessage(t *testing.T) {
	type received struct {
		clientID string
		m        Message
	}
	got := make(chan received, 1)
	s, _ := newTestServer(t, func(cfg *ServerConfig) {
		cfg.OnMessage = func(clientID string, m Message) {
			got <- received{clientID, m}
		}
	})

	if _, err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	conn, clientID, _ := connectClient(t, s)
	sent := protocol.Msg{Topic: "status", Data: []byte("ok")}
	if err := protocol.WritePayload(conn, protocol.KindMsg, sent); err != nil {
		t.Fatalf("write msg: %v", err)
	}

	select {
	case r := <-got:
		if r.clientID != clientID {
			t.Errorf("clientID is %q, want %q", r.clientID, clientID)
		}

		if r.m.Topic != sent.Topic || !bytes.Equal(r.m.Data, sent.Data) {
			t.Errorf("message %+v, want %+v", r.m, sent)
		}
	case <-time.After(time.Second):
		t.Fatal("OnMessage not called")
	}
}

func TestServerSendMessage(t *testing.T) {
	s := startNewTestServer(t)
	conn, clientID, _ := connectClient(t, s)

	want := Message{Topic: "status", Data: []byte("ok")}
	if err := s.Send(clientID, want); err != nil {
		t.Fatalf("Send: %v", err)
	}

	f, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if f.Kind != protocol.KindMsg {
		t.Fatalf("kind = %v, want msg", f.Kind)
	}
	var got protocol.Msg
	if err := got.UnmarshalBinary(f.Payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Topic != want.Topic || !bytes.Equal(got.Data, want.Data) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestServerSendErrors(t *testing.T) {
	t.Run("not connected", func(t *testing.T) {
		s := startNewTestServer(t)
		err := s.Send("nodekey:anything", Message{Topic: "t"})
		if !errors.Is(err, ErrNotConnected) {
			t.Fatalf("err = %v, want ErrNotConnected", err)
		}
	})

	t.Run("unknown client", func(t *testing.T) {
		s := startNewTestServer(t)
		connectClient(t, s)

		other := key.NewNode().Public().String()
		if err := s.Send(other, Message{Topic: "t"}); !errors.Is(err, ErrUnknownClient) {
			t.Fatalf("err = %v, want ErrUnknownClient", err)
		}
	})

	t.Run("invalid message", func(t *testing.T) {
		s := startNewTestServer(t)
		_, clientID, _ := connectClient(t, s)

		if err := s.Send(clientID, Message{Topic: ""}); !errors.Is(err, protocol.ErrMalformedPayload) {
			t.Fatalf("err = %v, want ErrMalformedPayload", err)
		}
	})
}

func TestServerSendQueueFull(t *testing.T) {
	s := startNewTestServer(t)
	_, clientID, _ := connectClient(t, s)

	accepted := 0
	var err error
	for accepted <= transport.DefaultQueueSize+1 {
		if err = s.Send(clientID, Message{Topic: "Darth", Data: []byte("Vader")}); err != nil {
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

func TestServerOnMessageCanSend(t *testing.T) {
	var s *Server
	s, _ = newTestServer(t, func(cfg *ServerConfig) {
		cfg.OnMessage = func(clientID string, m Message) {
			if err := s.Send(clientID, Message{Topic: "echo", Data: m.Data}); err != nil {
				t.Errorf("Send from callback: %v", err)
			}
		}
	})

	if _, err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	conn, _, _ := connectClient(t, s)

	if err := protocol.WritePayload(conn, protocol.KindMsg, protocol.Msg{Topic: "ping", Data: []byte("hi")}); err != nil {
		t.Fatalf("write Msg: %v", err)
	}

	f, err := protocol.ReadFrame(conn) // a deadlock would show up as a deadline error here
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	var echo protocol.Msg
	if err := echo.UnmarshalBinary(f.Payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if echo.Topic != "echo" || string(echo.Data) != "hi" {
		t.Errorf("echo = %+v", echo)
	}
}

func TestServerClientCloseEndsSession(t *testing.T) {
	s := startNewTestServer(t)
	conn, clientID, done := connectClient(t, s)

	if err := protocol.WritePayload(conn, protocol.KindClose, protocol.Close{}); err != nil {
		t.Fatalf("write Close: %v", err)
	}
	assertClosed(t, done)

	if err := s.Send(clientID, Message{Topic: "t"}); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Send after client Close: %v, want ErrNotConnected", err)
	}
}

func TestServerMalformedFrameEndsSession(t *testing.T) {
	cases := []struct {
		name  string
		frame protocol.Frame
	}{
		{name: "malformed msg", frame: protocol.Frame{Kind: protocol.KindMsg, Payload: []byte{0x05, 'h'}}},
		{name: "malformed close", frame: protocol.Frame{Kind: protocol.KindClose, Payload: []byte{0x02}}},
		{name: "Hello after handshake", frame: protocol.Frame{Kind: protocol.KindHello, Payload: nil}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var called atomic.Bool
			s, _ := newTestServer(t, func(cfg *ServerConfig) {
				cfg.OnMessage = func(string, Message) { called.Store(true) }
			})

			if _, err := s.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			conn, _, done := connectClient(t, s)

			if err := protocol.WriteFrame(conn, tc.frame); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			assertClosed(t, done)

			if called.Load() {
				t.Error("OnMessage called for a malformed message")
			}
		})
	}
}

func TestServerSessionEndStopsWriter(t *testing.T) {
	s := startNewTestServer(t)
	conn, _, done := connectClient(t, s)

	s.mu.Lock()
	w := s.sess.writer
	s.mu.Unlock()
	conn.Close() // client goes away
	assertClosed(t, done)

	if err := w.Enqueue(protocol.Frame{Kind: protocol.KindMsg, Payload: []byte{0x01, 't'}}); !errors.Is(err, transport.ErrClosed) {
		t.Errorf("Enqueue after session end: %v, want transport.ErrClosed", err)
	}
	s.mu.Lock()
	sess := s.sess
	s.mu.Unlock()
	if sess != nil {
		t.Error("session still registered after it ended")
	}
}

func TestServerStopEndsSession(t *testing.T) {
	s := startNewTestServer(t)
	conn, _, done := connectClient(t, s)

	start := time.Now()
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := protocol.ReadFrame(conn); err == nil {
		t.Fatal("ReadFrame succeeded after Stop")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("client noticed after %v: Stop didn't close the session's conn", elapsed)
	}
	assertClosed(t, done)
}

func TestServerOnMessageNil(t *testing.T) {
	s := startNewTestServer(t)
	conn, _, done := connectClient(t, s)

	if err := protocol.WritePayload(conn, protocol.KindMsg, protocol.Msg{Topic: "hello"}); err != nil {
		t.Fatalf("write msg: %v", err)
	}

	// handler process frames in order so by the time it handles this close
	// it has already dispatched the Msg to the no-op callback
	if err := protocol.WritePayload(conn, protocol.KindClose, protocol.Close{}); err != nil {
		t.Fatalf("write close: %v", err)
	}

	assertClosed(t, done)
}

type disconnectEvent struct {
	info ClientInfo
	err  error
}

// lifecycle records OnClientConnected / OnClientDisconnected calls.
type lifecycle struct {
	connected    chan ClientInfo
	disconnected chan disconnectEvent
}

func recordLifecycle(cfg *ServerConfig) *lifecycle {
	l := &lifecycle{
		connected:    make(chan ClientInfo, 4),
		disconnected: make(chan disconnectEvent, 4),
	}
	cfg.OnClientConnected = func(c ClientInfo) { l.connected <- c }
	cfg.OnClientDisconnected = func(c ClientInfo, err error) { l.disconnected <- disconnectEvent{c, err} }
	return l
}

// next waits for one value from ch, failing the test after a timeout.
func next[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(time.Second):
		t.Fatal("callback not called")
		var zero T
		return zero
	}
}

// none checks nothing more arrives on ch for a short while.
func none[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected extra callback: %+v", v)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestServerDisconnectReason(t *testing.T) {
	cases := []struct {
		name    string
		cause   error
		stopped bool
		want    error
	}{
		{"clean close", clientCloseError{protocol.CloseErrNone}, false, nil},
		{"close with reason", clientCloseError{protocol.CloseErrError}, false, clientCloseError{protocol.CloseErrError}},
		{"connection lost", io.EOF, false, io.EOF},
		{"stopped", io.EOF, true, ErrClosed},
		{"stopped wins over clean close", clientCloseError{protocol.CloseErrNone}, true, ErrClosed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := disconnectReason(tc.cause, tc.stopped)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("got %v, want nil", got)
				}
				return
			}
			if !errors.Is(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestServerOnClientConnected(t *testing.T) {
	var l *lifecycle
	s, _ := newTestServer(t, func(cfg *ServerConfig) { l = recordLifecycle(cfg) })
	if _, err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, clientID, _ := connectClient(t, s)

	got := next(t, l.connected)
	if got.ID != clientID || got.Name != testClientName {
		t.Errorf("connected %+v, want ID %q name %q", got, clientID, testClientName)
	}
	none(t, l.connected)
}

func TestServerOnClientConnectedCanSend(t *testing.T) {
	var s *Server
	s, _ = newTestServer(t, func(cfg *ServerConfig) {
		cfg.OnClientConnected = func(c ClientInfo) {
			if err := s.Send(c.ID, Message{Topic: "greeting", Data: []byte("hi")}); err != nil {
				t.Errorf("Send from OnClientConnected: %v", err)
			}
		}
	})
	if _, err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	conn, _, _ := connectClient(t, s)

	f, err := protocol.ReadFrame(conn) // the first frame after HelloAck
	if err != nil || f.Kind != protocol.KindMsg {
		t.Fatalf("greeting: kind %v, err %v", f.Kind, err)
	}
	var m protocol.Msg
	if err := m.UnmarshalBinary(f.Payload); err != nil || m.Topic != "greeting" {
		t.Fatalf("greeting: %+v, err %v", m, err)
	}
}

func TestServerOnClientDisconnected(t *testing.T) {
	cases := []struct {
		name  string
		end   func(t *testing.T, s *Server, conn net.Conn)
		check func(t *testing.T, err error)
	}{
		{
			name: "clean close",
			end: func(t *testing.T, s *Server, conn net.Conn) {
				protocol.WritePayload(conn, protocol.KindClose, protocol.Close{})
			},
			check: func(t *testing.T, err error) {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
			},
		},
		{
			name: "close with reason",
			end: func(t *testing.T, s *Server, conn net.Conn) {
				protocol.WritePayload(conn, protocol.KindClose, protocol.Close{Err: protocol.CloseErrError})
			},
			check: func(t *testing.T, err error) {
				var ce clientCloseError
				if !errors.As(err, &ce) || ce.reason != protocol.CloseErrError {
					t.Fatalf("err = %v, want clientCloseError(error)", err)
				}
			},
		},
		{
			name: "connection lost",
			end:  func(t *testing.T, s *Server, conn net.Conn) { conn.Close() },
			check: func(t *testing.T, err error) {
				if err == nil || errors.Is(err, ErrClosed) {
					t.Fatalf("err = %v, want a connection error", err)
				}
			},
		},
		{
			name: "server stopped",
			end:  func(t *testing.T, s *Server, conn net.Conn) { s.Stop() },
			check: func(t *testing.T, err error) {
				if !errors.Is(err, ErrClosed) {
					t.Fatalf("err = %v, want ErrClosed", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var l *lifecycle
			s, _ := newTestServer(t, func(cfg *ServerConfig) { l = recordLifecycle(cfg) })
			if _, err := s.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			conn, clientID, done := connectClient(t, s)
			next(t, l.connected)

			tc.end(t, s, conn)
			assertClosed(t, done)

			ev := next(t, l.disconnected)
			if ev.info.ID != clientID {
				t.Errorf("disconnected ID %q, want %q", ev.info.ID, clientID)
			}
			tc.check(t, ev.err)
			none(t, l.disconnected) // exactly once
		})
	}
}

func TestServerLifecycleOrder(t *testing.T) {
	events := make(chan string, 8)
	s, _ := newTestServer(t, func(cfg *ServerConfig) {
		cfg.OnClientConnected = func(ClientInfo) { events <- "connected" }
		cfg.OnMessage = func(string, Message) { events <- "message" }
		cfg.OnClientDisconnected = func(ClientInfo, error) { events <- "disconnected" }
	})
	if _, err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	conn, _, done := connectClient(t, s)

	protocol.WritePayload(conn, protocol.KindMsg, protocol.Msg{Topic: "t"})
	protocol.WritePayload(conn, protocol.KindClose, protocol.Close{})
	assertClosed(t, done)

	for _, want := range []string{"connected", "message", "disconnected"} {
		if got := next(t, events); got != want {
			t.Fatalf("event %q, want %q", got, want)
		}
	}
}
