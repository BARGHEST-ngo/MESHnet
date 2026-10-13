package meshnet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
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
	mu         sync.Mutex
	peers      map[netip.Addr]key.NodePublic
	closeCalls atomic.Int32
	connected  chan string // client IDs, signalled by newTestServer's OnClientConnected wrapper
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

func (f *fakeTailcat) addPeer(addr net.Addr, k key.NodePublic) {
	ap := netip.MustParseAddrPort(addr.String())
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.peers == nil {
		f.peers = make(map[netip.Addr]key.NodePublic)
	}
	f.peers[ap.Addr()] = k
}

func (f *fakeTailcat) PeerKey(remote net.Addr) (key.NodePublic, bool) {
	ap, err := netip.ParseAddrPort(remote.String())
	if err != nil {
		return key.NodePublic{}, false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.peers[ap.Addr()]
	return k, ok
}

func newTestServer(t *testing.T, setup func(cfg *ServerConfig)) (*Server, *fakeTailcat) {
	t.Helper()
	fake := &fakeTailcat{
		connected: make(chan string, 4),
		peers:     make(map[netip.Addr]key.NodePublic),
	}
	cfg := &ServerConfig{}
	if setup != nil {
		setup(cfg)
	}

	appConnected := cfg.OnClientConnected // the test's own callback, may be nil
	cfg.OnClientConnected = func(c ClientInfo) {
		if appConnected != nil {
			appConnected(c)
		}
		fake.connected <- c.ID
	}

	s := NewServer(*cfg)
	s.newTailcat = func() tailcatServer { return fake }
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

type addrConn struct {
	net.Conn
	remote net.Addr
}

func (c addrConn) RemoteAddr() net.Addr { return c.remote }

var (
	testClientAddr       = tcpAddr("[fd7a:115c:a1e0::1]:40000")
	testClientStreamAddr = tcpAddr("[fd7a:115c:a1e0::1]:40001")
	testOtherHostAddr    = tcpAddr("[fd7a:115c:a1e0::99]:40001")
)

// Run handleControl on one end of the pipe and perform handshake from the other side (as the client would)
func connectClient(t *testing.T, s *Server, fake *fakeTailcat) (conn net.Conn, clientID string, done <-chan struct{}) {
	t.Helper()
	server, client := net.Pipe()
	deadline := time.Now().Add(2 * time.Second)
	server.SetDeadline(deadline)
	client.SetDeadline(deadline)

	finished := make(chan struct{})
	go func() {
		s.handleControl(addrConn{server, testClientAddr})
		close(finished)
	}()

	t.Cleanup(func() {
		client.Close()
		<-finished
	})
	clientPub := key.NewNode().Public()
	fake.addPeer(testClientAddr, clientPub)
	clientID = clientPub.String()
	hello := protocol.Hello{Version: protocol.Version, Key: clientID, Name: testClientName, PIN: s.PairingPIN()}
	if err := protocol.WritePayload(client, protocol.KindHello, hello); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	f, err := protocol.ReadFrame(client)
	if err != nil || f.Kind != protocol.KindHelloAck {
		t.Fatalf("handshake: kind %v, err %v", f.Kind, err)
	}

	select {
	case <-fake.connected:
	case <-time.After(time.Second):
		t.Fatal("OnClientConnected never fired")
	}
	return client, clientID, finished
}

func startNewTestServer(t *testing.T) (*Server, *fakeTailcat) {
	t.Helper()
	s, f := newTestServer(t, nil)
	if _, err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	s.NewPairing()
	return s, f
}

// startPairing starts s and opens a pairing, so a client can connect.
func startPairing(t *testing.T, s *Server) {
	t.Helper()
	if _, err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := s.NewPairing(); err != nil {
		t.Fatalf("new pairing: %v", err)
	}
}

func TestServerReceiveMessage(t *testing.T) {
	type received struct {
		clientID string
		m        Message
	}
	got := make(chan received, 1)
	s, fake := newTestServer(t, func(cfg *ServerConfig) {
		cfg.OnMessage = func(clientID string, m Message) {
			got <- received{clientID, m}
		}
	})

	startPairing(t, s)

	conn, clientID, _ := connectClient(t, s, fake)
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
	s, fake := startNewTestServer(t)
	conn, clientID, _ := connectClient(t, s, fake)

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
		s, _ := startNewTestServer(t)
		err := s.Send("nodekey:anything", Message{Topic: "t"})
		if !errors.Is(err, ErrNotConnected) {
			t.Fatalf("err = %v, want ErrNotConnected", err)
		}
	})

	t.Run("unknown client", func(t *testing.T) {
		s, fake := startNewTestServer(t)
		connectClient(t, s, fake)

		other := key.NewNode().Public().String()
		if err := s.Send(other, Message{Topic: "t"}); !errors.Is(err, ErrUnknownClient) {
			t.Fatalf("err = %v, want ErrUnknownClient", err)
		}
	})

	t.Run("invalid message", func(t *testing.T) {
		s, fake := startNewTestServer(t)
		_, clientID, _ := connectClient(t, s, fake)

		if err := s.Send(clientID, Message{Topic: ""}); !errors.Is(err, protocol.ErrMalformedPayload) {
			t.Fatalf("err = %v, want ErrMalformedPayload", err)
		}
	})
}

func TestServerSendQueueFull(t *testing.T) {
	s, fake := startNewTestServer(t)
	_, clientID, _ := connectClient(t, s, fake)

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
	s, fake := newTestServer(t, func(cfg *ServerConfig) {
		cfg.OnMessage = func(clientID string, m Message) {
			if err := s.Send(clientID, Message{Topic: "echo", Data: m.Data}); err != nil {
				t.Errorf("Send from callback: %v", err)
			}
		}
	})

	startPairing(t, s)
	conn, _, _ := connectClient(t, s, fake)

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
	s, fake := startNewTestServer(t)
	conn, clientID, done := connectClient(t, s, fake)

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
			s, fake := newTestServer(t, func(cfg *ServerConfig) {
				cfg.OnMessage = func(string, Message) { called.Store(true) }
			})

			startPairing(t, s)
			conn, _, done := connectClient(t, s, fake)

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
	s, fake := startNewTestServer(t)
	conn, clientID, done := connectClient(t, s, fake)

	s.mu.Lock()
	sess, err := s.sessionForClientLocked(clientID)
	if err != nil {
		t.Fatalf("session for client ID %s: %v", clientID, err)
	}
	w := sess.writer
	s.mu.Unlock()
	conn.Close() // client goes away
	assertClosed(t, done)

	if err := w.Enqueue(protocol.Frame{Kind: protocol.KindMsg, Payload: []byte{0x01, 't'}}); !errors.Is(err, transport.ErrClosed) {
		t.Errorf("Enqueue after session end: %v, want transport.ErrClosed", err)
	}
	s.mu.Lock()
	sess, _ = s.sessionForClientLocked(clientID)
	s.mu.Unlock()
	if sess != nil {
		t.Error("session still registered after it ended")
	}
}

func TestServerStopEndsSession(t *testing.T) {
	s, fake := startNewTestServer(t)
	conn, _, done := connectClient(t, s, fake)

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
	s, fake := startNewTestServer(t)
	conn, _, done := connectClient(t, s, fake)

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
	s, fake := newTestServer(t, func(cfg *ServerConfig) { l = recordLifecycle(cfg) })
	startPairing(t, s)
	_, clientID, _ := connectClient(t, s, fake)

	got := next(t, l.connected)
	if got.ID != clientID || got.Name != testClientName {
		t.Errorf("connected %+v, want ID %q name %q", got, clientID, testClientName)
	}
	none(t, l.connected)
}

func TestServerOnClientConnectedCanSend(t *testing.T) {
	var s *Server
	s, fake := newTestServer(t, func(cfg *ServerConfig) {
		cfg.OnClientConnected = func(c ClientInfo) {
			if err := s.Send(c.ID, Message{Topic: "greeting", Data: []byte("hi")}); err != nil {
				t.Errorf("Send from OnClientConnected: %v", err)
			}
		}
	})
	startPairing(t, s)
	conn, _, _ := connectClient(t, s, fake)

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
			s, fake := newTestServer(t, func(cfg *ServerConfig) { l = recordLifecycle(cfg) })
			startPairing(t, s)
			conn, clientID, done := connectClient(t, s, fake)
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
	s, fake := newTestServer(t, func(cfg *ServerConfig) {
		cfg.OnClientConnected = func(ClientInfo) { events <- "connected" }
		cfg.OnMessage = func(string, Message) { events <- "message" }
		cfg.OnClientDisconnected = func(ClientInfo, error) { events <- "disconnected" }
	})
	startPairing(t, s)
	conn, _, done := connectClient(t, s, fake)

	protocol.WritePayload(conn, protocol.KindMsg, protocol.Msg{Topic: "t"})
	protocol.WritePayload(conn, protocol.KindClose, protocol.Close{})
	assertClosed(t, done)

	for _, want := range []string{"connected", "message", "disconnected"} {
		if got := next(t, events); got != want {
			t.Fatalf("event %q, want %q", got, want)
		}
	}
}

type streamResult struct {
	conn net.Conn
	err  error
}

// goRequestStream runs RequestStream in the background and reports its result.
func goRequestStream(ctx context.Context, s *Server, clientID, target string) <-chan streamResult {
	res := make(chan streamResult, 1)
	go func() {
		conn, err := s.RequestStream(ctx, clientID, target)
		res <- streamResult{conn, err}
	}()
	return res
}

// readStreamRequest reads the next frame from the client's end and decodes it.
func readStreamRequest(t *testing.T, conn net.Conn) protocol.StreamRequest {
	t.Helper()
	f, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if f.Kind != protocol.KindStreamRequest {
		t.Fatalf("got %v frame, want %v", f.Kind, protocol.KindStreamRequest)
	}
	var r protocol.StreamRequest
	if err := r.UnmarshalBinary(f.Payload); err != nil {
		t.Fatalf("decode StreamRequest: %v", err)
	}
	return r
}

func pendingStreams(s *Server, clientID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, _ := s.sessionForClientLocked(clientID)
	if sess == nil {
		return 0
	}
	return len(sess.pending)
}

func TestServerRequestStreamErrors(t *testing.T) {
	t.Run("no client connected", func(t *testing.T) {
		s, _ := startNewTestServer(t)
		t.Cleanup(func() { s.Stop() })

		_, err := s.RequestStream(context.Background(), "nodekey:anyone", "5555")
		if !errors.Is(err, ErrNotConnected) {
			t.Fatalf("got %v, want ErrNotConnected", err)
		}
	})

	t.Run("unknown client", func(t *testing.T) {
		s, fake := startNewTestServer(t)
		t.Cleanup(func() { s.Stop() })
		_, id, _ := connectClient(t, s, fake)

		_, err := s.RequestStream(context.Background(), "nodekey:someone-else", "5555")
		if !errors.Is(err, ErrUnknownClient) {
			t.Fatalf("got %v, want ErrUnknownClient", err)
		}
		if n := pendingStreams(s, id); n != 0 {
			t.Fatalf("%d pending requests, want 0", n)
		}
	})

	for _, tc := range []struct{ name, target string }{
		{"empty target", ""},
		{"target too long", strings.Repeat("a", 256)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, fake := startNewTestServer(t)
			t.Cleanup(func() { s.Stop() })
			_, clientID, _ := connectClient(t, s, fake)

			_, err := s.RequestStream(context.Background(), clientID, tc.target)
			if !errors.Is(err, protocol.ErrMalformedPayload) {
				t.Fatalf("got %v, want ErrMalformedPayload", err)
			}
			if n := pendingStreams(s, clientID); n != 0 {
				t.Fatalf("%d pending requests, want 0", n)
			}
		})
	}
}

func TestServerRequestStreamSendsFrame(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	conn, clientID, _ := connectClient(t, s, fake)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := goRequestStream(ctx, s, clientID, "5555")
	if r := readStreamRequest(t, conn); r.ID != 1 || r.Target != "5555" {
		t.Fatalf("first request %+v, want ID 1 target 5555", r)
	}

	second := goRequestStream(ctx, s, clientID, "capture")
	if r := readStreamRequest(t, conn); r.ID != 2 || r.Target != "capture" {
		t.Fatalf("second request %+v, want ID 2 target capture", r)
	}

	if n := pendingStreams(s, clientID); n != 2 {
		t.Fatalf("%d pending requests, want 2", n)
	}

	// Nothing answers, so both wait until the caller gives up.
	cancel()
	for _, res := range []<-chan streamResult{first, second} {
		if r := next(t, res); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", r.err)
		}
	}
	if n := pendingStreams(s, clientID); n != 0 {
		t.Fatalf("%d pending requests after cancel, want 0", n)
	}
}

func TestServerRequestStreamContextExpires(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	_, clientID, _ := connectClient(t, s, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	conn, err := s.RequestStream(ctx, clientID, "5555")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
	if conn != nil {
		t.Fatalf("got a stream with an error")
	}
	if n := pendingStreams(s, clientID); n != 0 {
		t.Fatalf("%d pending requests, want 0", n)
	}
}

func TestServerRequestStreamSessionEnds(t *testing.T) {
	cases := []struct {
		name string
		end  func(s *Server, conn net.Conn)
	}{
		{"client drops", func(_ *Server, conn net.Conn) { conn.Close() }},
		{"server stops", func(s *Server, _ net.Conn) { s.Stop() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, fake := startNewTestServer(t)
			t.Cleanup(func() { s.Stop() })
			conn, clientID, _ := connectClient(t, s, fake)

			res := goRequestStream(context.Background(), s, clientID, "5555")
			readStreamRequest(t, conn) // the request is out and waiting

			tc.end(s, conn)

			if r := next(t, res); !errors.Is(r.err, ErrNotConnected) {
				t.Fatalf("got %v, want ErrNotConnected", r.err)
			}
		})
	}
}

func tcpAddr(s string) net.Addr {
	return net.TCPAddrFromAddrPort(netip.MustParseAddrPort(s))
}

// dialStream plays the client answering a stream request
// it connects to the server's stream port from addr and writes id.
func dialStream(t *testing.T, s *Server, from net.Addr, id uint64) net.Conn {
	t.Helper()
	server, client := net.Pipe()
	deadline := time.Now().Add(2 * time.Second)
	server.SetDeadline(deadline)
	client.SetDeadline(deadline)
	t.Cleanup(func() { client.Close() })

	go s.handleStream(addrConn{server, from})
	if err := protocol.WriteStreamID(client, id); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write stream ID: %v", err)
	}
	return client
}

func assertRefused(t *testing.T, stream net.Conn) {
	t.Helper()
	if _, err := stream.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read: %v, want EOF (stream refused)", err)
	}
}

func pipeBytes(t *testing.T, from, to net.Conn, msg string) {
	t.Helper()
	wrote := make(chan error, 1)
	go func() {
		_, err := from.Write([]byte(msg))
		wrote <- err
	}()
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(to, got); err != nil {
		t.Fatalf("read %q: %v", msg, err)
	}
	if err := <-wrote; err != nil {
		t.Fatalf("write %q: %v", msg, err)
	}
	if string(got) != msg {
		t.Fatalf("got %q, want %q", got, msg)
	}
}

func TestServerRequestStreamReturnsStream(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	conn, clientID, _ := connectClient(t, s, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res := goRequestStream(ctx, s, clientID, "5555")

	req := readStreamRequest(t, conn)
	client := dialStream(t, s, testClientStreamAddr, req.ID)

	r := next(t, res)
	if r.err != nil {
		t.Fatalf("RequestStream: %v", r.err)
	}
	t.Cleanup(func() { r.conn.Close() })

	pipeBytes(t, r.conn, client, "server to client")
	pipeBytes(t, client, r.conn, "client to server")
}

func TestServerStreamRefused(t *testing.T) {
	t.Run("unknown ID", func(t *testing.T) {
		s, fake := startNewTestServer(t)
		t.Cleanup(func() { s.Stop() })
		connectClient(t, s, fake)

		assertRefused(t, dialStream(t, s, testClientStreamAddr, 99))
	})

	t.Run("no client connected", func(t *testing.T) {
		s, _ := startNewTestServer(t)
		t.Cleanup(func() { s.Stop() })

		assertRefused(t, dialStream(t, s, testClientStreamAddr, 1))
	})

	t.Run("dial-back after the request expired", func(t *testing.T) {
		s, fake := startNewTestServer(t)
		t.Cleanup(func() { s.Stop() })
		conn, clientID, _ := connectClient(t, s, fake)

		ctx, cancel := context.WithCancel(context.Background())
		res := goRequestStream(ctx, s, clientID, "5555")
		req := readStreamRequest(t, conn)
		cancel()
		if r := next(t, res); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("RequestStream: %v, want context.Canceled", r.err)
		}

		assertRefused(t, dialStream(t, s, testClientStreamAddr, req.ID))
	})
}

func TestServerStreamWrongSender(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	conn, clientID, _ := connectClient(t, s, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res := goRequestStream(ctx, s, clientID, "5555")
	req := readStreamRequest(t, conn)

	assertRefused(t, dialStream(t, s, testOtherHostAddr, req.ID))

	dialStream(t, s, testClientStreamAddr, req.ID)
	r := next(t, res)
	if r.err != nil {
		t.Fatalf("RequestStream after a refused sender: %v", r.err)
	}
	r.conn.Close()
}

func TestServerStreamClosesBeforeID(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	connectClient(t, s, fake)

	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		s.handleStream(addrConn{server, testClientStreamAddr})
		close(done)
	}()

	client.Write([]byte{0, 0, 0}) // part of an ID, then gone
	client.Close()
	assertClosed(t, done)
}

func TestServerStreamsCloseWithSession(t *testing.T) {
	cases := []struct {
		name string
		end  func(s *Server, conn net.Conn)
	}{
		{"client drops", func(_ *Server, conn net.Conn) { conn.Close() }},
		{"server stops", func(s *Server, _ net.Conn) { s.Stop() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, fake := startNewTestServer(t)
			t.Cleanup(func() { s.Stop() })
			conn, clientID, done := connectClient(t, s, fake)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			res := goRequestStream(ctx, s, clientID, "5555")
			req := readStreamRequest(t, conn)
			client := dialStream(t, s, testClientStreamAddr, req.ID)
			if r := next(t, res); r.err != nil {
				t.Fatalf("RequestStream: %v", r.err)
			}

			tc.end(s, conn)
			assertClosed(t, done) // the session has been torn down

			assertRefused(t, client) // and the stream went with it
		})
	}
}

// tryPair connects from the fake client's address with the right PIN, claiming key k.
func tryPair(t *testing.T, s *Server, k key.NodePublic) protocol.CloseErr {
	t.Helper()
	server, client := net.Pipe()
	client.SetDeadline(time.Now().Add(2 * time.Second))

	finished := make(chan struct{})
	go func() {
		s.handleControl(addrConn{server, testClientAddr})
		close(finished)
	}()
	t.Cleanup(func() {
		client.Close()
		<-finished
	})

	hello := protocol.Hello{
		Version: protocol.Version,
		Key:     k.String(),
		Name:    testClientName,
		PIN:     s.PairingPIN(),
	}
	if err := protocol.WritePayload(client, protocol.KindHello, hello); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	f, err := protocol.ReadFrame(client)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if f.Kind == protocol.KindHelloAck {
		return protocol.CloseErrNone
	}
	var c protocol.Close
	if err := c.UnmarshalBinary(f.Payload); err != nil {
		t.Fatalf("reply was %v, not a HelloAck or Close: %v", f.Kind, err)
	}
	return c.Err
}

func TestServerHelloKeyMustBeThePeer(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	tunnelKey := key.NewNode().Public()

	if err := tryPair(t, s, tunnelKey); err != protocol.CloseErrInvalidKey {
		t.Fatalf("peer unknown to tailcat: %v, want invalid_key", err)
	}

	fake.addPeer(testClientAddr, tunnelKey)

	if err := tryPair(t, s, key.NewNode().Public()); err != protocol.CloseErrInvalidKey {
		t.Fatalf("claiming another key: %v, want invalid_key", err)
	}

	if err := tryPair(t, s, tunnelKey); err != protocol.CloseErrNone {
		t.Fatalf("the tunnel's own key: %v, want to pair", err)
	}
}

func TestServerAllowClient(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	stranger := key.NewNode().Public()

	if !s.allowClient(stranger) {
		t.Fatal("before pairing: a new client was refused")
	}

	_, clientID, _ := connectClient(t, s, fake)
	paired, _ := parseKey(clientID)

	if s.allowClient(stranger) {
		t.Fatal("after pairing: a stranger was allowed")
	}
	if !s.allowClient(*paired) {
		t.Fatal("after pairing: the paired client was refused")
	}
}

func TestServerStreamFromUnpairedPeer(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	connectClient(t, s, fake)
	fake.addPeer(testOtherHostAddr, key.NewNode().Public())
	assertRefused(t, dialStream(t, s, testOtherHostAddr, 1))
}
