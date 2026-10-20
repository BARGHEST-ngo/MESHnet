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
	mu           sync.Mutex
	peers        map[netip.Addr]key.NodePublic
	closeCalls   atomic.Int32
	connected    chan string // client IDs, signalled by newTestServer's OnClientConnected wrapper
	disconnected chan string
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

func (f *fakeTailcat) DisconnectClient(k key.NodePublic) bool {
	select {
	case f.disconnected <- k.String():
	default:
	}
	return true
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
		connected:    make(chan string, 4),
		disconnected: make(chan string, 4),
		peers:        make(map[netip.Addr]key.NodePublic),
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

func tryPair(t *testing.T, s *Server, k key.NodePublic) protocol.CloseErr {
	t.Helper()
	return tryPairPIN(t, s, k, s.PairingPIN())
}

// tryPairPIN connects from the fake client's address with the right PIN, claiming key k.
func tryPairPIN(t *testing.T, s *Server, k key.NodePublic, pin string) protocol.CloseErr {
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
		PIN:     pin,
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
	s, fake := newTestServer(t, nil)
	if _, err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { s.Stop() })
	stranger := key.NewNode().Public()

	if s.allowClient(stranger) {
		t.Fatal("no pairing pending: a stranger was allowed")
	}

	if _, err := s.NewPairing(); err != nil {
		t.Fatalf("NewPairing: %v", err)
	}
	if !s.allowClient(stranger) {
		t.Fatal("pairing pending: a new client was refused")
	}

	conn, clientID, done := connectClient(t, s, fake)
	paired, _ := parseKey(clientID)
	if s.allowClient(stranger) {
		t.Fatal("after pairing: a stranger was allowed")
	}
	if !s.allowClient(*paired) {
		t.Fatal("after pairing: the paired client was refused")
	}

	conn.Close()
	assertClosed(t, done)
	if s.allowClient(*paired) {
		t.Fatal("after its session ended: the client was still allowed")
	}
}

func TestServerStreamFromUnpairedPeer(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	connectClient(t, s, fake)
	fake.addPeer(testOtherHostAddr, key.NewNode().Public())
	assertRefused(t, dialStream(t, s, testOtherHostAddr, 1))
}

func TestServerNewPairing(t *testing.T) {
	s, _ := newTestServer(t, nil)

	if _, err := s.NewPairing(); !errors.Is(err, ErrClosed) {
		t.Fatalf("before Start: %v, want ErrClosed", err)
	}

	if _, err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := s.PairingPIN(); got != "" {
		t.Fatalf("PairingPIN before any pairing = %q, want empty", got)
	}

	pin, err := s.NewPairing()
	if err != nil {
		t.Fatalf("NewPairing: %v", err)
	}
	if len(pin) != 6 || strings.Trim(pin, "0123456789") != "" {
		t.Fatalf("PIN %q, want 6 digits", pin)
	}
	if got := s.PairingPIN(); got != pin {
		t.Fatalf("PairingPIN = %q, want %q", got, pin)
	}

	s.Stop()
	if got := s.PairingPIN(); got != "" {
		t.Fatalf("PairingPIN after Stop = %q, want empty", got)
	}
	if _, err := s.NewPairing(); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Stop: %v, want ErrClosed", err)
	}
}

// newPeer registers a new tunnel peer at the fake client's address and returns its key.
func newPeer(fake *fakeTailcat) key.NodePublic {
	k := key.NewNode().Public()
	fake.addPeer(testClientAddr, k)
	return k
}

// With no pairing pending the server pairs with nobody, whatever PIN is sent.
func TestServerNoPairingPending(t *testing.T) {
	s, fake := newTestServer(t, nil)
	if _, err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { s.Stop() })

	if got := tryPairPIN(t, s, newPeer(fake), "123456"); got != protocol.CloseErrPairingLocked {
		t.Fatalf("got %v, want pairing_locked", got)
	}
}

func TestServerPINSingleUse(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	pin := s.PairingPIN()

	if got := tryPairPIN(t, s, newPeer(fake), pin); got != protocol.CloseErrNone {
		t.Fatalf("first use: %v, want to pair", got)
	}
	if got := s.PairingPIN(); got != "" {
		t.Fatalf("PairingPIN after pairing = %q, want empty", got)
	}
	if got := tryPairPIN(t, s, newPeer(fake), pin); got != protocol.CloseErrPairingLocked {
		t.Fatalf("second use: %v, want pairing_locked", got)
	}
}

func TestServerWrongPINs(t *testing.T) {
	t.Run("fewer than the limit leave the PIN usable", func(t *testing.T) {
		s, fake := startNewTestServer(t)
		t.Cleanup(func() { s.Stop() })
		pin := s.PairingPIN()

		for i := range maxFailuresPerPIN - 1 {
			if got := tryPairPIN(t, s, newPeer(fake), "000000"); got != protocol.CloseErrWrongPIN {
				t.Fatalf("wrong PIN %d: %v, want wrong_pin", i, got)
			}
		}
		if got := tryPairPIN(t, s, newPeer(fake), pin); got != protocol.CloseErrNone {
			t.Fatalf("right PIN: %v, want to pair", got)
		}
	})

	t.Run("the limit burns the pairing", func(t *testing.T) {
		s, fake := startNewTestServer(t)
		t.Cleanup(func() { s.Stop() })
		pin := s.PairingPIN()

		for i := range maxFailuresPerPIN {
			if got := tryPairPIN(t, s, newPeer(fake), "000000"); got != protocol.CloseErrWrongPIN {
				t.Fatalf("wrong PIN %d: %v, want wrong_pin", i, got)
			}
		}
		if got := tryPairPIN(t, s, newPeer(fake), pin); got != protocol.CloseErrPairingLocked {
			t.Fatalf("right PIN after the limit: %v, want pairing_locked", got)
		}
		if got := s.PairingPIN(); got != "" {
			t.Fatalf("PairingPIN after the limit = %q, want empty", got)
		}

		// A new pairing starts again with a full set of attempts.
		fresh, err := s.NewPairing()
		if err != nil {
			t.Fatalf("NewPairing: %v", err)
		}
		if got := tryPairPIN(t, s, newPeer(fake), fresh); got != protocol.CloseErrNone {
			t.Fatalf("new pairing: %v, want to pair", got)
		}
	})
}

func TestServerPairingExpires(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	pin := s.PairingPIN()

	s.mu.Lock()
	s.now = func() time.Time { return time.Now().Add(pairingTTL + time.Second) }
	s.mu.Unlock()

	if got := s.PairingPIN(); got != "" {
		t.Fatalf("PairingPIN after expiry = %q, want empty", got)
	}
	if got := tryPairPIN(t, s, newPeer(fake), pin); got != protocol.CloseErrPairingLocked {
		t.Fatalf("expired PIN: %v, want pairing_locked", got)
	}
}

func TestServerReplaceAndCancelPairing(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })

	first := s.PairingPIN()
	second, err := s.NewPairing()
	for err == nil && second == first { // a one-in-a-million repeat
		second, err = s.NewPairing()
	}
	if err != nil {
		t.Fatalf("NewPairing: %v", err)
	}

	if got := tryPairPIN(t, s, newPeer(fake), first); got != protocol.CloseErrWrongPIN {
		t.Fatalf("replaced PIN: %v, want wrong_pin", got)
	}

	s.CancelPairing()
	if got := s.PairingPIN(); got != "" {
		t.Fatalf("PairingPIN after cancel = %q, want empty", got)
	}
	if got := tryPairPIN(t, s, newPeer(fake), second); got != protocol.CloseErrPairingLocked {
		t.Fatalf("cancelled PIN: %v, want pairing_locked", got)
	}
}

func TestServerNewPairingWhenFull(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	connectClient(t, s, fake)

	if _, err := s.NewPairing(); !errors.Is(err, ErrServerFull) {
		t.Fatalf("NewPairing with a client connected: %v, want ErrServerFull", err)
	}
}

// The point of pairings on demand: after a client leaves, a new one can pair
// on the same server run, with a new PIN and no restart.
func TestServerPairAgainWithoutRestart(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })

	conn, firstID, done := connectClient(t, s, fake)
	conn.Close()
	assertClosed(t, done) // the first session has ended

	if _, err := s.NewPairing(); err != nil {
		t.Fatalf("NewPairing after the client left: %v", err)
	}
	_, secondID, _ := connectClient(t, s, fake)
	if secondID == firstID {
		t.Fatal("the second client has the first client's ID")
	}
	if err := s.Send(secondID, Message{Topic: "status", Data: []byte("ok")}); err != nil {
		t.Fatalf("Send to the second client: %v", err)
	}
	if err := s.Send(firstID, Message{Topic: "status"}); !errors.Is(err, ErrUnknownClient) {
		t.Fatalf("Send to the first client: %v, want ErrUnknownClient", err)
	}
}

func TestServerSessionEndRevokesKey(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	conn, clientID, done := connectClient(t, s, fake)
	paired, _ := parseKey(clientID)

	conn.Close() // the client goes away
	assertClosed(t, done)

	if got := next(t, fake.disconnected); got != clientID {
		t.Fatalf("tailcat told to drop %s, want %s", got, clientID)
	}
	if s.allowClient(*paired) {
		t.Fatal("the revoked key is still allowed onto the tunnel")
	}
}

func TestServerDisconnectClient(t *testing.T) {
	var l *lifecycle
	s, fake := newTestServer(t, func(cfg *ServerConfig) { l = recordLifecycle(cfg) })
	startPairing(t, s)
	t.Cleanup(func() { s.Stop() })
	_, clientID, done := connectClient(t, s, fake)

	if err := s.DisconnectClient(clientID); err != nil {
		t.Fatalf("DisconnectClient: %v", err)
	}

	ev := next(t, l.disconnected)
	if ev.info.ID != clientID || !errors.Is(ev.err, ErrDisconnectedByServer) {
		t.Fatalf("disconnected %s with %v, want %s with ErrDisconnectedByServer", ev.info.ID, ev.err, clientID)
	}
	if got := next(t, fake.disconnected); got != clientID {
		t.Fatalf("tailcat told to drop %s, want %s", got, clientID)
	}
	assertClosed(t, done)
	if err := s.Send(clientID, Message{Topic: "status"}); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Send after DisconnectClient: %v, want ErrNotConnected", err)
	}
}

func TestServerDisconnectClientErrors(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })

	if err := s.DisconnectClient("nodekey:nobody"); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("no client connected: %v, want ErrNotConnected", err)
	}

	connectClient(t, s, fake)
	if err := s.DisconnectClient("nodekey:someone-else"); !errors.Is(err, ErrUnknownClient) {
		t.Fatalf("unknown client: %v, want ErrUnknownClient", err)
	}
}

func TestServerDisconnectClientFreesSlot(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	_, clientID, _ := connectClient(t, s, fake)

	if _, err := s.NewPairing(); !errors.Is(err, ErrServerFull) {
		t.Fatalf("NewPairing with a client connected: %v, want ErrServerFull", err)
	}
	if err := s.DisconnectClient(clientID); err != nil {
		t.Fatalf("DisconnectClient: %v", err)
	}
	if _, err := s.NewPairing(); err != nil {
		t.Fatalf("NewPairing straight after DisconnectClient: %v", err)
	}
}

func TestServerStopDoesNotRevokeKeys(t *testing.T) {
	var l *lifecycle
	s, fake := newTestServer(t, func(cfg *ServerConfig) { l = recordLifecycle(cfg) })
	startPairing(t, s)
	_, _, done := connectClient(t, s, fake)

	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertClosed(t, done)

	if ev := next(t, l.disconnected); !errors.Is(ev.err, ErrClosed) {
		t.Fatalf("disconnected with %v, want ErrClosed", ev.err)
	}
	none(t, fake.disconnected)
}

func TestServerListClients(t *testing.T) {
	t.Run("none connected", func(t *testing.T) {
		s, _ := newTestServer(t, nil)
		if len(s.Clients()) != 0 {
			t.Errorf("expected 0 clients, got %d", len(s.Clients()))
		}
	})

	t.Run("single client", func(t *testing.T) {
		s, fake := newTestServer(t, nil)
		startPairing(t, s)
		_, id, _ := connectClient(t, s, fake)

		if len(s.Clients()) != 1 {
			t.Fatalf("expected 1 client, got %d", len(s.Clients()))
		}

		if s.Clients()[0].ID != id {
			t.Errorf("expected id %q, got %q", id, s.Clients()[0].ID)
		}

		if err := s.DisconnectClient(id); err != nil {
			t.Fatalf("disconnect client: %v", err)
		}

		if len(s.Clients()) != 0 {
			t.Errorf("client still listed after disconnecting")
		}
	})
}

func TestServerPairingExhaustedAcrossPairings(t *testing.T) {
	s, fake := newTestServer(t, nil)
	if _, err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { s.Stop() })

	var pin string
	failures, perPIN := 0, 0
	for failures < maxPairingFailures {
		if perPIN == 0 {
			var err error
			if pin, err = s.NewPairing(); err != nil {
				t.Fatalf("NewPairing after %d failures: %v", failures, err)
			}
		}
		if got := tryPairPIN(t, s, newPeer(fake), "000000"); got != protocol.CloseErrWrongPIN {
			t.Fatalf("wrong PIN %d: %v, want wrong_pin", failures, got)
		}
		failures++
		perPIN = (perPIN + 1) % maxFailuresPerPIN
	}

	if err := tryPairPIN(t, s, newPeer(fake), pin); err != protocol.CloseErrPairingLocked {
		t.Fatalf("right PIN after the run's limit: %v, want pairing_locked", err)
	}
	if _, err := s.NewPairing(); !errors.Is(err, ErrPairingExhausted) {
		t.Fatalf("NewPairing after the run's limit: %v, want ErrPairingExhausted", err)
	}
}

func TestServerWrongKeysExhaustPairing(t *testing.T) {
	s, _ := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })

	for i := range maxPairingFailures {
		if err := tryPair(t, s, key.NewNode().Public()); err != protocol.CloseErrInvalidKey {
			t.Fatalf("wrong key %d: %v, want invalid_key", i, err)
		}
	}
	if _, err := s.NewPairing(); !errors.Is(err, ErrPairingExhausted) {
		t.Fatalf("NewPairing after forged keys: %v, want ErrPairingExhausted", err)
	}
}

func TestServerAllowedDestination(t *testing.T) {
	cases := []struct {
		addr    netip.AddrPort
		allowed bool
	}{
		{addr: netip.MustParseAddrPort("1.1.1.1:443"), allowed: true},
		{addr: netip.MustParseAddrPort("[2606:4700:4700::1111]:443"), allowed: true},
		{addr: netip.MustParseAddrPort("1.1.1.1:0"), allowed: false},
		{addr: netip.AddrPort{}, allowed: false},
		// loopback
		{addr: netip.MustParseAddrPort("127.0.0.1:80"), allowed: false},
		{addr: netip.MustParseAddrPort("[::1]:80"), allowed: false},
		// private
		{addr: netip.MustParseAddrPort("10.0.0.1:80"), allowed: false},
		{addr: netip.MustParseAddrPort("172.16.0.1:80"), allowed: false},
		{addr: netip.MustParseAddrPort("192.168.1.1:80"), allowed: false},
		{addr: netip.MustParseAddrPort("[fd7a:115c:a1e0::1]:80"), allowed: false}, // tailcat-assigned range
		// link-local, including the cloud metadata address
		{addr: netip.MustParseAddrPort("169.254.169.254:80"), allowed: false},
		{addr: netip.MustParseAddrPort("[fe80::1]:80"), allowed: false},
		// CGNAT and "this host"
		{addr: netip.MustParseAddrPort("100.64.0.1:80"), allowed: false},
		{addr: netip.MustParseAddrPort("0.1.2.3:80"), allowed: false},
		{addr: netip.MustParseAddrPort("0.0.0.0:80"), allowed: false},
		{addr: netip.MustParseAddrPort("[::]:80"), allowed: false},
		// multicast and broadcast
		{addr: netip.MustParseAddrPort("224.0.0.1:80"), allowed: false},
		{addr: netip.MustParseAddrPort("[ff02::1]:80"), allowed: false},
		{addr: netip.MustParseAddrPort("255.255.255.255:80"), allowed: false},
		// IPv4-mapped IPv6
		{addr: netip.MustParseAddrPort("[::ffff:1.1.1.1]:443"), allowed: true},
		{addr: netip.MustParseAddrPort("[::ffff:10.0.0.1]:80"), allowed: false},
		{addr: netip.MustParseAddrPort("[::ffff:100.64.0.1]:80"), allowed: false},
	}

	for _, tc := range cases {
		got := allowedDestination(tc.addr)
		if got != tc.allowed {
			t.Errorf("%v: %v, wanted %v", tc.addr, got, tc.allowed)
		}
	}
}

func TestServerForwardInstallsHandler(t *testing.T) {
	if NewServer(ServerConfig{}).buildTailcat().(*tailcat.Server).OnTCPForward != nil {
		t.Fatalf("OnTCPForward handler installed when it should not")
	}

	if NewServer(ServerConfig{Forward: true}).buildTailcat().(*tailcat.Server).OnTCPForward == nil {
		t.Fatalf("OnTCPForward handler not installed when it should")
	}
}

func TestServerForwardTCPRefusesDestination(t *testing.T) {
	cases := []struct {
		addr netip.AddrPort
		want bool
	}{
		{addr: netip.MustParseAddrPort("127.0.0.1:80"), want: false},
		{addr: netip.MustParseAddrPort("1.1.1.1:443"), want: true},
	}

	for _, tc := range cases {
		s := NewServer(ServerConfig{Forward: true})
		got := s.forwardTCP(tc.addr)
		if got == nil && tc.want {
			t.Errorf("%v: wanted handler, got none", tc.addr)
		} else if got != nil && !tc.want {
			t.Errorf("%v: got a handler, want nil", tc.addr)
		}
	}
}

func startForwardServer(t *testing.T, forward bool) (*Server, *fakeTailcat) {
	t.Helper()
	s, fake := newTestServer(t, func(cfg *ServerConfig) { cfg.Forward = forward })
	if _, err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	s.NewPairing()
	t.Cleanup(func() { s.Stop() })
	return s, fake
}

func TestServerSetForwardErrors(t *testing.T) {
	t.Run("ServerConfig.Forward not set", func(t *testing.T) {
		s, fake := startForwardServer(t, false)
		_, id, _ := connectClient(t, s, fake)
		t.Cleanup(func() { s.Stop() })

		if err := s.SetForwarding(id, true); !errors.Is(err, ErrForwardingDisabled) {
			t.Errorf("got %v, want ErrForwardingDisabled", err)
		}
	})

	t.Run("no connected clients", func(t *testing.T) {
		s, _ := startForwardServer(t, true)

		if err := s.SetForwarding("some-client-id", true); !errors.Is(err, ErrNotConnected) {
			t.Errorf("got %v, want ErrNotConnected", err)
		}
	})

	t.Run("wrong client ID", func(t *testing.T) {
		s, fake := startForwardServer(t, true)
		connectClient(t, s, fake)
		t.Cleanup(func() { s.Stop() })

		if err := s.SetForwarding("some-client-id", true); !errors.Is(err, ErrUnknownClient) {
			t.Errorf("got %v, want ErrUnknownClient", err)
		}
	})

	t.Run("set forward after stop", func(t *testing.T) {
		s, _ := startForwardServer(t, true)
		if err := s.Stop(); err != nil {
			t.Fatalf("stop: %v", err)
		}

		if err := s.SetForwarding("some-client-id", true); !errors.Is(err, ErrNotConnected) {
			t.Errorf("got %v, want ErrNotConnected", err)
		}
	})
}

// echoListener accepts connections on loopback and echoes back what it reads
func echoListener(t *testing.T) netip.AddrPort {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				io.Copy(c, c)
			}()
		}
	}()
	return netip.MustParseAddrPort(ln.Addr().String())
}

// forward runs handleForward for a connection from the test client to dst
func forward(t *testing.T, s *Server, dst netip.AddrPort) (client net.Conn, done <-chan struct{}) {
	t.Helper()
	server, client := net.Pipe()
	finished := make(chan struct{})
	go func() {
		s.handleForwardTCP(addrConn{server, testClientStreamAddr}, dst)
		close(finished)
	}()
	t.Cleanup(func() {
		client.Close()
		<-finished
	})
	return client, finished
}

// forwardingClient starts a forwarding server with one connected client that has forwarding enabled
func forwardingClient(t *testing.T) (s *Server, id string) {
	t.Helper()
	s, fake := startForwardServer(t, true)
	_, id, _ = connectClient(t, s, fake)
	if err := s.SetForwarding(id, true); err != nil {
		t.Fatalf("SetForwarding: %v", err)
	}
	return s, id
}

func echoOnce(t *testing.T, conn net.Conn) {
	t.Helper()
	want := []byte("hello through the exit node")
	if _, err := conn.Write(want); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("echo %q, want %q", got, want)
	}
}

// wantClosed checks the server closed the forwarded conn and its handler returned
func wantClosed(t *testing.T, client net.Conn, done <-chan struct{}) {
	t.Helper()
	client.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read: %v, want EOF", err)
	}
	assertClosed(t, done)
}

func TestServerForward(t *testing.T) {
	s, id := forwardingClient(t)
	s.mu.Lock()
	sess := s.sessions[id]
	s.mu.Unlock()

	client, done := forward(t, s, echoListener(t))
	echoOnce(t, client)
	client.Close()
	assertClosed(t, done)

	s.mu.Lock()
	n := len(sess.forwarded)
	s.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d forwarded conns left after the handler returned, want 0", n)
	}
}

func TestServerForwardingEnds(t *testing.T) {
	cases := map[string]func(s *Server, id string) error{
		"switched off": func(s *Server, id string) error { return s.SetForwarding(id, false) },
		"session ends": (*Server).DisconnectClient,
	}

	for name, end := range cases {
		t.Run(name, func(t *testing.T) {
			s, id := forwardingClient(t)
			dst := echoListener(t)
			client, done := forward(t, s, dst)
			echoOnce(t, client)

			if err := end(s, id); err != nil {
				t.Fatalf("end: %v", err)
			}
			wantClosed(t, client, done)

			client, done = forward(t, s, dst) // new forwards are refused
			wantClosed(t, client, done)
		})
	}
}
