package meshnet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
	"github.com/BARGHEST-ngo/MESHnet/internal/transport"
)

// gvisorTimeout mimics the error tailcat's connections return on a deadline:
// it reports Timeout() but is not os.ErrDeadlineExceeded.
type gvisorTimeout struct{}

func (gvisorTimeout) Error() string   { return "i/o timeout" }
func (gvisorTimeout) Timeout() bool   { return true }
func (gvisorTimeout) Temporary() bool { return true }

func TestTimeoutErr(t *testing.T) {
	gv := &net.OpError{Op: "read", Net: "tcp", Err: gvisorTimeout{}}
	cases := []struct {
		name string
		err  error
		want error
	}{
		{"std deadline", os.ErrDeadlineExceeded, ErrPeerTimeout},
		{"gvisor deadline", gv, ErrPeerTimeout},
		{"wrapped gvisor deadline", fmt.Errorf("protocol: read header: %w", gv), ErrPeerTimeout},
		{"eof", io.EOF, io.EOF},
		{"closed", net.ErrClosed, net.ErrClosed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := timeoutErr(tc.err); !errors.Is(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
	if got := timeoutErr(nil); got != nil {
		t.Fatalf("nil: got %v", got)
	}
}

func TestDeadlineReaderTimesOut(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	timeout := 50 * time.Millisecond
	r := deadlineReader{conn: a, timeout: timeout}

	start := time.Now()
	_, err := r.Read(make([]byte, 1)) // nothing is ever written
	if !errors.Is(timeoutErr(err), ErrPeerTimeout) {
		t.Fatalf("Read err %v, want a timeout", err)
	}
	if elapsed := time.Since(start); elapsed < timeout {
		t.Fatalf("timed out after %v, before the %v timeout", elapsed, timeout)
	}
}

// Bytes that keep arriving must hold the read open, even when the whole read
// takes longer than the timeout.
func TestDeadlineReaderSlowTrickle(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	const n = 8
	gap := 40 * time.Millisecond
	r := deadlineReader{conn: a, timeout: 200 * time.Millisecond}

	go func() {
		for i := range n {
			time.Sleep(gap)
			if _, err := b.Write([]byte{byte(i)}); err != nil {
				return
			}
		}
	}()

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("ReadFull over a slow link: %v", err)
	}
}

// liveServer starts a test server with a short liveness timeout. NewServer
// raises anything under its minimum, so the timeout is set afterwards.
func liveServer(t *testing.T, timeout time.Duration) (*Server, *fakeTailcat, *lifecycle) {
	t.Helper()
	var l *lifecycle
	s, fake := newTestServer(t, func(cfg *ServerConfig) { l = recordLifecycle(cfg) })
	s.cfg.LivenessTimeout = timeout
	if _, err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	s.NewPairing()
	t.Cleanup(func() { s.Stop() })
	return s, fake, l
}

func TestServerPeerTimeout(t *testing.T) {
	s, fake, l := liveServer(t, 50*time.Millisecond)

	connectClient(t, s, fake) // then say nothing

	ev := next(t, l.disconnected)
	if !errors.Is(ev.err, ErrPeerTimeout) {
		t.Fatalf("disconnected with %v, want ErrPeerTimeout", ev.err)
	}
}

func TestServerPingGetsPong(t *testing.T) {
	s, fake := startNewTestServer(t)
	t.Cleanup(func() { s.Stop() })
	conn, _, _ := connectClient(t, s, fake)

	if err := protocol.WriteFrame(conn, protocol.Frame{Kind: protocol.KindPing}); err != nil {
		t.Fatalf("write ping: %v", err)
	}
	f, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read pong: %v", err)
	}
	if f.Kind != protocol.KindPong || len(f.Payload) != 0 {
		t.Fatalf("got %v with %d payload bytes, want an empty Pong", f.Kind, len(f.Payload))
	}
}

func TestServerPingsKeepSessionAlive(t *testing.T) {
	timeout := 200 * time.Millisecond
	s, fake, l := liveServer(t, timeout)
	conn, _, _ := connectClient(t, s, fake)

	// Ping for well over the timeout.
	for start := time.Now(); time.Since(start) < 2*timeout; {
		if err := protocol.WriteFrame(conn, protocol.Frame{Kind: protocol.KindPing}); err != nil {
			t.Fatalf("write ping: %v", err)
		}
		if _, err := protocol.ReadFrame(conn); err != nil {
			t.Fatalf("read pong: %v", err)
		}
		time.Sleep(timeout / 5)
	}

	select {
	case ev := <-l.disconnected:
		t.Fatalf("session ended while pinging: %v", ev.err)
	default:
	}
}

func TestServerBadLivenessFrames(t *testing.T) {
	cases := []struct {
		name  string
		frame protocol.Frame
		want  error // nil: any error that is not a timeout
	}{
		{"ping with payload", protocol.Frame{Kind: protocol.KindPing, Payload: []byte{1}}, protocol.ErrMalformedPayload},
		{"pong", protocol.Frame{Kind: protocol.KindPong}, nil}, // the server never pings
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, fake, l := liveServer(t, time.Minute)
			conn, _, _ := connectClient(t, s, fake)

			if err := protocol.WriteFrame(conn, tc.frame); err != nil {
				t.Fatalf("write: %v", err)
			}

			ev := next(t, l.disconnected)
			switch {
			case ev.err == nil || errors.Is(ev.err, ErrPeerTimeout):
				t.Fatalf("disconnected with %v, want a protocol error", ev.err)
			case tc.want != nil && !errors.Is(ev.err, tc.want):
				t.Fatalf("disconnected with %v, want %v", ev.err, tc.want)
			}
		})
	}
}

func TestServerLivenessTimeoutConfig(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"unset uses the default", 0, defaultLivenessTimeout},
		{"too low is raised", 5 * time.Second, 2 * pingInterval},
		{"negative is raised", -time.Second, 2 * pingInterval},
		{"minimum is kept", 2 * pingInterval, 2 * pingInterval},
		{"larger is kept", 2 * time.Minute, 2 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewServer(ServerConfig{LivenessTimeout: tc.in})
			if got := s.cfg.LivenessTimeout; got != tc.want {
				t.Fatalf("LivenessTimeout %v, want %v", got, tc.want)
			}
		})
	}
}

// connectedLive is connected with the liveness timings set before Connect.
func connectedLive(t *testing.T, interval, timeout time.Duration, setup func(cfg *ClientConfig)) (*Client, net.Conn, chan stateEvent) {
	t.Helper()
	c, _, server, states := fakeConnect(t, setup)
	c.pingInterval = interval
	c.livenessTimeout = timeout
	goServer(t, server, func() { acceptHello(t, server) })
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	wantStates(t, states, StateConnecting, StateConnected)
	return c, server, states
}

func TestClientPeerTimeout(t *testing.T) {
	// A long interval keeps pings out of the way; the server never answers.
	_, _, states := connectedLive(t, time.Hour, 50*time.Millisecond, nil)

	evs := wantStates(t, states, StateErrored)
	if !errors.Is(evs[0].err, ErrPeerTimeout) {
		t.Fatalf("Errored with %v, want ErrPeerTimeout", evs[0].err)
	}
	none(t, states)
}

func TestClientSendsPings(t *testing.T) {
	_, server, _ := connectedLive(t, 20*time.Millisecond, time.Minute, nil)

	for i := range 3 {
		f, err := protocol.ReadFrame(server)
		if err != nil {
			t.Fatalf("read ping %d: %v", i, err)
		}
		if f.Kind != protocol.KindPing || len(f.Payload) != 0 {
			t.Fatalf("frame %d: %v with %d payload bytes, want an empty Ping", i, f.Kind, len(f.Payload))
		}
	}
}

func TestClientPongsKeepConnectionAlive(t *testing.T) {
	timeout := 200 * time.Millisecond
	_, server, states := connectedLive(t, timeout/10, timeout, nil)

	// Answer every ping until the connection closes.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			f, err := protocol.ReadFrame(server)
			if err != nil {
				return
			}
			if f.Kind != protocol.KindPing {
				continue
			}
			if err := protocol.WriteFrame(server, protocol.Frame{Kind: protocol.KindPong}); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		server.Close()
		<-done
	})

	select {
	case ev := <-states:
		t.Fatalf("state %v (%v) while pongs were flowing", ev.state, ev.err)
	case <-time.After(2 * timeout):
	}
}

func TestClientBadLivenessFrames(t *testing.T) {
	cases := []struct {
		name  string
		frame protocol.Frame
		want  error // nil: any error that is not a timeout
	}{
		{"pong with payload", protocol.Frame{Kind: protocol.KindPong, Payload: []byte{1}}, protocol.ErrMalformedPayload},
		{"ping", protocol.Frame{Kind: protocol.KindPing}, nil}, // the server never pings
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, server, states := connectedLive(t, time.Hour, time.Minute, nil)

			if err := protocol.WriteFrame(server, tc.frame); err != nil {
				t.Fatalf("write: %v", err)
			}

			err := wantStates(t, states, StateErrored)[0].err
			switch {
			case err == nil || errors.Is(err, ErrPeerTimeout):
				t.Fatalf("Errored with %v, want a protocol error", err)
			case tc.want != nil && !errors.Is(err, tc.want):
				t.Fatalf("Errored with %v, want %v", err, tc.want)
			}
		})
	}
}

// Disconnect from inside the Connected callback used to hand a nil writer to the pinger and panic.
func TestClientDisconnectInConnectedCallback(t *testing.T) {
	var c *Client
	c, _, server, states := fakeConnect(t, func(cfg *ClientConfig) {
		cfg.OnStateChange = func(s State, _ error) {
			if s == StateConnected {
				c.Disconnect()
			}
		}
	})
	goServer(t, server, func() { acceptHello(t, server) })

	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	drain(states)
	if got := c.State(); got != StateDisconnected {
		t.Fatalf("state %v, want Disconnected", got)
	}
}

func TestClientPingLoopStops(t *testing.T) {
	c := NewClient(ClientConfig{})
	c.pingInterval = time.Hour
	w := transport.NewWriter(io.Discard)

	done := make(chan struct{})
	go func() {
		c.pingLoop(w)
		close(done)
	}()

	assertOpen(t, done)
	w.Close()
	assertClosed(t, done)
}
