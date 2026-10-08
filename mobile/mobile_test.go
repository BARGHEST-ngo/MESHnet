package meshnetmobile

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	meshnet "github.com/BARGHEST-ngo/MESHnet"
)

type stateCall struct {
	state, err string
}

type messageCall struct {
	topic string
	data  []byte
}

type streamCall struct {
	target string
	stream Stream
}

type recorder struct {
	states   chan stateCall
	messages chan messageCall
	streams  chan streamCall
}

func newRecorder() *recorder {
	return &recorder{
		states:   make(chan stateCall, 8),
		messages: make(chan messageCall, 8),
		streams:  make(chan streamCall, 8),
	}
}

func (r *recorder) OnMessage(topic string, data []byte) {
	r.messages <- messageCall{topic: topic, data: data}
}

func (r *recorder) OnStateChange(state, err string) {
	r.states <- stateCall{state, err}
}

func (r *recorder) OnStreamRequest(target string, stream Stream) {
	r.streams <- streamCall{target, stream}
}

func nextState(t *testing.T, r *recorder) stateCall {
	t.Helper()
	select {
	case s := <-r.states:
		return s
	case <-time.After(time.Second):
		t.Fatal("OnStateChange not called")
		return stateCall{}
	}
}

func TestErrString(t *testing.T) {
	if got := errString(nil); got != "" {
		t.Errorf("errString(nil) = %q, want empty", got)
	}

	if got := errString(errors.New("luke")); got != "luke" {
		t.Errorf("errString(luke) = %q, want %q", got, "luke")
	}
}

func TestClientBeforeConnect(t *testing.T) {
	r := newRecorder()
	c := NewClient("not-an-address", "123456", "skywalker", r)

	if got := c.State(); got != "Disconnected" {
		t.Errorf("State() = %q, want Disconnected", got)
	}

	if got := c.ID(); got != "" {
		t.Errorf("ID() = %q want empty", got)
	}

	if err := c.Send("Darth", []byte("Vader")); !errors.Is(err, meshnet.ErrNotConnected) {
		t.Errorf("Send err = %v, want ErrNotConnected", err)
	}

	if err := c.Disconnect(); err != nil {
		t.Errorf("Disconnect err = %v, want nil", err)
	}

	select {
	case s := <-r.states:
		t.Fatalf("unexpected state change: %+v", s)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestClientConnectFailure(t *testing.T) {
	r := newRecorder()
	c := NewClient("not-an-address", "123456", "skywalker", r)

	if err := c.Connect(); err == nil {
		t.Fatal("Connect to bad address succeeded")
	}

	connectingState := stateCall{"Connecting", ""}
	if s := nextState(t, r); s != connectingState {
		t.Errorf("first state = %+v, want %+v", s, connectingState)
	}

	if s := nextState(t, r); s.state != "Errored" || s.err == "" {
		t.Errorf("second state %+v, want Errored", s)
	}

	select {
	case s := <-r.states:
		t.Fatalf("unexpected state change: %+v", s)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestClientConnectTwice(t *testing.T) {
	c := NewClient("not-an-address", "123456", "test", newRecorder())
	c.Connect()

	if err := c.Connect(); !errors.Is(err, meshnet.ErrClosed) {
		t.Fatalf("second Connect err = %v, want ErrClosed", err)
	}
}

func TestClientNilListener(t *testing.T) {
	c := NewClient("not-an-address", "123456", "test", nil)

	// Connect reports state changes; with a nil Listener these must not panic.
	if err := c.Connect(); err == nil {
		t.Fatal("Connect to a bad address succeeded")
	}
}

func TestClientConfigOnMessage(t *testing.T) {
	r := newRecorder()
	cfg := clientConfig("addr", "123456", "test", r)

	cfg.OnMessage(meshnet.Message{Topic: "status", Data: []byte{0x00, 0xff, 'x'}})

	select {
	case m := <-r.messages:
		if m.topic != "status" || !bytes.Equal(m.data, []byte{0x00, 0xff, 'x'}) {
			t.Errorf("got %q % x, want status 00 ff 78", m.topic, m.data)
		}
	default:
		t.Fatal("Listener.OnMessage not called")
	}
}

func TestClientConfigOnStateChange(t *testing.T) {
	r := newRecorder()
	cfg := clientConfig("addr", "123456", "test", r)

	cfg.OnStateChange(meshnet.StateConnected, nil)
	cfg.OnStateChange(meshnet.StateErrored, errors.New("boom"))

	if s := nextState(t, r); s != (stateCall{"Connected", ""}) {
		t.Errorf("first state %+v, want {Connected, \"\"}", s)
	}
	if s := nextState(t, r); s != (stateCall{"Errored", "boom"}) {
		t.Errorf("second state %+v, want {Errored, boom}", s)
	}
}

func TestClientConfigOnStreamRequest(t *testing.T) {
	r := newRecorder()
	cfg := clientConfig("addr", "123456", "test", r)

	ours, theirs := net.Pipe()
	defer ours.Close()
	theirs.SetDeadline(time.Now().Add(time.Second))
	ours.SetDeadline(time.Now().Add(time.Second))

	cfg.OnStreamRequest("Alderaan", theirs)
	var call streamCall
	select {
	case call = <-r.streams:
	default:
		t.Fatal("Listener.OnStreamRequest not called")
	}
	defer call.stream.Close()

	if call.target != "Alderaan" {
		t.Errorf("target %q, want Alderaan", call.target)
	}

	go ours.Write([]byte("hello"))
	buf := make([]byte, len("hello"))
	if _, err := io.ReadFull(call.stream, buf); err != nil || string(buf) != "hello" {
		t.Errorf("read %q: %v, want 'hello'", buf, err)
	}
}

func TestClientConfigNilOnStreamRequest(t *testing.T) {
	cfg := clientConfig("addr", "123456", "test", nil)

	ours, theirs := net.Pipe()
	defer ours.Close()
	ours.SetDeadline(time.Now().Add(time.Second))

	cfg.OnStreamRequest("Alderaan", theirs)
	if _, err := ours.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read: %v, want EOF (stream closed)", err)
	}
}

func readN(t *testing.T, r io.Reader, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(b)
}

func TestProxyCopiesBothWays(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ourEnd, theirEnd := net.Pipe()
	defer ourEnd.Close()
	ourEnd.SetDeadline(time.Now().Add(2 * time.Second))

	done := make(chan error, 1)
	go func() {
		done <- Proxy(theirEnd, ln.Addr().String())
	}()

	local, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer local.Close()
	local.SetDeadline(time.Now().Add(2 * time.Second))

	go ourEnd.Write([]byte("up"))
	if got := readN(t, local, 2); got != "up" {
		t.Fatalf("local got %q, want up", got)
	}

	go local.Write([]byte("down"))
	if got := readN(t, ourEnd, 4); got != "down" {
		t.Fatalf("stream got %q, want down", got)
	}

	// Proxy returns once both directions have finished.
	ourEnd.Close()
	local.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Proxy: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Proxy did not return")
	}
}

func TestProxyDialFails(t *testing.T) {
	ourEnd, theirEnd := net.Pipe()
	defer ourEnd.Close()
	ourEnd.SetDeadline(time.Now().Add(2 * time.Second))

	if err := Proxy(theirEnd, "127.0.0.1:0"); err == nil {
		t.Fatal("Proxy to a closed port succeeded")
	}
	if _, err := ourEnd.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read: %v, want EOF (stream closed)", err)
	}
}
