package meshnetmobile

import (
	"errors"
	"testing"
	"time"

	meshnet "github.com/BARGHEST-ngo/MESHnet"
)

type stateCall struct{ state, err string }

type recorder struct {
	states   chan stateCall
	messages chan string
}

func newRecorder() *recorder {
	return &recorder{
		states:   make(chan stateCall, 8),
		messages: make(chan string, 8),
	}
}

func (r *recorder) OnMessage(topic string, data []byte) {
	r.messages <- topic + ":" + string(data)
}

func (r *recorder) OnStateChange(state, err string) {
	r.states <- stateCall{state, err}
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
