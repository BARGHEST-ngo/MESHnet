package meshnet

import (
	"bytes"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
	"github.com/BARGHEST-ngo/MESHnet/internal/transport"
)

// Returns a Client on a pipe as if Connect has succeeded with the server on the other end.
// done is closed when the Client readloop exits
// register callbacks on Client via setup
func pipeClient(t *testing.T, setup func(c *Client)) (c *Client, server net.Conn, done <-chan struct{}) {
	t.Helper()
	serverEnd, clientEnd := net.Pipe()
	deadline := time.Now().Add(2 * time.Second)
	serverEnd.SetDeadline(deadline)
	clientEnd.SetDeadline(deadline)

	c = NewClient(ClientConfig{})
	if setup != nil {
		setup(c)
	}

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
	_, server, _ := pipeClient(t, func(c *Client) {
		c.OnMessage(func(m Message) { got <- m })
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

func TestOnMessageCanSendClient(t *testing.T) {
	_, server, _ := pipeClient(t, func(c *Client) {
		c.OnMessage(func(m Message) {
			if err := c.Send(m); err != nil {
				t.Errorf("send from callback: %v", err)
			}
		})
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

func TestDisconnectTwice(t *testing.T) {
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
			_, server, done := pipeClient(t, func(c *Client) {
				c.OnMessage(func(Message) { called.Store(true) })
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

func TestOnMessageNilClient(t *testing.T) {
	_, server, done := pipeClient(t, func(c *Client) { c.OnMessage(nil) })

	if err := protocol.WritePayload(server, protocol.KindMsg, protocol.Msg{Topic: "hello"}); err != nil {
		t.Fatalf("write msg: %v", err)
	}

	if err := protocol.WritePayload(server, protocol.KindClose, protocol.Close{}); err != nil {
		t.Fatalf("write close: %v", err)
	}

	assertClosed(t, done)
}

func TestOnMessageAfterConnect(t *testing.T) {
	c, _, _ := pipeClient(t, nil)
	defer func() {
		if recover() == nil {
			t.Fatal("OnMesage after connect did not panic")
		}
	}()
	c.OnMessage(func(m Message) {})
}
