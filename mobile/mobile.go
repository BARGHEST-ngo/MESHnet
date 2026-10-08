package meshnetmobile

import (
	"context"
	"net"
	"time"

	meshnet "github.com/BARGHEST-ngo/MESHnet"
)

type Listener interface {
	OnMessage(topic string, data []byte)
	OnStateChange(state string, err string)
	OnStreamRequest(target string, stream Stream)
}
type noopListener struct{}

func (noopListener) OnMessage(topic string, data []byte)          {}
func (noopListener) OnStateChange(state string, err string)       {}
func (noopListener) OnStreamRequest(target string, stream Stream) { stream.Close() }

type Stream interface {
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close() error
}

const connectTimeout = 30 * time.Second

type Client struct {
	c *meshnet.Client
}

func clientConfig(serverAddr, pairingPIN, name string, l Listener) meshnet.ClientConfig {
	if l == nil {
		l = noopListener{}
	}

	return meshnet.ClientConfig{
		ServerAddr: serverAddr,
		PairingPIN: pairingPIN,
		Name:       name,
		OnMessage: func(m meshnet.Message) {
			l.OnMessage(m.Topic, m.Data)
		},
		OnStateChange: func(s meshnet.State, e error) {
			l.OnStateChange(s.String(), errString(e))
		},
		OnStreamRequest: func(target string, stream net.Conn) {
			l.OnStreamRequest(target, stream)
		},
	}
}

func NewClient(serverAddr, pairingPIN, name string, l Listener) *Client {
	return &Client{c: meshnet.NewClient(clientConfig(serverAddr, pairingPIN, name, l))}
}

func (c *Client) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	return c.c.Connect(ctx)
}

func (c *Client) Disconnect() error {
	return c.c.Disconnect()
}

func (c *Client) Send(topic string, data []byte) error {
	return c.c.Send(meshnet.Message{Topic: topic, Data: data})
}

func (c *Client) State() string {
	return c.c.State().String()
}

func (c *Client) ID() string {
	return c.c.ID()
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
