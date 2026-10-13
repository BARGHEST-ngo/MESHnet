package meshnet

import "errors"

type State int

const (
	StateDisconnected State = iota
	StateConnecting
	StateConnected
	StateDisconnecting
	StateErrored
)

var (
	ErrClosed         = errors.New("meshnet: closed")
	ErrAlreadyStarted = errors.New("meshnet: already started")
	ErrNotConnected   = errors.New("meshnet: not connected")
	ErrUnknownClient  = errors.New("meshnet: unknown clientID")
	ErrSendQueueFull  = errors.New("meshnet: too many messages enqueued")
	ErrServerClosed   = errors.New("meshnet: server closed the connection")
	ErrServerFull     = errors.New("meshnet: maximum clients connected")
)

func (s State) String() string {
	switch s {
	case StateConnected:
		return "Connected"
	case StateConnecting:
		return "Connecting"
	case StateDisconnected:
		return "Disconnected"
	case StateDisconnecting:
		return "Disconnecting"
	case StateErrored:
		return "Errored"
	default:
		return "Unknown State"
	}
}

type ClientInfo struct {
	ID   string
	Name string
}

type Message struct {
	Topic string //app-defined key
	Data  []byte
}
