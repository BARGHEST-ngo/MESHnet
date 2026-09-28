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

type ClientInfo struct{}

type Message struct {
	Topic string //app-defined key
	Data  []byte
}
