// Copyright (c) BARGHEST
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshnet

import (
	"errors"
	"net"
	"time"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
	"github.com/BARGHEST-ngo/MESHnet/internal/transport"
)

type State int

const (
	StateDisconnected State = iota
	StateConnecting
	StateConnected
	StateDisconnecting
	StateErrored
)

var (
	ErrClosed               = errors.New("meshnet: closed")
	ErrAlreadyStarted       = errors.New("meshnet: already started")
	ErrNotConnected         = errors.New("meshnet: not connected")
	ErrUnknownClient        = errors.New("meshnet: unknown clientID")
	ErrSendQueueFull        = errors.New("meshnet: too many messages enqueued")
	ErrServerClosed         = errors.New("meshnet: server closed the connection")
	ErrServerFull           = errors.New("meshnet: maximum clients connected")
	ErrDisconnectedByServer = errors.New("meshnet: client removed by server")
	ErrPairingExhausted     = errors.New("meshnet: too many failed pairing attempts, restart the server")
	ErrForwardingDisabled   = errors.New("meshnet: forwarding is disabled")
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

type PacketConn interface {
	net.Conn
	net.PacketConn
}

var closeTimeout = 2 * time.Second

func sendClose(conn net.Conn, w *transport.Writer) error {
	payload, err := protocol.Close{}.MarshalBinary()
	if err != nil {
		return err
	}

	conn.SetWriteDeadline(time.Now().Add(closeTimeout))
	return w.Finish(protocol.Frame{Kind: protocol.KindClose, Payload: payload})
}
