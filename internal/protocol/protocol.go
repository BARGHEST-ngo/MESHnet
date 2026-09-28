package protocol

import (
	"encoding"
	"errors"
	"net"
)

const (
	Version     uint16 = 1
	ControlPort uint16 = 1
)

var (
	ErrMalformedPayload = errors.New("protocol: malformed payload")
)

func WritePayload(conn net.Conn, k Kind, p encoding.BinaryMarshaler) error {
	payload, err := p.MarshalBinary()
	if err != nil {
		return err
	}

	return writeFrame(conn, Frame{
		Kind:    k,
		Payload: payload,
	})
}
