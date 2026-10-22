// Copyright (c) BARGHEST
// SPDX-License-Identifier: AGPL-3.0-or-later

package protocol

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
)

const (
	Version     uint16 = 1
	ControlPort uint16 = 1
	StreamPort  uint16 = 2

	uint16Size    = 2
	uint64Size    = 8
	strPrefixSize = 1
)

var (
	ErrMalformedPayload = errors.New("protocol: malformed payload")
)

func WritePayload(conn net.Conn, k Kind, p encoding.BinaryMarshaler) error {
	payload, err := p.MarshalBinary()
	if err != nil {
		return err
	}

	return WriteFrame(conn, Frame{
		Kind:    k,
		Payload: payload,
	})
}

// Appends one uint8 length prefixed string to b
func appendString(b []byte, s string) ([]byte, error) {
	if len(s) > math.MaxUint8 {
		return b, fmt.Errorf("%w: string length %d exceeds %d", ErrMalformedPayload, len(s), math.MaxUint8)
	}

	b = append(b, byte(len(s)))
	b = append(b, s...)
	return b, nil
}

// Reads one uint8 length prefixed string from the front of b and returns it with the unread bytes
func readString(b []byte) (string, []byte, error) {
	if len(b) == 0 {
		return "", nil, fmt.Errorf("%w: missing string length", ErrMalformedPayload)
	}

	n := int(b[0])
	remaining := b[strPrefixSize:]
	if len(remaining) < n {
		return "", nil, fmt.Errorf("%w: string length %d exceeds %d remaining bytes", ErrMalformedPayload, n, len(remaining))
	}

	return string(remaining[:n]), remaining[n:], nil
}

func readUint16(b []byte) (uint16, []byte, error) {
	if len(b) < uint16Size {
		return 0, nil, fmt.Errorf("%w: need %d bytes for uint16, got %d", ErrMalformedPayload, uint16Size, len(b))
	}
	return binary.BigEndian.Uint16(b), b[uint16Size:], nil
}

func readUint64(b []byte) (uint64, []byte, error) {
	if len(b) < uint64Size {
		return 0, nil, fmt.Errorf("%w: need %d bytes for uint64, got %d", ErrMalformedPayload, uint64Size, len(b))
	}
	return binary.BigEndian.Uint64(b), b[uint64Size:], nil
}
