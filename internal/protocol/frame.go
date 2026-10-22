// Copyright (c) BARGHEST
// SPDX-License-Identifier: AGPL-3.0-or-later

package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type Kind uint8

const (
	KindInvalid       Kind = iota
	KindHello              // client > server: identify and pair
	KindHelloAck           // server > client: handshake accepted
	KindMsg                // application message
	KindPing               // alive check
	KindPong               // response to ping
	KindClose              // closing, with a reason
	KindStreamRequest      // request a stream

	kindEnd
)

func (k Kind) String() string {
	switch k {
	case KindInvalid:
		return "invalid"
	case KindHello:
		return "hello"
	case KindHelloAck:
		return "hello-ack"
	case KindMsg:
		return "msg"
	case KindPing:
		return "ping"
	case KindPong:
		return "pong"
	case KindClose:
		return "close"
	case KindStreamRequest:
		return "stream-request"
	default:
		return fmt.Sprintf("[%d] unknown-kind", k)
	}
}

func (k Kind) valid() bool {
	return k >= KindHello && k < kindEnd
}

const (
	kindOffset = 4
	headerSize = kindOffset + 1

	MaxPayloadSize = 1 << 20 // 1MiB
)

var (
	// 0 or not defined
	ErrUnknownKind = errors.New("protocol: unknown frame kind")
	// Exceeds MaxPayloadSize
	ErrFrameTooLarge = errors.New("protocol: frame too large")
)

type Frame struct {
	Kind    Kind
	Payload []byte
}

// Reads a single frame
// Returns io.EOF when the stream ends cleanly between frames
// Returns io.ErrUnexpectedEOF if stream ends partway through header or payload
// After any error the reader is no longer positioned at a boundary and connection should be closed
func ReadFrame(r io.Reader) (Frame, error) {
	kind, length, err := readHeader(r)
	if err != nil {
		return Frame{}, err
	}

	payload, err := readPayload(r, length)
	if err != nil {
		return Frame{}, err
	}

	return Frame{
		Kind:    kind,
		Payload: payload,
	}, nil
}

func readHeader(r io.Reader) (Kind, uint32, error) {
	var header [headerSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if err == io.EOF {
			return KindInvalid, 0, io.EOF
		}
		return KindInvalid, 0, fmt.Errorf("protocol: read header: %w", err)
	}

	length := binary.BigEndian.Uint32(header[:kindOffset])
	kind := Kind(header[kindOffset])
	if !kind.valid() {
		return KindInvalid, 0, fmt.Errorf("%w: kind %d", ErrUnknownKind, kind)
	}

	if length > MaxPayloadSize {
		return KindInvalid, 0, fmt.Errorf("%w: size %d", ErrFrameTooLarge, length)
	}
	return kind, length, nil
}

func readPayload(r io.Reader, length uint32) ([]byte, error) {
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("protocol: read payload: %w", err)
	}
	return payload, nil
}

func WriteFrame(w io.Writer, f Frame) error {
	if err := f.Validate(); err != nil {
		return err
	}

	buf := make([]byte, headerSize+len(f.Payload))
	binary.BigEndian.PutUint32(buf[:kindOffset], uint32(len(f.Payload)))
	buf[kindOffset] = byte(f.Kind)
	copy(buf[headerSize:], f.Payload)

	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("protocol: write frame: %w", err)
	}

	return nil
}

func (f Frame) Validate() error {
	if !f.Kind.valid() {
		return fmt.Errorf("%w: kind %d", ErrUnknownKind, f.Kind)
	}

	if len(f.Payload) > MaxPayloadSize {
		return fmt.Errorf("%w: size %d", ErrFrameTooLarge, len(f.Payload))
	}
	return nil
}
