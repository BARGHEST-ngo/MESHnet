package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type Kind uint8

// 0 Kind is invalid
const (
	KindHello    Kind = iota + 1 // client > server: identify and pair
	KindHelloAck                 // server > client: handshake accepted
	KindMsg                      // application message
	KindPing                     // alive check
	KindPong                     // response to ping
	KindClose                    // closing, with a reason
)

func (k Kind) String() string {
	switch k {
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
	default:
		return fmt.Sprintf("[%d] unknown-kind", k)
	}
}

func (k Kind) valid() bool {
	return k >= KindHello && k <= KindClose
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
			return 0, 0, io.EOF
		}
		return 0, 0, fmt.Errorf("protocol: read header: %w", err)
	}

	length := binary.BigEndian.Uint32(header[:kindOffset])
	kind := Kind(header[kindOffset])
	if !kind.valid() {
		return 0, 0, fmt.Errorf("%w: kind %d", ErrUnknownKind, kind)
	}

	if length > MaxPayloadSize {
		return 0, 0, fmt.Errorf("%w: size %d", ErrFrameTooLarge, length)
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
	if !f.Kind.valid() {
		return fmt.Errorf("%w: kind %d", ErrUnknownKind, f.Kind)
	}

	payloadSize := len(f.Payload)
	if payloadSize > MaxPayloadSize {
		return fmt.Errorf("%w: size %d", ErrFrameTooLarge, payloadSize)
	}

	buf := make([]byte, headerSize+payloadSize)
	binary.BigEndian.PutUint32(buf[:kindOffset], uint32(payloadSize))
	buf[kindOffset] = byte(f.Kind)
	copy(buf[headerSize:], f.Payload)

	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("protocol: write frame: %w", err)
	}

	return nil
}
