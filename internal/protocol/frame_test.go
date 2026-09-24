package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"
)

func fullPayload() []byte {
	b := make([]byte, MaxPayloadSize)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func rawHeader(length uint32, kind Kind) []byte {
	b := binary.BigEndian.AppendUint32(nil, length)
	return append(b, byte(kind))
}

func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		frame Frame
	}{
		{
			name:  "KindHello with payload",
			frame: Frame{Kind: KindHello, Payload: []byte("abc")},
		},
		{
			name:  "KindHelloAck with payload",
			frame: Frame{Kind: KindHelloAck, Payload: []byte("abc")},
		},
		{
			name:  "KindMsg with payload",
			frame: Frame{Kind: KindMsg, Payload: []byte("abc")},
		},
		{
			name:  "KindPing with payload",
			frame: Frame{Kind: KindPing, Payload: []byte("abc")},
		},
		{
			name:  "KindPong with payload",
			frame: Frame{Kind: KindPong, Payload: []byte("abc")},
		},
		{
			name:  "KindClose with payload",
			frame: Frame{Kind: KindClose, Payload: []byte("abc")},
		},
		{
			name:  "MaxPayloadSize zero",
			frame: Frame{Kind: KindHello, Payload: make([]byte, MaxPayloadSize)},
		},
		{
			name:  "MaxPayloadSize data",
			frame: Frame{Kind: KindHello, Payload: fullPayload()},
		},
		{
			name:  "Empty payload",
			frame: Frame{Kind: KindHello, Payload: nil},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b bytes.Buffer
			if err := WriteFrame(&b, tc.frame); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}

			f, err := ReadFrame(&b)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}

			if f.Kind != tc.frame.Kind {
				t.Errorf("kind: %v, want %v", f.Kind, tc.frame.Kind)
			}

			if !bytes.Equal(tc.frame.Payload, f.Payload) {
				t.Errorf("payload: %q, want %q", f.Payload, tc.frame.Payload)
			}

			if b.Len() != 0 {
				t.Errorf("%d bytes left after read, want 0", b.Len())
			}
		})
	}
}

func TestReadFrameErrors(t *testing.T) {
	cases := []struct {
		name  string
		input []byte
		want  error
	}{
		{
			name:  "partial header",
			input: []byte{'a', 'b'},
			want:  io.ErrUnexpectedEOF,
		},
		{
			name:  "header with no payload",
			input: rawHeader(3, KindHello),
			want:  io.ErrUnexpectedEOF,
		},
		{
			name:  "partial payload",
			input: append(rawHeader(3, KindHello), 'a'),
			want:  io.ErrUnexpectedEOF,
		},
		{
			name:  "kind zero",
			input: rawHeader(0, KindInvalid),
			want:  ErrUnknownKind,
		},
		{
			name:  "kind undefined",
			input: rawHeader(0, 250),
			want:  ErrUnknownKind,
		},
		{
			name:  "payload too large",
			input: rawHeader(MaxPayloadSize+1, KindHello),
			want:  ErrFrameTooLarge,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(tc.input))
			if err == nil {
				t.Fatalf("expected %v, got nil err", err)
			}

			if !errors.Is(err, tc.want) {
				t.Errorf("wanted %v, got %v", tc.want, err)
			}
		})
	}
}

func TestWriteFrameErrors(t *testing.T) {
	cases := []struct {
		name  string
		frame Frame
		want  error
	}{
		{
			name:  "kind zero",
			frame: Frame{Kind: KindInvalid, Payload: nil},
			want:  ErrUnknownKind,
		},
		{
			name:  "kind undefined",
			frame: Frame{Kind: 150, Payload: nil},
			want:  ErrUnknownKind,
		},
		{
			name:  "payload too large",
			frame: Frame{Kind: KindHello, Payload: make([]byte, MaxPayloadSize+1)},
			want:  ErrFrameTooLarge,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b bytes.Buffer
			err := WriteFrame(&b, tc.frame)
			if err == nil {
				t.Fatalf("expected %v, got nil err", tc.want)
			}

			if !errors.Is(err, tc.want) {
				t.Errorf("wanted %v, got %v", tc.want, err)
			}
		})
	}
}

type failWriter struct {
	err error
}

func (w failWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func TestWriteFrameWriterError(t *testing.T) {
	writeErr := errors.New("some error happened")
	w := failWriter{err: writeErr}
	err := WriteFrame(w, Frame{Kind: KindHello, Payload: []byte("abc")})
	if err == nil {
		t.Fatal("WriteFrame: got nil error, want writer's error")
	}

	if !errors.Is(err, writeErr) {
		t.Errorf("WriteFrame error: %v, want to wrap %v", err, writeErr)
	}
}

func encodeFrame(f *testing.F, frame Frame) []byte {
	var b bytes.Buffer
	if err := WriteFrame(&b, frame); err != nil {
		f.Fatalf("WriteFrame: %v", err)
	}
	return b.Bytes()
}

func FuzzReadFrame(f *testing.F) {
	validFrames := []Frame{
		{Kind: KindHello, Payload: []byte{'a', 'b'}},
		{Kind: KindHello, Payload: nil},
		{Kind: KindClose, Payload: []byte{'a', 'b'}},
		{Kind: KindHello, Payload: []byte{0x00, 0xff, 0x00}},
	}

	byteSeeds := [][]byte{
		{},
		{0x00, 0x00}, // partial header
		rawHeader(3, KindMsg),
		append(rawHeader(3, KindMsg), 'a'), //partial payload
		rawHeader(0, KindInvalid),
		rawHeader(0, KindClose+1),
		rawHeader(0, 255),
		rawHeader(MaxPayloadSize, KindMsg),
		rawHeader(MaxPayloadSize+1, KindMsg),
		rawHeader(math.MaxUint32, KindMsg),
	}

	for _, frame := range validFrames {
		b := encodeFrame(f, frame)
		f.Add(b)
	}

	for _, b := range byteSeeds {
		f.Add(b)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		frame, err := ReadFrame(r)
		if err != nil {
			if frame.Kind != 0 {
				t.Fatalf("non-zero frame return with an error: %v", err)
			}
			return
		}

		if !frame.Kind.valid() {
			t.Errorf("invalid kind %d", frame.Kind)
		}

		if len(frame.Payload) > MaxPayloadSize {
			t.Errorf("payload size %d exceeds max allowed", len(frame.Payload))
		}

		consumed := data[:len(data)-r.Len()]
		var b bytes.Buffer
		if err := WriteFrame(&b, frame); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}

		if !bytes.Equal(b.Bytes(), consumed) {
			t.Fatalf("re-encoded % x, consumed % x", b.Bytes(), consumed)
		}

	})

}
