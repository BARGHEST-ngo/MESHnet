package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

// Returns id as the 8 big-endian bytes StreamRequest puts on the wire
func u64(id uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, id)
}

func TestRoundTripStreamRequest(t *testing.T) {
	cases := []struct {
		name string
		in   StreamRequest
	}{
		{name: "port target", in: StreamRequest{ID: 1, Target: "5555"}},
		{name: "max id", in: StreamRequest{ID: math.MaxUint64, Target: "adb"}},
		{name: "max target length", in: StreamRequest{ID: 2, Target: maxString()}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.in.MarshalBinary()
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			var decoded StreamRequest
			if err := decoded.UnmarshalBinary(b); err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			if decoded != tc.in {
				t.Errorf("expected %+v, got %+v", tc.in, decoded)
			}
		})
	}
}

func TestMalformedStreamRequest(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{name: "empty", in: nil},
		{name: "truncated id", in: u64(1)[:7]},
		{name: "id only, no target length", in: u64(1)},
		{name: "target length past end", in: join(u64(1), []byte{0x05, 'a'})},
		{name: "trailing bytes", in: join(u64(1), lenPrefixed("a"), []byte{'b'})},
		{name: "id 0", in: join(u64(0), lenPrefixed("5555"))},
		{name: "empty target", in: join(u64(1), lenPrefixed(""))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r StreamRequest
			if err := r.UnmarshalBinary(tc.in); err != nil {
				if !errors.Is(err, ErrMalformedPayload) {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				t.Errorf("expected %v, got nil", ErrMalformedPayload)
			}

			if r != (StreamRequest{}) {
				t.Errorf("receiver changed to %+v after a failed decode", r)
			}
		})
	}
}

func TestMarshalStreamRequestErrors(t *testing.T) {
	cases := []struct {
		name string
		in   StreamRequest
	}{
		{name: "id 0", in: StreamRequest{ID: 0, Target: "5555"}},
		{name: "empty target", in: StreamRequest{ID: 1, Target: ""}},
		{name: "target too long", in: StreamRequest{ID: 1, Target: maxString() + "a"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.in.MarshalBinary(); err != nil {
				if !errors.Is(err, ErrMalformedPayload) {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				t.Errorf("expected %v, got nil", ErrMalformedPayload)
			}
		})
	}
}

func TestKindStreamRequest(t *testing.T) {
	if !KindStreamRequest.valid() {
		t.Errorf("KindStreamRequest is not valid")
	}

	if kindEnd.valid() {
		t.Errorf("kindEnd is valid, want it one past the last valid kind")
	}

	if got := KindStreamRequest.String(); got != "stream-request" {
		t.Errorf("expected stream-request, got %v", got)
	}

	payload, err := StreamRequest{ID: 7, Target: "5555"}.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	var b bytes.Buffer
	if err := WriteFrame(&b, Frame{Kind: KindStreamRequest, Payload: payload}); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	f, err := ReadFrame(&b)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}

	if f.Kind != KindStreamRequest || !bytes.Equal(f.Payload, payload) {
		t.Errorf("got %v % x, want %v % x", f.Kind, f.Payload, KindStreamRequest, payload)
	}

	// The first kind past the end is still rejected
	if _, err := ReadFrame(bytes.NewReader(rawHeader(0, kindEnd))); !errors.Is(err, ErrUnknownKind) {
		t.Errorf("expected %v, got %v", ErrUnknownKind, err)
	}
}
