// Copyright (c) BARGHEST
// SPDX-License-Identifier: AGPL-3.0-or-later

package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
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

func TestWriteStreamID(t *testing.T) {
	cases := []struct {
		name string
		id   uint64
		want []byte
	}{
		{name: "one", id: 1, want: []byte{0, 0, 0, 0, 0, 0, 0, 1}},
		{name: "byte order", id: 0x0102030405060708, want: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
		{name: "max value", id: math.MaxUint64, want: bytes.Repeat([]byte{0xff}, 8)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b bytes.Buffer
			if err := WriteStreamID(&b, tc.id); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !bytes.Equal(b.Bytes(), tc.want) {
				t.Errorf("wrote % x, want % x", b.Bytes(), tc.want)
			}
		})
	}

	t.Run("writer error", func(t *testing.T) {
		want := errors.New("boom")
		if err := WriteStreamID(failWriter{err: want}, 1); !errors.Is(err, want) {
			t.Errorf("expected %v, got %v", want, err)
		}
	})
}

func TestReadStreamID(t *testing.T) {
	cases := []struct {
		name          string
		in            []byte
		want          uint64
		wantRemaining []byte
		err           error
	}{
		{name: "empty", in: nil, err: io.EOF},
		{name: "seven bytes", in: u64(1)[:7], err: io.ErrUnexpectedEOF},
		{name: "id 0", in: u64(0), err: ErrMalformedPayload},
		{name: "exact", in: u64(1), want: 1},
		{name: "max value", in: u64(math.MaxUint64), want: math.MaxUint64},
		{name: "leaves the stream data unread", in: join(u64(7), []byte("adb")), want: 7, wantRemaining: []byte("adb")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := bytes.NewReader(tc.in)
			got, err := ReadStreamID(r)
			if tc.err != nil {
				if !errors.Is(err, tc.err) {
					t.Errorf("expected %v, got %v", tc.err, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.want != got {
				t.Errorf("expected %d, got %d", tc.want, got)
			}

			remaining, _ := io.ReadAll(r)
			if !bytes.Equal(remaining, tc.wantRemaining) {
				t.Errorf("remaining = % x, want % x", remaining, tc.wantRemaining)
			}
		})
	}
}
