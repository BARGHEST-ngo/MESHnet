// Copyright (c) BARGHEST
// SPDX-License-Identifier: AGPL-3.0-or-later

package protocol

import (
	"bytes"
	"encoding"
	"errors"
	"math"
	"strings"
	"testing"
)

var testClientKey = "nodekey:" + strings.Repeat("ab", 32)

func maxString() string {
	return strings.Repeat("a", math.MaxUint8)
}

func TestAppendString(t *testing.T) {
	cases := []struct {
		name string
		in   string
		err  error
	}{
		{name: "empty string", in: "", err: nil},
		{name: "max length", in: maxString(), err: nil},
		{name: "too large", in: maxString() + "b", err: ErrMalformedPayload},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := appendString(nil, tc.in)
			if tc.err != nil {
				if !errors.Is(err, tc.err) {
					t.Errorf("expected %v, got %v", tc.err, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(b) != strPrefixSize+len(tc.in) {
				t.Errorf("expected len(b): %d, got %d", strPrefixSize+len(tc.in), len(b))
			}

			if b[0] != byte(len(tc.in)) {
				t.Errorf("expected prefix %d, got %d", byte(len(tc.in)), b[0])
			}

			if string(b[1:]) != tc.in {
				t.Errorf("expected %q, got %q", tc.in, string(b[1:]))
			}
		})
	}
}

func TestReadString(t *testing.T) {
	cases := []struct {
		name          string
		in            []byte
		want          string
		wantRemaining []byte
		err           error
	}{
		{name: "empty input", in: nil, err: ErrMalformedPayload},
		{name: "length byte only", in: []byte{0x03}, err: ErrMalformedPayload},
		{name: "length past end", in: []byte{0x05, 'n', 'o'}, err: ErrMalformedPayload},
		{name: "max length but short", in: append([]byte{0xff}, strings.Repeat("a", 254)...), err: ErrMalformedPayload},
		{name: "zero-length string", in: []byte{0x00}, want: ""},
		{name: "zero-length with remainder", in: []byte{0x00, 0xaa}, want: "", wantRemaining: []byte{0xaa}},
		{name: "simple", in: []byte{0x02, 'n', 'o'}, want: "no"},
		{name: "with remainder", in: []byte{0x02, 'n', 'o', 0x06, '1'}, want: "no", wantRemaining: []byte{0x06, '1'}},
		{name: "binary bytes", in: []byte{0x02, 0x00, 0xff}, want: "\x00\xff"},
		{name: "max length", in: append([]byte{0xff}, maxString()...), want: maxString()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, remaining, err := readString(tc.in)
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
				t.Errorf("expected %q, got %q", tc.want, got)
			}

			if !bytes.Equal(remaining, tc.wantRemaining) {
				t.Errorf("remaining = % x, want % x", remaining, tc.wantRemaining)
			}
		})
	}
}

func TestReadUint16(t *testing.T) {
	cases := []struct {
		name          string
		in            []byte
		want          uint16
		wantRemaining []byte
		err           error
	}{
		{name: "empty", in: nil, err: ErrMalformedPayload},
		{name: "single byte", in: []byte{0x00}, err: ErrMalformedPayload},
		{name: "exact", in: []byte{0x00, 0x01}, want: 1},
		{name: "max value", in: []byte{0xff, 0xff}, want: math.MaxUint16},
		{name: "remainder", in: []byte{0x00, 0x01, 0xaa}, want: 1, wantRemaining: []byte{0xaa}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, remaining, err := readUint16(tc.in)
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

			if !bytes.Equal(remaining, tc.wantRemaining) {
				t.Errorf("remaining = % x, want % x", remaining, tc.wantRemaining)
			}
		})
	}
}

func TestReadUint64(t *testing.T) {
	cases := []struct {
		name          string
		in            []byte
		want          uint64
		wantRemaining []byte
		err           error
	}{
		{name: "empty", in: nil, err: ErrMalformedPayload},
		{name: "seven bytes", in: make([]byte, 7), err: ErrMalformedPayload},
		{name: "exact", in: u64(1), want: 1},
		{name: "max value", in: u64(math.MaxUint64), want: math.MaxUint64},
		{name: "remainder", in: join(u64(1), []byte{0xaa}), want: 1, wantRemaining: []byte{0xaa}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, remaining, err := readUint64(tc.in)
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

			if !bytes.Equal(remaining, tc.wantRemaining) {
				t.Errorf("remaining = % x, want % x", remaining, tc.wantRemaining)
			}
		})
	}
}

func TestRoundTripClose(t *testing.T) {
	cases := []struct {
		name string
		in   Close
	}{
		{name: "normal close", in: Close{Err: CloseErrError, Message: "hello world"}},
		{name: "empty message", in: Close{Err: CloseErrNone}},
		{name: "max message", in: Close{Err: CloseErrError, Message: maxString()}},
		{name: "unknown error", in: Close{Err: CloseErr(200), Message: "unknown new error"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.in.MarshalBinary()
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			var decoded Close
			if err := decoded.UnmarshalBinary(b); err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			if decoded.Err != tc.in.Err {
				t.Errorf("expected %v, got %v", tc.in.Err, decoded.Err)
			}

			if decoded.Message != tc.in.Message {
				t.Errorf("expected %q, got %q", tc.in.Message, decoded.Message)
			}
		})
	}
}

func TestRoundTripHelloAck(t *testing.T) {
	cases := []struct {
		name string
		in   HelloAck
	}{
		{name: "normal ack", in: HelloAck{Version: 1, ClientID: testClientKey}},
		{name: "max version", in: HelloAck{Version: math.MaxUint16, ClientID: testClientKey}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.in.MarshalBinary()
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			var decoded HelloAck
			if err := decoded.UnmarshalBinary(b); err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			if decoded.Version != tc.in.Version {
				t.Errorf("expected %v, got %v", tc.in.Version, decoded.Version)
			}

			if decoded.ClientID != tc.in.ClientID {
				t.Errorf("expected %q, got %q", tc.in.ClientID, decoded.ClientID)
			}
		})
	}
}

func TestRoundTripHello(t *testing.T) {
	cases := []struct {
		name string
		in   Hello
	}{
		{name: "leading zero", in: Hello{Version: 1, Key: testClientKey, Name: "test name", PIN: "012345"}},
		{name: "empty name", in: Hello{Version: math.MaxUint16, Key: testClientKey, Name: "", PIN: "123456"}},
		{name: "max name", in: Hello{Version: 1, Key: testClientKey, Name: strings.Repeat("a", MaxNameLen), PIN: "123456"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.in.MarshalBinary()
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			var decoded Hello
			if err := decoded.UnmarshalBinary(b); err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			if decoded.Version != tc.in.Version {
				t.Errorf("expected %v, got %v", tc.in.Version, decoded.Version)
			}

			if decoded.Key != tc.in.Key {
				t.Errorf("expected %q, got %q", tc.in.Key, decoded.Key)
			}

			if decoded.Name != tc.in.Name {
				t.Errorf("expected %q, got %q", tc.in.Name, decoded.Name)
			}

			if decoded.PIN != tc.in.PIN {
				t.Errorf("expected %q, got %q", tc.in.PIN, decoded.PIN)
			}
		})
	}
}

func TestRoundTripMsg(t *testing.T) {
	cases := []struct {
		name string
		in   Msg
	}{
		{name: "no data", in: Msg{Topic: "topic", Data: nil}},
		{name: "text data", in: Msg{Topic: "topic", Data: []byte("hot")}},
		{name: "max topic length", in: Msg{Topic: maxString(), Data: nil}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.in.MarshalBinary()
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			var decoded Msg
			if err := decoded.UnmarshalBinary(b); err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			if decoded.Topic != tc.in.Topic {
				t.Errorf("expected %v, got %v", tc.in.Topic, decoded.Topic)
			}

			if !bytes.Equal(tc.in.Data, decoded.Data) {
				t.Errorf("expected %v, got %v", tc.in.Data, decoded.Data)
			}
		})
	}
}

func TestMalformedClose(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{name: "empty", in: nil},
		{name: "code only, no length", in: []byte{0x01}},
		{name: "length past end", in: []byte{0x01, 0x05, 'a'}},
		{name: "trailing bytes", in: []byte{0x01, 0x01, 'a', 'b'}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("%v", tc.in)
			var cl Close
			if err := cl.UnmarshalBinary(tc.in); err != nil {
				if !errors.Is(err, ErrMalformedPayload) {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				t.Errorf("expected %v, got nil", ErrMalformedPayload)
			}
		})
	}
}

// Returns s with u8 length prefix, if appendString changes this will tell us
func lenPrefixed(s string) []byte {
	len := byte(len(s))
	return append([]byte{len}, s...)
}

func join(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}

var v1 = []byte{0x00, 0x01} // uint16 version 1

func TestMalformedHello(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{name: "truncated after key", in: join(v1, lenPrefixed(testClientKey))},
		{name: "truncated after name", in: join(v1, lenPrefixed(testClientKey), lenPrefixed("n"))},
		{name: "version 0", in: join([]byte{0, 0}, lenPrefixed(testClientKey), lenPrefixed("n"), lenPrefixed("123456"))},
		{name: "invalid key", in: join(v1, lenPrefixed(testClientKey[:32]), lenPrefixed("n"), lenPrefixed("123456"))},
		{name: "pin too short", in: join(v1, lenPrefixed(testClientKey), lenPrefixed("n"), lenPrefixed("12345"))},
		{name: "pin too long", in: join(v1, lenPrefixed(testClientKey), lenPrefixed("n"), lenPrefixed("1234567"))},
		{name: "pin with letter", in: join(v1, lenPrefixed(testClientKey), lenPrefixed("n"), lenPrefixed("12a456"))},
		{name: "empty pin", in: join(v1, lenPrefixed(testClientKey), lenPrefixed("n"), lenPrefixed(""))},
		{name: "name too long", in: join(v1, lenPrefixed(testClientKey), lenPrefixed(strings.Repeat("n", MaxNameLen+1)), lenPrefixed("123456"))},
		{name: "trailing bytes", in: join(v1, lenPrefixed(testClientKey), lenPrefixed("n"), lenPrefixed("123456"), []byte{0xff})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var h Hello
			if err := h.UnmarshalBinary(tc.in); err != nil {
				if !errors.Is(err, ErrMalformedPayload) {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				t.Errorf("expected %v, got nil", ErrMalformedPayload)
			}
		})
	}
}

func TestMalformedHelloAck(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{name: "empty", in: nil},
		{name: "broken version", in: []byte{0x00}},
		{name: "version only", in: v1},
		{name: "version zero", in: join([]byte{0, 0}, lenPrefixed(testClientKey))},
		{name: "short key", in: join(v1, lenPrefixed(testClientKey[:32]))},
		{name: "invalid key prefix", in: join(v1, lenPrefixed("nodekez:"+testClientKey[8:]))},
		{name: "trailing bytes", in: join(v1, lenPrefixed(testClientKey), []byte{0xff})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var h HelloAck
			if err := h.UnmarshalBinary(tc.in); err != nil {
				if !errors.Is(err, ErrMalformedPayload) {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				t.Errorf("expected %v, got nil", ErrMalformedPayload)
			}
		})
	}
}

func TestMalformedMsg(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{name: "empty", in: nil},
		{name: "empty topic", in: join(lenPrefixed(""), []byte("data"))},
		{name: "topic length past end", in: []byte{0x05, 'a', 'b'}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m Msg
			if err := m.UnmarshalBinary(tc.in); err != nil {
				if !errors.Is(err, ErrMalformedPayload) {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				t.Errorf("expected %v, got nil", ErrMalformedPayload)
			}
		})
	}
}

func TestMarshal(t *testing.T) {
	cases := []struct {
		name  string
		value encoding.BinaryMarshaler
	}{
		{name: "close message too long", value: Close{Err: CloseErrError, Message: maxString() + "a"}},
		{name: "hello invalid PIN", value: Hello{Version: Version, Key: testClientKey, PIN: "12345"}},
		{name: "hello invalid key", value: Hello{Version: Version, Key: testClientKey, PIN: "123456", Name: strings.Repeat("a", MaxNameLen+1)}},
		{name: "helloAck invalid version", value: HelloAck{Version: 0, ClientID: testClientKey}},
		{name: "helloAck invalid key", value: HelloAck{Version: 1, ClientID: "nodekey:invalid-key"}},
		{name: "msg empty topic", value: Msg{Topic: ""}},
	}

	for _, tc := range cases {
		_, err := tc.value.MarshalBinary()
		if err != nil {
			if !errors.Is(err, ErrMalformedPayload) {
				t.Fatalf("unexpected error: %v", err)
			}
		} else {
			t.Errorf("expected %v, got nil", ErrMalformedPayload)
		}
	}
}
