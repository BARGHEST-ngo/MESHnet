package protocol

import (
	"bytes"
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
