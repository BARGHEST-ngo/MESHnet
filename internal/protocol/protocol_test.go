package protocol

import (
	"errors"
	"math"
	"strings"
	"testing"
)

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
