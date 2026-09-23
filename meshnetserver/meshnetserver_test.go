package meshnetserver

import (
	"strings"
	"testing"

	"tailscale.com/types/key"
)

func TestParseAllowedClients(t *testing.T) {
	k1 := key.NewNode().Public().String()
	k2 := key.NewNode().Public().String()
	k3 := key.NewNode().Public().String()
	tests := []struct {
		name    string
		in      []string
		wantLen int
		wantErr bool
	}{
		{name: "nil input", in: nil, wantLen: 0, wantErr: false},
		{name: "empty slice", in: []string{}, wantLen: 0, wantErr: false},
		{name: "one valid", in: []string{k1}, wantLen: 1, wantErr: false},
		{name: "several valid", in: []string{k1, k2, k3}, wantLen: 3, wantErr: false},
		{name: "bad entry", in: []string{"rubbish"}, wantLen: 0, wantErr: true},
		{name: "mixed entry", in: []string{k1, "rubbish", k2}, wantLen: 0, wantErr: true},
		{name: "missing prefix", in: []string{strings.TrimPrefix(k1, "nodekey:")}, wantLen: 0, wantErr: true},
		{name: "whitespace", in: []string{" " + k1 + " "}, wantLen: 1, wantErr: false},
		{name: "empty key", in: []string{""}, wantLen: 0, wantErr: true},
		{name: "zero key", in: []string{key.NodePublic{}.String()}, wantLen: 0, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseAllowedClients(tc.in)
			if err != nil && !tc.wantErr {
				t.Errorf("unexpected error: %v", err)
			}

			if tc.wantErr {
				return
			}

			if len(got) != tc.wantLen {
				t.Errorf("expected %d, got %d", tc.wantLen, len(got))
			}

			for i := range got {
				if got[i].String() != strings.TrimSpace(tc.in[i]) {
					t.Errorf("unexpected key found")
				}
			}
		})
	}
}
