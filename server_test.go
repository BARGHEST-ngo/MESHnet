package meshnet

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/types/key"
)

type fakeTailcat struct {
	closeCalls atomic.Int32
}

const fakeAddress = "fake-address"

func (f *fakeTailcat) TailcatAddr() tailcat.Addr {
	return fakeAddress
}

func (f *fakeTailcat) Start() error {
	return nil
}

func (f *fakeTailcat) Close() error {
	f.closeCalls.Add(1)
	return nil
}

func (f *fakeTailcat) AddAllowedClient(k key.NodePublic) {}

func newTestServer(t *testing.T) (*Server, *fakeTailcat) {
	t.Helper()
	fake := &fakeTailcat{}
	s := NewServer(ServerConfig{})
	s.newTailcat = func([]key.NodePublic) tailcatServer {
		return fake
	}
	return s, fake
}

func assertClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("channel not closed")
	}
}

func assertOpen(t *testing.T, ch <-chan struct{}) {
	select {
	case <-ch:
		t.Fatal("channel closed, want open")
	default:
	}
}

func TestStartStop(t *testing.T) {
	t.Run("basic start stop", func(t *testing.T) {
		s, fake := newTestServer(t)
		addr, err := s.Start()
		if err != nil {
			t.Fatalf("Start: %v", err)
		}

		if addr != fakeAddress {
			t.Errorf("addr = %q, want %q", addr, fakeAddress)
		}

		if err := s.Stop(); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		if n := fake.closeCalls.Load(); n != 1 {
			t.Errorf("Close called %d times, want 1", n)
		}
	})

	t.Run("done before start", func(t *testing.T) {
		s, _ := newTestServer(t)
		assertClosed(t, s.Done())

		if !errors.Is(s.Err(), ErrClosed) {
			t.Errorf("expected %v, got %v", ErrClosed, s.Err())
		}
	})

	t.Run("done while running", func(t *testing.T) {
		s, _ := newTestServer(t)
		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}

		assertOpen(t, s.Done())
	})

	t.Run("done after stop", func(t *testing.T) {
		s, _ := newTestServer(t)
		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if err := s.Stop(); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		assertClosed(t, s.Done())
		if !errors.Is(s.Err(), ErrClosed) {
			t.Errorf("wanted %v, got %v", ErrClosed, s.Err())
		}
	})

	t.Run("start twice", func(t *testing.T) {
		s, _ := newTestServer(t)
		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if _, err := s.Start(); !errors.Is(err, ErrAlreadyStarted) {
			t.Errorf("wanted %v, got %v", ErrAlreadyStarted, err)
		}
		assertOpen(t, s.Done())
	})

	t.Run("stop twice", func(t *testing.T) {
		s, fake := newTestServer(t)
		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if err := s.Stop(); err != nil {
			t.Fatalf("stop: %v", err)
		}

		if err := s.Stop(); err != nil {
			t.Errorf("wanted %v, got %v", nil, err)
		}
		assertClosed(t, s.Done())
		if n := fake.closeCalls.Load(); n != 1 {
			t.Errorf("Close called %d times, want 1", n)
		}
	})

	t.Run("stop before start", func(t *testing.T) {
		s, fake := newTestServer(t)
		if err := s.Stop(); err != nil {
			t.Fatalf("stop: %v", err)
		}

		if n := fake.closeCalls.Load(); n != 0 {
			t.Errorf("Close called %d times, want 0", n)
		}
	})

	t.Run("restart", func(t *testing.T) {
		s, fake := newTestServer(t)
		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		first := s.Done()

		if err := s.Stop(); err != nil {
			t.Fatalf("stop: %v", err)
		}

		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		second := s.Done()

		assertClosed(t, first)
		assertOpen(t, second)

		if n := fake.closeCalls.Load(); n != 1 {
			t.Errorf("Close called %d times, want 1", n)
		}
	})

	t.Run("concurrent stop", func(t *testing.T) {
		s, fake := newTestServer(t)
		if _, err := s.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 10 {
			wg.Go(func() {
				<-start
				if err := s.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			})
		}
		close(start)
		wg.Wait()
		if n := fake.closeCalls.Load(); n != 1 {
			t.Errorf("Close called %d times, want 1", n)
		}
		assertClosed(t, s.Done())
		if !errors.Is(s.Err(), ErrClosed) {
			t.Errorf("wanted %v, got %v", ErrClosed, s.Err())
		}
	})
}

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
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
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
