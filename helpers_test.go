package meshnet

import (
	"testing"
	"time"
)

// These will be merged with the Server OnClientConenct/Disconnect
// Duplicating here for now so Client tests can pass
// Will remove when merging main to this branch after Server PR is closed

// next waits for one value from ch, failing the test after a timeout.
func next[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(time.Second):
		t.Fatal("callback not called")
		var zero T
		return zero
	}
}

// none checks nothing more arrives on ch for a short while.
func none[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected extra callback: %+v", v)
	case <-time.After(50 * time.Millisecond):
	}
}

// drain collects everything that arrives on ch until it has been quiet for
// a short while, then returns it.
func drain[T any](ch <-chan T) []T {
	var got []T
	for {
		select {
		case v := <-ch:
			got = append(got, v)
		case <-time.After(50 * time.Millisecond):
			return got
		}
	}
}
