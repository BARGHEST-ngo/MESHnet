package transport

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
)

// Message frame identified by its payload
func frame(i int) protocol.Frame {
	return protocol.Frame{Kind: protocol.KindMsg, Payload: []byte{byte(i >> 8), byte(i)}}
}

func pipe(t *testing.T) (writer, reader net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	deadline := time.Now().Add(2 * time.Second)
	a.SetDeadline(deadline)
	b.SetDeadline(deadline)
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

func TestWriterOrder(t *testing.T) {
	numFrames := 50
	pipeWriter, pipeReader := pipe(t)
	w := NewWriter(pipeWriter)
	defer w.Close()

	for i := range numFrames {
		if err := w.Enqueue(frame(i)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	for i := range numFrames {
		f, err := protocol.ReadFrame(pipeReader)
		if err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}

		if got := int(binary.BigEndian.Uint16(f.Payload)); got != i {
			t.Errorf("frame %d: got sequence %d", i, got)
		}
	}
}

func TestConcurrentEnqueue(t *testing.T) {
	pipeWriter, pipeReader := pipe(t)
	w := NewWriter(pipeWriter)
	defer w.Close()

	const senders, perSender = 10, 50
	start := make(chan struct{})
	var wg sync.WaitGroup

	for s := range senders {
		wg.Go(func() {
			<-start
			for i := range perSender {
				id := s*perSender + i
				for {
					err := w.Enqueue(frame(id))
					if err == nil {
						break
					}
					if !errors.Is(err, ErrFull) {
						t.Errorf("enqueue %d: %v", id, err)
						return
					}
				}
			}
		})
	}

	close(start)

	seen := make(map[int]struct{})
	for range senders * perSender {
		f, err := protocol.ReadFrame(pipeReader)
		if err != nil {
			t.Fatalf("read frame after %d frames: %v", len(seen), err)
		}
		id := int(binary.BigEndian.Uint16(f.Payload))
		if _, ok := seen[id]; ok {
			t.Fatalf("frame %d received twice", id)
		}
		seen[id] = struct{}{}
	}

	wg.Wait()
	if len(seen) != senders*perSender {
		t.Fatalf("expected %d unique frames, got %d", senders*perSender, len(seen))
	}
}

func TestQueueFull(t *testing.T) {
	pipeWriter, _ := pipe(t)
	w := NewWriter(pipeWriter)
	defer w.Close()

	var err error
	accepted := 0
	for accepted <= DefaultQueueSize+1 {
		if err = w.Enqueue(frame(accepted)); err != nil {
			break
		}
		accepted++
	}

	if !errors.Is(err, ErrFull) {
		t.Fatalf("after %d frames: %v, want ErrFull", accepted, err)
	}
	pipeWriter.Close()
	w.Close()
}

func TestEnqueueAfterClose(t *testing.T) {
	pipeWriter, _ := pipe(t)
	w := NewWriter(pipeWriter)
	w.Close()
	if err := w.Enqueue(frame(0)); err != nil {
		if !errors.Is(err, ErrClosed) {
			t.Errorf("expected ErrClose, got %v", err)
		}
	} else {
		t.Fatal("expected ErrFull")
	}
}

func TestInvalidFrame(t *testing.T) {
	pipeWriter, pipeReader := pipe(t)
	w := NewWriter(pipeWriter)
	defer w.Close()

	if err := w.Enqueue(protocol.Frame{Kind: protocol.KindInvalid}); !errors.Is(err, protocol.ErrUnknownKind) {
		t.Errorf("expected ErrUnknownKind, got %v", err)
	}

	largePayload := make([]byte, protocol.MaxPayloadSize+1)
	if err := w.Enqueue(protocol.Frame{Kind: protocol.KindHello, Payload: largePayload}); !errors.Is(err, protocol.ErrFrameTooLarge) {
		t.Errorf("expected ErrFrameTooLarge, got %v", err)
	}

	if err := w.Enqueue(frame(1)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	f, err := protocol.ReadFrame(pipeReader)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}

	if f.Kind != protocol.KindMsg {
		t.Errorf("expected Msg kind, got %v", f.Kind)
	}
	if got := int(binary.BigEndian.Uint16(f.Payload)); got != 1 {
		t.Errorf("frame %d: got sequence %d", 1, got)
	}
}

func TestCloseTwice(t *testing.T) {
	pipeWriter, _ := pipe(t)
	w := NewWriter(pipeWriter)
	w.Close()

	closed := make(chan struct{})

	go func() {
		w.Close()
		close(closed) // must not panic
	}()

	select {
	case <-closed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second Close() blocked")
	}
}
