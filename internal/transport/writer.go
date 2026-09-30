package transport

import (
	"errors"
	"io"
	"sync"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
)

var (
	ErrFull   = errors.New("transport: send queue full")
	ErrClosed = errors.New("transport: writer closed")
)

const DefaultQueueSize = 64

type Writer struct {
	conn   io.Writer
	queue  chan protocol.Frame
	done   chan struct{}
	exited chan struct{}

	closeOnce sync.Once
	mu        sync.Mutex
	err       error
}

func NewWriter(w io.Writer) *Writer {
	writer := &Writer{
		queue:  make(chan protocol.Frame, DefaultQueueSize),
		done:   make(chan struct{}),
		exited: make(chan struct{}),
		conn:   w,
	}

	go writer.run()
	return writer
}

func (w *Writer) Enqueue(f protocol.Frame) error {
	if err := f.Validate(); err != nil {
		return err
	}
	select {
	case <-w.done:
		return ErrClosed
	case <-w.exited:
		return ErrClosed
	default:
		select {
		case w.queue <- f:
			return nil
		default:
			return ErrFull
		}
	}
}

func (w *Writer) Close() {
	w.closeOnce.Do(func() { close(w.done) })
	<-w.exited
}

func (w *Writer) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *Writer) run() {
	defer close(w.exited)

	for {
		select {
		case <-w.done:
			return
		case f := <-w.queue:
			// Catch invalid frames before writing
			if err := protocol.WriteFrame(w.conn, f); err != nil {
				w.mu.Lock()
				w.err = err
				w.mu.Unlock()
				return
			}
		}
	}
}
