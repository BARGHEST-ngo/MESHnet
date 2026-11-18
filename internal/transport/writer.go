// Copyright (c) BARGHEST
// SPDX-License-Identifier: AGPL-3.0-or-later

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
	done   chan struct{} // closed in Close(), tells run() to stop
	exited chan struct{} // closed when run() returns
	finish chan protocol.Frame

	closeOnce sync.Once
	mu        sync.Mutex
	err       error
	finishing bool
}

func NewWriter(w io.Writer) *Writer {
	writer := &Writer{
		queue:  make(chan protocol.Frame, DefaultQueueSize),
		done:   make(chan struct{}),
		exited: make(chan struct{}),
		finish: make(chan protocol.Frame),
		conn:   w,
	}

	go writer.run()
	return writer
}

func (w *Writer) Enqueue(f protocol.Frame) error {
	if err := f.Validate(); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finishing {
		return ErrClosed
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

// Why the writer closed
func (w *Writer) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *Writer) Done() <-chan struct{} {
	return w.exited
}

// Finish stops new frames being queued and flushes the existing queue
// Writes the final frame and stops the writer
func (w *Writer) Finish(f protocol.Frame) error {
	if err := f.Validate(); err != nil {
		return err
	}

	w.mu.Lock()
	if w.finishing {
		w.mu.Unlock()
		return ErrClosed
	}
	w.finishing = true
	w.mu.Unlock()

	select {
	case w.finish <- f:
	case <-w.done:
		return ErrClosed
	case <-w.exited:
		return ErrClosed
	}

	<-w.exited
	return w.Err()
}

func (w *Writer) run() {
	defer close(w.exited)

	for {
		select {
		case <-w.done:
			return
		case f := <-w.queue:
			if !w.write(f) {
				return
			}
		// Writer told to close, flush the queue and send final frame
		case last := <-w.finish:
			for {
				select {
				case f := <-w.queue:
					if !w.write(f) {
						return
					}
				default:
					w.write(last)
					return
				}
			}
		}
	}
}

func (w *Writer) write(f protocol.Frame) bool {
	if err := protocol.WriteFrame(w.conn, f); err != nil {
		w.mu.Lock()
		w.err = err
		w.mu.Unlock()
		return false
	}
	return true
}
