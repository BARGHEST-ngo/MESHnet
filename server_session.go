package meshnet

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
	"github.com/BARGHEST-ngo/MESHnet/internal/transport"
	"tailscale.com/types/key"
)

// session is one paired client's connection
// It guards its own state, callers use its methods and never need the server's lock
type session struct {
	// Set when the session is created and never changed
	conn       net.Conn
	clientInfo ClientInfo
	key        key.NodePublic

	mu           sync.Mutex
	writer       *transport.Writer // nil before a handshake completes
	nextStreamID uint64
	pending      map[uint64]chan net.Conn // stream requests waiting
	streams      []net.Conn               // streams handed out, closed with the session
	endReason    error                    // why the server ended the session, if it did
	closed       bool
}

type clientCloseError struct{ reason protocol.CloseErr }

func (e clientCloseError) Error() string {
	return "meshnet: client closed: " + e.reason.String()
}

func (sess *session) ready(w *transport.Writer) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.writer = w
}

func (sess *session) sender() (*transport.Writer, error) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.writer == nil {
		return nil, ErrNotConnected
	}

	if sess.closed {
		return nil, ErrNotConnected
	}
	return sess.writer, nil
}

func (sess *session) requestStream() (uint64, chan net.Conn, *transport.Writer, error) {
	sess.mu.Lock()
	defer sess.mu.Unlock()

	if sess.writer == nil || sess.closed {
		return 0, nil, nil, ErrNotConnected
	}

	sess.nextStreamID++
	id := sess.nextStreamID
	ch := make(chan net.Conn, 1)
	sess.pending[id] = ch
	return id, ch, sess.writer, nil
}

func (sess *session) cancelStream(id uint64) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	delete(sess.pending, id)
}

func (sess *session) deliverStream(id uint64, conn net.Conn) (chan net.Conn, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	ch, ok := sess.pending[id]
	if !ok || sess.closed {
		return nil, false
	}
	delete(sess.pending, id)
	sess.streams = append(sess.streams, conn)
	return ch, true
}

func (sess *session) endedBy(reason error) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.endReason = reason
}

func (sess *session) close() (*transport.Writer, []net.Conn, error) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.closed {
		return sess.writer, nil, sess.endReason
	}
	sess.closed = true
	streams := sess.streams
	sess.streams = nil
	return sess.writer, streams, sess.endReason
}

func (s *Server) serve(sess *session) error {
	r := deadlineReader{
		conn:    sess.conn,
		timeout: s.cfg.LivenessTimeout,
	}
	for {
		f, err := protocol.ReadFrame(r)
		if err != nil {
			return timeoutErr(err)
		}

		if err := s.handleFrame(sess, f); err != nil {
			return err // Client close, malformed frame
		}
	}
}

func (s *Server) handleFrame(sess *session, f protocol.Frame) error {
	switch f.Kind {
	case protocol.KindMsg:
		return s.handleMsg(sess, f)
	case protocol.KindClose:
		return s.handleClose(sess, f)
	case protocol.KindPing:
		return s.handlePing(sess, f)
	default:
		return fmt.Errorf("unexpected %v frame after handshake", f.Kind)
	}
}

func (s *Server) handleMsg(sess *session, f protocol.Frame) error {
	var m protocol.Msg
	if err := m.UnmarshalBinary(f.Payload); err != nil {
		return err
	}

	s.cfg.OnMessage(sess.clientInfo.ID, Message{Topic: m.Topic, Data: m.Data})
	return nil
}

func (s *Server) handlePing(sess *session, f protocol.Frame) error {
	if len(f.Payload) != 0 {
		return fmt.Errorf("ping: %w", protocol.ErrMalformedPayload)
	}

	w, err := sess.sender()
	if err != nil {
		return err
	}

	if err := w.Enqueue(protocol.Frame{Kind: protocol.KindPong}); err != nil && !errors.Is(err, transport.ErrFull) {
		return fmt.Errorf("ping: %w", err)
	}
	// Full queue is fine, queued frames are traffic
	return nil
}

func (s *Server) handleClose(sess *session, f protocol.Frame) error {
	var cl protocol.Close
	if err := cl.UnmarshalBinary(f.Payload); err != nil {
		return err
	}

	return clientCloseError{reason: cl.Err}
}

func (s *Server) sessionForClientLocked(id string) (*session, error) {
	if len(s.sessions) == 0 {
		return nil, ErrNotConnected
	}

	sess := s.sessions[id]
	if sess == nil {
		return nil, ErrUnknownClient
	}

	return sess, nil
}

func (s *Server) sessionForClient(id string) (*session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionForClientLocked(id)
}

func enqueueErr(err error) error {
	if errors.Is(err, transport.ErrFull) {
		return ErrSendQueueFull
	}

	if errors.Is(err, transport.ErrClosed) {
		return ErrNotConnected
	}

	return err
}

// Convert why a session ended into what an app is told
// stopped is true when the server was stopping at the time
func disconnectReason(cause error, stopped bool) error {
	var ce clientCloseError
	switch {
	case stopped:
		return ErrClosed
	case errors.As(cause, &ce) && ce.reason == protocol.CloseErrNone:
		return nil // clean disconnect
	default:
		return cause // closed with a reason
	}
}

// tear down a session
func (s *Server) endSession(sess *session, cause error) {
	sess.conn.Close()
	s.clearSession(sess)

	w, streams, reason := sess.close()

	s.mu.Lock()
	stopped := s.srv == nil
	s.mu.Unlock()

	if w != nil {
		w.Close()
	}
	for _, stream := range streams {
		stream.Close()
	}

	if reason == nil {
		reason = disconnectReason(cause, stopped)
	}
	s.cfg.Logf("meshnet: client %s disconnected: %v", sess.clientInfo.ID, reason)
	s.cfg.OnClientDisconnected(sess.clientInfo, reason)
}

// clearSession removes sess from the server and revokes its key on the tunnel
// tailcat drops the peer and the allowClient hook won't let it back in
func (s *Server) clearSession(sess *session) {
	s.mu.Lock()
	srv := s.srv
	if s.sessions[sess.clientInfo.ID] == sess {
		delete(s.sessions, sess.clientInfo.ID)
	}
	s.mu.Unlock()

	if srv != nil {
		srv.DisconnectClient(sess.key)
	}
}
