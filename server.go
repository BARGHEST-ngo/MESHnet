package meshnet

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"fmt"
	"math/big"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
	"github.com/BARGHEST-ngo/MESHnet/internal/transport"
	"github.com/tailscale/tailcat"
	"tailscale.com/types/key"
)

type tailcatServer interface {
	Start() error
	Close() error
	TailcatAddr() tailcat.Addr
	AddAllowedClient(k key.NodePublic)
}

type ServerConfig struct {
	AllowedClients       []string
	Logf                 func(format string, args ...any)
	OnMessage            func(clientID string, m Message)
	OnClientConnected    func(c ClientInfo)
	OnClientDisconnected func(c ClientInfo, e error)
	LivenessTimeout      time.Duration // Minimum 20s
}

// session is the state for one paired client's connection
// Stop uses it to close the connection, Send (will) write to it.
// per-client state will live here (writer queue, liveness, streams)
// currently one per server run
type session struct {
	conn         net.Conn
	clientInfo   ClientInfo
	writer       *transport.Writer
	nextStreamID uint64
	pending      map[uint64]chan net.Conn
	streams      []net.Conn
}

type pairing struct {
	pin  string
	used bool // the PIN has been redeemed, no more pairings this run
	// later - attempt limits, expiry time
}

type Server struct {
	mu      sync.Mutex // guards all before line break
	srv     tailcatServer
	done    chan struct{}
	err     error
	pairing pairing // to be a map of potential pairings for multiple clients
	sess    *session

	cfg        ServerConfig
	newTailcat func([]key.NodePublic) tailcatServer
}

func NewServer(cfg ServerConfig) *Server {
	// nil Logf in tailcat pushes everything to log.Printf
	// if nil is sent give it an empty func to discard everything
	if cfg.Logf == nil {
		cfg.Logf = func(format string, args ...any) {}
	}
	cfg.AllowedClients = slices.Clone(cfg.AllowedClients)

	if cfg.OnMessage == nil {
		cfg.OnMessage = func(clientID string, m Message) {}
	}

	if cfg.OnClientConnected == nil {
		cfg.OnClientConnected = func(c ClientInfo) {}
	}

	if cfg.OnClientDisconnected == nil {
		cfg.OnClientDisconnected = func(c ClientInfo, e error) {}
	}

	switch {
	case cfg.LivenessTimeout == 0:
		cfg.LivenessTimeout = defaultLivenessTimeout

	case cfg.LivenessTimeout < 2*pingInterval:
		cfg.LivenessTimeout = 2 * pingInterval
	}

	done := make(chan struct{})
	close(done)
	s := &Server{
		done: done,
		err:  ErrClosed,
		cfg:  cfg,
	}
	s.newTailcat = s.buildTailcat
	return s
}

func newPairing() (pairing, error) {
	pin, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return pairing{}, fmt.Errorf("generate PIN: %w", err)
	}

	return pairing{
		pin:  fmt.Sprintf("%06d", pin),
		used: false,
	}, nil
}

func (p *pairing) redeem(pin string) protocol.CloseErr {
	if p.used {
		return protocol.CloseErrPairingLocked
	}

	if subtle.ConstantTimeCompare([]byte(p.pin), []byte(pin)) != 1 {
		return protocol.CloseErrWrongPIN
	}

	p.used = true
	return protocol.CloseErrNone
}

func (s *Server) buildTailcat(allow []key.NodePublic) tailcatServer {
	srv := &tailcat.Server{
		OnTCP:          s.dispatch,
		Logf:           s.cfg.Logf,
		AllowedClients: allow,
	}
	return srv
}

func (s *Server) Start() (addr string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv != nil {
		return "", ErrAlreadyStarted
	}

	allowList, err := parseAllowedClients(s.cfg.AllowedClients)
	if err != nil {
		return "", fmt.Errorf("server: start: %w", err)
	}

	p, err := newPairing()
	if err != nil {
		return "", fmt.Errorf("server: start: %w", err)
	}

	srv := s.newTailcat(allowList)
	if err := srv.Start(); err != nil {
		return "", fmt.Errorf("server: start: %w", err)
	}

	s.pairing = p
	s.done = make(chan struct{})
	s.err = nil
	s.srv = srv

	return string(srv.TailcatAddr()), nil
}

func (s *Server) Stop() error {
	s.mu.Lock()
	if s.srv == nil {
		// Not running or already shutting down
		done := s.done
		s.mu.Unlock()
		<-done
		return nil
	}

	srv := s.srv
	sess := s.sess
	done := s.done
	s.srv = nil
	s.sess = nil
	s.err = ErrClosed
	s.mu.Unlock()

	if sess != nil {
		sess.conn.Close()
	}

	err := srv.Close()
	close(done)
	if err != nil {
		return fmt.Errorf("server: stop: %w", err)
	}

	return err
}

// send message to a client
func (s *Server) Send(clientID string, m Message) error {
	s.mu.Lock()
	if s.sess == nil || s.sess.writer == nil {
		s.mu.Unlock()
		return ErrNotConnected
	}

	if s.sess.clientInfo.ID != clientID {
		s.mu.Unlock()
		return ErrUnknownClient
	}
	w := s.sess.writer
	s.mu.Unlock()

	payload, err := protocol.Msg{Topic: m.Topic, Data: m.Data}.MarshalBinary()
	if err != nil {
		return err
	}

	return enqueueErr(w.Enqueue(protocol.Frame{Kind: protocol.KindMsg, Payload: payload}))
}

func (s *Server) RequestStream(ctx context.Context, clientID, target string) (net.Conn, error) {
	s.mu.Lock()
	sess := s.sess
	if sess == nil || sess.writer == nil {
		s.mu.Unlock()
		return nil, ErrNotConnected
	}

	if sess.clientInfo.ID != clientID {
		s.mu.Unlock()
		return nil, ErrUnknownClient
	}

	sess.nextStreamID++
	id := sess.nextStreamID
	ch := make(chan net.Conn, 1)
	sess.pending[id] = ch
	w := sess.writer
	s.mu.Unlock()

	payload, err := protocol.StreamRequest{ID: id, Target: target}.MarshalBinary()
	if err == nil {
		err = enqueueErr(w.Enqueue(protocol.Frame{Kind: protocol.KindStreamRequest, Payload: payload}))
	}
	if err != nil {
		s.mu.Lock()
		delete(sess.pending, id)
		s.mu.Unlock()
		return nil, err
	}

	select {
	case conn := <-ch:
		return conn, nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(sess.pending, id)
		s.mu.Unlock()
		select { // the handler may have delivered just before we dropped it
		case conn := <-ch:
			conn.Close()
		default:
		}
		return nil, ctx.Err()
	case <-w.Done():
		s.mu.Lock()
		delete(sess.pending, id)
		s.mu.Unlock()
		return nil, ErrNotConnected
	}
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

func (s *Server) Clients() []ClientInfo {
	// TODO
	return make([]ClientInfo, 0)
}

// Done returns a channel that is closed when the current server has fully stopped and the tailcat server has closed
// if the server is not ruhnning the channel is already closed
// Call Done() after Stgart() to wait on that session, each start creates a new channel
func (s *Server) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// Err tells you why the server stopped, nil when the server is running, ErrClosed once Done is closed
// (and before the first start)
func (s *Server) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Returns the current PIN or empty string if the server isn't running
func (s *Server) PairingPIN() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv == nil {
		return ""
	}
	return s.pairing.pin
}

func parseAllowedClients(clients []string) ([]key.NodePublic, error) {
	out := make([]key.NodePublic, 0, len(clients))

	for i, c := range clients {
		c = strings.TrimSpace(c)
		k, err := parseKey(c)
		if err != nil {
			return nil, fmt.Errorf("allow client [%d]: %w", i, err)
		}

		out = append(out, *k)
	}

	return out, nil
}

func parseKey(k string) (*key.NodePublic, error) {
	out := &key.NodePublic{}
	if err := out.UnmarshalText([]byte(k)); err != nil {
		return nil, fmt.Errorf("server: parse key: %q: %w", k, err)
	}

	if out.IsZero() {
		return nil, fmt.Errorf("server: parse key: %q: invalid key", k)
	}
	return out, nil
}

func (s *Server) dispatch(port uint16) func(net.Conn) {
	switch port {
	case protocol.ControlPort:
		return s.handleControl
	default:
		return nil
	}
}

func (s *Server) handleControl(conn net.Conn) {
	sess, ok := s.handleShake(conn)
	if !ok {
		conn.Close()
		return
	}
	s.cfg.OnClientConnected(sess.clientInfo)

	cause := s.serve(sess)
	s.endSession(sess, cause)
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

// TODO - If the server starts with a pre-approved list of client keys, the PIN is not required
// but this handshake handler currently demands one
// Not needed in our existing use-case for MESHnet so will come back to that one...
func (s *Server) handleShake(conn net.Conn) (*session, bool) {
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	f, err := protocol.ReadFrame(conn)
	if err != nil {
		s.cfg.Logf("handleShake: %v", err)
		return nil, false
	}

	if f.Kind != protocol.KindHello {
		s.cfg.Logf("handleshake: unexpected frame %v", f.Kind)
		return nil, false
	}

	var h protocol.Hello
	if err := h.UnmarshalBinary(f.Payload); err != nil {
		c := protocol.Close{
			Err:     protocol.CloseErrError,
			Message: "malformed hello payload",
		}

		if err := protocol.WritePayload(conn, protocol.KindClose, c); err != nil {
			s.cfg.Logf("handleShake: %v", err)
		}
		return nil, false
	}

	// Likely going to refactor this switch block
	// Confirms a Hello message is valid
	// key-to-tunnel-IP check will go here..
	clientKey, keyParseErr := parseKey(h.Key)
	s.mu.Lock()
	var reason protocol.CloseErr
	var sess *session
	srv := s.srv
	switch {
	case h.Version != protocol.Version:
		reason = protocol.CloseErrVersionMismatch
	case keyParseErr != nil:
		reason = protocol.CloseErrInvalidKey
	default:
		reason = s.pairing.redeem(h.PIN)
	}

	if reason == protocol.CloseErrNone {
		sess = &session{
			conn: conn,
			clientInfo: ClientInfo{
				ID:   clientKey.String(),
				Name: h.Name,
			},
			pending: make(map[uint64]chan net.Conn),
		}
		s.sess = sess
	}
	s.mu.Unlock()

	if reason != protocol.CloseErrNone {
		c := protocol.Close{
			Err: reason,
		}
		if err := protocol.WritePayload(conn, protocol.KindClose, c); err != nil {
			s.cfg.Logf("handleShake: %v", err)
		}
		return nil, false
	}

	srv.AddAllowedClient(*clientKey)

	if err := protocol.WritePayload(conn, protocol.KindHelloAck, protocol.HelloAck{
		Version:  protocol.Version,
		ClientID: sess.clientInfo.ID,
	}); err != nil {
		s.cfg.Logf("handleShake: %v", err)
		s.clearSession(sess)
		return nil, false
	}

	w := transport.NewWriter(conn)
	s.mu.Lock()
	sess.writer = w
	s.mu.Unlock()

	conn.SetDeadline(time.Time{})

	return sess, true
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

	err := sess.writer.Enqueue(protocol.Frame{Kind: protocol.KindPong})
	if err != nil && !errors.Is(err, transport.ErrFull) {
		return fmt.Errorf("ping: %w", err)
	}
	// Full queue is fine, queued frames are traffic
	return nil
}

type clientCloseError struct{ reason protocol.CloseErr }

func (e clientCloseError) Error() string {
	return "meshnet: client closed: " + e.reason.String()
}

func (s *Server) handleClose(sess *session, f protocol.Frame) error {
	var cl protocol.Close
	if err := cl.UnmarshalBinary(f.Payload); err != nil {
		return err
	}

	return clientCloseError{reason: cl.Err}
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
// close the conn and unblock a writer stuck on a dead peer
// stop the writer and clear it
func (s *Server) endSession(sess *session, cause error) {
	sess.conn.Close()

	s.mu.Lock()
	w := sess.writer
	stopped := s.srv == nil
	s.mu.Unlock()
	if w != nil {
		w.Close()
	}
	s.clearSession(sess)

	reason := disconnectReason(cause, stopped)
	s.cfg.Logf("meshnet: client %s disconnected: %v", sess.clientInfo.ID, reason)
	s.cfg.OnClientDisconnected(sess.clientInfo, reason)
}

func (s *Server) clearSession(sess *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sess == sess {
		s.sess = nil
	}
}
