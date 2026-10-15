package meshnet

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"fmt"
	"math/big"
	"net"
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
	PeerKey(remote net.Addr) (key.NodePublic, bool)
	DisconnectClient(k key.NodePublic) bool
}

type ServerConfig struct {
	Logf                 func(format string, args ...any)
	OnMessage            func(clientID string, m Message)
	OnClientConnected    func(c ClientInfo)
	OnClientDisconnected func(c ClientInfo, e error)
	LivenessTimeout      time.Duration // Minimum 20s
}

// session is one paired client's connection
// one per connected client
type session struct {
	conn         net.Conn
	clientInfo   ClientInfo
	key          key.NodePublic
	writer       *transport.Writer
	nextStreamID uint64
	pending      map[uint64]chan net.Conn
	streams      []net.Conn
	endReason    error
}

type pairing struct {
	pin      string
	expires  time.Time
	failures int
}

const (
	pairingTTL         = 10 * time.Minute
	maxPairingFailures = 3
	// Starting at 1, may be configurable
	// Note - pairing below and relevant functions assume only one pairing at once
	maxClients = 1
)

type Server struct {
	mu       sync.Mutex // guards all before line break
	srv      tailcatServer
	done     chan struct{}
	err      error
	pairing  *pairing // to be a map of potential pairings for multiple clients
	sessions map[string]*session

	cfg        ServerConfig
	newTailcat func() tailcatServer
	now        func() time.Time
}

func NewServer(cfg ServerConfig) *Server {
	// nil Logf in tailcat pushes everything to log.Printf
	// if nil is sent give it an empty func to discard everything
	if cfg.Logf == nil {
		cfg.Logf = func(format string, args ...any) {}
	}

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
		now:  time.Now,
	}
	s.newTailcat = s.buildTailcat
	return s
}

func (s *Server) buildTailcat() tailcatServer {
	srv := &tailcat.Server{
		OnTCP:       s.dispatch,
		Logf:        s.cfg.Logf,
		AllowClient: s.allowClient,
	}
	return srv
}

func (s *Server) Start() (addr string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv != nil {
		return "", ErrAlreadyStarted
	}

	srv := s.newTailcat()
	if err := srv.Start(); err != nil {
		return "", fmt.Errorf("server: start: %w", err)
	}

	s.done = make(chan struct{})
	s.err = nil
	s.srv = srv
	s.sessions = make(map[string]*session)

	return string(srv.TailcatAddr()), nil
}

func (s *Server) NewPairing() (string, error) {
	pin, err := newPIN()
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv == nil {
		return "", ErrClosed
	}

	if len(s.sessions) >= maxClients {
		return "", ErrServerFull
	}

	s.pairing = &pairing{pin: pin, expires: s.now().Add(pairingTTL)}
	return pin, nil
}

func newPIN() (string, error) {
	pin, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return "", fmt.Errorf("generate PIN: %w", err)
	}
	return fmt.Sprintf("%06d", pin), nil
}

// nil if none or expired, s.mu must be locked
func (s *Server) pendingPairingLocked() *pairing {
	if s.pairing != nil && !s.now().Before(s.pairing.expires) {
		s.pairing = nil
	}
	return s.pairing
}

// Attempt to redeemLocked pin, s.mu must be locked
func (s *Server) redeemLocked(pin string) protocol.CloseErr {
	p := s.pendingPairingLocked()
	if p == nil {
		return protocol.CloseErrPairingLocked
	}

	if subtle.ConstantTimeCompare([]byte(p.pin), []byte(pin)) != 1 {
		p.failures++
		if p.failures >= maxPairingFailures {
			s.pairing = nil
		}
		return protocol.CloseErrWrongPIN
	}
	s.pairing = nil
	return protocol.CloseErrNone
}

func (s *Server) CancelPairing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pairing = nil
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
	sessions := s.sessions
	done := s.done
	s.srv = nil
	s.sessions = nil
	s.pairing = nil
	s.err = ErrClosed
	s.mu.Unlock()

	for _, sess := range sessions {
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
	sess, err := s.sessionForClient(clientID)
	if err != nil {
		return err
	}

	s.mu.Lock()
	w := sess.writer
	s.mu.Unlock()

	payload, err := protocol.Msg{Topic: m.Topic, Data: m.Data}.MarshalBinary()
	if err != nil {
		return err
	}

	return enqueueErr(w.Enqueue(protocol.Frame{Kind: protocol.KindMsg, Payload: payload}))
}

func (s *Server) RequestStream(ctx context.Context, clientID, target string) (net.Conn, error) {
	sess, err := s.sessionForClient(clientID)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
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
		s.dropPendingStream(sess, id)
		return nil, err
	}

	select {
	case conn := <-ch:
		return conn, nil
	case <-ctx.Done():
		s.dropPendingStream(sess, id)
		select { // the handler may have delivered just before we dropped it
		case conn := <-ch:
			conn.Close()
		default:
		}
		return nil, ctx.Err()
	case <-w.Done():
		s.dropPendingStream(sess, id)
		return nil, ErrNotConnected
	}
}

func (s *Server) dropPendingStream(sess *session, id uint64) {
	s.mu.Lock()
	delete(sess.pending, id)
	s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]ClientInfo, 0, len(s.sessions))
	for _, sess := range s.sessions {
		out = append(out, sess.clientInfo)
	}
	return out
}

// DisconnectClient ends the session of the client with this ID and removes the client from the tunnel
// It returns after the client's slot is freed, NewPairing can be called immediately
func (s *Server) DisconnectClient(clientID string) error {
	sess, err := s.sessionForClient(clientID)
	if err != nil {
		return err
	}

	s.mu.Lock()
	sess.endReason = ErrDisconnectedByServer
	s.mu.Unlock()
	s.clearSession(sess)
	sess.conn.Close()
	return nil
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

// Returns the current PIN or empty string there is none
func (s *Server) PairingPIN() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.pendingPairingLocked(); p != nil {
		return p.pin
	}
	return ""
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
	case protocol.StreamPort:
		return s.handleStream
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

	clientKey, keyParseErr := parseKey(h.Key)
	s.mu.Lock()
	srv := s.srv
	s.mu.Unlock()

	keyIsPeer := false
	if srv != nil && keyParseErr == nil {
		peer, ok := srv.PeerKey(conn.RemoteAddr())
		keyIsPeer = ok && peer == *clientKey
	}

	s.mu.Lock()
	var reason protocol.CloseErr
	var sess *session
	switch {
	case h.Version != protocol.Version:
		reason = protocol.CloseErrVersionMismatch
	case keyParseErr != nil || !keyIsPeer:
		reason = protocol.CloseErrInvalidKey
	default:
		reason = s.redeemLocked(h.PIN)
	}

	if reason == protocol.CloseErrNone {
		sess = &session{
			conn: conn,
			clientInfo: ClientInfo{
				ID:   clientKey.String(),
				Name: h.Name,
			},
			pending: make(map[uint64]chan net.Conn),
			key:     *clientKey,
		}
		s.sessions[clientKey.String()] = sess
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

func (s *Server) allowClient(k key.NodePublic) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingPairingLocked() != nil || s.sessions[k.String()] != nil
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
func (s *Server) handleStream(conn net.Conn) {
	s.mu.Lock()
	srv := s.srv
	s.mu.Unlock()

	// The session is the one belonging to the key the tunnel authenticated
	var peer key.NodePublic
	ok := false
	if srv != nil {
		peer, ok = srv.PeerKey(conn.RemoteAddr())
	}
	if !ok {
		conn.Close()
		return
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	id, err := protocol.ReadStreamID(conn)
	if err != nil {
		s.cfg.Logf("meshnet: stream: %v", err)
		conn.Close()
		return
	}
	conn.SetReadDeadline(time.Time{})

	s.mu.Lock()
	sess, err := s.sessionForClientLocked(peer.String())
	if err != nil {
		s.mu.Unlock()
		s.cfg.Logf("meshnet: stream: %v", err)
		conn.Close()
		return
	}

	ch, ok := sess.pending[id]
	delete(sess.pending, id)
	if ok {
		sess.streams = append(sess.streams, conn)
	}
	s.mu.Unlock()
	if !ok {
		conn.Close()
		return
	}
	ch <- conn
}

// returns the connected session for client id, s.mu must be held
func (s *Server) sessionForClientLocked(id string) (*session, error) {
	if len(s.sessions) == 0 {
		return nil, ErrNotConnected
	}

	sess := s.sessions[id]
	if sess == nil {
		return nil, ErrUnknownClient
	}

	// Paired but handshake not completed
	if sess.writer == nil {
		return nil, ErrNotConnected
	}
	return sess, nil
}

// sessionForClientLocked for callers that don't hold s.mu
func (s *Server) sessionForClient(id string) (*session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionForClientLocked(id)
}

// tear down a session
// clearSession must be run before the stream list is read
// once the session is out of the map handleStream cannot add any new streams
func (s *Server) endSession(sess *session, cause error) {
	sess.conn.Close()
	s.clearSession(sess)

	s.mu.Lock()
	w := sess.writer
	stopped := s.srv == nil
	streams := sess.streams
	reason := sess.endReason
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
