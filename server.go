package meshnet

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
	"github.com/tailscale/tailcat"
	"tailscale.com/types/key"
)

const (
	pairingTTL         = 10 * time.Minute
	maxFailuresPerPIN  = 3  // max wrong PINs per pairing
	maxPairingFailures = 10 // max failed pairings per server run (Wrong PIN or key)
	// Starting at 1, may be configurable
	// Note - pairing below and relevant functions assume only one pairing at once
	maxClients = 1
)

type ServerConfig struct {
	Logf                 func(format string, args ...any)
	OnMessage            func(clientID string, m Message)
	OnClientConnected    func(c ClientInfo)
	OnClientDisconnected func(c ClientInfo, e error)
	LivenessTimeout      time.Duration // Minimum 20s
}

type Server struct {
	mu              sync.Mutex // guards all before line break
	srv             tailcatServer
	done            chan struct{}
	err             error
	pairing         *pairing // to be a map of potential pairings for multiple clients
	sessions        map[string]*session
	pairingFailures int

	cfg        ServerConfig
	newTailcat func() tailcatServer
	now        func() time.Time
}

type tailcatServer interface {
	Start() error
	Close() error
	TailcatAddr() tailcat.Addr
	PeerKey(remote net.Addr) (key.NodePublic, bool)
	DisconnectClient(k key.NodePublic) bool
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
	s.pairingFailures = 0

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

	if s.pairingFailures >= maxPairingFailures {
		return "", ErrPairingExhausted
	}

	if len(s.sessions) >= maxClients {
		return "", ErrServerFull
	}

	s.pairing = &pairing{pin: pin, expires: s.now().Add(pairingTTL)}
	return pin, nil
}

func (s *Server) CancelPairing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pairing = nil
}

// Returns the current PIN or empty string if there is none
func (s *Server) PairingPIN() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.pendingPairingLocked(); p != nil {
		return p.pin
	}
	return ""
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

// send message to a client
func (s *Server) Send(clientID string, m Message) error {
	sess, err := s.sessionForClient(clientID)
	if err != nil {
		return err
	}

	w, err := sess.sender()
	if err != nil {
		return err
	}

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

	id, ch, w, err := sess.requestStream()
	if err != nil {
		return nil, err
	}

	payload, err := protocol.StreamRequest{ID: id, Target: target}.MarshalBinary()
	if err == nil {
		err = enqueueErr(w.Enqueue(protocol.Frame{Kind: protocol.KindStreamRequest, Payload: payload}))
	}
	if err != nil {
		sess.cancelStream(id)
		return nil, err
	}

	select {
	case conn := <-ch:
		return conn, nil
	case <-ctx.Done():
		sess.cancelStream(id)
		select { // the handler may have delivered just before we dropped it
		case conn := <-ch:
			conn.Close()
		default:
		}
		return nil, ctx.Err()
	case <-w.Done():
		sess.cancelStream(id)
		return nil, ErrNotConnected
	}
}

// DisconnectClient ends the session of the client with this ID and removes the client from the tunnel
// It returns after the client's slot is freed, NewPairing can be called immediately
func (s *Server) DisconnectClient(clientID string) error {
	sess, err := s.sessionForClient(clientID)
	if err != nil {
		return err
	}

	if _, err := sess.sender(); err != nil {
		return err // handshake pending or already closed
	}

	sess.endedBy(ErrDisconnectedByServer)
	s.clearSession(sess)
	sess.conn.Close()
	return nil
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

func (s *Server) buildTailcat() tailcatServer {
	srv := &tailcat.Server{
		OnTCP:       s.dispatch,
		Logf:        s.cfg.Logf,
		AllowClient: s.allowClient,
	}
	return srv
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
