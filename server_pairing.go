package meshnet

import (
	"crypto/rand"
	"crypto/subtle"
	"fmt"
	"math/big"
	"net"
	"time"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
	"github.com/BARGHEST-ngo/MESHnet/internal/transport"
	"tailscale.com/types/key"
)

type pairing struct {
	pin      string
	expires  time.Time
	failures int
}

func (s *Server) allowClient(k key.NodePublic) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingPairingLocked() != nil || s.sessions[k.String()] != nil
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
		s.pairingFailures++
		if s.pairingFailures >= maxPairingFailures {
			s.pairing = nil
		}
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

	sess.ready(transport.NewWriter(conn))
	conn.SetDeadline(time.Time{})

	return sess, true
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

// Attempt to redeem pin, s.mu must be locked
func (s *Server) redeemLocked(pin string) protocol.CloseErr {
	p := s.pendingPairingLocked()
	if p == nil {
		return protocol.CloseErrPairingLocked
	}

	if subtle.ConstantTimeCompare([]byte(p.pin), []byte(pin)) != 1 {
		p.failures++
		s.pairingFailures++
		if p.failures >= maxFailuresPerPIN || s.pairingFailures >= maxPairingFailures {
			s.pairing = nil
		}
		return protocol.CloseErrWrongPIN
	}
	s.pairing = nil
	return protocol.CloseErrNone
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
