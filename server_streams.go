package meshnet

import (
	"net"
	"time"

	"github.com/BARGHEST-ngo/MESHnet/internal/protocol"
	"tailscale.com/types/key"
)

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

	sess, err := s.sessionForClient(peer.String())
	if err != nil {
		s.cfg.Logf("meshnet: stream: %v", err)
		conn.Close()
		return
	}

	ch, ok := sess.deliverStream(id, conn)
	if !ok {
		conn.Close() // unknown request, or the session has closed
		return
	}
	ch <- conn
}
