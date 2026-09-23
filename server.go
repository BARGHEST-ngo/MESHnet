package meshnet

import (
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"

	"github.com/tailscale/tailcat"
	"tailscale.com/types/key"
)

type tailcatServer interface {
	Start() error
	Close() error
	TailcatAddr() tailcat.Addr
}

type ServerConfig struct {
	AllowedClients []string
	Logf           func(format string, args ...any)
}

type Server struct {
	mu   sync.Mutex // guards srv, done, err
	srv  tailcatServer
	done chan struct{}
	err  error

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

	srv := s.newTailcat(allowList)
	if err := srv.Start(); err != nil {
		return "", fmt.Errorf("server: start: %w", err)
	}

	s.done = make(chan struct{})
	s.err = nil
	s.srv = srv

	return string(srv.TailcatAddr()), nil
}

func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.srv == nil {
		return nil
	}

	srv := s.srv
	s.srv = nil
	s.err = ErrClosed

	err := srv.Close()
	if err != nil {
		err = fmt.Errorf("server: stop: %w", err)
	}

	close(s.done)
	return err
}

// send message to a client
func (s *Server) Send() error {
	return nil
}

func (s *Server) Clients() []ClientInfo {
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

func parseAllowedClients(clients []string) ([]key.NodePublic, error) {
	out := make([]key.NodePublic, 0, len(clients))

	for i, c := range clients {
		k := key.NodePublic{}
		c = strings.TrimSpace(c)
		if err := k.UnmarshalText([]byte(c)); err != nil {
			return nil, fmt.Errorf("server: allow client [%d] %q: %w", i, c, err)
		}
		if k.IsZero() {
			return nil, fmt.Errorf("server: allow client [%d] %q: invalid client", i, c)
		}
		out = append(out, k)
	}

	return out, nil
}

func (s *Server) dispatch(port uint16) func(net.Conn) {
	switch port {
	default:
		return nil
	}
}
