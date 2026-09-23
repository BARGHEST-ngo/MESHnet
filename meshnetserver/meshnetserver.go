package meshnetserver

import (
	"fmt"
	"net"
	"slices"
	"strings"

	meshnet "github.com/BARGHEST-ngo/MESHnet"
	"github.com/tailscale/tailcat"
	"tailscale.com/types/key"
)

type Config struct {
	AllowedClients []string
	Logf           func(format string, args ...any)
}

type Server struct {
	srv *tailcat.Server
	cfg Config
}

func New(cfg Config) *Server {
	// nil Logf in tailcat pushes everything to log.Printf
	// if nil is sent give it an empty func to discard everything
	if cfg.Logf == nil {
		cfg.Logf = func(format string, args ...any) {}
	}
	cfg.AllowedClients = slices.Clone(cfg.AllowedClients)
	return &Server{
		cfg: cfg,
	}
}

func (s *Server) Start() (addr string, err error) {
	allowList, err := parseAllowedClients(s.cfg.AllowedClients)
	if err != nil {
		return "", err
	}

	srv := &tailcat.Server{
		OnTCP:          s.dispatch,
		Logf:           s.cfg.Logf,
		AllowedClients: allowList,
	}

	if err := srv.Start(); err != nil {
		return "", err
	}

	s.srv = srv

	return string(srv.TailcatAddr()), nil
}

func (s *Server) Stop() error {
	if s.srv == nil {
		return nil
	}
	if err := s.srv.Close(); err != nil {
		return err
	}
	s.srv = nil
	return nil
}

// send message to a client
func (s *Server) Send() error {
	return nil
}

func (s *Server) Clients() []meshnet.ClientInfo {
	return make([]meshnet.ClientInfo, 0)
}

func parseAllowedClients(clients []string) ([]key.NodePublic, error) {
	out := make([]key.NodePublic, 0, len(clients))

	for i, c := range clients {
		k := key.NodePublic{}
		c = strings.TrimSpace(c)
		if err := k.UnmarshalText([]byte(c)); err != nil {
			return nil, fmt.Errorf("meshnetserver allow client [%d] %q: %w", i, c, err)
		}
		if k.IsZero() {
			return nil, fmt.Errorf("meshnetserver allow client [%d] %q: invalid client", i, c)
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

func (s *Server) handleADB(nc net.Conn)     {}
func (s *Server) handleCapture(nc net.Conn) {}
func (s *Server) handleMessage(nc net.Conn) {}
