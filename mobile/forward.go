// Copyright (c) BARGHEST
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshnetmobile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"syscall"
	"time"

	meshnet "github.com/BARGHEST-ngo/MESHnet"
	"github.com/tailscale/tailcat"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/fdbased"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"
)

var (
	errAlreadyForwarding = errors.New("meshnetmobile: already forwarding")
)

const (
	forwardMTU     = 1280
	udpIdleTimeout = 60 * time.Second
)

type forwarder struct {
	dialer dialer
	stack  *stack.Stack
	tun    *os.File
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	conns  map[io.Closer]struct{} // live flows, both sides
	closed bool
}

type idleConn struct {
	meshnet.PacketConn
	timeout time.Duration
}

func (c idleConn) Read(b []byte) (int, error) {
	c.SetDeadline(time.Now().Add(c.timeout))
	return c.PacketConn.Read(b)
}

func (c idleConn) Write(b []byte) (int, error) {
	c.SetDeadline(time.Now().Add(c.timeout))
	return c.PacketConn.Write(b)
}

// dropOversize drops datagrams too big for the tunnel instead of failing the flow
type dropOversize struct{ meshnet.PacketConn }

func (c dropOversize) Write(b []byte) (int, error) {
	if len(b) > tailcat.MaxUDPPayload {
		return len(b), nil
	}
	return c.PacketConn.Write(b)
}

type dialer interface {
	DialTCP(ctx context.Context, dst netip.AddrPort) (net.Conn, error)
	DialUDP(ctx context.Context, dst netip.AddrPort) (meshnet.PacketConn, error)
}

func newForwarder(fd int, mtu uint32, d dialer) (*forwarder, error) {
	tunFile := os.NewFile(uintptr(fd), "tun")
	ep, err := fdbased.New(&fdbased.Options{
		FDs:            []int{fd},
		MTU:            mtu,
		EthernetHeader: false,
	})
	if err != nil {
		tunFile.Close()
		return nil, fmt.Errorf("meshnetmobile: new fd endoint: %w", err)
	}

	s := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol, ipv6.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol},
	})

	// Create disabled
	if err := s.CreateNICWithOptions(1, ep, stack.NICOptions{Disabled: true}); err != nil {
		tunFile.Close()
		s.Close()
		return nil, fmt.Errorf("meshnetmobile: createNIC: %v", err)
	}

	s.SetRouteTable([]tcpip.Route{
		{Destination: header.IPv4EmptySubnet, NIC: 1},
		{Destination: header.IPv6EmptySubnet, NIC: 1},
	})

	if err := s.SetSpoofing(1, true); err != nil {
		tunFile.Close()
		s.Close()
		return nil, fmt.Errorf("meshnetmobile: setSpoofing: %v", err)
	}

	if err := s.SetPromiscuousMode(1, true); err != nil {
		tunFile.Close()
		s.Close()
		return nil, fmt.Errorf("meshnetmobile: setPromiscuous: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	f := &forwarder{
		tun:    tunFile,
		stack:  s,
		dialer: d,
		ctx:    ctx,
		cancel: cancel,
		conns:  make(map[io.Closer]struct{}),
	}
	tcpFwd := tcp.NewForwarder(s, 0, 1024, f.handleTCP)
	s.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpFwd.HandlePacket)

	udpFwd := udp.NewForwarder(s, f.handleUDP)
	s.SetTransportProtocolHandler(udp.ProtocolNumber, udpFwd.HandlePacket)

	// Now start reading packets
	if err := s.EnableNIC(1); err != nil {
		return nil, fmt.Errorf("meshnetmobile: enableNIC: %v", err)
	}
	return f, nil
}

func (f *forwarder) handleTCP(fr *tcp.ForwarderRequest) {
	id := fr.ID()
	dst := netip.AddrPortFrom(addrFrom(id.LocalAddress), id.LocalPort)

	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	out, err := f.dialer.DialTCP(ctx, dst)
	if err != nil {
		fr.Complete(true)
		return
	}

	var wq waiter.Queue
	ep, tcpErr := fr.CreateEndpoint(&wq)
	if tcpErr != nil {
		fr.Complete(true)
		out.Close()
		return
	}
	fr.Complete(false)
	in := gonet.NewTCPConn(&wq, ep)

	if !f.track(in) {
		out.Close()
		return
	}
	defer f.untrack(in)
	tailcat.ProxyConns(in, out)
}

func (f *forwarder) handleUDP(fr *udp.ForwarderRequest) bool {
	id := fr.ID()
	dst := netip.AddrPortFrom(addrFrom(id.LocalAddress), id.LocalPort)

	var wq waiter.Queue
	ep, err := fr.CreateEndpoint(&wq)
	if err != nil {
		return false
	}
	go f.proxyUDP(gonet.NewUDPConn(&wq, ep), dst)
	return true
}

// proxyUDP relays one flow between the device and dst through the server
func (f *forwarder) proxyUDP(in *gonet.UDPConn, dst netip.AddrPort) {
	if !f.track(in) {
		return
	}
	defer f.untrack(in)

	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	out, err := f.dialer.DialUDP(ctx, dst)
	cancel()
	if err != nil {
		in.Close()
		return
	}
	tailcat.ProxyPacketConns(idleConn{in, udpIdleTimeout}, dropOversize{out})
}

// addrFrom converts a gVisor address, with IPv4 unmapped
func addrFrom(a tcpip.Address) netip.Addr {
	ip, _ := netip.AddrFromSlice(a.AsSlice())
	return ip.Unmap()
}

func (f *forwarder) track(c io.Closer) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		c.Close()
		return false
	}
	f.conns[c] = struct{}{}
	return true
}

func (f *forwarder) untrack(c io.Closer) {
	f.mu.Lock()
	delete(f.conns, c)
	f.mu.Unlock()
}

func (f *forwarder) stop() {
	f.cancel()

	f.mu.Lock()
	f.closed = true
	conns := f.conns
	f.conns = nil
	f.mu.Unlock()

	for c := range conns {
		c.Close()
	}
	f.stack.RemoveNIC(1) // stop the fd reader and wait for it
	f.stack.Close()
	f.tun.Close()
}

func (c *Client) StartForwarding(fd int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fwd != nil {
		syscall.Close(fd)
		return errAlreadyForwarding
	}
	if c.c.State() != meshnet.StateConnected {
		syscall.Close(fd)
		return meshnet.ErrNotConnected
	}
	f, err := newForwarder(fd, forwardMTU, c.c)
	if err != nil {
		return err
	}
	c.fwd = f
	return nil
}

func (c *Client) StopForwarding() {
	c.mu.Lock()
	fwd := c.fwd
	c.fwd = nil
	c.mu.Unlock()

	if fwd != nil {
		fwd.stop()
	}
}
