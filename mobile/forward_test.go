// Copyright (c) BARGHEST
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshnetmobile

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"syscall"
	"testing"
	"time"

	meshnet "github.com/BARGHEST-ngo/MESHnet"
	"github.com/tailscale/tailcat"
)

// tunPair stands in for the VPN: fd goes to the forwarder, peer is the other end
func tunPair(t *testing.T) (fd int, peer *os.File) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	peer = os.NewFile(uintptr(fds[1]), "peer")
	t.Cleanup(func() { peer.Close() })
	return fds[0], peer
}

// fakeDialer is a dialer that is never reached in these tests
type fakeDialer struct{}

func (fakeDialer) DialTCP(context.Context, netip.AddrPort) (net.Conn, error) {
	return nil, errors.New("not used")
}

func (fakeDialer) DialUDP(context.Context, netip.AddrPort) (meshnet.PacketConn, error) {
	return nil, errors.New("not used")
}

// udpPair returns two loopback UDP sockets, a connected to b
func udpPair(t *testing.T) (a, b *net.UDPConn) {
	t.Helper()
	b, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	a, err = net.DialUDP("udp", nil, b.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

func TestStartForwardingNotConnected(t *testing.T) {
	c := NewClient("addr", "123456", "test", nil)
	fd, peer := tunPair(t)

	if err := c.StartForwarding(fd); !errors.Is(err, meshnet.ErrNotConnected) {
		t.Fatalf("err = %v, want ErrNotConnected", err)
	}
	if _, err := peer.Write([]byte{0}); err == nil {
		t.Fatal("fd still open, want closed")
	}
}

func TestStopForwardingWhenNotForwarding(t *testing.T) {
	c := NewClient("addr", "123456", "test", nil)
	c.StopForwarding()
	c.StopForwarding()
}

func TestForwarderStop(t *testing.T) {
	fd, peer := tunPair(t)
	f, err := newForwarder(fd, forwardMTU, fakeDialer{})
	if err != nil {
		t.Fatalf("newForwarder: %v", err)
	}

	tracked, other := net.Pipe()
	defer other.Close()
	if !f.track(tracked) {
		t.Fatal("track before stop refused")
	}

	f.stop()

	if _, err := peer.Write([]byte{0}); err == nil {
		t.Fatal("fd still open, want closed")
	}
	select {
	case <-f.ctx.Done():
	default:
		t.Fatal("dial context not cancelled")
	}
	other.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := other.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("tracked conn: read %v, want EOF", err)
	}

	late, lateOther := net.Pipe()
	defer lateOther.Close()
	if f.track(late) {
		t.Fatal("track after stop accepted")
	}
	lateOther.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := lateOther.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("late conn: read %v, want EOF", err)
	}
}

func TestIdleConnTimesOut(t *testing.T) {
	a, _ := udpPair(t)
	c := idleConn{a, 50 * time.Millisecond}

	start := time.Now()
	_, err := c.Read(make([]byte, 1))
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("read err %v, want a timeout", err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("timed out early")
	}
}

func TestIdleConnTrafficKeepsAlive(t *testing.T) {
	a, b := udpPair(t)
	timeout := 100 * time.Millisecond
	c := idleConn{a, timeout}

	go func() {
		for range 6 {
			time.Sleep(timeout / 2)
			b.WriteToUDP([]byte{1}, a.LocalAddr().(*net.UDPAddr))
		}
	}()
	for i := range 6 {
		if _, err := c.Read(make([]byte, 1)); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
}

func TestDropOversize(t *testing.T) {
	a, b := udpPair(t)
	c := dropOversize{a}
	buf := make([]byte, 2048)

	big := make([]byte, tailcat.MaxUDPPayload+1)
	if n, err := c.Write(big); err != nil || n != len(big) {
		t.Fatalf("oversize write: %d, %v; want %d, nil", n, err, len(big))
	}
	b.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := b.Read(buf); err == nil {
		t.Fatal("oversize datagram was sent")
	}

	fits := make([]byte, tailcat.MaxUDPPayload)
	if _, err := c.Write(fits); err != nil {
		t.Fatalf("write: %v", err)
	}
	b.SetReadDeadline(time.Now().Add(time.Second))
	n, err := b.Read(buf)
	if err != nil || n != len(fits) {
		t.Fatalf("read %d, %v; want %d", n, err, len(fits))
	}
}
