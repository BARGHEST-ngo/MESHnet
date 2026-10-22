// Copyright (c) BARGHEST
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshnetmobile

import (
	"context"
	"net"
	"runtime"
	"time"

	"tailscale.com/net/netmon"
)

// Ported from the tailcat POC as a stopgap
//
// Android does not let apps list network interfaces the way Go does, so
// tailscale's network monitor fails to start with "netlinkrib: permission
// denied". Until the app can supply real network details, we report a single
// made-up interface and resolve DNS through fixed public servers.
//
// TODO: have the consuming app provide its interfaces and DNS servers instead
func init() {
	if runtime.GOOS != "android" {
		return
	}
	netmon.RegisterInterfaceGetter(syntheticInterfaces)
	useBootstrapDNS()
}

func syntheticInterfaces() ([]netmon.Interface, error) {
	iface := netmon.Interface{
		Interface: &net.Interface{
			Index: 1,
			MTU:   1500,
			Name:  "wlan0",
			Flags: net.FlagUp | net.FlagBroadcast | net.FlagRunning | net.FlagMulticast,
		},
	}
	if ip := outboundIP(); ip != nil {
		iface.AltAddrs = []net.Addr{&net.IPNet{IP: ip, Mask: net.CIDRMask(24, 32)}}
	}
	return []netmon.Interface{iface}, nil
}

func useBootstrapDNS() {
	net.DefaultResolver.PreferGo = true
	net.DefaultResolver.Dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
		d := net.Dialer{Timeout: 5 * time.Second}
		if c, err := d.DialContext(ctx, "udp", "8.8.8.8:53"); err == nil {
			return c, nil
		}
		return d.DialContext(ctx, "udp", "1.1.1.1:53")
	}
}

// outboundIP returns the local address the kernel would use to reach the public internet
func outboundIP() net.IP {
	c, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil
	}
	defer c.Close()
	if ua, ok := c.LocalAddr().(*net.UDPAddr); ok {
		return ua.IP
	}
	return nil
}
