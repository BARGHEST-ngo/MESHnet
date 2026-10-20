package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	meshnet "github.com/BARGHEST-ngo/MESHnet"
)

// First run `go run ./examples/server`
// Copy the address and pin output
// `go run ./examples/client -addr '<addr>' -pin <pin> [-v]`
func main() {
	verbose := flag.Bool("v", false, "verbose logging (MESHnet and tailcat)")
	addr := flag.String("addr", "", "server address")
	pin := flag.String("pin", "", "pairing PIN")
	dns := flag.String("dns", "", "look up this name through the server (needs forwarding on)")
	myip := flag.Bool("myip", false, "fetch this client's public IP through the server (needs forwarding on)")

	flag.Parse()

	if *addr == "" || *pin == "" {
		flag.Usage()
		os.Exit(2)
	}
	var logf func(format string, args ...any)
	if *verbose {
		logf = log.Printf
	}

	ended := make(chan error, 1)
	c := meshnet.NewClient(meshnet.ClientConfig{
		ServerAddr: *addr,
		PairingPIN: *pin,
		Name:       "example client",
		Logf:       logf,
		OnMessage: func(m meshnet.Message) {
			log.Printf("received: %s %q", m.Topic, m.Data)
		},
		OnStateChange: func(s meshnet.State, e error) {
			log.Printf("state: %v (%v)", s, e)
			if s == meshnet.StateDisconnected || s == meshnet.StateErrored {
				select {
				case ended <- e:
				default:
				}
			}
		},
		OnStreamRequest: func(target string, stream net.Conn) {
			defer stream.Close()
			if target != "echo" {
				log.Printf("refusing stream for %q", target)
				return
			}
			log.Printf("stream opened for %q, echoing", target)
			io.Copy(stream, stream)
		},
	})

	connectCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Connect(connectCtx); err != nil {
		log.Fatal(err)
	}

	log.Printf("connected as %s", c.ID())

	if err := c.Send(meshnet.Message{Topic: "test topic", Data: []byte("hello world")}); err != nil {
		log.Fatal(err)
	}

	if *dns != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		addrs, err := resolverThrough(c).LookupHost(ctx, *dns)
		cancel()
		log.Printf("dns: %s -> %v (err %v)", *dns, addrs, err)
	}

	if *myip {
		resp, err := httpThrough(c).Get("http://icanhazip.com")
		if err != nil {
			log.Printf("myip: %v", err)
		} else {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Printf("myip: %s", strings.TrimSpace(string(body)))
		}
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	select {
	case <-sigCtx.Done():
		if err := c.Disconnect(); err != nil {
			log.Printf("disconnect: %v", err)
		}
	case err := <-ended:
		log.Printf("connection ended: %v", err)
		os.Exit(1)
	}
}

func resolverThrough(c *meshnet.Client) *net.Resolver {
	dns := netip.MustParseAddrPort("1.1.1.1:53")
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			if network == "tcp" {
				return c.DialTCP(ctx, dns)
			}
			return c.DialUDP(ctx, dns)
		},
	}
}

func httpThrough(c *meshnet.Client) *http.Client {
	r := resolverThrough(c)
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(addr)
				if err != nil {
					return nil, err
				}
				ips, err := r.LookupNetIP(ctx, "ip4", host)
				if err != nil {
					return nil, err
				}
				p, err := strconv.ParseUint(port, 10, 16)
				if err != nil {
					return nil, err
				}
				return c.DialTCP(ctx, netip.AddrPortFrom(ips[0], uint16(p)))
			},
		},
	}
}
