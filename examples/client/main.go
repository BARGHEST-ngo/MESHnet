package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
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
