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

func main() {
	addr := flag.String("addr", "", "server address")
	pin := flag.String("pin", "", "pairing PIN")
	flag.Parse()

	if *addr == "" || *pin == "" {
		flag.Usage()
		os.Exit(2)
	}

	c := meshnet.NewClient(meshnet.ClientConfig{
		ServerAddr: *addr,
		PairingPIN: *pin,
		Name:       "example client",
		Logf:       log.Printf,
	})

	connectCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Connect(connectCtx); err != nil {
		log.Fatal(err)
	}

	log.Printf("connected as %s", c.ID())

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	select {
	case <-sigCtx.Done():
		if err := c.Disconnect(); err != nil {
			log.Printf("disconnect: %v", err)
		}
	}
}
