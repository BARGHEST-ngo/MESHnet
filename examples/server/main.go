package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"

	meshnet "github.com/BARGHEST-ngo/MESHnet"
)

func main() {
	verbose := flag.Bool("v", false, "verbose logging (MESHnet and tailcat)")
	flag.Parse()

	var logf func(format string, args ...any)
	if *verbose {
		logf = log.Printf
	}
	srv := meshnet.NewServer(meshnet.ServerConfig{
		Logf: logf,
	})

	srv.OnMessage(func(clientID string, m meshnet.Message) {
		log.Printf("echoing message from %s: %s %q", clientID, m.Topic, m.Data)
		if err := srv.Send(clientID, m); err != nil {
			log.Fatalf("echoing message: %v", err)
		}
	})

	addr, err := srv.Start()
	if err != nil {
		log.Fatalf("server start: %v", err)
	}

	log.Printf("server address: %s", addr)
	log.Printf("server pairing PIN: %s", srv.PairingPIN())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	select {
	case <-ctx.Done():
		if err := srv.Stop(); err != nil {
			log.Printf("stop: %v", err)
		}
	case <-srv.Done():
		log.Printf("server stopped: %v", srv.Err())
	}
}
