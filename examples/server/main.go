package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	meshnet "github.com/BARGHEST-ngo/MESHnet"
)

func main() {
	srv := meshnet.NewServer(meshnet.ServerConfig{
		Logf: nil,
	})

	srv.OnMessage(func(clientID string, m meshnet.Message) {
		log.Printf("echoing message received from %q: %v", clientID, m)
		if err := srv.Send(clientID, m); err != nil {
			log.Fatal("echoing message: %v", err)
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
