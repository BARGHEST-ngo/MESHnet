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
		Logf: log.Printf,
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
		log.Print("server stopped: %v", srv.Err())
	}
}
