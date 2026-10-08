package main

import (
	"bufio"
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"time"

	meshnet "github.com/BARGHEST-ngo/MESHnet"
)

func main() {
	verbose := flag.Bool("v", false, "verbose logging (MESHnet and tailcat)")
	flag.Parse()

	var logf func(format string, args ...any)
	if *verbose {
		logf = log.Printf
	}

	var srv *meshnet.Server
	srv = meshnet.NewServer(meshnet.ServerConfig{
		Logf: logf,
		OnMessage: func(clientID string, m meshnet.Message) {
			log.Printf("echoing message from %s: %s %q", clientID, m.Topic, m.Data)
			if err := srv.Send(clientID, m); err != nil {
				log.Fatalf("echoing message: %v", err)
			}
		},
		OnClientConnected: func(c meshnet.ClientInfo) {
			log.Printf("client connected: %s (%s)", c.ID, c.Name)
			go streamDemo(srv, c.ID)
		},
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

func streamDemo(srv *meshnet.Server, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := srv.RequestStream(ctx, id, "echo")
	if err != nil {
		log.Printf("request stream: %v", err)
		return
	}
	defer stream.Close()

	if _, err := io.WriteString(stream, "hello over a stream\n"); err != nil {
		log.Printf("stream write: %v", err)
		return
	}

	reply, err := bufio.NewReader(stream).ReadString('\n')
	if err != nil {
		log.Printf("stream read: %v", err)
		return
	}

	log.Printf("stream echo: %q", reply)
}
