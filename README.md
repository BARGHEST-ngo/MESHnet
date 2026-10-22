# MESHnet
A small Go library that connects a server to its clients over an encrypted, peer-to-peer tunnel built on [tailcat](https://github.com/tailscale/tailcat). The server shares an address, the client joins with a one-time PIN, and from then on the two can exchange messages, open streams and, optionally, route the client's traffic out through the server.

## Features

- **PIN Pairing** Client access to the server is restricted behind PIN access, shared out-of-band. There are no accounts, no pre-shared key lists, and no coordination server to run.
- **Messagess** Small `{Topic, Data}` messages in both directions
- **Streams** The server can request a client for a raw `net.Conn` for bulk transfer or for proxying a local service (for example `adb`).
- **Forwarding** A paired client can send its TCP and UDP traffic out through the server's network
- **Ephemeral** Nothing is persisted. The server gets a new key and address on every start, and a lost connection (intentional or not) is a finished session - a Client must reconnect from scratch with a new PIN

## Install
Requires Go 1.27 or later.

## Quick start
Run the example server and pass its address and PIN to the example client:

```sh
go run ./examples/server
# server address: <addr>
# server pairing PIN: 123456

go run ./examples/client -addr '<addr>' -pin 123456
```

Add `-v` to either for verbose logging  

### Basic Usage
#### Server
```go
srv := meshnet.NewServer(meshnet.ServerConfig{
	OnMessage: func(clientID string, m meshnet.Message) {
		log.Printf("%s: %s %q", clientID, m.Topic, m.Data)
	},
	OnClientConnected: func(c meshnet.ClientInfo) {
		log.Printf("connected: %s (%s)", c.ID, c.Name)
	},
})

addr, err := srv.Start()
if err != nil {
    log.Fatal(err)
}

pin, err := srv.NewPairing()
if err != nil {
    log.Fatal(err)
}

log.Printf("address: %s PIN: %s", addr, pin)
<-srv.Done()
```

>NOTE: Only one Client is permitted to connect per `pairing`. For multiple clients, generate multiple PINs and share one to each Client wishing to connect

#### Client
```go
c := meshnet.NewClient(meshnet.ClientConfig{
    ServerAddr: addr,
	PairingPIN: pin,
    Name:       "my device",
    OnMessage: func(m meshnet.Message) {
		log.Printf("received: %s %q", m.Topic, m.Data)
	},
    OnStreamRequest: func(target string, stream net.Conn) {
		defer stream.Close()
		// Handle the stream, or return to refuse it.
	},
})

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := c.Connect(ctx); err != nil {
    log.Fatal(err)
}
defer c.Disconnect()

c.Send(meshnet.Message{Topic: "greeting", Data: []byte("hello")})
```

#### Forwarding
Network forwarding is disabled by default on the server and off per-client until the app turns it on

```go
srv := meshnet.NewServer(meshnet.ServerConfig{
    Foward: true,
    //...
})

// After a client connects:
srv.SetForwarding(clientID, true)

// On the Client:
conn, err := c.DialTCP(ctx, netip.MustParseAddrPort("93.184.215.14:443"))