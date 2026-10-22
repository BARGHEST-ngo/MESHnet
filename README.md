# MESHnet
A small Go library that connects a server to its clients over an encrypted, peer-to-peer tunnel built on [tailcat](https://github.com/tailscale/tailcat). The server shares an address, the client joins with a one-time PIN, and from then on the two can exchange messages, open streams and, optionally, route the client's traffic out through the server.

## Use Cases

**Remote digital forensics**  
As a forensic analyst, I need to examine a phone that is in someone else's hands, possibly in another country  

**Peer-to-peer file sharing**  
As someone handling sensitive files, I need to send them directly to one trusted person over an encrypted tunnel, without uploading them to a cloud service first.  

**Private one-to-one messaging**  
As a small team or a pair of contacts, we need a direct, end-to-end encrypted channel for messages that leaves nothing behind when the session ends.  

**Remote support**  
As someone helping a colleague or relative, I need to reach a service running on their device, such as `adb`, without asking them to change router settings.

**Trusted exit for a device**  
As a traveller on hotel or conference Wi-Fi, I need my phone's traffic to leave through a machine I control, so that the local network sees only an encrypted tunnel.

**Field device check-in**  
As someone running devices in the field, I need them to report back and receive commands from my machine, with no inbound ports open at either end.
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
```

## Pairing Clients
1. The server starts and prints its `address`. Treat the address as a secret
2. The app calls `NewPairing()` to create a 6-digit PIN which is valid for one Client
3. Share the address and PIN over separate channels with at least one being end-to-end encrypted, such as Signal
4. The client connects with both, a Client with only the server address cannot join.

A PIN is single-use. Three wrong guesses cancel the pairing and a new PIN is required. 