<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/meshnet-dark.png">
  <img src="assets/meshnet-light.png" alt="MESHnet" width="360">
</picture>

**An open protocol for peer-to-peer, end-to-end encrypted connections between devices. No accounts, no control plane, a fresh session every time.**

[![Go Reference](https://pkg.go.dev/badge/github.com/BARGHEST-ngo/MESHnet.svg)](https://pkg.go.dev/github.com/BARGHEST-ngo/MESHnet)
[![License: AGPL-3.0-or-later](https://img.shields.io/badge/license-AGPL--3.0--or--later-blue.svg)](LICENSE)
![Status: pre-alpha](https://img.shields.io/badge/status-pre--alpha-orange.svg)

> **Pre-alpha.** The API and wire protocol will change. MESHnet has not had an independent security audit.

## What is MESHnet?

MESHnet lets one device reach another with an **address** and a **6-digit PIN**. It finds the best path between the two, wherever they are, and keeps the connection encrypted end to end.

- **Peer-to-peer.** Direct connections with NAT traversal, and a relay fallback when that fails. No open ports or router changes.
- **End-to-end encrypted.** All traffic runs inside a WireGuard tunnel. Relays cannot read it.
- **No control plane.** Unlike Tailscale, there are no accounts and no coordination server. Unlike plain WireGuard, there are no keys to copy or endpoints to configure.
- **Ephemeral.** New keys on every server start. A PIN works once and expires after 10 minutes.

Built on MESHnet: [MESHdrop](https://github.com/BARGHEST-ngo/MESHdrop) (private file transfer) and [MESHforensics](https://github.com/BARGHEST-ngo/MESHforensics) (remote mobile forensics, integration in progress).

## Protocol

MESHnet is a small session protocol that runs inside a WireGuard tunnel provided by [tailcat](https://github.com/tailscale/tailcat), which gives it Tailscale's data plane without Tailscale's control plane.

1. The server starts and gets a fresh address that encodes its public key.
2. The address and a PIN are shared with the client out of band, for example over Signal.
3. The client connects. The tunnel is bootstrapped through a DERP relay, then upgrades to a direct connection when NAT allows.
4. Inside the tunnel, the client sends its PIN. The server checks it, and checks that the client's key matches the tunnel peer.

On top of the tunnel, apps get **messages** (`{Topic, Data}` in both directions), **streams** (raw `net.Conn` pipes, for example to proxy `adb`) and **forwarding** (the client's TCP and UDP traffic exits through the server).

**Limits to know:** three wrong PINs cancel a pairing, and a server accepts one client at a time. The address is the real secret, so share it like a password. Relays cannot read traffic, but they can see IP addresses, timing and data volume. MESHnet is not an anonymity tool.

## Getting started

Requires Go 1.27.1 or later.

```sh
go get github.com/BARGHEST-ngo/MESHnet
```

For Android, a client AAR (`meshnetmobile`) is attached to each [release](https://github.com/BARGHEST-ngo/MESHnet/releases).

Try the examples on two machines:

```console
$ go run ./examples/server
2026/11/04 13:30:01 server address: tcomFwWCCcjS5nKNqAod034nWoJZ...
2026/11/04 13:30:01 server pairing PIN: 482913

$ go run ./examples/client -addr 'tcomFwWCCcjS5nKNqAod034nWoJZ...' -pin 482913
```

## Usage

**Server**

```go
srv := meshnet.NewServer(meshnet.ServerConfig{
	OnMessage: func(clientID string, m meshnet.Message) {
		log.Printf("%s: %s %q", clientID, m.Topic, m.Data)
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

**Client**

```go
c := meshnet.NewClient(meshnet.ClientConfig{
	ServerAddr: addr,
	PairingPIN: pin,
	Name:       "my device",
	OnMessage: func(m meshnet.Message) {
		log.Printf("received: %s %q", m.Topic, m.Data)
	},
})

if err := c.Connect(ctx); err != nil {
	log.Fatal(err)
}
defer c.Disconnect()

c.Send(meshnet.Message{Topic: "greeting", Data: []byte("hello")})
```

Streams (`srv.RequestStream`) and forwarding (`ServerConfig.Forward`, `srv.SetForwarding`, `c.DialTCP`, `c.DialUDP`) are covered in the [Go docs](https://pkg.go.dev/github.com/BARGHEST-ngo/MESHnet).

## License

MESHnet is licensed under the [GNU Affero General Public License v3.0 or later](LICENSE). If you distribute software built on MESHnet, or run a modified version as a network service, you must make its source available under the same license.

## Contribution

Bug reports, ideas and pull requests are welcome. Please open an [issue](https://github.com/BARGHEST-ngo/MESHnet/issues) before starting larger changes, and run the tests before opening a pull request:

```sh
go test -race ./...
```

Please report security issues privately to [BARGHEST](https://barghest.asia) rather than in a public issue.
