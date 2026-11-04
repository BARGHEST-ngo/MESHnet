<picture>
  <source width="250" srcset="assets/meshnet-dark.png" media="(prefers-color-scheme: dark)" />
  <source width="250" srcset="assets/meshnet-light.png" media="(prefers-color-scheme: light)" />
  <img width="250" src="assets/meshnet-light.png" alt="MESHnet" />
</picture>

[Protocol](#protocol) | [Getting started](#getting-started) | [Usage](#usage) | [License](#license) | [Contribution](#contribution)

[![Go Reference](https://pkg.go.dev/badge/github.com/BARGHEST-ngo/MESHnet.svg)](https://pkg.go.dev/github.com/BARGHEST-ngo/MESHnet)
[![License: AGPL-3.0-or-later](https://img.shields.io/badge/license-AGPL--3.0--or--later-blue.svg)](LICENSE)
![Status: pre-alpha](https://img.shields.io/badge/status-pre--alpha-orange.svg)

**An open protocol for peer-to-peer, end-to-end encrypted connections between devices**

MESHnet lets one device reach another with an address and a 6-digit PIN. It finds the best path between the two, wherever they are, and keeps the connection encrypted end to end inside a WireGuard tunnel. There are no accounts and no control plane, and the Authority gets new keys every time it starts. MESHnet is a Go library with an Android client, and it already powers [MESHdrop](https://github.com/BARGHEST-ngo/MESHdrop) for private file transfer, with [MESHforensics](https://github.com/BARGHEST-ngo/MESHforensics) integration in progress.

>_Note: MESHnet is pre-alpha. The API and wire protocol will change, and it has not yet had an independent security audit._

## What is MESHnet?

- **Peer-to-peer.** Direct connections with NAT traversal, and a relay fallback when that fails. No open ports or router changes.
- **End-to-end encrypted.** All traffic runs inside a WireGuard tunnel. Relays cannot read it.
- **No control plane.** Unlike Tailscale, there are no accounts and no coordination server. Unlike plain WireGuard, there are no keys to copy or endpoints to configure.
- **Works where UDP is blocked.** If UDP or WireGuard is blocked, for example by deep packet inspection, traffic falls back to the relay over HTTPS on port 443, still end-to-end encrypted.
- **Ephemeral.** New keys every time the Authority starts. A PIN works once and expires after 10 minutes.

## Protocol

MESHnet connects two devices with different roles: an **Authority** and a **Peer**. The Authority is one of the two devices, not a third-party server. The connection runs inside a WireGuard tunnel provided by [tailcat](https://github.com/tailscale/tailcat), which gives MESHnet Tailscale's data plane without Tailscale's control plane.

1. The Authority starts with new keys and waits at a DERP relay. Its address holds its public keys and the relay to meet at. It does **not** contain the Authority's IP address, so a leaked address doesn't reveal where the device is.
2. The Authority shares the address and a PIN with the Peer out of band, for example over Signal.
3. The Peer meets the Authority at the relay, and the encrypted WireGuard tunnel comes up through it.
4. Inside the tunnel, the Peer sends the PIN. The Authority checks it, and checks that the Peer's key matches the tunnel.
5. In parallel, the two swap their IP:port endpoints over the relay and try UDP hole-punching. If it works, traffic moves to the direct path. If not, it stays on the relay, still encrypted.

| | Authority | Peer |
| --- | --- | --- |
| Keys and address | New on every start | New for every session |
| Admission | Issues and checks PINs, can disconnect the Peer | Presents the PIN |
| Messages | Sends and receives `{Topic, Data}` | Sends and receives `{Topic, Data}` |
| Streams | Requests raw `net.Conn` pipes by name, for example `adb` | Accepts or refuses each request |
| Forwarding | Grants it per Peer, off by default | Sends TCP and UDP traffic out through the Authority |
| Session | Ends when the Authority stops | Can leave at any time |

In the Go API, the Authority is `meshnet.Server` and the Peer is `meshnet.Client`.

**Limits to know:** three wrong PINs cancel a pairing, and an Authority accepts one Peer at a time. The address is the real secret, so share it like a password. Relays cannot read traffic, but they can see IP addresses, timing and data volume. Once connected, each device can learn the other's IP address. MESHnet is not an anonymity tool.

## Getting started

Requires Go 1.27.1 or later.

```sh
go get github.com/BARGHEST-ngo/MESHnet
```

For Android, an AAR (`meshnetmobile`, Peer role only) is attached to each [release](https://github.com/BARGHEST-ngo/MESHnet/releases).

Try the examples on two machines:

```console
$ go run ./examples/server
2026/11/04 13:30:01 server address: tcomFwWCCcjS5nKNqAod034nWoJZ...
2026/11/04 13:30:01 server pairing PIN: 482913

$ go run ./examples/client -addr 'tcomFwWCCcjS5nKNqAod034nWoJZ...' -pin 482913
```

## Usage

**Authority (`Server`)**

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

**Peer (`Client`)**

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
