package meshnet

type State int

const (
	StateDisconnected State = iota
	StateConnecting
	StateConnected
	StateDisconnecting
	StateErrored
)

var stateName = map[State]string{
	StateDisconnected:  "Disconnected",
	StateConnecting:    "Connecting",
	StateConnected:     "Connected",
	StateDisconnecting: "Disconnecting",
	StateErrored:       "Errored",
}

func (s State) String() string {
	return stateName[s]
}

type ClientInfo struct{}
