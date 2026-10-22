// Copyright (c) BARGHEST
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshnet

import (
	"errors"
	"net"
	"time"
)

var ErrPeerTimeout = errors.New("meshnet: peer timeout")

const (
	pingInterval           = 10 * time.Second
	defaultLivenessTimeout = pingInterval * 3
)

type deadlineReader struct {
	conn    net.Conn
	timeout time.Duration
}

func (r deadlineReader) Read(b []byte) (int, error) {
	if err := r.conn.SetReadDeadline(time.Now().Add(r.timeout)); err != nil {
		return 0, err
	}

	return r.conn.Read(b)
}

// timeoutErr turns a read-deadline error into ErrPeerTimeout.
// It checks net.Error and not os.ErrDeadlineExceeded because tailcat's connections (gvisor)
// report deadlines with their own error type.
func timeoutErr(err error) error {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return ErrPeerTimeout
	}
	return err
}
