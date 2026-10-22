// Copyright (c) BARGHEST
// SPDX-License-Identifier: AGPL-3.0-or-later

package protocol

import "fmt"

type CloseErr uint8

const (
	CloseErrNone CloseErr = iota
	CloseErrVersionMismatch
	CloseErrWrongPIN
	CloseErrPairingLocked
	CloseErrInvalidKey
	CloseErrError
)

const (
	MaxMessageLen = 255

	closeErrSize = 1
)

func (e CloseErr) String() string {
	switch e {
	case CloseErrNone:
		return "none"
	case CloseErrVersionMismatch:
		return "version_mismatch"
	case CloseErrWrongPIN:
		return "wrong_pin"
	case CloseErrPairingLocked:
		return "pairing_locked"
	case CloseErrInvalidKey:
		return "invalid_key"
	case CloseErrError:
		return "error"
	default:
		return fmt.Sprintf("[%d] unknown error", e)
	}
}

type Close struct {
	Err     CloseErr
	Message string
}

func (c Close) validate() error {
	if len(c.Message) > MaxMessageLen {
		return fmt.Errorf("%w: message length %d exceeds %d", ErrMalformedPayload, len(c.Message), MaxMessageLen)
	}
	return nil
}

func (c Close) MarshalBinary() ([]byte, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}

	b := make([]byte, 0)
	b = append(b, byte(c.Err))
	b, err := appendString(b, c.Message)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (c *Close) UnmarshalBinary(b []byte) error {
	if len(b) < closeErrSize {
		return fmt.Errorf("%w: missing close error code", ErrMalformedPayload)
	}

	closeErr := CloseErr(b[0])
	msg, remaining, err := readString(b[closeErrSize:])
	if err != nil {
		return err
	}

	if len(remaining) != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrMalformedPayload, len(remaining))
	}

	decoded := Close{
		Err:     closeErr,
		Message: msg,
	}
	if err := decoded.validate(); err != nil {
		return err
	}

	*c = decoded
	return nil
}
