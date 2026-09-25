package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

const (
	PINLength     = 6
	MaxNameLen    = 64
	MaxMessageLen = 255

	keyPrefix     = "nodekey:"
	keyLen        = len(keyPrefix) + 64 //32 byte key hex-encoded
	versionSize   = 2
	strPrefixSize = 1
	closeErrSize  = 1
)

type CloseErr uint8

const (
	CloseErrNone CloseErr = iota
	CloseErrVersionMismatch
	CloseErrWrongPIN
	CloseErrPairingLocked
	CloseErrError
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

type Hello struct {
	Version uint16
	Key     string
	Name    string
	PIN     string
}

func (h Hello) validate() error {
	if h.Version == 0 {
		return fmt.Errorf("%w: version 0 is invalid", ErrMalformedPayload)
	}

	if err := validateKey("key", h.Key); err != nil {
		return err
	}

	if len(h.Name) > MaxNameLen {
		return fmt.Errorf("%w: name length %d exceeds %d", ErrMalformedPayload, len(h.Name), MaxNameLen)
	}

	if len(h.PIN) != PINLength {
		return fmt.Errorf("%w: invalid PIN length %d, expect %d", ErrMalformedPayload, len(h.PIN), PINLength)
	}

	for _, c := range h.PIN {
		if c < '0' || c > '9' {
			return fmt.Errorf("%w: PIN must only contain digits", ErrMalformedPayload)
		}
	}

	return nil
}

func (h Hello) MarshalBinary() ([]byte, error) {
	if err := h.validate(); err != nil {
		return nil, err
	}

	b := make([]byte, 0)
	b = binary.BigEndian.AppendUint16(b, h.Version)
	b, err := appendString(b, h.Key)
	if err != nil {
		return nil, err
	}

	b, err = appendString(b, h.Name)
	if err != nil {
		return nil, err
	}

	b, err = appendString(b, h.PIN)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (h *Hello) UnmarshalBinary(b []byte) error {
	version, remaining, err := readUint16(b)
	if err != nil {
		return err
	}

	key, remaining, err := readString(remaining)
	if err != nil {
		return err
	}

	name, remaining, err := readString(remaining)
	if err != nil {
		return err
	}

	pin, remaining, err := readString(remaining)
	if err != nil {
		return err
	}

	if len(remaining) != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrMalformedPayload, len(remaining))
	}

	decoded := Hello{
		Version: version,
		Key:     key,
		Name:    name,
		PIN:     pin,
	}

	if err := decoded.validate(); err != nil {
		return err
	}

	*h = decoded
	return nil
}

type HelloAck struct {
	Version  uint16
	ClientID string
}

func (h HelloAck) validate() error {
	if h.Version == 0 {
		return fmt.Errorf("%w: version 0 is invalid", ErrMalformedPayload)
	}

	return validateKey("client ID", h.ClientID)
}

func (h HelloAck) MarshalBinary() ([]byte, error) {
	if err := h.validate(); err != nil {
		return nil, err
	}

	b := make([]byte, 0)
	b = binary.BigEndian.AppendUint16(b, h.Version)
	b, err := appendString(b, h.ClientID)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (h *HelloAck) UnmarshalBinary(b []byte) error {
	version, remaining, err := readUint16(b)
	if err != nil {
		return err
	}

	clientID, remaining, err := readString(remaining)
	if err != nil {
		return err
	}

	if len(remaining) != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrMalformedPayload, len(remaining))
	}

	decoded := HelloAck{
		Version:  version,
		ClientID: clientID,
	}

	if err := decoded.validate(); err != nil {
		return err
	}

	*h = decoded
	return nil
}

func validateKey(field, s string) error {
	if len(s) != keyLen {
		return fmt.Errorf("%w: %s length %d, want %d", ErrMalformedPayload, field, len(s), keyLen)
	}

	if !strings.HasPrefix(s, keyPrefix) {
		return fmt.Errorf("%w: %s missing %q prefix", ErrMalformedPayload, field, keyPrefix)
	}
	return nil
}

// Appends one uint8 length prefixed string to b
func appendString(b []byte, s string) ([]byte, error) {
	if len(s) > math.MaxUint8 {
		return b, fmt.Errorf("%w: string length %d exceeds %d", ErrMalformedPayload, len(s), math.MaxUint8)
	}

	b = append(b, byte(len(s)))
	b = append(b, s...)
	return b, nil
}

// Reads one uint8 length prefixed string from the front of b and returns it with the unread bytes
func readString(b []byte) (string, []byte, error) {
	if len(b) == 0 {
		return "", nil, fmt.Errorf("%w: missing string length", ErrMalformedPayload)
	}

	n := int(b[0])
	remaining := b[strPrefixSize:]
	if len(remaining) < n {
		return "", nil, fmt.Errorf("%w: string length %d exceeds %d remaining bytes", ErrMalformedPayload, n, len(remaining))
	}

	return string(remaining[:n]), remaining[n:], nil
}

func readUint16(b []byte) (uint16, []byte, error) {
	if len(b) < versionSize {
		return 0, nil, fmt.Errorf("%w: need %d bytes for uint16, got %d", ErrMalformedPayload, versionSize, len(b))
	}
	return binary.BigEndian.Uint16(b), b[versionSize:], nil
}
