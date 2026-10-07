package protocol

import (
	"encoding/binary"
	"fmt"
	"strings"
)

const (
	PINLength  = 6
	MaxNameLen = 64

	keyPrefix = "nodekey:"
	keyLen    = len(keyPrefix) + 64 //32 byte key hex-encoded
)

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
