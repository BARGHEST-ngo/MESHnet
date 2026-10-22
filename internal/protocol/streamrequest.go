// Copyright (c) BARGHEST
// SPDX-License-Identifier: AGPL-3.0-or-later

package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

type StreamRequest struct {
	ID     uint64
	Target string
}

func (r StreamRequest) validate() error {
	if r.ID == 0 {
		return fmt.Errorf("%w: missing stream request ID", ErrMalformedPayload)
	}

	if r.Target == "" {
		return fmt.Errorf("%w: missing target", ErrMalformedPayload)
	}
	return nil
}

func (r StreamRequest) MarshalBinary() ([]byte, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}

	b := binary.BigEndian.AppendUint64(nil, r.ID)
	return appendString(b, r.Target)
}

func (r *StreamRequest) UnmarshalBinary(b []byte) error {
	id, remaining, err := readUint64(b)
	if err != nil {
		return err
	}

	target, remaining, err := readString(remaining)
	if err != nil {
		return err
	}

	if len(remaining) != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrMalformedPayload, len(remaining))
	}

	decoded := StreamRequest{
		ID:     id,
		Target: target,
	}
	if err := decoded.validate(); err != nil {
		return err
	}
	*r = decoded
	return nil
}

func WriteStreamID(w io.Writer, id uint64) error {
	_, err := w.Write(binary.BigEndian.AppendUint64(nil, id))
	return err
}

func ReadStreamID(r io.Reader) (uint64, error) {
	var b [uint64Size]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, fmt.Errorf("protocol: read stream id: %w", err)
	}

	id := binary.BigEndian.Uint64(b[:])
	if id == 0 {
		return 0, fmt.Errorf("%w: stream id 0", ErrMalformedPayload)
	}

	return id, nil
}
