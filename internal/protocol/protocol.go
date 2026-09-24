package protocol

import "errors"

const Version uint16 = 1

var (
	ErrMalformedPayload = errors.New("protocol: malformed payload")
)
