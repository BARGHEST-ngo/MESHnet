package protocol

import "fmt"

type Msg struct {
	Topic string
	Data  []byte
}

func (m Msg) validate() error {
	if m.Topic == "" {
		return fmt.Errorf("%w: missing topic string", ErrMalformedPayload)
	}
	return nil
}

func (m Msg) MarshalBinary() ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}

	b := make([]byte, 0)
	b, err := appendString(b, m.Topic)
	if err != nil {
		return nil, err
	}
	b = append(b, m.Data...)
	return b, nil
}

func (m *Msg) UnmarshalBinary(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("%w: missing Topic string", ErrMalformedPayload)
	}

	topic, remaining, err := readString(b)
	if err != nil {
		return err
	}

	decoded := Msg{
		Topic: topic,
		Data:  remaining,
	}

	if err := decoded.validate(); err != nil {
		return err
	}

	*m = decoded
	return nil
}
