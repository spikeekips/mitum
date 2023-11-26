package jsonenc

import "github.com/spikeekips/mitum/util/encoder"

type Decodable interface {
	DecodeJSON([]byte, encoder.Encoder) error
}
