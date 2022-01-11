package jsonenc

import (
	"bytes"

	"github.com/spikeekips/mitum/util/hint"
)

const (
	NULL      = "null"
	HintedTag = "_hint"
)

var nullbytes = []byte("null")

type HintedHead struct {
	H hint.Hint `json:"_hint"`
}

func NewHintedHead(h hint.Hint) HintedHead {
	return HintedHead{H: h}
}

type Decodable interface {
	DecodeJSON([]byte, *Encoder) error
}

func isNil(b []byte) bool {
	i := bytes.TrimSpace(b)

	return len(i) < 1 || bytes.Equal(i, nullbytes)
}
