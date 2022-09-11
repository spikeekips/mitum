package encoder

import (
	"io"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type Encoder interface { //nolint:interfacebloat //...
	hint.Hinter
	Add(DecodeDetail) error
	AddHinter(hint.Hinter) error
	Marshal(interface{}) ([]byte, error) // NOTE native marshaler func
	Unmarshal([]byte, interface{}) error // NOTE native unmarshaler func
	StreamEncoder(io.Writer) util.StreamEncoder
	StreamDecoder(io.Reader) util.StreamDecoder
	Decode([]byte) (interface{}, error)                        // NOTE decode by hint inside []byte
	DecodeWithHint([]byte, hint.Hint) (interface{}, error)     // NOTE decode []byte by given hint
	DecodeWithHintType([]byte, hint.Type) (interface{}, error) // NOTE decode []byte by given type
	DecodeWithFixedHintType(string, int) (interface{}, error)  // NOTE decode string by fixed type size
	DecodeSlice([]byte) ([]interface{}, error)                 // NOTE decode sliced data
}

type DecodeFunc func([]byte, hint.Hint) (interface{}, error)

type DecodeDetail struct {
	Instance interface{}
	Decode   DecodeFunc
	Desc     string
	Hint     hint.Hint
}

func (d DecodeDetail) IsValid([]byte) error {
	if err := d.Hint.IsValid(nil); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid hint in DecodeDetail")
	}

	if d.Decode == nil && d.Instance == nil {
		return util.ErrInvalid.Errorf("instance and decode func are empty in DecodeDetail")
	}

	return nil
}
