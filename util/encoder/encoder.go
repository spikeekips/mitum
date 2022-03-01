package encoder

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type Encoder interface {
	hint.Hinter
	Add(DecodeDetail) error
	AddHinter(hint.Hinter) error
	Marshal(interface{}) ([]byte, error)                       // NOTE native marshaler func
	Unmarshal([]byte, interface{}) error                       // NOTE native unmarshaler func
	Decode([]byte) (interface{}, error)                        // NOTE decode by hint inside []byte
	DecodeWithHint([]byte, hint.Hint) (interface{}, error)     // NOTE decode []byte by given hint
	DecodeWithHintType([]byte, hint.Type) (interface{}, error) // NOTE decode []byte by given type
	DecodeWithFixedHintType(string, int) (interface{}, error)  // NOTE decode string by fixed type size
	DecodeSlice([]byte) ([]interface{}, error)                 // NOTE decode sliced data
}

type DecodeFunc func([]byte, hint.Hint) (interface{}, error)

type DecodeDetail struct {
	Hint     hint.Hint
	Decode   DecodeFunc
	Desc     string
	Instance interface{}
}

func (d DecodeDetail) IsValid([]byte) error {
	if err := d.Hint.IsValid(nil); err != nil {
		return util.InvalidError.Wrapf(err, "invalid hint in DecodeDetail")
	}

	if d.Decode == nil && d.Instance == nil {
		return util.InvalidError.Errorf("instance and decode func are empty in DecodeDetail")
	}

	return nil
}
