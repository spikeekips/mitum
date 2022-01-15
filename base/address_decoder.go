package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type AddressDecoder struct {
	t hint.Type
	b []byte
}

func (decoder *AddressDecoder) Decode(enc encoder.Encoder) (Address, error) {
	i, err := enc.DecodeWithHintType(decoder.b, decoder.t)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode address")
	}
	if i == nil {
		return nil, nil
	}

	ad, ok := i.(Address)
	if !ok {
		return nil, errors.Errorf("failed to decode address; not Address, %T", i)
	}

	return ad, nil
}

func (decoder *AddressDecoder) UnmarshalText(b []byte) error {
	_, t, err := hint.ParseFixedTypedString(string(b), AddressTypeSize)
	if err != nil {
		return errors.Wrap(err, "failed AddressDecoder")
	}

	decoder.t = t
	decoder.b = b

	return nil
}

// ParseAddressFromString parses and decodes Address from string.
func ParseAddressFromString(s string, enc encoder.Encoder) (Address, error) {
	if len(s) < 1 {
		return nil, nil
	}

	_, t, err := hint.ParseFixedTypedString(s, AddressTypeSize)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse address from string")
	}

	decoder := AddressDecoder{t: t, b: []byte(s)}

	return decoder.Decode(enc)
}
