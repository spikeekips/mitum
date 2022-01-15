package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

const PKKeyTypeSize = 3

type basePKKeyDecoder struct {
	t hint.Type
	b []byte
}

func (decoder basePKKeyDecoder) Decode(enc encoder.Encoder) (PKKey, error) {
	i, err := enc.DecodeWithHintType(decoder.b, decoder.t)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode PKKey")
	}
	if i == nil {
		return nil, nil
	}

	k, ok := i.(PKKey)
	if !ok {
		return nil, errors.Errorf("failed to decode PKKey; not PKKey, %T", i)
	}

	return k, nil
}

func (decoder *basePKKeyDecoder) UnmarshalText(b []byte) error {
	body, t, err := hint.ParseFixedTypedString(string(b), PKKeyTypeSize)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal PKKey; not PKKey")
	}

	decoder.t = t
	decoder.b = []byte(body)

	return nil
}

type PrivatekeyDecoder struct {
	basePKKeyDecoder
}

func (decoder PrivatekeyDecoder) Decode(enc encoder.Encoder) (Privatekey, error) {
	k, err := decoder.basePKKeyDecoder.Decode(enc)
	switch {
	case err != nil:
		return nil, errors.Wrap(err, "failed PrivatekeyDecoder")
	case k == nil:
		return nil, nil
	}

	priv, ok := k.(Privatekey)
	if !ok {
		return nil, errors.Errorf("not Privatekey: %T", k)
	}

	return priv, nil
}

type PublickeyDecoder struct {
	basePKKeyDecoder
}

func (decoder PublickeyDecoder) Decode(enc encoder.Encoder) (Publickey, error) {
	k, err := decoder.basePKKeyDecoder.Decode(enc)
	switch {
	case err != nil:
		return nil, errors.Wrap(err, "failed PublickeyDecoder")
	case k == nil:
		return nil, nil
	}

	pub, ok := k.(Publickey)
	if !ok {
		return nil, errors.Errorf("not Publickey: %T", k)
	}

	return pub, nil
}

func ParsePrivatekey(s string, enc encoder.Encoder) (Privatekey, error) {
	if len(s) < 1 {
		return nil, nil
	}

	decoder := &PrivatekeyDecoder{}
	if err := decoder.UnmarshalText([]byte(s)); err != nil {
		return nil, errors.Wrap(err, "failed to parse Privatekey")
	}

	return decoder.Decode(enc)
}

func ParsePublickey(s string, enc encoder.Encoder) (Publickey, error) {
	if len(s) < 1 {
		return nil, nil
	}

	decoder := &PublickeyDecoder{}
	if err := decoder.UnmarshalText([]byte(s)); err != nil {
		return nil, errors.Wrap(err, "failed to parse Publickey")
	}

	return decoder.Decode(enc)
}
