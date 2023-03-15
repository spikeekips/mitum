package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var StringAddressHint = hint.MustNewHint("sas-v2")

type StringAddress struct {
	BaseStringAddress
}

func NewStringAddress(s string) StringAddress {
	return StringAddress{
		BaseStringAddress: NewBaseStringAddressWithHint(StringAddressHint, s),
	}
}

func ParseStringAddress(s string) (StringAddress, error) {
	b, t, err := hint.ParseFixedTypedString(s, AddressTypeSize)

	switch {
	case err != nil:
		return StringAddress{}, errors.Wrap(err, "parse StringAddress")
	case t != StringAddressHint.Type():
		return StringAddress{}, util.ErrInvalid.Errorf("wrong hint type in StringAddress")
	}

	return NewStringAddress(b), nil
}

func (ad StringAddress) IsValid([]byte) error {
	if err := ad.BaseHinter.IsValid(StringAddressHint.Type().Bytes()); err != nil {
		return util.ErrInvalid.Wrapf(err, "wrong hint in StringAddress")
	}

	if err := ad.BaseStringAddress.IsValid(nil); err != nil {
		return errors.Wrap(err, "invalid StringAddress")
	}

	return nil
}

func (ad *StringAddress) UnmarshalText(b []byte) error {
	ad.s = string(b) + StringAddressHint.Type().String()

	return nil
}
