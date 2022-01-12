package base

import (
	"regexp"

	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

var StringAddressHint = hint.MustNewHint("sas-v2")

var (
	MaxAddressSize             = 100
	MinAddressSize             = AddressTypeSize + 3
	reBlankStringAddressString = regexp.MustCompile(`[\s][\s]*`)
	REStringAddressString      = `[a-zA-Z0-9][\w\-\.\!\$\*\@]*[a-zA-Z0-9]`
	reStringAddressString      = regexp.MustCompile(`^` + REStringAddressString + `$`)
)

type StringAddress struct {
	hint.BaseHinter
	s string
}

func NewStringAddress(s string) StringAddress {
	return NewStringAddressWithHint(StringAddressHint, s)
}

func NewStringAddressWithHint(ht hint.Hint, s string) StringAddress {
	n := s + ht.Type().String()

	return StringAddress{BaseHinter: hint.NewBaseHinter(ht), s: n}
}

func ParseStringAddress(s string) (StringAddress, error) {
	b, t, err := hint.ParseFixedTypedString(s, AddressTypeSize)
	switch {
	case err != nil:
		return StringAddress{}, err
	case t != StringAddressHint.Type():
		return StringAddress{}, util.InvalidError.Errorf("wrong hint type in string address")
	}

	return NewStringAddress(b), nil
}

func (ad StringAddress) IsValid([]byte) error {
	if err := ad.BaseHinter.IsValid(nil); err != nil {
		return util.InvalidError.Wrapf(err, "wrong hint in string address")
	}

	switch l := len(ad.s); {
	case l < MinAddressSize:
		return util.InvalidError.Errorf("too short string address")
	case l > MaxAddressSize:
		return util.InvalidError.Errorf("too long string address")
	}

	p := ad.s[:len(ad.s)-AddressTypeSize]
	if reBlankStringAddressString.MatchString(p) {
		return util.InvalidError.Errorf("string address string, %q has blank", ad)
	}

	if !reStringAddressString.MatchString(p) {
		return util.InvalidError.Errorf("invalid string address string, %q", ad)
	}

	switch {
	case len(ad.Hint().Type().String()) != AddressTypeSize:
		return util.InvalidError.Errorf("wrong hint of string address")
	case ad.s[len(ad.s)-AddressTypeSize:] != ad.Hint().Type().String():
		return util.InvalidError.Errorf(
			"wrong type of string address; %v != %v", ad.s[len(ad.s)-AddressTypeSize:], ad.Hint().Type())
	}

	return nil
}

func (ad StringAddress) String() string {
	return ad.s
}

func (ad StringAddress) Bytes() []byte {
	return []byte(ad.s)
}

func (ad StringAddress) Equal(b Address) bool {
	if b == nil {
		return false
	}

	if ad.Hint().Type() != b.Hint().Type() {
		return false
	}

	return ad.s == b.String()
}

func (ad StringAddress) MarshalText() ([]byte, error) {
	return []byte(ad.s), nil
}

func (ad *StringAddress) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	ad.s = string(b)

	return nil
}
