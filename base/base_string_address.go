package base

import (
	"regexp"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	MaxAddressSize             = 100
	MinAddressSize             = AddressTypeSize + 3
	reBlankStringAddressString = regexp.MustCompile(`\s+`)
	REStringAddressString      = `[a-zA-Z0-9][\w\-\.\!\$\*\@]*[a-zA-Z0-9]`
	reStringAddressString      = regexp.MustCompile(`^` + REStringAddressString + `$`)
)

type BaseStringAddress struct {
	s string
	hint.BaseHinter
}

func NewBaseStringAddressWithHint(ht hint.Hint, s string) BaseStringAddress {
	ad := BaseStringAddress{BaseHinter: hint.NewBaseHinter(ht)}
	ad.s = s + ht.Type().String()

	return ad
}

func (ad BaseStringAddress) IsValid([]byte) error {
	switch l := len(ad.s); {
	case l < MinAddressSize:
		return util.ErrInvalid.Errorf("too short string address")
	case l > MaxAddressSize:
		return util.ErrInvalid.Errorf("too long string address")
	}

	p := ad.s[:len(ad.s)-AddressTypeSize]
	if reBlankStringAddressString.MatchString(p) {
		return util.ErrInvalid.Errorf("string address string, %q has blank", ad)
	}

	if !reStringAddressString.MatchString(p) {
		return util.ErrInvalid.Errorf("invalid string address string, %q", ad)
	}

	switch {
	case len(ad.Hint().Type().String()) != AddressTypeSize:
		return util.ErrInvalid.Errorf("wrong hint of string address")
	case ad.s[len(ad.s)-AddressTypeSize:] != ad.Hint().Type().String():
		return util.ErrInvalid.Errorf(
			"wrong type of string address; %v != %v", ad.s[len(ad.s)-AddressTypeSize:], ad.Hint().Type())
	}

	return nil
}

func (ad BaseStringAddress) String() string {
	return ad.s
}

func (ad BaseStringAddress) Bytes() []byte {
	return []byte(ad.s)
}

func (ad BaseStringAddress) Equal(b Address) bool {
	if b == nil {
		return false
	}

	return ad.s == b.String()
}

func (ad BaseStringAddress) MarshalText() ([]byte, error) {
	return []byte(ad.s), nil
}
