package hint

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type SetHinter interface {
	SetHint(Hint) Hinter
}

type BaseHinter struct {
	HT Hint `json:"_hint"` //nolint:tagliatelle //...
}

func NewBaseHinter(ht Hint) BaseHinter {
	return BaseHinter{HT: ht}
}

func (ht BaseHinter) Hint() Hint {
	return ht.HT
}

func (BaseHinter) SetHint(n Hint) Hinter {
	return BaseHinter{HT: n}
}

func (ht BaseHinter) IsValid(expectedType []byte) error {
	if err := ht.HT.IsValid(nil); err != nil {
		return errors.WithMessage(err, "invalid hint in BaseHinter")
	}

	if len(expectedType) > 0 {
		if t := Type(string(expectedType)); t != ht.HT.Type() {
			return util.ErrInvalid.Errorf("type does not match in BaseHinter, %q != %q", ht.HT.Type(), t)
		}
	}

	return nil
}

func (ht BaseHinter) Bytes() []byte {
	return ht.HT.Bytes()
}
