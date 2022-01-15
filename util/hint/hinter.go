package hint

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type SetHinter interface {
	SetHint(Hint) Hinter
}

type BaseHinter struct {
	HT Hint `json:"_hint"`
}

func NewBaseHinter(ht Hint) BaseHinter {
	return BaseHinter{HT: ht}
}

func (ht BaseHinter) Hint() Hint {
	return ht.HT
}

func (ht BaseHinter) SetHint(n Hint) Hinter {
	ht.HT = n

	return ht
}

func (ht BaseHinter) IsValid(expectedType []byte) error {
	if err := ht.HT.IsValid(nil); err != nil {
		return errors.Wrap(err, "invalid hint in BaseHinter")
	}

	if len(expectedType) > 0 {
		if t := Type(string(expectedType)); t != ht.HT.Type() {
			return util.InvalidError.Errorf("type does not match in BaseHinter, %q != %q", ht.HT.Type(), t)
		}
	}

	return nil
}
