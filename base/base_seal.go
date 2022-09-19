package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

type BaseSeal struct {
	h    util.Hash
	sign BaseSign
	body []SealBody
	hint.BaseHinter
}

func NewBaseSeal(ht hint.Hint, body []SealBody) BaseSeal {
	return BaseSeal{
		BaseHinter: hint.NewBaseHinter(ht),
		body:       body,
	}
}

func (sl BaseSeal) Hash() util.Hash {
	return sl.h
}

func (sl BaseSeal) Signs() Sign {
	return sl.sign
}

func (sl BaseSeal) Body() []SealBody {
	return sl.body
}

func (sl BaseSeal) IsValid(networkID []byte) error {
	e := util.StringErrorFunc("invalid BaseSeal")

	if len(sl.body) < 1 {
		return e(util.ErrInvalid.Errorf("empty body in BaseSeal"), "")
	}

	// NOTE BaseHinter should be checked by parent instance with it's own
	// hint.Type.
	c := make([]util.IsValider, len(sl.body)+2)
	c[0] = sl.h
	c[1] = sl.sign

	for i := range sl.body {
		c[2+i] = sl.body[i]
	}

	if err := util.CheckIsValid(nil, false, c...); err != nil {
		return e(util.ErrInvalid.Wrap(err), "")
	}

	// NOTE check sign with body hash
	if err := sl.sign.Verify(networkID, sl.signedHashBytes()); err != nil {
		return e(util.ErrInvalid.Wrap(err), "")
	}

	return nil
}

func (sl *BaseSeal) Sign(priv Privatekey, networkID []byte) error {
	sign, err := NewBaseSignFromBytes(priv, networkID, sl.signedHashBytes())
	if err != nil {
		return errors.Wrap(err, "failed to sign BaseSeal")
	}

	sl.sign = sign

	// NOTE update  hash
	sl.h = valuehash.NewSHA256(util.ConcatByters(sl.BaseHinter, sl.sign))

	return nil
}

func (sl BaseSeal) signedHashBytes() []byte {
	hs := make([][]byte, len(sl.body)+1)
	hs[0] = sl.Hint().Bytes()

	for i := range sl.body {
		hs[i+1] = sl.body[i].HashBytes()
	}

	return util.ConcatBytesSlice(hs...)
}
