package base

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

type baseSealJSONMarshaler struct {
	hint.BaseHinter
	Signed BaseSigned `json:"signed"`
	H      util.Hash  `json:"hash"`
	Body   []SealBody `json:"body"`
}

func (sl BaseSeal) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseSealJSONMarshaler{
		BaseHinter: sl.BaseHinter,
		Signed:     sl.signed,
		H:          sl.h,
		Body:       sl.body,
	})
}

type baseSealJSONUnmarshaler struct {
	H      valuehash.HashDecoder `json:"hash"`
	Signed json.RawMessage       `json:"signed"`
	Body   []json.RawMessage     `json:"body"`
}

func (sl *BaseSeal) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to unmarshal json of BaseSeal")

	var u baseSealJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	var us BaseSigned
	if err := us.DecodeJSON(u.Signed, enc); err != nil {
		return e(err, "")
	}

	bs := make([]SealBody, len(u.Body))
	for i := range u.Body {
		hinter, err := enc.Decode(u.Body[i])
		if err != nil {
			return errors.Wrap(err, "failed to decode seal body")
		}

		j, ok := hinter.(SealBody)
		if !ok {
			return e(nil, "expected SealBody, not %T", hinter)
		}
		bs[i] = j
	}

	sl.h = u.H.Hash()
	sl.signed = us
	sl.body = bs

	return nil
}
