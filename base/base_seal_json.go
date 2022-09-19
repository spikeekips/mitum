package base

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

type baseSealJSONMarshaler struct {
	Hash util.Hash  `json:"hash"`
	Sign BaseSign   `json:"sign"`
	Body []SealBody `json:"body"`
	hint.BaseHinter
}

func (sl BaseSeal) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseSealJSONMarshaler{
		BaseHinter: sl.BaseHinter,
		Sign:       sl.sign,
		Hash:       sl.h,
		Body:       sl.body,
	})
}

type baseSealJSONUnmarshaler struct {
	Hash valuehash.HashDecoder `json:"hash"`
	Sign json.RawMessage       `json:"sign"`
	Body []json.RawMessage     `json:"body"`
}

func (sl *BaseSeal) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to unmarshal json of BaseSeal")

	var u baseSealJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	var us BaseSign
	if err := us.DecodeJSON(u.Sign, enc); err != nil {
		return e(err, "")
	}

	sl.body = make([]SealBody, len(u.Body))

	for i := range u.Body {
		if err := encoder.Decode(enc, u.Body[i], &sl.body[i]); err != nil {
			return errors.Wrap(err, "failed to decode seal body")
		}
	}

	sl.h = u.Hash.Hash()
	sl.sign = us

	return nil
}
