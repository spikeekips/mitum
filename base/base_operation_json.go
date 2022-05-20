package base

import (
	"encoding/json"

	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

type BaseOperationJSONMarshaler struct {
	Hash   util.Hash `json:"hash"`
	Fact   Fact      `json:"fact"`
	Signed []Signed  `json:"signed"`
	hint.BaseHinter
}

func (op BaseOperation) JSONMarshaler() BaseOperationJSONMarshaler {
	return BaseOperationJSONMarshaler{
		BaseHinter: op.BaseHinter,
		Hash:       op.h,
		Fact:       op.fact,
		Signed:     op.signed,
	}
}

func (op BaseOperation) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(op.JSONMarshaler())
}

type BaseOperationJSONUnmarshaler struct {
	Hash   valuehash.HashDecoder `json:"hash"`
	Fact   json.RawMessage       `json:"fact"`
	Signed []json.RawMessage     `json:"signed"`
}

func (op *BaseOperation) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode BaseOperation")

	var u BaseOperationJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	op.h = u.Hash.Hash()

	switch hinter, err := enc.Decode(u.Fact); {
	case err != nil:
		return e(err, "failed to decode fact")
	default:
		i, ok := hinter.(Fact)
		if !ok {
			return e(nil, "not Fact, %T", hinter)
		}

		op.fact = i
	}

	op.signed = make([]Signed, len(u.Signed))

	for i := range u.Signed {
		var ub BaseSigned
		if err := ub.DecodeJSON(u.Signed[i], enc); err != nil {
			return e(err, "failed to decode signed")
		}

		op.signed[i] = ub
	}

	return nil
}
