package base

import (
	"encoding/json"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var BaseStateHint = hint.MustNewHint("base-state-v0.0.1")

type BaseState struct {
	hint.BaseHinter
	h      util.Hash
	height Height
	k      string
	v      StateValue
	ops    []util.Hash
}

func NewBaseState(height Height, k string, v StateValue) BaseState {
	s := BaseState{
		BaseHinter: hint.NewBaseHinter(BaseStateHint),
		height:     height,
		k:          k,
		v:          v,
	}

	s.h = s.hash()

	return s
}

func (s BaseState) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid base state")

	vs := make([]util.IsValider, len(s.ops)+5)
	vs[0] = s.BaseHinter
	vs[1] = s.h
	vs[2] = s.height
	vs[3] = util.DummyIsValider(func([]byte) error {
		if len(s.k) < 1 {
			return util.InvalidError.Errorf("empty state key")
		}

		return nil
	})
	vs[4] = s.v
	for i := range s.ops {
		vs[i+5] = s.ops[i]
	}

	if err := util.CheckIsValid(nil, false, vs...); err != nil {
		return e(err, "")
	}

	return nil
}

func (s BaseState) Hash() util.Hash {
	return s.h
}

func (s BaseState) Key() string {
	return s.k
}

func (s BaseState) Value() StateValue {
	return s.v
}

func (s BaseState) Height() Height {
	return s.height
}

func (s BaseState) Operations() []util.Hash {
	return s.ops
}

func (s *BaseState) SetOperations(ops []util.Hash) {
	s.ops = ops
}

func (s BaseState) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter([]byte(s.k)),
		util.DummyByter(func() []byte {
			if s.v == nil {
				return nil
			}

			return s.v.HashBytes()
		}),
		util.DummyByter(func() []byte {
			if len(s.ops) < 1 {
				return nil
			}

			bs := make([][]byte, len(s.ops))
			for i := range s.ops {
				bs[i] = s.ops[i].Bytes()
			}

			return util.ConcatBytesSlice(bs...)
		}),
	))
}

type baseStateJSONMarshaler struct {
	hint.BaseHinter
	H      util.Hash   `json:"hash"`
	Height Height      `json:"height"`
	K      string      `json:"key"`
	V      StateValue  `json:"value"`
	OPS    []util.Hash `json:"operations"`
}

func (s BaseState) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseStateJSONMarshaler{
		BaseHinter: s.BaseHinter,
		H:          s.h,
		Height:     s.height,
		K:          s.k,
		V:          s.v,
		OPS:        s.ops,
	})
}

type baseStateJSONUnmarshaler struct {
	H      valuehash.HashDecoder   `json:"hash"`
	Height Height                  `json:"height"`
	K      string                  `json:"key"`
	V      json.RawMessage         `json:"value"`
	OPS    []valuehash.HashDecoder `json:"operations"`
}

func (s *BaseState) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to unmarshal BaseState")

	var u baseStateJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	s.h = u.H.Hash()
	s.height = u.Height
	s.k = u.K

	s.ops = make([]util.Hash, len(u.OPS))
	for i := range u.OPS {
		s.ops[i] = u.OPS[i].Hash()
	}

	switch i, err := DecodeStateValue(u.V, enc); {
	case err != nil:
		return e(err, "")
	default:
		s.v = i
	}

	return nil
}

func DecodeStateValue(b []byte, enc encoder.Encoder) (StateValue, error) {
	e := util.StringErrorFunc("failed to decode StateValue")

	i, err := enc.Decode(b)
	if err != nil {
		return nil, e(err, "")
	}

	j, ok := i.(StateValue)
	if !ok {
		return nil, e(err, "not StateValue, %T", i)
	}

	return j, nil
}
