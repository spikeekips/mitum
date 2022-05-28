package base

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/assert"
)

func EqualSuffrageCandidate(t *assert.Assertions, a, b SuffrageCandidate) {
	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	aht := a.(hint.Hinter).Hint()
	bht := b.(hint.Hinter).Hint()

	t.True(aht.Equal(bht))

	t.True(IsEqualNode(a, b))
	t.Equal(a.Start(), b.Start())
	t.Equal(a.Deadline(), b.Deadline())
}

var DummySuffrageProofHint = hint.MustNewHint("dummy-suffrage-proof-v0.0.1")

type DummySuffrageProof struct {
	hint.BaseHinter
	st State `json:"state"`
}

var _ = (interface{})(DummySuffrageProof{}).(SuffrageProof)

func NewDummySuffrageProof() DummySuffrageProof {
	return DummySuffrageProof{
		BaseHinter: hint.NewBaseHinter(DummySuffrageProofHint),
	}
}

func (p DummySuffrageProof) Map() BlockMap {
	return nil
}

func (p DummySuffrageProof) State() State {
	return p.st
}

func (p DummySuffrageProof) SetState(st State) DummySuffrageProof {
	p.st = st

	return p
}

func (p DummySuffrageProof) ACCEPTVoteproof() ACCEPTVoteproof {
	return nil
}

func (p DummySuffrageProof) Proof() fixedtree.Proof {
	return fixedtree.Proof{}
}

func (p DummySuffrageProof) Suffrage() (Suffrage, error) {
	return nil, nil
}

func (p DummySuffrageProof) SuffrageHeight() Height {
	return NilHeight
}

func (p DummySuffrageProof) Prove(previousState State) error {
	return nil
}

type dummySuffrageProofMarshaler struct {
	hint.BaseHinter
	ST State `json:"state"`
}

func (p DummySuffrageProof) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(dummySuffrageProofMarshaler{
		BaseHinter: p.BaseHinter,
		ST:         p.st,
	})
}

type dummySuffrageProofUnmarshaler struct {
	ST json.RawMessage `json:"state"`
}

func (p *DummySuffrageProof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode DummySuffrageProof")

	var u dummySuffrageProofUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	if u.ST != nil {
		switch hinter, err := enc.Decode(u.ST); {
		case err != nil:
			return e(err, "")
		default:
			i, ok := hinter.(State)
			if !ok {
				return e(nil, "expected State, but %T", hinter)
			}

			p.st = i
		}
	}

	return nil
}
