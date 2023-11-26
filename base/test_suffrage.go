//go:build test
// +build test

package base

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/assert"
)

func EqualSuffrageCandidateStateValue(t *assert.Assertions, a, b SuffrageCandidateStateValue) {
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
	st State
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

func (p *DummySuffrageProof) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringError("failed to decode DummySuffrageProof")

	var u dummySuffrageProofUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	if u.ST != nil {
		switch hinter, err := enc.Decode(u.ST); {
		case err != nil:
			return e.Wrap(err)
		default:
			i, ok := hinter.(State)
			if !ok {
				return e.Errorf("expected State, but %T", hinter)
			}

			p.st = i
		}
	}

	return nil
}

func EqualSuffrageProof(t *assert.Assertions, a, b SuffrageProof) {
	ah := a.(hint.Hinter)
	bh := b.(hint.Hinter)

	t.True(ah.Hint().Equal(bh.Hint()))

	t.Equal(a.SuffrageHeight(), b.SuffrageHeight())

	EqualBlockMap(t, a.Map(), b.Map())
	t.True(IsEqualState(a.State(), b.State()))

	ap := a.Proof().Nodes()
	bp := b.Proof().Nodes()
	t.Equal(len(ap), len(bp))
	for i := range ap {
		t.True(ap[i].Equal(bp[i]))
	}

	asuf, err := a.Suffrage()
	t.NoError(err)
	bsuf, err := b.Suffrage()
	t.NoError(err)

	switch {
	case asuf == nil && bsuf == nil:
	default:
		anodes := asuf.Nodes()
		bnodes := bsuf.Nodes()
		t.Equal(len(anodes), len(bnodes))
		t.True(IsEqualNodes(anodes, bnodes))

	}
}
