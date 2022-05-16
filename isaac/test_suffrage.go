//go:build test
// +build test

package isaac

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
)

func (suf Suffrage) Locals() []LocalNode {
	locals := make([]LocalNode, suf.Len())
	for i := range suf.ns {
		locals[i] = suf.ns[i].(LocalNode)
	}

	return locals
}

func NewTestSuffrage(n int, extras ...LocalNode) (Suffrage, []LocalNode) {
	locals := make([]LocalNode, n+len(extras))
	nodes := make([]base.Node, n+len(extras))
	for i := range make([]int, n) {
		l := NewLocalNode(base.NewMPrivatekey(), base.NewStringAddress(fmt.Sprintf("no%02d", i)))

		nodes[i] = l
		locals[i] = l
	}

	for i := range extras {
		l := extras[i]
		locals[i+n] = l
		nodes[i+n] = l
	}

	suf, _ := NewSuffrage(nodes)

	sort.Slice(locals, func(i, j int) bool {
		return locals[i].Address().String() < locals[j].Address().String()
	})

	return suf, locals
}

var DummySuffrageProofHint = hint.MustNewHint("dummy-suffrage-proof-v0.0.1")

type DummySuffrageProof struct {
	hint.BaseHinter
	st base.State `json:"state"`
}

var _ = (interface{})(DummySuffrageProof{}).(SuffrageProof)

func NewDummySuffrageProof() DummySuffrageProof {
	return DummySuffrageProof{
		BaseHinter: hint.NewBaseHinter(DummySuffrageProofHint),
	}
}

func (p DummySuffrageProof) Map() base.BlockMap {
	return nil
}

func (p DummySuffrageProof) State() base.State {
	return p.st
}

func (p DummySuffrageProof) SetState(st base.State) DummySuffrageProof {
	p.st = st

	return p
}

func (p DummySuffrageProof) ACCEPTVoteproof() base.ACCEPTVoteproof {
	return nil
}

func (p DummySuffrageProof) Proof() fixedtree.Proof {
	return fixedtree.Proof{}
}

func (p DummySuffrageProof) Suffrage() (base.Suffrage, error) {
	return nil, nil
}

func (p DummySuffrageProof) Prove(previousState base.State) error {
	return nil
}

type dummySuffrageProofMarshaler struct {
	hint.BaseHinter
	ST base.State `json:"state"`
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
			i, ok := hinter.(base.State)
			if !ok {
				return e(nil, "expected base.State, but %T", hinter)
			}

			p.st = i
		}
	}

	return nil
}
