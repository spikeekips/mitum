package isaacstates

import (
	"context"
	"sort"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testSuffrageStateBuilder struct {
	isaac.BaseTestBallots
	isaacdatabase.BaseTestDatabase
}

func (t *testSuffrageStateBuilder) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testSuffrageStateBuilder) prepare(point base.Point, previous base.State, locals, newlocals []isaac.LocalNode) isaacblock.SuffrageProof {
	newnodes := make([]base.Node, len(newlocals))
	for i := range newnodes {
		newnodes[i] = newlocals[i]
	}

	switch {
	case point.Height() == base.GenesisHeight && previous != nil:
		t.NoError(errors.Errorf("previous state was given for genesis"))
	case point.Height() != base.GenesisHeight && previous == nil:
		t.NoError(errors.Errorf("empty previous state was given"))
	}

	var previoushash util.Hash
	if previous != nil {
		previoushash = previous.Hash()
	}

	blockMap, err := newTestBlockMap(point.Height(), nil, previoushash, t.Local, t.NodePolicy.NetworkID())
	t.NoError(err)

	var suffrageheight base.Height
	if previous != nil {
		suffrageheight = previous.Value().(base.SuffrageStateValue).Height() + 1
	}

	newstate, _ := t.SuffrageState(point.Height(), suffrageheight, newnodes)
	newstate = base.NewBaseState(
		point.Height(),
		isaac.SuffrageStateKey,
		newstate.Value(),
		previoushash,
		[]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256(), valuehash.RandomSHA256()},
	)

	states := t.States(point.Height(), 6)
	states = append(states, newstate)

	w, _ := fixedtree.NewWriter(base.StateFixedtreeHint, uint64(len(states)))
	for i := range states {
		t.NoError(w.Add(uint64(i), fixedtree.NewBaseNode(states[i].Hash().String())))
	}
	_ = w.Write(func(uint64, fixedtree.Node) error {
		return nil
	})

	tr, err := w.Tree()
	t.NoError(err)

	proof, err := tr.Proof(newstate.Hash().String())
	t.NoError(err)

	afact := t.NewACCEPTBallotFact(point, valuehash.RandomSHA256(), blockMap.Manifest().Hash())
	voteproof, err := t.NewACCEPTVoteproof(afact, t.Local, locals)
	t.NoError(err)

	return isaacblock.NewSuffrageProof(isaacblock.SuffrageProofHint, blockMap, newstate, proof, voteproof)
}

func (t *testSuffrageStateBuilder) newProofs(n int) map[base.Height]isaac.SuffrageProof {
	locals := []isaac.LocalNode{t.Local}

	p := base.GenesisPoint
	proofs := map[base.Height]isaac.SuffrageProof{}
	for i := range make([]byte, 14) {
		newnodes, _ := t.Locals(i)
		newlocals := make([]isaac.LocalNode, len(locals)+len(newnodes))
		copy(newlocals[:len(locals)], locals)
		copy(newlocals[len(locals):], newnodes)

		var previous base.State
		if p.Height() == base.GenesisHeight {
			previous = nil
		} else {
			previous = proofs[p.Height()-1].State()
		}

		proof := t.prepare(p, previous, locals, newlocals)
		proofs[p.Height()] = proof

		p = p.NextHeight()
		locals = newlocals
	}

	return proofs
}

func (t *testSuffrageStateBuilder) compareSuffrage(expectedstate base.State, suf base.Suffrage) {
	expected, err := isaac.NewSuffrageFromState(expectedstate)
	t.NoError(err)

	t.Equal(len(expected.Nodes()), len(suf.Nodes()))
	for i := range expected.Nodes() {
		a := expected.Nodes()[i]

		t.True(suf.Exists(a.Address()))
		t.True(suf.ExistsPublickey(a.Address(), a.Publickey()))
	}
}

func (t *testSuffrageStateBuilder) TestBuildOneFromGenesis() {
	proofs := t.newProofs(1)
	last := proofs[0]

	expected := []base.Height{0}
	var fetched []base.Height

	s := NewSuffrageStateBuilder(
		t.NodePolicy.NetworkID(),
		func(context.Context) (isaac.SuffrageProof, bool, error) {
			return proofs[last.State().Height()], true, nil
		},
		func(_ context.Context, height base.Height) (isaac.SuffrageProof, bool, error) {
			switch {
			case height < base.GenesisHeight, height > last.State().Height():
				return nil, false, errors.Errorf("invalid height request, %d", height)
			}

			fetched = append(fetched, height)

			proof, found := proofs[height]
			t.True(found)

			return proof, found, nil
		},
	)
	s.numbatches = 3

	suf, err := s.Build(context.Background(), nil)
	t.NoError(err)
	t.NotNil(suf)

	t.compareSuffrage(last.State(), suf)

	sort.Slice(fetched, func(i, j int) bool {
		return fetched[i] < fetched[j]
	})

	t.Equal(expected, fetched)
}

func (t *testSuffrageStateBuilder) TestBuildFromGenesis() {
	proofs := t.newProofs(14)
	last := proofs[13]

	expected := []base.Height{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
	fetched := make([]base.Height, len(expected))

	s := NewSuffrageStateBuilder(
		t.NodePolicy.NetworkID(),
		func(context.Context) (isaac.SuffrageProof, bool, error) {
			return proofs[last.State().Height()], true, nil
		},
		func(_ context.Context, height base.Height) (isaac.SuffrageProof, bool, error) {
			switch {
			case height < base.GenesisHeight, height > last.State().Height():
				return nil, false, errors.Errorf("invalid height request, %d", height)
			}

			fetched[height.Int64()] = height

			proof, found := proofs[height]
			t.True(found)

			return proof, found, nil
		},
	)
	s.numbatches = 3

	suf, err := s.Build(context.Background(), nil)
	t.NoError(err)
	t.NotNil(suf)

	t.compareSuffrage(last.State(), suf)

	sort.Slice(fetched, func(i, j int) bool {
		return fetched[i] < fetched[j]
	})

	t.Equal(expected, fetched)
}

func (t *testSuffrageStateBuilder) TestBuildNotFromGenesis() {
	proofs := t.newProofs(14)
	last := proofs[13]

	localheight := base.Height(3)

	expected := []base.Height{4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
	fetched := make([]base.Height, len(expected))

	s := NewSuffrageStateBuilder(
		t.NodePolicy.NetworkID(),
		func(context.Context) (isaac.SuffrageProof, bool, error) {
			return proofs[last.State().Height()], true, nil
		},
		func(_ context.Context, height base.Height) (isaac.SuffrageProof, bool, error) {
			switch {
			case height <= localheight, height > last.State().Height():
				return nil, false, errors.Errorf("invalid height request, %d", height)
			}

			fetched[(height - localheight - 1).Int64()] = height

			proof, found := proofs[height]
			t.True(found)

			return proof, found, nil
		},
	)
	s.numbatches = 3

	suf, err := s.Build(context.Background(), proofs[localheight].State())
	t.NoError(err)
	t.NotNil(suf)

	t.compareSuffrage(last.State(), suf)

	sort.Slice(fetched, func(i, j int) bool {
		return fetched[i] < fetched[j]
	})

	t.Equal(expected, fetched)
}

func (t *testSuffrageStateBuilder) TestBuildLastNotFromGenesis() {
	proofs := t.newProofs(14)
	last := proofs[13]

	localheight := base.Height(13)

	s := NewSuffrageStateBuilder(
		t.NodePolicy.NetworkID(),
		func(context.Context) (isaac.SuffrageProof, bool, error) {
			return proofs[last.State().Height()], true, nil
		},
		func(_ context.Context, height base.Height) (isaac.SuffrageProof, bool, error) {
			return nil, false, errors.Errorf("invalid height request, %d", height)
		},
	)
	s.numbatches = 3

	suf, err := s.Build(context.Background(), proofs[localheight].State())
	t.NoError(err)
	t.NotNil(suf)

	t.compareSuffrage(last.State(), suf)
}

func TestSuffrageStateBuilder(t *testing.T) {
	suite.Run(t, new(testSuffrageStateBuilder))
}
