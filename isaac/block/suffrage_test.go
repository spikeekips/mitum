package isaacblock

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testSuffrageProof struct {
	isaac.BaseTestBallots
	isaacdatabase.BaseTestDatabase
	locals   []base.LocalNode
	nodes    []base.Node
	point    base.Point
	blockMap base.BlockMap
	previous base.State
	current  base.State
	states   []base.State
	proof    fixedtree.Proof
}

func (t *testSuffrageProof) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testSuffrageProof) prepare(point base.Point) {
	t.point = point

	locals, _ := t.Locals(3)
	locals = append(locals, t.Local)
	t.locals = locals
	t.nodes = make([]base.Node, len(t.locals))
	for i := range t.nodes {
		t.nodes[i] = t.locals[i]
	}

	var previousHash util.Hash
	if point.Height() == base.GenesisHeight {
		t.previous = nil
	} else {
		t.previous, _ = t.SuffrageState(t.point.Height()-1, t.point.Height()-2, t.nodes)
		previousHash = t.previous.Hash()
	}

	t.blockMap = t.newmap(t.point.Height(), t.Local)

	var suffrageheight base.Height
	if t.previous != nil {
		suffrageheight = t.previous.Value().(base.SuffrageNodesStateValue).Height() + 1
	}

	current, _ := t.SuffrageState(t.point.Height(), suffrageheight, t.nodes)
	t.current = base.NewBaseState(
		t.point.Height(),
		isaac.SuffrageStateKey,
		current.Value(),
		previousHash,
		[]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256(), valuehash.RandomSHA256()},
	)

	t.states = t.States(t.point.Height(), 6)
	t.states = append(t.states, t.current)

	w, _ := fixedtree.NewWriter(base.StateFixedtreeHint, uint64(len(t.states)))
	for i := range t.states {
		t.NoError(w.Add(uint64(i), fixedtree.NewBaseNode(t.states[i].Hash().String())))
	}
	_ = w.Write(func(uint64, fixedtree.Node) error {
		return nil
	})

	tr, err := w.Tree()
	t.NoError(err)

	t.proof, _ = tr.Proof(t.current.Hash().String())
}

func (t *testSuffrageProof) newitem(ty base.BlockMapItemType) BlockMapItem {
	return NewLocalBlockMapItem(ty, util.UUID().String(), 1)
}

func (t *testSuffrageProof) newmap(height base.Height, local base.LocalNode) BlockMap {
	m := NewBlockMap(LocalFSWriterHint, jsonenc.JSONEncoderHint)

	for _, i := range []base.BlockMapItemType{
		base.BlockMapItemTypeProposal,
		base.BlockMapItemTypeOperations,
		base.BlockMapItemTypeOperationsTree,
		base.BlockMapItemTypeStates,
		base.BlockMapItemTypeStatesTree,
		base.BlockMapItemTypeVoteproofs,
	} {
		t.NoError(m.SetItem(t.newitem(i)))
	}

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
	switch {
	case t.previous == nil:
		manifest.SetSuffrage(nil)
	default:
		manifest.SetSuffrage(t.previous.Hash())
	}

	m.SetManifest(manifest)
	t.NoError(m.Sign(local.Address(), local.Privatekey(), t.LocalParams.NetworkID()))

	return m
}

func (t *testSuffrageProof) TestInvalid() {
	t.prepare(base.RawPoint(33, 0))

	t.Run("ok", func() {
		p := NewSuffrageProof(t.blockMap, t.current, t.proof)
		t.NoError(p.IsValid(t.LocalParams.NetworkID()))

		_ = (interface{})(p).(base.SuffrageProof)
	})

	t.Run("invalid hint type", func() {
		p := NewSuffrageProof(t.blockMap, t.current, t.proof)
		p.BaseHinter = hint.NewBaseHinter(hint.MustNewHint("findme-v0.0.1"))

		err := p.IsValid(t.LocalParams.NetworkID())
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "type does not match")
	})

	t.Run("nil map", func() {
		p := NewSuffrageProof(nil, t.current, t.proof)

		err := p.IsValid(t.LocalParams.NetworkID())
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
	})

	t.Run("nil state", func() {
		p := NewSuffrageProof(t.blockMap, nil, t.proof)

		err := p.IsValid(t.LocalParams.NetworkID())
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
	})

	t.Run("wrong suffrage state", func() {
		p := NewSuffrageProof(t.blockMap, t.states[0], t.proof)

		err := p.IsValid(t.LocalParams.NetworkID())
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "expected SuffrageNodesStateValue")
	})

	t.Run("height not match", func() {
		current := base.NewBaseState(
			t.point.Height()+1,
			isaac.SuffrageStateKey,
			t.current.Value(),
			t.current.Previous(),
			t.current.Operations(),
		)

		p := NewSuffrageProof(t.blockMap, current, t.proof)

		err := p.IsValid(t.LocalParams.NetworkID())
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "state height does not match with manifest")
	})
}

func (t *testSuffrageProof) TestProve() {
	t.Run("ok", func() {
		t.prepare(base.RawPoint(33, 0))

		p := NewSuffrageProof(t.blockMap, t.current, t.proof)
		t.NoError(p.Prove(t.previous))
	})

	t.Run("previous hash does not match", func() {
		t.prepare(base.RawPoint(33, 0))
		previous, _ := t.SuffrageState(t.point.Height(), t.point.Height(), t.nodes)

		p := NewSuffrageProof(t.blockMap, t.current, t.proof)
		err := p.Prove(previous)
		t.Error(err)
		t.ErrorContains(err, "invalid previous state; higher height")
	})

	t.Run("invalid suffrage height", func() {
		t.prepare(base.RawPoint(33, 0))
		previous, _ := t.SuffrageState(t.point.Height()-1, t.point.Height()+100, t.nodes)

		current := base.NewBaseState(
			t.point.Height(),
			isaac.SuffrageStateKey,
			t.current.Value(),
			previous.Hash(),
			[]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256(), valuehash.RandomSHA256()},
		)

		p := NewSuffrageProof(t.blockMap, current, t.proof)
		err := p.Prove(previous)
		t.Error(err)
		t.ErrorContains(err, "invalid previous state value")
	})

	t.Run("genesis", func() {
		t.prepare(base.GenesisPoint)

		p := NewSuffrageProof(t.blockMap, t.current, t.proof)
		t.NoError(p.Prove(t.previous))
	})

	t.Run("not nil previous for genesis", func() {
		t.prepare(base.GenesisPoint)
		previous, _ := t.SuffrageState(t.point.Height(), t.point.Height(), t.nodes)

		p := NewSuffrageProof(t.blockMap, t.current, t.proof)
		err := p.Prove(previous)
		t.Error(err)
		t.ErrorContains(err, "previous state should be nil for genesis")
	})
}

func (t *testSuffrageProof) TestEncode() {
	tt := new(encoder.BaseTestEncode)
	enc := jsonenc.NewEncoder()

	hints := []encoder.DecodeDetail{
		{Hint: BlockMapHint, Instance: BlockMap{}},
		{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}},
		{Hint: base.StringAddressHint, Instance: base.StringAddress{}},
		{Hint: base.DummyManifestHint, Instance: base.DummyManifest{}},
		{Hint: base.BaseStateHint, Instance: base.BaseState{}},
		{Hint: isaac.SuffrageNodeStateValueHint, Instance: isaac.SuffrageNodeStateValue{}},
		{Hint: isaac.SuffrageNodesStateValueHint, Instance: isaac.SuffrageNodesStateValue{}},
		{Hint: base.DummyNodeHint, Instance: base.BaseNode{}},
		{Hint: isaac.ACCEPTBallotSignFactHint, Instance: isaac.ACCEPTBallotSignFact{}},
		{Hint: isaac.ACCEPTBallotFactHint, Instance: isaac.ACCEPTBallotFact{}},
		{Hint: SuffrageProofHint, Instance: SuffrageProof{}},
	}
	for i := range hints {
		t.NoError(enc.Add(hints[i]))
	}

	tt.Encode = func() (interface{}, []byte) {
		t.prepare(base.RawPoint(33, 0))
		p := NewSuffrageProof(t.blockMap, t.current, t.proof)

		b, err := enc.Marshal(p)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return p, b
	}
	tt.Decode = func(b []byte) interface{} {
		hinter, err := enc.Decode(b)
		t.NoError(err)

		i, ok := hinter.(SuffrageProof)
		t.True(ok)

		return i
	}
	tt.Compare = func(a, b interface{}) {
		ah, ok := a.(SuffrageProof)
		t.True(ok)
		bh, ok := b.(SuffrageProof)
		t.True(ok)

		t.NoError(bh.IsValid(t.LocalParams.NetworkID()))

		t.True(ah.Hint().Equal(bh.Hint()))
		base.EqualBlockMap(t.Assert(), ah.m, bh.m)
		t.True(base.IsEqualState(ah.st, bh.st))

		ap := ah.proof.Nodes()
		bp := bh.proof.Nodes()
		t.Equal(len(ap), len(bp))
		for i := range ap {
			t.True(ap[i].Equal(bp[i]))
		}
	}

	suite.Run(t.T(), tt)
}

func TestSuffrageProof(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testSuffrageProof))
}
