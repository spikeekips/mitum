package isaac

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testVoteproof struct {
	suite.Suite
	local     base.LocalNode
	networkID base.NetworkID
}

func (t *testVoteproof) SetupTest() {
	t.local = base.RandomLocalNode()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testVoteproof) validINITVoteproof(point base.Point) INITVoteproof {
	ifact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	isignfact := NewINITBallotSignFact(ifact)
	t.NoError(isignfact.NodeSign(t.local.Privatekey(), t.networkID, t.local.Address()))

	ivp := NewINITVoteproof(ifact.Point().Point)
	ivp.
		SetMajority(ifact).
		SetSignFacts([]base.BallotSignFact{isignfact}).
		SetThreshold(base.Threshold(100)).
		Finish()

	return ivp
}

func (t *testVoteproof) validINITExpelVoteproof(point base.Point, expels []base.SuffrageExpelOperation) INITExpelVoteproof {
	expelfacts := make([]util.Hash, len(expels))
	for i := range expels {
		expelfacts[i] = expels[i].Fact().Hash()
	}

	ifact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), expelfacts)

	isignfact := NewINITBallotSignFact(ifact)
	t.NoError(isignfact.NodeSign(t.local.Privatekey(), t.networkID, t.local.Address()))

	ivp := NewINITExpelVoteproof(ifact.Point().Point)
	ivp.
		SetMajority(ifact).
		SetSignFacts([]base.BallotSignFact{isignfact}).
		SetThreshold(base.Threshold(100))
	ivp.SetExpels(expels)
	ivp.Finish()

	return ivp
}

func (t *testVoteproof) TestNewINIT() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))

	t.NoError(ivp.IsValid(t.networkID))
}

func (t *testVoteproof) TestEmptyID() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))
	ivp.id = ""

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "empty id")
}

func (t *testVoteproof) TestInvalidStage() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))
	ivp.point = base.NewStagePoint(ivp.point.Point, base.StageUnknown)

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "wrong stage")
}

func (t *testVoteproof) TestInvalidVoteResult() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))
	ivp.SetResult(base.VoteResultNotYet)

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "not yet finished")
}

func (t *testVoteproof) TestZeroFinishedAt() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))
	ivp.finishedAt = time.Time{}

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.ErrorContains(err, "not yet finished")
}

func (t *testVoteproof) TestEmptySignFacts() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))
	ivp.SetSignFacts(nil).Finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "empty sign facts")
}

func (t *testVoteproof) TestNilMajority() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))
	ivp.SetMajority(nil)

	err := ivp.IsValid(t.networkID)
	t.NoError(err)

	t.Equal(base.VoteResultDraw, ivp.Result())
}

func (t *testVoteproof) TestInvalidPoint() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))
	ivp.point = base.NewStagePoint(base.NewPoint(base.NilHeight, base.Round(0)), ivp.point.Stage())

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "invalid point")
}

func (t *testVoteproof) TestDuplicatedNodeInSignFact() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))

	sfs := ivp.SignFacts()
	isf := sfs[0].(INITBallotSignFact)

	isignfact := NewINITBallotSignFact(isf.Fact().(INITBallotFact))
	t.NoError(isignfact.NodeSign(t.local.Privatekey(), t.networkID, isf.Node()))

	sfs = append(sfs, isignfact)

	ivp.SetSignFacts(sfs).Finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "duplicated node found")
}

func (t *testVoteproof) TestInvalidSignFact() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))

	ifact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	isignfact := NewINITBallotSignFact(ifact)
	t.NoError(isignfact.NodeSign(t.local.Privatekey(), util.UUID().Bytes(), base.RandomAddress(""))) // wrong network id

	ivp.SetMajority(ifact).SetSignFacts([]base.BallotSignFact{isignfact}).Finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "verify sign")
}

func (t *testVoteproof) TestWrongPointOfSignFact() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))

	ifact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	wfact := NewINITBallotFact(base.RawPoint(34, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	isignfact := NewINITBallotSignFact(ifact)
	t.NoError(isignfact.NodeSign(t.local.Privatekey(), t.networkID, base.RandomAddress("")))

	wsignfact := NewINITBallotSignFact(wfact)
	t.NoError(wsignfact.NodeSign(t.local.Privatekey(), t.networkID, base.RandomAddress("")))

	ivp.SetMajority(ifact).SetSignFacts([]base.BallotSignFact{isignfact, wsignfact}).Finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "point does not match")
	t.ErrorContains(err, "invalid sign fact")
}

func (t *testVoteproof) TestWrongPointOfMajority() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))

	ifact := NewINITBallotFact(base.NewPoint(ivp.Point().Height()+1, ivp.Point().Round()), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	isignfact := NewINITBallotSignFact(ifact)

	t.NoError(isignfact.NodeSign(t.local.Privatekey(), t.networkID, base.RandomAddress("")))

	ivp.SetMajority(ifact).SetSignFacts([]base.BallotSignFact{isignfact}).Finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "point does not match")
	t.ErrorContains(err, "invalid majority")
}

func (t *testVoteproof) TestMajorityNotFoundInSignFacts() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))

	fact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	ivp.SetMajority(fact).Finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "majoirty not found in sign facts")
}

func (t *testVoteproof) TestWrongMajorityWithSuffrage() {
	suf, nodes := NewTestSuffrage(4)
	t.local = nodes[0]

	ivp := t.validINITVoteproof(base.RawPoint(33, 55))

	fact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	newsignfact := func(node base.LocalNode) INITBallotSignFact {
		signfact := NewINITBallotSignFact(fact)

		t.NoError(signfact.NodeSign(node.Privatekey(), t.networkID, node.Address()))

		return signfact
	}

	sfs := ivp.SignFacts()
	sfs = append(sfs, newsignfact(nodes[1]), newsignfact(nodes[2]), newsignfact(nodes[3]))
	ivp.SetSignFacts(sfs).SetThreshold(base.Threshold(67)).Finish()

	t.NoError(ivp.IsValid(t.networkID))

	err := base.IsValidVoteproofWithSuffrage(ivp, suf, ivp.Threshold())
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "wrong majority")
}

func (t *testVoteproof) TestUnknownNode() {
	ivp := t.validINITVoteproof(base.RawPoint(33, 55))

	ivp.SetThreshold(base.Threshold(100))

	t.NoError(ivp.IsValid(t.networkID))

	suf, _ := NewTestSuffrage(1)

	err := base.IsValidVoteproofWithSuffrage(ivp, suf, ivp.Threshold())
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "unknown node found")
}

func (t *testVoteproof) TestNewACCEPT() {
	afact := NewACCEPTBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	node := base.RandomAddress("")
	asignfact := NewACCEPTBallotSignFact(afact)

	t.NoError(asignfact.NodeSign(t.local.Privatekey(), t.networkID, node))

	avp := NewACCEPTVoteproof(afact.Point().Point)
	avp.
		SetMajority(afact).
		SetSignFacts([]base.BallotSignFact{asignfact}).
		SetThreshold(base.Threshold(100)).
		Finish()

	t.NoError(avp.IsValid(t.networkID))
}

func (t *testVoteproof) expels(
	height base.Height,
	expelnodes []base.Node,
	signnodes []base.Node,
) (
	[]util.Hash,
	[]base.SuffrageExpelOperation,
) {
	expelfacts := make([]util.Hash, len(expelnodes))
	expels := make([]base.SuffrageExpelOperation, len(expelnodes))

	for i := range expelnodes {
		fact := NewSuffrageExpelFact(expelnodes[i].Address(), height, height+1, util.UUID().String())
		expelfacts[i] = fact.Hash()
		expels[i] = NewSuffrageExpelOperation(fact)
	}

	for i := range signnodes {
		node := signnodes[i].(base.LocalNode)

		for j := range expels {
			w := expels[j].(SuffrageExpelOperation)
			t.NoError(w.NodeSign(node.Privatekey(), t.networkID, node.Address()))
			expels[j] = w
		}

	}

	return expelfacts, expels
}

func (t *testVoteproof) signsINITExpelVoteproof(vp INITExpelVoteproof, majority base.INITBallotFact, mnodes, others []base.Node) INITExpelVoteproof {
	var signfacts []base.BallotSignFact

	if majority.Proposal() != nil {
		for i := range mnodes {
			node := mnodes[i].(base.LocalNode)

			signfact := NewINITBallotSignFact(majority)
			t.NoError(signfact.NodeSign(node.Privatekey(), t.networkID, node.Address()))
			signfacts = append(signfacts, signfact)
		}
	}

	for i := range others {
		node := others[i].(base.LocalNode)

		ifact := NewINITBallotFact(vp.Point().Point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)
		signfact := NewINITBallotSignFact(ifact)
		t.NoError(signfact.NodeSign(node.Privatekey(), t.networkID, node.Address()))
		signfacts = append(signfacts, signfact)
	}

	switch {
	case majority.Proposal() == nil:
		vp.SetMajority(nil)
	default:
		vp.SetMajority(majority)
	}

	vp.SetSignFacts(signfacts)

	return vp
}

func (t *testVoteproof) TestINITWithExpels() {
	t.Run("ok: 1/4 expel node", func() {
		point := base.RawPoint(33, 0)

		expelnode := base.RandomLocalNode()
		nodes := []base.Node{t.local, base.RandomLocalNode(), base.RandomLocalNode(), expelnode}
		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		_, expels := t.expels(point.Height()-1, []base.Node{expelnode}, nodes[:3])

		ivp := t.validINITExpelVoteproof(point, expels)
		ivp = t.signsINITExpelVoteproof(ivp, ivp.Majority().(INITBallotFact), nodes[:3], nil)

		t.T().Log("voteresult:", ivp.Result())
		t.T().Log("sign facts:", len(ivp.SignFacts()))
		t.T().Log("expels:", len(ivp.Expels()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		t.NoError(IsValidVoteproofWithSuffrage(ivp, suf))
	})

	t.Run("ok: 2/5 expel nodes", func() {
		point := base.RawPoint(33, 0)

		nodes := []base.Node{t.local, base.RandomLocalNode(), base.RandomLocalNode()}
		expelnodes := []base.Node{base.RandomLocalNode(), base.RandomLocalNode()}
		nodes = append(nodes, expelnodes...)

		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		_, expels := t.expels(point.Height()-1, expelnodes, nodes[:3])

		ivp := t.validINITExpelVoteproof(point, expels)
		ivp = t.signsINITExpelVoteproof(ivp, ivp.Majority().(INITBallotFact), nodes[:3], nil)

		t.T().Log("voteresult:", ivp.Result())
		t.T().Log("sign facts:", len(ivp.SignFacts()))
		t.T().Log("expels:", len(ivp.Expels()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		t.NoError(IsValidVoteproofWithSuffrage(ivp, suf))
	})

	t.Run("ok: draw, expel nodes", func() {
		point := base.RawPoint(33, 0)

		nodes := []base.Node{t.local, base.RandomLocalNode(), base.RandomLocalNode()}
		expelnodes := []base.Node{base.RandomLocalNode(), base.RandomLocalNode()}
		nodes = append(nodes, expelnodes...)

		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		_, expels := t.expels(point.Height()-1, expelnodes, nodes[:3])

		ivp := t.validINITExpelVoteproof(point, expels)
		ivp = t.signsINITExpelVoteproof(ivp, INITBallotFact{}, nil, nodes[:3])

		t.T().Log("voteresult:", ivp.Result())
		t.T().Log("sign facts:", len(ivp.SignFacts()))
		t.T().Log("expels:", len(ivp.Expels()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		t.NoError(IsValidVoteproofWithSuffrage(ivp, suf))
	})

	t.Run("ok: 2/5 expel nodes, but not enough signs", func() {
		point := base.RawPoint(33, 0)

		nodes := []base.Node{t.local, base.RandomLocalNode(), base.RandomLocalNode()}
		expelnodes := []base.Node{base.RandomLocalNode(), base.RandomLocalNode()}
		nodes = append(nodes, expelnodes...)

		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		_, expels := t.expels(point.Height()-1, expelnodes, nodes[:3])

		ivp := t.validINITExpelVoteproof(point, expels)
		ivp = t.signsINITExpelVoteproof(ivp, ivp.Majority().(INITBallotFact), nodes[:2], nil)

		t.T().Log("voteresult:", ivp.Result())
		t.T().Log("sign facts:", len(ivp.SignFacts()))
		t.T().Log("expels:", len(ivp.Expels()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		err = IsValidVoteproofWithSuffrage(ivp, suf)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "wrong result")
	})

	t.Run("ok, but is stuck", func() {
		point := base.RawPoint(33, 0)

		expelnode := base.RandomLocalNode()
		nodes := []base.Node{t.local, base.RandomLocalNode(), base.RandomLocalNode(), expelnode}
		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		_, expels := t.expels(point.Height()-1, []base.Node{expelnode}, nodes[:3])

		ifact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)
		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.local.Privatekey(), t.networkID, t.local.Address()))

		ivp := NewINITExpelVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetExpels(expels)
		ivp.Finish()

		ivp = t.signsINITExpelVoteproof(ivp, ivp.Majority().(INITBallotFact), nodes[:3], nil)

		t.T().Log("voteresult:", ivp.Result())
		t.T().Log("sign facts:", len(ivp.SignFacts()))
		t.T().Log("expels:", len(ivp.Expels()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		t.NoError(IsValidVoteproofWithSuffrage(ivp, suf))
	})
}

func (t *testVoteproof) TestINITWithSIGN() {
	t.Run("all signs", func() {
		point := base.RawPoint(33, 1)

		expelnode := base.RandomLocalNode()
		nodes := []base.Node{t.local, base.RandomLocalNode(), base.RandomLocalNode(), expelnode}
		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		expelfacts, expels := t.expels(point.Height()-1, []base.Node{expelnode}, nodes[:3])

		sfact := NewSuffrageConfirmBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), expelfacts)

		ivp := NewINITExpelVoteproof(sfact.Point().Point)
		ivp.
			SetMajority(sfact).
			SetThreshold(base.Threshold(100))
		ivp.SetExpels(expels)
		ivp.Finish()

		var signfacts []base.BallotSignFact
		for i := range nodes[:3] {
			node := nodes[i].(base.LocalNode)

			signfact := NewINITBallotSignFact(sfact)
			t.NoError(signfact.NodeSign(node.Privatekey(), t.networkID, node.Address()))
			signfacts = append(signfacts, signfact)
		}

		ivp.SetSignFacts(signfacts)

		t.T().Log("voteresult:", ivp.Result())
		t.T().Log("sign facts:", len(ivp.SignFacts()))
		t.T().Log("expels:", len(ivp.Expels()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		t.NoError(IsValidVoteproofWithSuffrage(ivp, suf))
	})

	t.Run("mixed signs", func() {
		point := base.RawPoint(33, 1)

		expelnode := base.RandomLocalNode()
		nodes := []base.Node{t.local, base.RandomLocalNode(), base.RandomLocalNode(), expelnode}
		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		expelfacts, expels := t.expels(point.Height()-1, []base.Node{expelnode}, nodes[:3])

		sfact := NewSuffrageConfirmBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), expelfacts)
		ifact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), expelfacts)

		ivp := NewINITExpelVoteproof(sfact.Point().Point)
		ivp.
			SetThreshold(base.Threshold(100))
		ivp.SetExpels(expels)
		ivp.Finish()

		var signfacts []base.BallotSignFact
		for i := range nodes[:3] {
			node := nodes[i].(base.LocalNode)

			var signfact base.BallotSignFact
			if i%2 == 0 {
				j := NewINITBallotSignFact(sfact)
				t.NoError(j.NodeSign(node.Privatekey(), t.networkID, node.Address()))

				signfact = j
			} else {
				j := NewINITBallotSignFact(ifact)
				t.NoError(j.NodeSign(node.Privatekey(), t.networkID, node.Address()))

				signfact = j
			}
			signfacts = append(signfacts, signfact)
		}

		ivp.SetSignFacts(signfacts)

		t.T().Log("voteresult:", ivp.Result())
		t.T().Log("sign facts:", len(ivp.SignFacts()))
		t.T().Log("expels:", len(ivp.Expels()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		t.NoError(IsValidVoteproofWithSuffrage(ivp, suf))
	})
}

func (t *testVoteproof) TestStuckVoteproof() {
	point := base.RawPoint(33, 44)

	localnodes := make([]base.LocalNode, 3)
	nodes := make([]base.Node, 3)

	localnodes[0] = t.local
	nodes[0] = t.local
	for i := range nodes[1:] {
		n := base.RandomLocalNode()
		nodes[i+1] = n
		localnodes[i+1] = n
	}
	expelnode := nodes[2]

	suf, err := NewSuffrage(nodes)
	t.NoError(err)

	expelfacts, expels := t.expels(point.Height(), []base.Node{expelnode}, nodes[:2])

	fact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), expelfacts)

	sfs := make([]base.BallotSignFact, 2)
	for i, node := range localnodes[:2] {
		sf := NewINITBallotSignFact(fact)
		t.NoError(sf.NodeSign(node.Privatekey(), t.networkID, node.Address()))

		sfs[i] = sf
	}

	t.Run("valid", func() {
		vp := NewINITStuckVoteproof(point)

		_ = vp.
			SetSignFacts(sfs).
			SetMajority(fact)
		_ = vp.SetExpels(expels)
		_ = vp.Finish()

		t.Nil(vp.Majority())
		t.Equal(base.VoteResultDraw, vp.Result())

		t.NoError(vp.IsValid(t.networkID))
		t.NoError(IsValidVoteproofWithSuffrage(vp, suf))
	})

	t.Run("empty expels", func() {
		vp := NewINITStuckVoteproof(point)

		_ = vp.
			SetSignFacts(sfs[:1]).
			SetMajority(fact)
		_ = vp.Finish()

		t.Nil(vp.Majority())
		t.Equal(base.VoteResultDraw, vp.Result())

		err := vp.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty expels")

		err = IsValidVoteproofWithSuffrage(vp, suf)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "not enough sign facts with expels")
	})

	t.Run("not enough sign facts", func() {
		vp := NewINITStuckVoteproof(point)

		_ = vp.
			SetSignFacts(sfs[:1]).
			SetMajority(fact)
		_ = vp.SetExpels(expels)
		_ = vp.Finish()

		t.Nil(vp.Majority())
		t.Equal(base.VoteResultDraw, vp.Result())

		t.NoError(vp.IsValid(t.networkID))
		err := IsValidVoteproofWithSuffrage(vp, suf)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "not enough sign facts with expels")
	})
}

func TestVoteproof(t *testing.T) {
	suite.Run(t, new(testVoteproof))
}

type baseTestBaseVoteproofEncode struct {
	encoder.BaseTestEncode
	enc       encoder.Encoder
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *baseTestBaseVoteproofEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotFactHint, Instance: INITBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotFactHint, Instance: ACCEPTBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITVoteproofHint, Instance: INITVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITExpelVoteproofHint, Instance: INITExpelVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITStuckVoteproofHint, Instance: INITStuckVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTVoteproofHint, Instance: ACCEPTVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTExpelVoteproofHint, Instance: ACCEPTExpelVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTStuckVoteproofHint, Instance: ACCEPTStuckVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotSignFactHint, Instance: INITBallotSignFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotSignFactHint, Instance: ACCEPTBallotSignFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageExpelFactHint, Instance: SuffrageExpelFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageExpelOperationHint, Instance: SuffrageExpelOperation{}}))
}

func testBaseVoteproofEncode() *baseTestBaseVoteproofEncode {
	t := new(baseTestBaseVoteproofEncode)

	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())

	t.Compare = func(a, b interface{}) {
		avp, ok := a.(base.Voteproof)
		t.True(ok)
		bvp, ok := b.(base.Voteproof)
		t.True(ok)

		t.NoError(bvp.IsValid(t.networkID))

		EqualVoteproof(t.Assert(), avp, bvp)
	}

	return t
}

func TestINITVoteproofJSON(tt *testing.T) {
	t := testBaseVoteproofEncode()

	t.Encode = func() (interface{}, []byte) {
		point := base.RawPoint(32, 44)

		sfs := make([]base.BallotSignFact, 2)
		for i := range sfs {
			node := base.RandomLocalNode()
			fact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)
			sf := NewINITBallotSignFact(fact)
			t.NoError(sf.NodeSign(node.Privatekey(), t.networkID, node.Address()))

			sfs[i] = sf
		}

		ivp := NewINITVoteproof(point)
		ivp.
			SetMajority(sfs[0].Fact().(base.BallotFact)).
			SetSignFacts(sfs).
			SetThreshold(base.Threshold(100)).
			Finish()

		b, err := t.enc.Marshal(&ivp)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return ivp, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(INITVoteproof)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}

func TestINITExpelVoteproofJSON(tt *testing.T) {
	t := testBaseVoteproofEncode()

	t.Encode = func() (interface{}, []byte) {
		point := base.RawPoint(32, 44)

		expelfact := NewSuffrageExpelFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		expel := NewSuffrageExpelOperation(expelfact)
		t.NoError(expel.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		sfs := make([]base.BallotSignFact, 2)
		for i := range sfs {
			node := base.RandomLocalNode()
			fact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{expelfact.Hash()})
			sf := NewINITBallotSignFact(fact)
			t.NoError(sf.NodeSign(node.Privatekey(), t.networkID, node.Address()))

			sfs[i] = sf
		}

		ivp := NewINITExpelVoteproof(point)
		ivp.
			SetMajority(sfs[0].Fact().(base.BallotFact)).
			SetSignFacts(sfs).
			SetThreshold(base.Threshold(100))
		ivp.SetExpels([]base.SuffrageExpelOperation{expel})
		ivp.Finish()

		b, err := t.enc.Marshal(&ivp)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return ivp, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(base.ExpelVoteproof)
		t.True(ok)

		ivp, ok := i.(INITExpelVoteproof)
		t.True(ok)

		t.NoError(ivp.IsValid(t.networkID))

		return i
	}

	suite.Run(tt, t)
}

func TestINITStuckVoteproofJSON(tt *testing.T) {
	t := testBaseVoteproofEncode()

	t.Encode = func() (interface{}, []byte) {
		point := base.RawPoint(32, 44)

		expelfact := NewSuffrageExpelFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		expel := NewSuffrageExpelOperation(expelfact)
		t.NoError(expel.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		sfs := make([]base.BallotSignFact, 2)
		for i := range sfs {
			node := base.RandomLocalNode()
			fact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{expelfact.Hash()})
			sf := NewINITBallotSignFact(fact)
			t.NoError(sf.NodeSign(node.Privatekey(), t.networkID, node.Address()))

			sfs[i] = sf
		}

		ivp := NewINITStuckVoteproof(point)
		ivp.
			SetMajority(sfs[0].Fact().(base.BallotFact)).
			SetSignFacts(sfs).
			SetThreshold(base.Threshold(100))
		ivp.SetExpels([]base.SuffrageExpelOperation{expel})
		ivp.Finish()

		b, err := t.enc.Marshal(&ivp)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return ivp, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(base.StuckVoteproof)
		t.True(ok)

		ivp, ok := i.(INITStuckVoteproof)
		t.True(ok)

		t.NoError(ivp.IsValid(t.networkID))

		return i
	}

	suite.Run(tt, t)
}

func TestACCEPTVoteproofJSON(tt *testing.T) {
	t := testBaseVoteproofEncode()

	t.Encode = func() (interface{}, []byte) {
		node := base.RandomAddress("")

		point := base.RawPoint(32, 44)

		afact := NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

		asignfact := NewACCEPTBallotSignFact(afact)

		t.NoError(asignfact.NodeSign(t.priv, t.networkID, node))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.
			SetMajority(afact).
			SetSignFacts([]base.BallotSignFact{asignfact}).
			SetThreshold(base.Threshold(100)).
			Finish()

		t.NoError(avp.IsValid(t.networkID))

		b, err := t.enc.Marshal(&avp)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return avp, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		avp, ok := i.(ACCEPTVoteproof)
		t.True(ok)

		t.NoError(avp.IsValid(t.networkID))

		return i
	}

	suite.Run(tt, t)
}

func TestACCEPTExpelVoteproofJSON(tt *testing.T) {
	t := testBaseVoteproofEncode()

	t.Encode = func() (interface{}, []byte) {
		node := base.RandomAddress("")

		point := base.RawPoint(32, 44)

		expelfact := NewSuffrageExpelFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		expel := NewSuffrageExpelOperation(expelfact)
		t.NoError(expel.NodeSign(t.priv, t.networkID, node))

		afact := NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{expelfact.Hash()})

		asignfact := NewACCEPTBallotSignFact(afact)

		t.NoError(asignfact.NodeSign(t.priv, t.networkID, node))

		avp := NewACCEPTExpelVoteproof(afact.Point().Point)
		avp.
			SetMajority(afact).
			SetSignFacts([]base.BallotSignFact{asignfact}).
			SetThreshold(base.Threshold(100))
		avp.SetExpels([]base.SuffrageExpelOperation{expel})
		avp.Finish()

		t.NoError(avp.IsValid(t.networkID))

		b, err := t.enc.Marshal(&avp)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return avp, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(base.ExpelVoteproof)
		t.True(ok)

		avp, ok := i.(ACCEPTExpelVoteproof)
		t.True(ok)

		t.NoError(avp.IsValid(t.networkID))

		return i
	}

	suite.Run(tt, t)
}

func TestACCEPTStuckVoteproofJSON(tt *testing.T) {
	t := testBaseVoteproofEncode()

	t.Encode = func() (interface{}, []byte) {
		node := base.RandomAddress("")

		point := base.RawPoint(32, 44)

		expelfact := NewSuffrageExpelFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		expel := NewSuffrageExpelOperation(expelfact)
		t.NoError(expel.NodeSign(t.priv, t.networkID, node))

		afact := NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{expelfact.Hash()})

		asignfact := NewACCEPTBallotSignFact(afact)

		t.NoError(asignfact.NodeSign(t.priv, t.networkID, node))

		avp := NewACCEPTStuckVoteproof(afact.Point().Point)
		avp.
			SetMajority(afact).
			SetSignFacts([]base.BallotSignFact{asignfact}).
			SetThreshold(base.Threshold(100))
		avp.SetExpels([]base.SuffrageExpelOperation{expel})
		avp.Finish()

		t.NoError(avp.IsValid(t.networkID))

		b, err := t.enc.Marshal(&avp)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return avp, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(base.StuckVoteproof)
		t.True(ok)

		avp, ok := i.(ACCEPTStuckVoteproof)
		t.True(ok)

		t.NoError(avp.IsValid(t.networkID))

		return i
	}

	suite.Run(tt, t)
}
