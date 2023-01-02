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
	local     LocalNode
	networkID base.NetworkID
}

func (t *testVoteproof) SetupTest() {
	t.local = RandomLocalNode()
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

func (t *testVoteproof) validINITWithdrawVoteproof(point base.Point, withdraws []base.SuffrageWithdrawOperation) INITWithdrawVoteproof {
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	ifact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), withdrawfacts)

	isignfact := NewINITBallotSignFact(ifact)
	t.NoError(isignfact.NodeSign(t.local.Privatekey(), t.networkID, t.local.Address()))

	ivp := NewINITWithdrawVoteproof(ifact.Point().Point)
	ivp.
		SetMajority(ifact).
		SetSignFacts([]base.BallotSignFact{isignfact}).
		SetThreshold(base.Threshold(100))
	ivp.SetWithdraws(withdraws)
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
	t.ErrorContains(err, "failed to verify sign")
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

	newsignfact := func(node LocalNode) INITBallotSignFact {
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

func (t *testVoteproof) withdraws(
	height base.Height,
	withdrawnodes []base.Node,
	signnodes []base.Node,
) (
	[]util.Hash,
	[]base.SuffrageWithdrawOperation,
) {
	withdrawfacts := make([]util.Hash, len(withdrawnodes))
	withdraws := make([]base.SuffrageWithdrawOperation, len(withdrawnodes))

	for i := range withdrawnodes {
		fact := NewSuffrageWithdrawFact(withdrawnodes[i].Address(), height, height+1, util.UUID().String())
		withdrawfacts[i] = fact.Hash()
		withdraws[i] = NewSuffrageWithdrawOperation(fact)
	}

	for i := range signnodes {
		node := signnodes[i].(base.LocalNode)

		for j := range withdraws {
			w := withdraws[j].(SuffrageWithdrawOperation)
			t.NoError(w.NodeSign(node.Privatekey(), t.networkID, node.Address()))
			withdraws[j] = w
		}

	}

	return withdrawfacts, withdraws
}

func (t *testVoteproof) signsINITWithdrawVoteproof(vp INITWithdrawVoteproof, majority base.INITBallotFact, mnodes, others []base.Node) INITWithdrawVoteproof {
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

func (t *testVoteproof) TestINITWithWithdraws() {
	t.Run("ok: 1/4 withdraw node", func() {
		point := base.RawPoint(33, 0)

		withdrawnode := RandomLocalNode()
		nodes := []base.Node{t.local, RandomLocalNode(), RandomLocalNode(), withdrawnode}
		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		_, withdraws := t.withdraws(point.Height()-1, []base.Node{withdrawnode}, nodes[:3])

		ivp := t.validINITWithdrawVoteproof(point, withdraws)
		ivp = t.signsINITWithdrawVoteproof(ivp, ivp.Majority().(INITBallotFact), nodes[:3], nil)

		t.T().Log("voteresult:", ivp.Result())
		t.T().Log("sign facts:", len(ivp.SignFacts()))
		t.T().Log("withdraws:", len(ivp.Withdraws()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		t.NoError(IsValidVoteproofWithSuffrage(ivp, suf))
	})

	t.Run("ok: 2/5 withdraw nodes", func() {
		point := base.RawPoint(33, 0)

		nodes := []base.Node{t.local, RandomLocalNode(), RandomLocalNode()}
		withdrawnodes := []base.Node{RandomLocalNode(), RandomLocalNode()}
		nodes = append(nodes, withdrawnodes...)

		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		_, withdraws := t.withdraws(point.Height()-1, withdrawnodes, nodes[:3])

		ivp := t.validINITWithdrawVoteproof(point, withdraws)
		ivp = t.signsINITWithdrawVoteproof(ivp, ivp.Majority().(INITBallotFact), nodes[:3], nil)

		t.T().Log("voteresult:", ivp.Result())
		t.T().Log("sign facts:", len(ivp.SignFacts()))
		t.T().Log("withdraws:", len(ivp.Withdraws()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		t.NoError(IsValidVoteproofWithSuffrage(ivp, suf))
	})

	t.Run("ok: draw, withdraw nodes", func() {
		point := base.RawPoint(33, 0)

		nodes := []base.Node{t.local, RandomLocalNode(), RandomLocalNode()}
		withdrawnodes := []base.Node{RandomLocalNode(), RandomLocalNode()}
		nodes = append(nodes, withdrawnodes...)

		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		_, withdraws := t.withdraws(point.Height()-1, withdrawnodes, nodes[:3])

		ivp := t.validINITWithdrawVoteproof(point, withdraws)
		ivp = t.signsINITWithdrawVoteproof(ivp, INITBallotFact{}, nil, nodes[:3])

		t.T().Log("voteresult:", ivp.Result())
		t.T().Log("sign facts:", len(ivp.SignFacts()))
		t.T().Log("withdraws:", len(ivp.Withdraws()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		t.NoError(IsValidVoteproofWithSuffrage(ivp, suf))
	})

	t.Run("ok: 2/5 withdraw nodes, but not enough signs", func() {
		point := base.RawPoint(33, 0)

		nodes := []base.Node{t.local, RandomLocalNode(), RandomLocalNode()}
		withdrawnodes := []base.Node{RandomLocalNode(), RandomLocalNode()}
		nodes = append(nodes, withdrawnodes...)

		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		_, withdraws := t.withdraws(point.Height()-1, withdrawnodes, nodes[:3])

		ivp := t.validINITWithdrawVoteproof(point, withdraws)
		ivp = t.signsINITWithdrawVoteproof(ivp, ivp.Majority().(INITBallotFact), nodes[:2], nil)

		t.T().Log("voteresult:", ivp.Result())
		t.T().Log("sign facts:", len(ivp.SignFacts()))
		t.T().Log("withdraws:", len(ivp.Withdraws()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		err = IsValidVoteproofWithSuffrage(ivp, suf)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "wrong result")
	})

	t.Run("ok, but is stuck", func() {
		point := base.RawPoint(33, 0)

		withdrawnode := RandomLocalNode()
		nodes := []base.Node{t.local, RandomLocalNode(), RandomLocalNode(), withdrawnode}
		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		_, withdraws := t.withdraws(point.Height()-1, []base.Node{withdrawnode}, nodes[:3])

		ifact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)
		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.local.Privatekey(), t.networkID, t.local.Address()))

		ivp := NewINITWithdrawVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetWithdraws(withdraws)
		ivp.Finish()

		ivp = t.signsINITWithdrawVoteproof(ivp, ivp.Majority().(INITBallotFact), nodes[:3], nil)

		t.T().Log("voteresult:", ivp.Result())
		t.T().Log("sign facts:", len(ivp.SignFacts()))
		t.T().Log("withdraws:", len(ivp.Withdraws()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		t.NoError(IsValidVoteproofWithSuffrage(ivp, suf))
	})
}

func (t *testVoteproof) TestINITWithSIGN() {
	t.Run("all signs", func() {
		point := base.RawPoint(33, 1)

		withdrawnode := RandomLocalNode()
		nodes := []base.Node{t.local, RandomLocalNode(), RandomLocalNode(), withdrawnode}
		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		withdrawfacts, withdraws := t.withdraws(point.Height()-1, []base.Node{withdrawnode}, nodes[:3])

		sfact := NewSuffrageConfirmBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), withdrawfacts)

		ivp := NewINITWithdrawVoteproof(sfact.Point().Point)
		ivp.
			SetMajority(sfact).
			SetThreshold(base.Threshold(100))
		ivp.SetWithdraws(withdraws)
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
		t.T().Log("withdraws:", len(ivp.Withdraws()))
		t.T().Log("suffrage len:", suf.Len())

		t.NoError(ivp.IsValid(t.networkID))

		t.NoError(IsValidVoteproofWithSuffrage(ivp, suf))
	})

	t.Run("mixed signs", func() {
		point := base.RawPoint(33, 1)

		withdrawnode := RandomLocalNode()
		nodes := []base.Node{t.local, RandomLocalNode(), RandomLocalNode(), withdrawnode}
		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		withdrawfacts, withdraws := t.withdraws(point.Height()-1, []base.Node{withdrawnode}, nodes[:3])

		sfact := NewSuffrageConfirmBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), withdrawfacts)
		ifact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), withdrawfacts)

		ivp := NewINITWithdrawVoteproof(sfact.Point().Point)
		ivp.
			SetThreshold(base.Threshold(100))
		ivp.SetWithdraws(withdraws)
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
		t.T().Log("withdraws:", len(ivp.Withdraws()))
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
		n := RandomLocalNode()
		nodes[i+1] = n
		localnodes[i+1] = n
	}
	withdrawnode := nodes[2]

	suf, err := NewSuffrage(nodes)
	t.NoError(err)

	withdrawfacts, withdraws := t.withdraws(point.Height(), []base.Node{withdrawnode}, nodes[:2])

	fact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), withdrawfacts)

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
		_ = vp.SetWithdraws(withdraws)
		_ = vp.Finish()

		t.Nil(vp.Majority())
		t.Equal(base.VoteResultDraw, vp.Result())

		t.NoError(vp.IsValid(t.networkID))
		t.NoError(IsValidVoteproofWithSuffrage(vp, suf))
	})

	t.Run("empty withdraws", func() {
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
		t.ErrorContains(err, "empty withdraws")

		err = IsValidVoteproofWithSuffrage(vp, suf)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "not enough sign facts with withdraws")
	})

	t.Run("not enough sign facts", func() {
		vp := NewINITStuckVoteproof(point)

		_ = vp.
			SetSignFacts(sfs[:1]).
			SetMajority(fact)
		_ = vp.SetWithdraws(withdraws)
		_ = vp.Finish()

		t.Nil(vp.Majority())
		t.Equal(base.VoteResultDraw, vp.Result())

		t.NoError(vp.IsValid(t.networkID))
		err := IsValidVoteproofWithSuffrage(vp, suf)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "not enough sign facts with withdraws")
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
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITWithdrawVoteproofHint, Instance: INITWithdrawVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITStuckVoteproofHint, Instance: INITStuckVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTVoteproofHint, Instance: ACCEPTVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTWithdrawVoteproofHint, Instance: ACCEPTWithdrawVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTStuckVoteproofHint, Instance: ACCEPTStuckVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotSignFactHint, Instance: INITBallotSignFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotSignFactHint, Instance: ACCEPTBallotSignFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageWithdrawFactHint, Instance: SuffrageWithdrawFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageWithdrawOperationHint, Instance: SuffrageWithdrawOperation{}}))
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
			node := RandomLocalNode()
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

func TestINITWithdrawVoteproofJSON(tt *testing.T) {
	t := testBaseVoteproofEncode()

	t.Encode = func() (interface{}, []byte) {
		point := base.RawPoint(32, 44)

		withdrawfact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		withdraw := NewSuffrageWithdrawOperation(withdrawfact)
		t.NoError(withdraw.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		sfs := make([]base.BallotSignFact, 2)
		for i := range sfs {
			node := RandomLocalNode()
			fact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{withdrawfact.Hash()})
			sf := NewINITBallotSignFact(fact)
			t.NoError(sf.NodeSign(node.Privatekey(), t.networkID, node.Address()))

			sfs[i] = sf
		}

		ivp := NewINITWithdrawVoteproof(point)
		ivp.
			SetMajority(sfs[0].Fact().(base.BallotFact)).
			SetSignFacts(sfs).
			SetThreshold(base.Threshold(100))
		ivp.SetWithdraws([]base.SuffrageWithdrawOperation{withdraw})
		ivp.Finish()

		b, err := t.enc.Marshal(&ivp)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return ivp, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(base.WithdrawVoteproof)
		t.True(ok)

		ivp, ok := i.(INITWithdrawVoteproof)
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

		withdrawfact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		withdraw := NewSuffrageWithdrawOperation(withdrawfact)
		t.NoError(withdraw.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		sfs := make([]base.BallotSignFact, 2)
		for i := range sfs {
			node := RandomLocalNode()
			fact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{withdrawfact.Hash()})
			sf := NewINITBallotSignFact(fact)
			t.NoError(sf.NodeSign(node.Privatekey(), t.networkID, node.Address()))

			sfs[i] = sf
		}

		ivp := NewINITStuckVoteproof(point)
		ivp.
			SetMajority(sfs[0].Fact().(base.BallotFact)).
			SetSignFacts(sfs).
			SetThreshold(base.Threshold(100))
		ivp.SetWithdraws([]base.SuffrageWithdrawOperation{withdraw})
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

func TestACCEPTWithdrawVoteproofJSON(tt *testing.T) {
	t := testBaseVoteproofEncode()

	t.Encode = func() (interface{}, []byte) {
		node := base.RandomAddress("")

		point := base.RawPoint(32, 44)

		withdrawfact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		withdraw := NewSuffrageWithdrawOperation(withdrawfact)
		t.NoError(withdraw.NodeSign(t.priv, t.networkID, node))

		afact := NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{withdrawfact.Hash()})

		asignfact := NewACCEPTBallotSignFact(afact)

		t.NoError(asignfact.NodeSign(t.priv, t.networkID, node))

		avp := NewACCEPTWithdrawVoteproof(afact.Point().Point)
		avp.
			SetMajority(afact).
			SetSignFacts([]base.BallotSignFact{asignfact}).
			SetThreshold(base.Threshold(100))
		avp.SetWithdraws([]base.SuffrageWithdrawOperation{withdraw})
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

		_, ok := i.(base.WithdrawVoteproof)
		t.True(ok)

		avp, ok := i.(ACCEPTWithdrawVoteproof)
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

		withdrawfact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		withdraw := NewSuffrageWithdrawOperation(withdrawfact)
		t.NoError(withdraw.NodeSign(t.priv, t.networkID, node))

		afact := NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{withdrawfact.Hash()})

		asignfact := NewACCEPTBallotSignFact(afact)

		t.NoError(asignfact.NodeSign(t.priv, t.networkID, node))

		avp := NewACCEPTStuckVoteproof(afact.Point().Point)
		avp.
			SetMajority(afact).
			SetSignFacts([]base.BallotSignFact{asignfact}).
			SetThreshold(base.Threshold(100))
		avp.SetWithdraws([]base.SuffrageWithdrawOperation{withdraw})
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
