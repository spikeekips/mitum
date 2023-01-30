package isaac

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testBaseBallot struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
	ballot    func(base.Point, []SuffrageWithdrawOperation, []util.Hash, []SuffrageWithdrawOperation, []util.Hash) base.Ballot
}

func (t *testBaseBallot) SetupTest() {
	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testBaseBallot) TestNew() {
	bl := t.ballot(base.RawPoint(33, 44), nil, nil, nil, nil)
	t.NoError(bl.IsValid(t.networkID))
}

func (t *testBaseBallot) TestEmptyVoteproof() {
	bl := t.ballot(base.RawPoint(33, 44), nil, nil, nil, nil)
	t.NoError(bl.IsValid(t.networkID))

	switch bt := bl.(type) {
	case INITBallot:
		bt.vp = nil
		bl = bt
	case ACCEPTBallot:
		bt.vp = nil
		bl = bt
	}
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
}

func (t *testBaseBallot) TestEmptySignFact() {
	bl := t.ballot(base.RawPoint(33, 44), nil, nil, nil, nil)
	t.NoError(bl.IsValid(t.networkID))

	switch bt := bl.(type) {
	case INITBallot:
		bt.signFact = nil
		bl = bt
	case ACCEPTBallot:
		bt.signFact = nil
		bl = bt
	}
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
}

func (t *testBaseBallot) TestWithdrawNodeInSigns() {
	node := base.RandomAddress("")
	point := base.RawPoint(33, 44)

	withdrawnode := RandomLocalNode()

	fact := NewSuffrageWithdrawFact(withdrawnode.Address(), point.Height()-1, point.Height()+1, util.UUID().String())
	withdraw := NewSuffrageWithdrawOperation(fact)

	t.NoError(withdraw.NodeSign(t.priv, t.networkID, node))
	t.NoError(withdraw.NodeSign(withdrawnode.Privatekey(), t.networkID, withdrawnode.Address()))

	bl := t.ballot(point, []SuffrageWithdrawOperation{withdraw}, []util.Hash{fact.Hash()}, []SuffrageWithdrawOperation{withdraw}, []util.Hash{fact.Hash()})
	t.NoError(bl.IsValid(t.networkID))
}

func (t *testBaseBallot) TestInvalidWithdraw() {
	t.Run("valid", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		withdraws := make([]SuffrageWithdrawOperation, 3)
		withdrawfacts := make([]util.Hash, len(withdraws))
		for i := range withdrawfacts {
			fact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
			withdrawfacts[i] = fact.Hash()
			withdraws[i] = NewSuffrageWithdrawOperation(fact)
			t.NoError(withdraws[i].NodeSign(t.priv, t.networkID, node))
		}

		bl := t.ballot(point, withdraws, withdrawfacts, withdraws, withdrawfacts)
		t.NoError(bl.IsValid(t.networkID))
	})

	t.Run("invalid", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		withdraws := make([]SuffrageWithdrawOperation, 3)
		withdrawfacts := make([]util.Hash, len(withdraws))
		for i := range withdrawfacts {
			fact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
			withdrawfacts[i] = fact.Hash()
			withdraws[i] = NewSuffrageWithdrawOperation(fact)

			if i != len(withdrawfacts)-1 {
				t.NoError(withdraws[i].NodeSign(t.priv, t.networkID, node))
			}
		}

		bl := t.ballot(point, nil, nil, withdraws, withdrawfacts)
		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty signs")
	})

	t.Run("wrong start height withdraw fact", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		withdraws := make([]SuffrageWithdrawOperation, 3)
		withdrawfacts := make([]util.Hash, len(withdraws))
		for i := range withdrawfacts {
			fact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()+1, point.Height()+2, util.UUID().String())
			withdrawfacts[i] = fact.Hash()
			withdraws[i] = NewSuffrageWithdrawOperation(fact)
			t.NoError(withdraws[i].NodeSign(t.priv, t.networkID, node))
		}

		bl := t.ballot(point, withdraws, withdrawfacts, withdraws, withdrawfacts)

		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "wrong start height in withdraw")
	})

	t.Run("duplicated withdraw node", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		withdrawnode := base.RandomAddress("")

		withdraws := make([]SuffrageWithdrawOperation, 3)
		withdrawfacts := make([]util.Hash, len(withdraws))
		for i := range withdrawfacts {
			var n base.Address

			if i%2 == 0 {
				n = withdrawnode
			} else {
				n = base.RandomAddress("")
			}

			fact := NewSuffrageWithdrawFact(n, point.Height()-1, point.Height()+1+base.Height(int64(i)), util.UUID().String())
			withdrawfacts[i] = fact.Hash()
			withdraws[i] = NewSuffrageWithdrawOperation(fact)
			t.NoError(withdraws[i].NodeSign(t.priv, t.networkID, node))
		}

		bl := t.ballot(point, withdraws, withdrawfacts, withdraws, withdrawfacts)

		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "duplicated withdraw node found")
	})
}

func (t *testBaseBallot) TestUnknownWithdraw() {
	node := base.RandomAddress("")
	point := base.RawPoint(33, 44)

	withdraws := make([]SuffrageWithdrawOperation, 3)
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdrawfacts {
		fact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		withdrawfacts[i] = fact.Hash()

		if i == len(withdrawfacts)-1 {
			fact = NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		}

		withdraws[i] = NewSuffrageWithdrawOperation(fact)
		t.NoError(withdraws[i].NodeSign(t.priv, t.networkID, node))
	}

	bl := t.ballot(point, nil, nil, withdraws, withdrawfacts)
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "withdraw fact hash not matched")
}

func (t *testBaseBallot) TestWithdrawNodeSelfSign() {
	point := base.RawPoint(33, 44)

	withdrawnode := RandomLocalNode()

	fact := NewSuffrageWithdrawFact(withdrawnode.Address(), point.Height()-1, point.Height()+1, util.UUID().String())
	withdraw := NewSuffrageWithdrawOperation(fact)

	t.NoError(withdraw.NodeSign(withdrawnode.Privatekey(), t.networkID, withdrawnode.Address()))

	bl := t.ballot(point, nil, nil, []SuffrageWithdrawOperation{withdraw}, []util.Hash{fact.Hash()})
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "valid node signs not found")
}

func (t *testBaseBallot) TestWithdrawsMismatch() {
	node := base.RandomAddress("")
	point := base.RawPoint(33, 44)

	withdraws := make([]SuffrageWithdrawOperation, 3)
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdrawfacts {
		fact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		withdrawfacts[i] = fact.Hash()
		withdraws[i] = NewSuffrageWithdrawOperation(fact)
		t.NoError(withdraws[i].NodeSign(t.priv, t.networkID, node))
	}

	bl := t.ballot(point, withdraws, withdrawfacts, withdraws, withdrawfacts[:len(withdrawfacts)-1])
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "number of withdraws not matched")
}

type testINITBallot struct {
	testBaseBallot
}

func (t *testINITBallot) SetupSuite() {
	t.ballot = func(point base.Point, _ []SuffrageWithdrawOperation, _ []util.Hash, withdraws []SuffrageWithdrawOperation, withdrawfacts []util.Hash) base.Ballot {
		block := valuehash.RandomSHA256()
		afact := NewACCEPTBallotFact(base.NewPoint(point.Height()-1, 44), valuehash.RandomSHA256(), block, nil)

		asignfact := NewACCEPTBallotSignFact(afact)
		t.NoError(asignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.
			SetMajority(afact).
			SetSignFacts([]base.BallotSignFact{asignfact}).
			SetThreshold(base.Threshold(100)).
			Finish()

		fact := NewINITBallotFact(point, block, valuehash.RandomSHA256(), withdrawfacts)

		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ws := make([]base.SuffrageWithdrawOperation, len(withdraws))
		for i := range withdraws {
			ws[i] = withdraws[i]
		}

		return NewINITBallot(avp, signfact, ws)
	}
}

func (t *testINITBallot) TestSignIsValid() {
	point := base.RawPoint(33, 1)

	withdrawfact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
	withdraw := NewSuffrageWithdrawOperation(withdrawfact)
	t.NoError(withdraw.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

	t.Run("valid", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{withdrawfact.Hash()})

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITWithdrawVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetWithdraws([]base.SuffrageWithdrawOperation{withdraw})
		ivp.Finish()

		fact := NewSuffrageConfirmBallotFact(point, ifact.PreviousBlock(), ifact.Proposal(), []util.Hash{withdrawfact.Hash()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, nil)
		t.NoError(bl.IsValid(t.networkID))
	})

	t.Run("not empty withdraws", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{withdrawfact.Hash()})

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITWithdrawVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetWithdraws([]base.SuffrageWithdrawOperation{withdraw})
		ivp.Finish()

		fact := NewSuffrageConfirmBallotFact(point, ifact.PreviousBlock(), ifact.Proposal(), []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, []base.SuffrageWithdrawOperation{withdraw})
		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "not empty withdraws")
	})

	t.Run("number of withdraws not matched", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{withdrawfact.Hash()})

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITWithdrawVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetWithdraws([]base.SuffrageWithdrawOperation{withdraw})
		ivp.Finish()

		fact := NewSuffrageConfirmBallotFact(point, ifact.PreviousBlock(), ifact.Proposal(), []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, nil)
		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "number of withdraws not matched")
	})

	t.Run("wrong voteproof; wrong stage", func() {
		block := valuehash.RandomSHA256()
		afact := NewACCEPTBallotFact(base.NewPoint(point.Height()-1, 44), valuehash.RandomSHA256(), block, nil)

		asignfact := NewACCEPTBallotSignFact(afact)
		t.NoError(asignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.
			SetMajority(afact).
			SetSignFacts([]base.BallotSignFact{asignfact}).
			SetThreshold(base.Threshold(100)).
			Finish()

		fact := NewSuffrageConfirmBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{withdrawfact.Hash()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(avp, signfact, nil)
		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "wrong voteproof")
		t.ErrorContains(err, "stage should be INIT")
	})

	t.Run("empty withdraws in voteproof", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100)).
			Finish()

		fact := NewSuffrageConfirmBallotFact(point, ifact.PreviousBlock(), ifact.Proposal(), []util.Hash{withdrawfact.Hash()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, nil)
		err := bl.IsValid(t.networkID)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "wrong voteproof")
	})

	t.Run("not majority in voteproof", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{withdrawfact.Hash()})

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITWithdrawVoteproof(ifact.Point().Point)
		ivp.
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetWithdraws([]base.SuffrageWithdrawOperation{withdraw})
		ivp.Finish()

		fact := NewSuffrageConfirmBallotFact(point, ifact.PreviousBlock(), ifact.Proposal(), []util.Hash{withdrawfact.Hash()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, nil)
		err := bl.IsValid(t.networkID)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "wrong voteproof")
		t.ErrorContains(err, `vote result should be "MAJORITY"`)
	})

	t.Run("wrong previous block", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{withdrawfact.Hash()})

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITWithdrawVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetWithdraws([]base.SuffrageWithdrawOperation{withdraw})
		ivp.Finish()

		fact := NewSuffrageConfirmBallotFact(point, valuehash.RandomSHA256(), ifact.Proposal(), []util.Hash{withdrawfact.Hash()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, nil)
		err := bl.IsValid(t.networkID)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "wrong voteproof")
		t.ErrorContains(err, "wrong previous block with suffrage confirm ballot fact")
	})

	t.Run("wrong proposal block", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{withdrawfact.Hash()})

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITWithdrawVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetWithdraws([]base.SuffrageWithdrawOperation{withdraw})
		ivp.Finish()

		fact := NewSuffrageConfirmBallotFact(point, ifact.PreviousBlock(), valuehash.RandomSHA256(), []util.Hash{withdrawfact.Hash()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, nil)
		err := bl.IsValid(t.networkID)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "wrong voteproof")
		t.ErrorContains(err, "wrong proposal with suffrage confirm ballot fact")
	})
}

func TestINITBallot(t *testing.T) {
	suite.Run(t, new(testINITBallot))
}

func TestACCEPTBallot(tt *testing.T) {
	t := new(testBaseBallot)

	t.ballot = func(point base.Point, iwithdraws []SuffrageWithdrawOperation, iwithdrawfacts []util.Hash, withdraws []SuffrageWithdrawOperation, withdrawfacts []util.Hash) base.Ballot {
		iws := make([]base.SuffrageWithdrawOperation, len(iwithdraws))
		for i := range iws {
			iws[i] = iwithdraws[i]
		}

		node := base.RandomAddress("")

		ifact := NewINITBallotFact(point,
			valuehash.RandomSHA256(), valuehash.RandomSHA256(),
			iwithdrawfacts,
		)

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, node))

		var ivp base.INITVoteproof

		if len(iws) > 0 {
			vp := NewINITWithdrawVoteproof(ifact.Point().Point)
			vp.
				SetMajority(ifact).
				SetSignFacts([]base.BallotSignFact{isignfact}).
				SetThreshold(base.Threshold(100))
			vp.SetWithdraws(iws)
			vp.Finish()

			ivp = vp
		} else {
			vp := NewINITVoteproof(ifact.Point().Point)
			vp.
				SetMajority(ifact).
				SetSignFacts([]base.BallotSignFact{isignfact}).
				SetThreshold(base.Threshold(100))
			vp.Finish()

			ivp = vp
		}

		fact := NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), withdrawfacts)

		signfact := NewACCEPTBallotSignFact(fact)

		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ws := make([]base.SuffrageWithdrawOperation, len(withdraws))
		for i := range withdraws {
			ws[i] = withdraws[i]
		}

		return NewACCEPTBallot(ivp, signfact, ws)
	}

	suite.Run(tt, t)
}

type testBaseINITBallotWithVoteproof struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testBaseINITBallotWithVoteproof) SetupTest() {
	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testBaseINITBallotWithVoteproof) TestValidINITVoteproofNone0Round() {
	node := base.RandomAddress("")

	ifact := NewINITBallotFact(base.RawPoint(44, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	isignfact := NewINITBallotSignFact(ifact)

	t.NoError(isignfact.NodeSign(t.priv, t.networkID, node))

	ivp := NewINITVoteproof(ifact.Point().Point)
	ivp.SetResult(base.VoteResultDraw).
		SetSignFacts([]base.BallotSignFact{isignfact}).
		SetThreshold(base.Threshold(100)).
		Finish()

	fact := NewINITBallotFact(base.NewPoint(ifact.Point().Height(), ifact.Point().Round()+1), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	signfact := NewINITBallotSignFact(fact)

	t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

	bl := NewINITBallot(ivp, signfact, nil)

	t.NoError(bl.IsValid(t.networkID))
}

func (t *testBaseINITBallotWithVoteproof) TestWrongHeightINITVoteproofNone0Round() {
	node := base.RandomAddress("")

	point := base.RawPoint(33, 44)
	ifact := NewINITBallotFact(base.NewPoint(point.Height()+2, base.Round(55)), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	isignfact := NewINITBallotSignFact(ifact)

	t.NoError(isignfact.NodeSign(t.priv, t.networkID, node))

	ivp := NewINITVoteproof(ifact.Point().Point)
	ivp.
		SetMajority(ifact).
		SetSignFacts([]base.BallotSignFact{isignfact}).
		SetThreshold(base.Threshold(100)).
		Finish()

	fact := NewINITBallotFact(base.NewPoint(point.Height()+1, ifact.Point().Round()+1), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	signfact := NewINITBallotSignFact(fact)

	t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

	bl := NewINITBallot(ivp, signfact, nil)

	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "next round not match")
}

func TestBaseINITBallotWithVoteproof(t *testing.T) {
	suite.Run(t, new(testBaseINITBallotWithVoteproof))
}

type baseTestBallotEncode struct {
	encoder.BaseTestEncode
	enc       encoder.Encoder
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *baseTestBallotEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageWithdrawFactHint, Instance: SuffrageWithdrawFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageWithdrawOperationHint, Instance: SuffrageWithdrawOperation{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotFactHint, Instance: INITBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotFactHint, Instance: ACCEPTBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITVoteproofHint, Instance: INITVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITWithdrawVoteproofHint, Instance: INITWithdrawVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTVoteproofHint, Instance: ACCEPTVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotSignFactHint, Instance: INITBallotSignFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotSignFactHint, Instance: ACCEPTBallotSignFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotHint, Instance: INITBallot{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotHint, Instance: ACCEPTBallot{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageConfirmBallotFactHint, Instance: SuffrageConfirmBallotFact{}}))
}

func testBallotEncode() *baseTestBallotEncode {
	t := new(baseTestBallotEncode)

	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())

	t.Compare = func(a, b interface{}) {
		as, ok := a.(base.Ballot)
		t.True(ok)
		bs, ok := b.(base.Ballot)
		t.True(ok)

		t.NoError(bs.IsValid(t.networkID))

		base.EqualBallot(t.Assert(), as, bs)

		aws := as.(base.HasWithdraws).Withdraws()
		bws := bs.(base.HasWithdraws).Withdraws()

		t.Equal(len(aws), len(bws))

		for i := range aws {
			aw := aws[i]
			bw := bws[i]

			t.True(aw.Hash().Equal(bw.Hash()))
		}
	}

	return t
}

func TestINITBallotJSON(tt *testing.T) {
	t := testBallotEncode()

	t.Encode = func() (interface{}, []byte) {
		node := base.RandomAddress("")

		block := valuehash.RandomSHA256()
		afact := NewACCEPTBallotFact(base.RawPoint(33, 44), valuehash.RandomSHA256(), block, nil)

		asignfact := NewACCEPTBallotSignFact(afact)

		t.NoError(asignfact.NodeSign(t.priv, t.networkID, node))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.
			SetMajority(afact).
			SetSignFacts([]base.BallotSignFact{asignfact}).
			SetThreshold(base.Threshold(100)).
			Finish()

		withdraws := make([]base.SuffrageWithdrawOperation, 3)
		withdrawfacts := make([]util.Hash, len(withdraws))
		for i := range withdrawfacts {
			fact := NewSuffrageWithdrawFact(base.RandomAddress(""), afact.Point().Height()-1, afact.Point().Height()+1, util.UUID().String())
			withdrawfacts[i] = fact.Hash()
			w := NewSuffrageWithdrawOperation(fact)
			t.NoError(w.NodeSign(t.priv, t.networkID, node))

			withdraws[i] = w
		}

		fact := NewINITBallotFact(base.NewPoint(afact.Point().Height()+1, base.Round(0)), block, valuehash.RandomSHA256(), withdrawfacts)

		signfact := NewINITBallotSignFact(fact)

		t.NoError(signfact.NodeSign(t.priv, t.networkID, node))

		bl := NewINITBallot(avp, signfact, withdraws)

		b, err := t.enc.Marshal(&bl)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(INITBallot)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}

func TestACCEPTBallotJSON(tt *testing.T) {
	t := testBallotEncode()

	t.Encode = func() (interface{}, []byte) {
		point := base.RawPoint(44, 55)
		node := base.RandomAddress("")

		withdraws := make([]base.SuffrageWithdrawOperation, 3)
		withdrawfacts := make([]util.Hash, len(withdraws))
		for i := range withdrawfacts {
			fact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
			withdrawfacts[i] = fact.Hash()

			w := NewSuffrageWithdrawOperation(fact)
			t.NoError(w.NodeSign(t.priv, t.networkID, node))

			withdraws[i] = w
		}

		ifact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), withdrawfacts)

		isignfact := NewINITBallotSignFact(ifact)

		t.NoError(isignfact.NodeSign(t.priv, t.networkID, node))

		ivp := NewINITWithdrawVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetWithdraws(withdraws)
		ivp.Finish()

		fact := NewACCEPTBallotFact(ifact.Point().Point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), withdrawfacts)

		signfact := NewACCEPTBallotSignFact(fact)

		t.NoError(signfact.NodeSign(t.priv, t.networkID, node))

		bl := NewACCEPTBallot(ivp, signfact, withdraws)

		b, err := t.enc.Marshal(&bl)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(ACCEPTBallot)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}

func TestINITBallotJSONWithSIGN(tt *testing.T) {
}
