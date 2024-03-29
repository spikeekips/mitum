package isaac

import (
	"testing"

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
	ballot    func(base.Point, []SuffrageExpelOperation, []util.Hash, []SuffrageExpelOperation, []util.Hash) base.Ballot
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
	t.ErrorIs(err, util.ErrInvalid)
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
	t.ErrorIs(err, util.ErrInvalid)
}

func (t *testBaseBallot) TestExpelNodeInSigns() {
	node := base.RandomAddress("")
	point := base.RawPoint(33, 44)

	expelnode := base.RandomLocalNode()

	fact := NewSuffrageExpelFact(expelnode.Address(), point.Height()-1, point.Height()+1, util.UUID().String())
	expel := NewSuffrageExpelOperation(fact)

	t.NoError(expel.NodeSign(t.priv, t.networkID, node))
	t.NoError(expel.NodeSign(expelnode.Privatekey(), t.networkID, expelnode.Address()))

	bl := t.ballot(point, []SuffrageExpelOperation{expel}, []util.Hash{fact.Hash()}, []SuffrageExpelOperation{expel}, []util.Hash{fact.Hash()})
	t.NoError(bl.IsValid(t.networkID))
}

func (t *testBaseBallot) TestInvalidExpel() {
	t.Run("valid", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		expels := make([]SuffrageExpelOperation, 3)
		expelfacts := make([]util.Hash, len(expels))
		for i := range expelfacts {
			fact := NewSuffrageExpelFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
			expelfacts[i] = fact.Hash()
			expels[i] = NewSuffrageExpelOperation(fact)
			t.NoError(expels[i].NodeSign(t.priv, t.networkID, node))
		}

		bl := t.ballot(point, expels, expelfacts, expels, expelfacts)
		t.NoError(bl.IsValid(t.networkID))
	})

	t.Run("invalid", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		expels := make([]SuffrageExpelOperation, 3)
		expelfacts := make([]util.Hash, len(expels))
		for i := range expelfacts {
			fact := NewSuffrageExpelFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
			expelfacts[i] = fact.Hash()
			expels[i] = NewSuffrageExpelOperation(fact)

			if i != len(expelfacts)-1 {
				t.NoError(expels[i].NodeSign(t.priv, t.networkID, node))
			}
		}

		bl := t.ballot(point, nil, nil, expels, expelfacts)
		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "empty signs")
	})

	t.Run("wrong start height expel fact", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		expels := make([]SuffrageExpelOperation, 3)
		expelfacts := make([]util.Hash, len(expels))
		for i := range expelfacts {
			fact := NewSuffrageExpelFact(base.RandomAddress(""), point.Height()+1, point.Height()+2, util.UUID().String())
			expelfacts[i] = fact.Hash()
			expels[i] = NewSuffrageExpelOperation(fact)
			t.NoError(expels[i].NodeSign(t.priv, t.networkID, node))
		}

		bl := t.ballot(point, expels, expelfacts, expels, expelfacts)

		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "wrong start height in expel")
	})

	t.Run("duplicated expel node", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		expelnode := base.RandomAddress("")

		expels := make([]SuffrageExpelOperation, 3)
		expelfacts := make([]util.Hash, len(expels))
		for i := range expelfacts {
			var n base.Address

			if i%2 == 0 {
				n = expelnode
			} else {
				n = base.RandomAddress("")
			}

			fact := NewSuffrageExpelFact(n, point.Height()-1, point.Height()+1+base.Height(int64(i)), util.UUID().String())
			expelfacts[i] = fact.Hash()
			expels[i] = NewSuffrageExpelOperation(fact)
			t.NoError(expels[i].NodeSign(t.priv, t.networkID, node))
		}

		bl := t.ballot(point, expels, expelfacts, expels, expelfacts)

		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "duplicated expel node found")
	})
}

func (t *testBaseBallot) TestUnknownExpel() {
	node := base.RandomAddress("")
	point := base.RawPoint(33, 44)

	expels := make([]SuffrageExpelOperation, 3)
	expelfacts := make([]util.Hash, len(expels))
	for i := range expelfacts {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		expelfacts[i] = fact.Hash()

		if i == len(expelfacts)-1 {
			fact = NewSuffrageExpelFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		}

		expels[i] = NewSuffrageExpelOperation(fact)
		t.NoError(expels[i].NodeSign(t.priv, t.networkID, node))
	}

	bl := t.ballot(point, nil, nil, expels, expelfacts)
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.ErrorIs(err, util.ErrInvalid)
	t.ErrorContains(err, "expel fact hash not matched")
}

func (t *testBaseBallot) TestExpelNodeSelfSign() {
	point := base.RawPoint(33, 44)

	expelnode := base.RandomLocalNode()

	fact := NewSuffrageExpelFact(expelnode.Address(), point.Height()-1, point.Height()+1, util.UUID().String())
	expel := NewSuffrageExpelOperation(fact)

	t.NoError(expel.NodeSign(expelnode.Privatekey(), t.networkID, expelnode.Address()))

	bl := t.ballot(point, nil, nil, []SuffrageExpelOperation{expel}, []util.Hash{fact.Hash()})
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.ErrorIs(err, util.ErrInvalid)
	t.ErrorContains(err, "valid node signs not found")
}

func (t *testBaseBallot) TestExpelsMismatch() {
	node := base.RandomAddress("")
	point := base.RawPoint(33, 44)

	expels := make([]SuffrageExpelOperation, 3)
	expelfacts := make([]util.Hash, len(expels))
	for i := range expelfacts {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
		expelfacts[i] = fact.Hash()
		expels[i] = NewSuffrageExpelOperation(fact)
		t.NoError(expels[i].NodeSign(t.priv, t.networkID, node))
	}

	bl := t.ballot(point, expels, expelfacts, expels, expelfacts[:len(expelfacts)-1])
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.ErrorIs(err, util.ErrInvalid)
	t.ErrorContains(err, "number of expels not matched")
}

type testINITBallot struct {
	testBaseBallot
}

func (t *testINITBallot) SetupSuite() {
	t.ballot = func(point base.Point, _ []SuffrageExpelOperation, _ []util.Hash, expels []SuffrageExpelOperation, expelfacts []util.Hash) base.Ballot {
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

		fact := NewINITBallotFact(point, block, valuehash.RandomSHA256(), expelfacts)

		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ws := make([]base.SuffrageExpelOperation, len(expels))
		for i := range expels {
			ws[i] = expels[i]
		}

		return NewINITBallot(avp, signfact, ws)
	}
}

func (t *testINITBallot) TestSignIsValid() {
	point := base.RawPoint(33, 1)

	expelfact := NewSuffrageExpelFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
	expel := NewSuffrageExpelOperation(expelfact)
	t.NoError(expel.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

	t.Run("valid", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{expelfact.Hash()})

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITExpelVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetExpels([]base.SuffrageExpelOperation{expel})
		ivp.Finish()

		fact := NewSuffrageConfirmBallotFact(point, ifact.PreviousBlock(), ifact.Proposal(), []util.Hash{expelfact.Hash()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, nil)
		t.NoError(bl.IsValid(t.networkID))
	})

	t.Run("not empty expels", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{expelfact.Hash()})

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITExpelVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetExpels([]base.SuffrageExpelOperation{expel})
		ivp.Finish()

		fact := NewSuffrageConfirmBallotFact(point, ifact.PreviousBlock(), ifact.Proposal(), []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, []base.SuffrageExpelOperation{expel})
		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "not empty expels")
	})

	t.Run("number of expels not matched", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{expelfact.Hash()})

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITExpelVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetExpels([]base.SuffrageExpelOperation{expel})
		ivp.Finish()

		fact := NewSuffrageConfirmBallotFact(point, ifact.PreviousBlock(), ifact.Proposal(), []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, nil)
		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "number of expels not matched")
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

		fact := NewSuffrageConfirmBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{expelfact.Hash()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(avp, signfact, nil)
		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "wrong voteproof")
		t.ErrorContains(err, "stage should be INIT")
	})

	t.Run("empty expels in voteproof", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100)).
			Finish()

		fact := NewSuffrageConfirmBallotFact(point, ifact.PreviousBlock(), ifact.Proposal(), []util.Hash{expelfact.Hash()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, nil)
		err := bl.IsValid(t.networkID)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "expected base.ExpelVoteproof,")
	})

	t.Run("not majority in voteproof", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{expelfact.Hash()})

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITExpelVoteproof(ifact.Point().Point)
		ivp.
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetExpels([]base.SuffrageExpelOperation{expel})
		ivp.Finish()

		fact := NewSuffrageConfirmBallotFact(point, ifact.PreviousBlock(), ifact.Proposal(), []util.Hash{expelfact.Hash()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, nil)
		err := bl.IsValid(t.networkID)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "wrong voteproof")
		t.ErrorContains(err, `vote result should be "MAJORITY"`)
	})

	t.Run("wrong previous block", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{expelfact.Hash()})

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITExpelVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetExpels([]base.SuffrageExpelOperation{expel})
		ivp.Finish()

		fact := NewSuffrageConfirmBallotFact(point, valuehash.RandomSHA256(), ifact.Proposal(), []util.Hash{expelfact.Hash()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, nil)
		err := bl.IsValid(t.networkID)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "wrong voteproof")
		t.ErrorContains(err, "wrong previous block with suffrage confirm ballot fact")
	})

	t.Run("wrong proposal block", func() {
		ifact := NewINITBallotFact(point.PrevRound(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{expelfact.Hash()})

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ivp := NewINITExpelVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetExpels([]base.SuffrageExpelOperation{expel})
		ivp.Finish()

		fact := NewSuffrageConfirmBallotFact(point, ifact.PreviousBlock(), valuehash.RandomSHA256(), []util.Hash{expelfact.Hash()})
		signfact := NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		bl := NewINITBallot(ivp, signfact, nil)
		err := bl.IsValid(t.networkID)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "wrong voteproof")
		t.ErrorContains(err, "wrong proposal with suffrage confirm ballot fact")
	})
}

func TestINITBallot(t *testing.T) {
	suite.Run(t, new(testINITBallot))
}

func TestACCEPTBallot(tt *testing.T) {
	t := new(testBaseBallot)

	t.ballot = func(point base.Point, iexpels []SuffrageExpelOperation, iexpelfacts []util.Hash, expels []SuffrageExpelOperation, expelfacts []util.Hash) base.Ballot {
		iws := make([]base.SuffrageExpelOperation, len(iexpels))
		for i := range iws {
			iws[i] = iexpels[i]
		}

		node := base.RandomAddress("")

		ifact := NewINITBallotFact(point,
			valuehash.RandomSHA256(), valuehash.RandomSHA256(),
			iexpelfacts,
		)

		isignfact := NewINITBallotSignFact(ifact)
		t.NoError(isignfact.NodeSign(t.priv, t.networkID, node))

		var ivp base.INITVoteproof

		if len(iws) > 0 {
			vp := NewINITExpelVoteproof(ifact.Point().Point)
			vp.
				SetMajority(ifact).
				SetSignFacts([]base.BallotSignFact{isignfact}).
				SetThreshold(base.Threshold(100))
			vp.SetExpels(iws)
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

		fact := NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), expelfacts)

		signfact := NewACCEPTBallotSignFact(fact)

		t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		ws := make([]base.SuffrageExpelOperation, len(expels))
		for i := range expels {
			ws[i] = expels[i]
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
	t.ErrorIs(err, util.ErrInvalid)
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
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageExpelFactHint, Instance: SuffrageExpelFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageExpelOperationHint, Instance: SuffrageExpelOperation{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotFactHint, Instance: INITBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotFactHint, Instance: ACCEPTBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITVoteproofHint, Instance: INITVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITExpelVoteproofHint, Instance: INITExpelVoteproof{}}))
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

		aws := as.(base.HasExpels).Expels()
		bws := bs.(base.HasExpels).Expels()

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

		expels := make([]base.SuffrageExpelOperation, 3)
		expelfacts := make([]util.Hash, len(expels))
		for i := range expelfacts {
			fact := NewSuffrageExpelFact(base.RandomAddress(""), afact.Point().Height()-1, afact.Point().Height()+1, util.UUID().String())
			expelfacts[i] = fact.Hash()
			w := NewSuffrageExpelOperation(fact)
			t.NoError(w.NodeSign(t.priv, t.networkID, node))

			expels[i] = w
		}

		fact := NewINITBallotFact(base.NewPoint(afact.Point().Height()+1, base.Round(0)), block, valuehash.RandomSHA256(), expelfacts)

		signfact := NewINITBallotSignFact(fact)

		t.NoError(signfact.NodeSign(t.priv, t.networkID, node))

		bl := NewINITBallot(avp, signfact, expels)

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

		expels := make([]base.SuffrageExpelOperation, 3)
		expelfacts := make([]util.Hash, len(expels))
		for i := range expelfacts {
			fact := NewSuffrageExpelFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
			expelfacts[i] = fact.Hash()

			w := NewSuffrageExpelOperation(fact)
			t.NoError(w.NodeSign(t.priv, t.networkID, node))

			expels[i] = w
		}

		ifact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), expelfacts)

		isignfact := NewINITBallotSignFact(ifact)

		t.NoError(isignfact.NodeSign(t.priv, t.networkID, node))

		ivp := NewINITExpelVoteproof(ifact.Point().Point)
		ivp.
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100))
		ivp.SetExpels(expels)
		ivp.Finish()

		fact := NewACCEPTBallotFact(ifact.Point().Point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), expelfacts)

		signfact := NewACCEPTBallotSignFact(fact)

		t.NoError(signfact.NodeSign(t.priv, t.networkID, node))

		bl := NewACCEPTBallot(ivp, signfact, expels)

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
