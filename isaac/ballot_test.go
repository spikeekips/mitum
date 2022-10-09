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
	ballot    func(base.Point, []SuffrageWithdrawOperation, []SuffrageWithdrawFact) base.Ballot
}

func (t *testBaseBallot) SetupTest() {
	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testBaseBallot) TestNew() {
	bl := t.ballot(base.RawPoint(33, 44), nil, nil)
	t.NoError(bl.IsValid(t.networkID))
}

func (t *testBaseBallot) TestEmptyVoteproof() {
	bl := t.ballot(base.RawPoint(33, 44), nil, nil)
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
	bl := t.ballot(base.RawPoint(33, 44), nil, nil)
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

func (t *testBaseBallot) TestInValidWithdraws() {
	t.Run("valid withdraws", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		withdraws := make([]SuffrageWithdrawOperation, 3)
		withdrawfacts := make([]SuffrageWithdrawFact, len(withdraws))
		for i := range withdrawfacts {
			withdrawfacts[i] = NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
			withdraws[i] = NewSuffrageWithdrawOperation(withdrawfacts[i])
			t.NoError(withdraws[i].NodeSign(t.priv, t.networkID, node))
		}

		bl := t.ballot(point, withdraws, withdrawfacts)
		t.NoError(bl.IsValid(t.networkID))
	})

	t.Run("withdraws mismatch", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		withdraws := make([]SuffrageWithdrawOperation, 3)
		withdrawfacts := make([]SuffrageWithdrawFact, len(withdraws))
		for i := range withdrawfacts {
			withdrawfacts[i] = NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
			withdraws[i] = NewSuffrageWithdrawOperation(withdrawfacts[i])
			t.NoError(withdraws[i].NodeSign(t.priv, t.networkID, node))
		}

		bl := t.ballot(point, withdraws, withdrawfacts[:len(withdrawfacts)-1])
		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "number of withdraws not matched")
	})

	t.Run("invalid withdraw", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		withdraws := make([]SuffrageWithdrawOperation, 3)
		withdrawfacts := make([]SuffrageWithdrawFact, len(withdraws))
		for i := range withdrawfacts {
			fact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
			withdrawfacts[i] = fact
			withdraws[i] = NewSuffrageWithdrawOperation(fact)

			if i != len(withdrawfacts)-1 {
				t.NoError(withdraws[i].NodeSign(t.priv, t.networkID, node))
			}
		}

		bl := t.ballot(point, withdraws, withdrawfacts)
		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty signs")
	})

	t.Run("unknonwn withdraw", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		withdraws := make([]SuffrageWithdrawOperation, 3)
		withdrawfacts := make([]SuffrageWithdrawFact, len(withdraws))
		for i := range withdrawfacts {
			fact := NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
			withdrawfacts[i] = fact

			if i == len(withdrawfacts)-1 {
				fact = NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
			}

			withdraws[i] = NewSuffrageWithdrawOperation(fact)
			t.NoError(withdraws[i].NodeSign(t.priv, t.networkID, node))
		}

		bl := t.ballot(point, withdraws, withdrawfacts)
		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "withdraw fact hash not matched")
	})

	t.Run("ok: withdraw node in sign", func() {
		node := base.RandomAddress("")
		point := base.RawPoint(33, 44)

		withdrawnode := RandomLocalNode()

		fact := NewSuffrageWithdrawFact(withdrawnode.Address(), point.Height()-1, point.Height()+1, util.UUID().String())
		withdraw := NewSuffrageWithdrawOperation(fact)

		t.NoError(withdraw.NodeSign(t.priv, t.networkID, node))
		t.NoError(withdraw.NodeSign(withdrawnode.Privatekey(), t.networkID, withdrawnode.Address()))

		bl := t.ballot(point, []SuffrageWithdrawOperation{withdraw}, []SuffrageWithdrawFact{fact})
		t.NoError(bl.IsValid(t.networkID))
	})

	t.Run("withdraw node in self sign", func() {
		point := base.RawPoint(33, 44)

		withdrawnode := RandomLocalNode()

		fact := NewSuffrageWithdrawFact(withdrawnode.Address(), point.Height()-1, point.Height()+1, util.UUID().String())
		withdraw := NewSuffrageWithdrawOperation(fact)

		t.NoError(withdraw.NodeSign(withdrawnode.Privatekey(), t.networkID, withdrawnode.Address()))

		bl := t.ballot(point, []SuffrageWithdrawOperation{withdraw}, []SuffrageWithdrawFact{fact})
		err := bl.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "valid node signs not found")
	})
}

func TestBaseINITBallot(tt *testing.T) {
	t := new(testBaseBallot)
	t.ballot = func(point base.Point, withdraws []SuffrageWithdrawOperation, withdrawfacts []SuffrageWithdrawFact) base.Ballot {
		wfacts := make([]base.SuffrageWithdrawFact, len(withdrawfacts))
		for i := range wfacts {
			wfacts[i] = withdrawfacts[i]
		}

		block := valuehash.RandomSHA256()
		afact := NewACCEPTBallotFact(base.NewPoint(point.Height()-1, 44), valuehash.RandomSHA256(), block, nil)

		asignfact := NewACCEPTBallotSignFact(base.RandomAddress(""), afact)
		t.NoError(asignfact.Sign(t.priv, t.networkID))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.SetResult(base.VoteResultMajority).
			SetMajority(afact).
			SetSignFacts([]base.BallotSignFact{asignfact}).
			SetThreshold(base.Threshold(100)).
			Finish()

		fact := NewINITBallotFact(point, block, valuehash.RandomSHA256(), wfacts)

		signfact := NewINITBallotSignFact(base.RandomAddress(""), fact)
		t.NoError(signfact.Sign(t.priv, t.networkID))

		ws := make([]base.SuffrageWithdrawOperation, len(withdraws))
		for i := range withdraws {
			ws[i] = withdraws[i]
		}

		return NewINITBallot(avp, signfact, ws)
	}

	suite.Run(tt, t)
}

func TestBaseACCEPTBallot(tt *testing.T) {
	t := new(testBaseBallot)
	t.ballot = func(point base.Point, withdraws []SuffrageWithdrawOperation, withdrawfacts []SuffrageWithdrawFact) base.Ballot {
		wfacts := make([]base.SuffrageWithdrawFact, len(withdrawfacts))
		for i := range wfacts {
			wfacts[i] = withdrawfacts[i]
		}

		node := base.RandomAddress("")

		ifact := NewINITBallotFact(point,
			valuehash.RandomSHA256(), valuehash.RandomSHA256(),
			nil,
		)

		isignfact := NewINITBallotSignFact(node, ifact)
		t.NoError(isignfact.Sign(t.priv, t.networkID))

		ivp := NewINITVoteproof(ifact.Point().Point)
		ivp.SetResult(base.VoteResultMajority).
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100)).
			Finish()

		fact := NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), wfacts)

		signfact := NewACCEPTBallotSignFact(base.RandomAddress(""), fact)

		t.NoError(signfact.Sign(t.priv, t.networkID))

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

	isignfact := NewINITBallotSignFact(node, ifact)

	t.NoError(isignfact.Sign(t.priv, t.networkID))

	ivp := NewINITVoteproof(ifact.Point().Point)
	ivp.SetResult(base.VoteResultDraw).
		SetSignFacts([]base.BallotSignFact{isignfact}).
		SetThreshold(base.Threshold(100)).
		Finish()

	fact := NewINITBallotFact(base.NewPoint(ifact.Point().Height(), ifact.Point().Round()+1), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	signfact := NewINITBallotSignFact(base.RandomAddress(""), fact)

	t.NoError(signfact.Sign(t.priv, t.networkID))

	bl := NewINITBallot(ivp, signfact, nil)

	t.NoError(bl.IsValid(t.networkID))
}

func (t *testBaseINITBallotWithVoteproof) TestWrongHeightINITVoteproofNone0Round() {
	node := base.RandomAddress("")

	point := base.RawPoint(33, 44)
	ifact := NewINITBallotFact(base.NewPoint(point.Height()+2, base.Round(55)), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	isignfact := NewINITBallotSignFact(node, ifact)

	t.NoError(isignfact.Sign(t.priv, t.networkID))

	ivp := NewINITVoteproof(ifact.Point().Point)
	ivp.SetResult(base.VoteResultMajority).
		SetMajority(ifact).
		SetSignFacts([]base.BallotSignFact{isignfact}).
		SetThreshold(base.Threshold(100)).
		Finish()

	fact := NewINITBallotFact(base.NewPoint(point.Height()+1, ifact.Point().Round()+1), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	signfact := NewINITBallotSignFact(base.RandomAddress(""), fact)

	t.NoError(signfact.Sign(t.priv, t.networkID))

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
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTVoteproofHint, Instance: ACCEPTVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotSignFactHint, Instance: INITBallotSignFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotSignFactHint, Instance: ACCEPTBallotSignFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotHint, Instance: INITBallot{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotHint, Instance: ACCEPTBallot{}}))
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

		aws := as.(BallotWithdraws).Withdraws()
		bws := bs.(BallotWithdraws).Withdraws()

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

		asignfact := NewACCEPTBallotSignFact(node, afact)

		t.NoError(asignfact.Sign(t.priv, t.networkID))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.SetResult(base.VoteResultMajority).
			SetMajority(afact).
			SetSignFacts([]base.BallotSignFact{asignfact}).
			SetThreshold(base.Threshold(100)).
			Finish()

		withdraws := make([]base.SuffrageWithdrawOperation, 3)
		withdrawfacts := make([]base.SuffrageWithdrawFact, len(withdraws))
		for i := range withdrawfacts {
			withdrawfacts[i] = NewSuffrageWithdrawFact(base.RandomAddress(""), afact.Point().Height()-1, afact.Point().Height()+1, util.UUID().String())
			w := NewSuffrageWithdrawOperation(withdrawfacts[i].(SuffrageWithdrawFact))
			t.NoError(w.NodeSign(t.priv, t.networkID, node))

			withdraws[i] = w
		}

		fact := NewINITBallotFact(base.NewPoint(afact.Point().Height()+1, base.Round(0)), block, valuehash.RandomSHA256(), withdrawfacts)

		signfact := NewINITBallotSignFact(node, fact)

		t.NoError(signfact.Sign(t.priv, t.networkID))

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
		node := base.RandomAddress("")

		ifact := NewINITBallotFact(base.RawPoint(44, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

		isignfact := NewINITBallotSignFact(node, ifact)

		t.NoError(isignfact.Sign(t.priv, t.networkID))

		ivp := NewINITVoteproof(ifact.Point().Point)
		ivp.SetResult(base.VoteResultMajority).
			SetMajority(ifact).
			SetSignFacts([]base.BallotSignFact{isignfact}).
			SetThreshold(base.Threshold(100)).
			Finish()

		withdraws := make([]base.SuffrageWithdrawOperation, 3)
		withdrawfacts := make([]base.SuffrageWithdrawFact, len(withdraws))
		for i := range withdrawfacts {
			withdrawfacts[i] = NewSuffrageWithdrawFact(base.RandomAddress(""), ifact.Point().Height()-1, ifact.Point().Height()+1, util.UUID().String())
			w := NewSuffrageWithdrawOperation(withdrawfacts[i].(SuffrageWithdrawFact))
			t.NoError(w.NodeSign(t.priv, t.networkID, node))

			withdraws[i] = w
		}

		fact := NewACCEPTBallotFact(ifact.Point().Point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), withdrawfacts)

		signfact := NewACCEPTBallotSignFact(node, fact)

		t.NoError(signfact.Sign(t.priv, t.networkID))

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
