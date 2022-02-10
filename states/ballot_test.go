package states

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
	ballot    func() base.Ballot
}

func (t *testBaseBallot) SetupTest() {
	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testBaseBallot) TestNew() {
	bl := t.ballot()
	t.NoError(bl.IsValid(t.networkID))
}

func (t *testBaseBallot) TestEmptyINITVoteproof() {
	bl := t.ballot()
	t.NoError(bl.IsValid(t.networkID))

	if bl.Point().Stage() == base.StageINIT {
		fact := bl.(base.INITBallot).BallotSignedFact().BallotFact()
		if fact.Point().Round() == 0 {
			t.T().Skip("0 round init ballot")

			return
		}
	}

	switch bt := bl.(type) {
	case INITBallot:
		bt.ivp = nil
		bl = bt
	case Proposal:
		return
	case ACCEPTBallot:
		bt.ivp = nil
		bl = bt
	}
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func (t *testBaseBallot) TestEmptyACCEPTVoteproof() {
	bl := t.ballot()
	t.NoError(bl.IsValid(t.networkID))

	switch bt := bl.(type) {
	case INITBallot:
		bt.avp = nil
		bl = bt
	case Proposal:
		return
	case ACCEPTBallot:
		bt.avp = nil
		bl = bt
	}
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func (t *testBaseBallot) TestEmptySignedFact() {
	bl := t.ballot()
	t.NoError(bl.IsValid(t.networkID))

	switch bt := bl.(type) {
	case INITBallot:
		bt.signedFact = nil
		bl = bt
	case Proposal:
		bt.signedFact = nil
		bl = bt
	case ACCEPTBallot:
		bt.signedFact = nil
		bl = bt
	}
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func TestINITBallot(tt *testing.T) {
	t := new(testBaseBallot)
	t.ballot = func() base.Ballot {
		block := valuehash.RandomSHA256()
		afact := NewACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256(), block)

		asignedFact := NewACCEPTBallotSignedFact(base.RandomAddress(""), afact)
		t.NoError(asignedFact.Sign(t.priv, t.networkID))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.finish()
		avp.SetResult(base.VoteResultMajority)
		avp.SetMajority(afact)
		avp.SetSignedFacts([]base.BallotSignedFact{asignedFact})

		avp.SetThreshold(base.Threshold(100))

		fact := NewINITBallotFact(base.NewPoint(afact.Point().Height()+1, base.Round(0)), block, valuehash.RandomSHA256())

		signedFact := NewINITBallotSignedFact(base.RandomAddress(""), fact)
		t.NoError(signedFact.Sign(t.priv, t.networkID))

		return NewINITBallot(nil, avp, signedFact)
	}

	suite.Run(tt, t)
}

func TestProposalBallot(tt *testing.T) {
	t := new(testBaseBallot)
	t.ballot = func() base.Ballot {
		fact := NewProposalFact(base.NewPoint(base.Height(32), base.Round(44)),
			[]util.Hash{
				valuehash.RandomSHA256(),
				valuehash.RandomSHA256(),
			},
		)

		signedFact := NewProposalSignedFact(
			base.RandomAddress(""),
			fact,
		)
		t.NoError(signedFact.Sign(t.priv, t.networkID))

		return NewProposal(signedFact)
	}

	suite.Run(tt, t)
}

func TestACCEPTBallot(tt *testing.T) {
	t := new(testBaseBallot)
	t.ballot = func() base.Ballot {
		node := base.RandomAddress("")

		afact := NewACCEPTBallotFact(base.NewPoint(base.Height(32), base.Round(44)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

		asignedFact := NewACCEPTBallotSignedFact(node, afact)
		t.NoError(asignedFact.Sign(t.priv, t.networkID))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.finish()
		avp.SetResult(base.VoteResultMajority)
		avp.SetMajority(afact)
		avp.SetSignedFacts([]base.BallotSignedFact{asignedFact})
		avp.SetThreshold(base.Threshold(100))

		ifact := NewINITBallotFact(base.NewPoint(afact.Point().Height()+1, base.Round(55)),
			valuehash.RandomSHA256(), valuehash.RandomSHA256(),
		)

		isignedFact := NewINITBallotSignedFact(node, ifact)
		t.NoError(isignedFact.Sign(t.priv, t.networkID))

		ivp := NewINITVoteproof(ifact.Point().Point)
		ivp.finish()
		ivp.SetResult(base.VoteResultMajority)
		ivp.SetMajority(ifact)
		ivp.SetSignedFacts([]base.BallotSignedFact{isignedFact})
		ivp.SetThreshold(base.Threshold(100))

		fact := NewACCEPTBallotFact(ifact.Point().Point, valuehash.RandomSHA256(), valuehash.RandomSHA256())

		signedFact := NewACCEPTBallotSignedFact(base.RandomAddress(""), fact)

		t.NoError(signedFact.Sign(t.priv, t.networkID))

		return NewACCEPTBallot(ivp, avp, signedFact)
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

func (t *testBaseINITBallotWithVoteproof) TestNoneNilINITVoteproof0Round() {
	node := base.RandomAddress("")

	block := valuehash.RandomSHA256()
	afact := NewACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256(), block)

	asignedFact := NewACCEPTBallotSignedFact(node, afact)

	t.NoError(asignedFact.Sign(t.priv, t.networkID))

	avp := NewACCEPTVoteproof(afact.Point().Point)
	avp.finish()
	avp.SetResult(base.VoteResultMajority)
	avp.SetMajority(afact)
	avp.SetSignedFacts([]base.BallotSignedFact{asignedFact})
	avp.SetThreshold(base.Threshold(100))

	ifact := NewINITBallotFact(base.NewPoint(avp.Point().Height(), base.Round(55)), // same height with previous accept voteproof
		valuehash.RandomSHA256(), valuehash.RandomSHA256(),
	)

	isignedFact := NewINITBallotSignedFact(node, ifact)

	t.NoError(isignedFact.Sign(t.priv, t.networkID))

	ivp := NewINITVoteproof(ifact.Point().Point)
	ivp.finish()
	ivp.SetResult(base.VoteResultMajority)
	ivp.SetMajority(ifact)
	ivp.SetSignedFacts([]base.BallotSignedFact{isignedFact})
	ivp.SetThreshold(base.Threshold(100))

	fact := NewINITBallotFact(base.NewPoint(afact.Point().Height()+1, base.Round(0)), block, valuehash.RandomSHA256())

	signedFact := NewINITBallotSignedFact(base.RandomAddress(""), fact)

	t.NoError(signedFact.Sign(t.priv, t.networkID))

	bl := NewINITBallot(ivp, avp, signedFact)

	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "nit voteproof should be nil in 0 round init ballot")
}

func (t *testBaseINITBallotWithVoteproof) TestValidINITVoteproofNone0Round() {
	node := base.RandomAddress("")

	block := valuehash.RandomSHA256()
	afact := NewACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256(), block)

	asignedFact := NewACCEPTBallotSignedFact(node, afact)

	t.NoError(asignedFact.Sign(t.priv, t.networkID))

	avp := NewACCEPTVoteproof(afact.Point().Point)
	avp.finish()
	avp.SetResult(base.VoteResultMajority)
	avp.SetMajority(afact)
	avp.SetSignedFacts([]base.BallotSignedFact{asignedFact})
	avp.SetThreshold(base.Threshold(100))

	ifact := NewINITBallotFact(base.NewPoint(avp.Point().Height()+1, base.Round(55)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	isignedFact := NewINITBallotSignedFact(node, ifact)

	t.NoError(isignedFact.Sign(t.priv, t.networkID))

	ivp := NewINITVoteproof(ifact.Point().Point)
	ivp.finish()
	ivp.SetResult(base.VoteResultMajority)
	ivp.SetMajority(ifact)
	ivp.SetSignedFacts([]base.BallotSignedFact{isignedFact})
	ivp.SetThreshold(base.Threshold(100))

	fact := NewINITBallotFact(base.NewPoint(ifact.Point().Height(), ifact.Point().Round()+1), block, valuehash.RandomSHA256())

	signedFact := NewINITBallotSignedFact(base.RandomAddress(""), fact)

	t.NoError(signedFact.Sign(t.priv, t.networkID))

	bl := NewINITBallot(ivp, avp, signedFact)

	t.NoError(bl.IsValid(t.networkID))
}

func (t *testBaseINITBallotWithVoteproof) TestWrongHeightINITVoteproofNone0Round() {
	node := base.RandomAddress("")

	block := valuehash.RandomSHA256()
	afact := NewACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256(), block)

	asignedFact := NewACCEPTBallotSignedFact(node, afact)

	t.NoError(asignedFact.Sign(t.priv, t.networkID))

	avp := NewACCEPTVoteproof(afact.Point().Point)
	avp.finish()
	avp.SetResult(base.VoteResultMajority)
	avp.SetMajority(afact)
	avp.SetSignedFacts([]base.BallotSignedFact{asignedFact})
	avp.SetThreshold(base.Threshold(100))

	ifact := NewINITBallotFact(base.NewPoint(avp.Point().Height()+2, base.Round(55)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	isignedFact := NewINITBallotSignedFact(node, ifact)

	t.NoError(isignedFact.Sign(t.priv, t.networkID))

	ivp := NewINITVoteproof(ifact.Point().Point)
	ivp.finish()
	ivp.SetResult(base.VoteResultMajority)
	ivp.SetMajority(ifact)
	ivp.SetSignedFacts([]base.BallotSignedFact{isignedFact})
	ivp.SetThreshold(base.Threshold(100))

	fact := NewINITBallotFact(base.NewPoint(afact.Point().Height()+1, ifact.Point().Round()+1), block, valuehash.RandomSHA256())

	signedFact := NewINITBallotSignedFact(base.RandomAddress(""), fact)

	t.NoError(signedFact.Sign(t.priv, t.networkID))

	bl := NewINITBallot(ivp, avp, signedFact)

	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "wrong height of init voteproof")
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
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotFactHint, Instance: INITBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ProposalFactHint, Instance: ProposalFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotFactHint, Instance: ACCEPTBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITVoteproofHint, Instance: INITVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTVoteproofHint, Instance: ACCEPTVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotSignedFactHint, Instance: INITBallotSignedFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ProposalSignedFactHint, Instance: ProposalSignedFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotSignedFactHint, Instance: ACCEPTBallotSignedFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotHint, Instance: INITBallot{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ProposalHint, Instance: Proposal{}}))
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

		base.CompareBallot(t.Assert(), as, bs)
	}

	return t
}

func TestINITBallotJSON(tt *testing.T) {
	t := testBallotEncode()

	t.Encode = func() (interface{}, []byte) {
		node := base.RandomAddress("")

		block := valuehash.RandomSHA256()
		afact := NewACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256(), block)

		asignedFact := NewACCEPTBallotSignedFact(node, afact)

		t.NoError(asignedFact.Sign(t.priv, t.networkID))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.finish()
		avp.SetResult(base.VoteResultMajority)
		avp.SetMajority(afact)
		avp.SetSignedFacts([]base.BallotSignedFact{asignedFact})
		avp.SetThreshold(base.Threshold(100))

		fact := NewINITBallotFact(base.NewPoint(afact.Point().Height()+1, base.Round(0)), block, valuehash.RandomSHA256())

		signedFact := NewINITBallotSignedFact(node, fact)

		t.NoError(signedFact.Sign(t.priv, t.networkID))

		bl := NewINITBallot(nil, avp, signedFact)

		b, err := t.enc.Marshal(bl)
		t.NoError(err)

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

func TestProposalBallotJSON(tt *testing.T) {
	t := testBallotEncode()

	t.Encode = func() (interface{}, []byte) {
		fact := NewProposalFact(base.NewPoint(base.Height(32), base.Round(44)),
			[]util.Hash{
				valuehash.RandomSHA256(),
				valuehash.RandomSHA256(),
			},
		)

		signedFact := NewProposalSignedFact(
			base.RandomAddress(""),
			fact,
		)
		t.NoError(signedFact.Sign(t.priv, t.networkID))

		bl := NewProposal(signedFact)

		b, err := t.enc.Marshal(bl)
		t.NoError(err)

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(Proposal)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}

func TestACCEPTBallotJSON(tt *testing.T) {
	t := testBallotEncode()

	t.Encode = func() (interface{}, []byte) {
		node := base.RandomAddress("")

		afact := NewACCEPTBallotFact(base.NewPoint(base.Height(32), base.Round(44)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

		asignedFact := NewACCEPTBallotSignedFact(node, afact)

		t.NoError(asignedFact.Sign(t.priv, t.networkID))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.finish()
		avp.SetResult(base.VoteResultMajority)
		avp.SetMajority(afact)
		avp.SetSignedFacts([]base.BallotSignedFact{asignedFact})
		avp.SetThreshold(base.Threshold(100))

		ifact := NewINITBallotFact(base.NewPoint(afact.Point().Height()+1, base.Round(55)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

		isignedFact := NewINITBallotSignedFact(node, ifact)

		t.NoError(isignedFact.Sign(t.priv, t.networkID))

		ivp := NewINITVoteproof(ifact.Point().Point)
		ivp.finish()
		ivp.SetResult(base.VoteResultMajority)
		ivp.SetMajority(ifact)
		ivp.SetSignedFacts([]base.BallotSignedFact{isignedFact})
		ivp.SetThreshold(base.Threshold(100))

		fact := NewACCEPTBallotFact(ifact.Point().Point, valuehash.RandomSHA256(), valuehash.RandomSHA256())

		signedFact := NewACCEPTBallotSignedFact(node, fact)

		t.NoError(signedFact.Sign(t.priv, t.networkID))

		bl := NewACCEPTBallot(ivp, avp, signedFact)

		b, err := t.enc.Marshal(bl)
		t.NoError(err)

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
