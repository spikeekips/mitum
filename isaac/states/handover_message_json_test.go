package isaacstates

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type baseEncodeTestHandoverMessage struct {
	encoder.BaseTestEncode
	enc       encoder.Encoder
	networkID base.NetworkID
	create    func() interface{}
	compare   func(interface{}, interface{}) error
	rtype     interface{}
}

func (t *baseEncodeTestHandoverMessage) SetupSuite() {
	t.enc = jsonenc.NewEncoder()

	hints := []encoder.DecodeDetail{
		{Hint: isaac.ProposalFactHint, Instance: isaac.ProposalFact{}},
		{Hint: isaac.ProposalSignFactHint, Instance: isaac.ProposalSignFact{}},
		{Hint: base.StringAddressHint, Instance: base.StringAddress{}},
		{Hint: base.MPublickeyHint, Instance: base.MPublickey{}},
		{Hint: isaac.INITBallotFactHint, Instance: isaac.INITBallotFact{}},
		{Hint: isaac.INITVoteproofHint, Instance: isaac.INITVoteproof{}},
		{Hint: isaac.INITBallotSignFactHint, Instance: isaac.INITBallotSignFact{}},
		{Hint: base.DummyBlockMapHint, Instance: base.DummyBlockMap{}},
		{Hint: base.DummyManifestHint, Instance: base.DummyManifest{}},
		{Hint: base.DummyNodeHint, Instance: base.BaseNode{}},
		{Hint: HandoverMessageChallengeResponseHint, Instance: HandoverMessageChallengeResponse{}},
		{Hint: HandoverMessageFinishHint, Instance: HandoverMessageFinish{}},
		{Hint: HandoverMessageChallengeStagePointHint, Instance: HandoverMessageChallengeStagePoint{}},
		{Hint: HandoverMessageChallengeBlockMapHint, Instance: HandoverMessageChallengeBlockMap{}},
		{Hint: HandoverMessageDataHint, Instance: HandoverMessageData{}},
		{Hint: HandoverMessageCancelHint, Instance: HandoverMessageCancel{}},
	}
	for i := range hints {
		t.NoError(t.enc.Add(hints[i]))
	}

	t.networkID = util.UUID().Bytes()
}

func (t *baseEncodeTestHandoverMessage) SetupTest() {
	t.BaseTestEncode.Encode = t.Encode
	t.BaseTestEncode.Decode = t.Decode
	t.BaseTestEncode.Compare = t.Compare
}

func (t *baseEncodeTestHandoverMessage) Encode() (interface{}, []byte) {
	i := t.create()

	b, err := t.enc.Marshal(i)
	t.NoError(err)

	t.T().Log("marshaled:", string(b))

	return i, b
}

func (t *baseEncodeTestHandoverMessage) Compare(a, b interface{}) {
	t.NotNil(a)
	t.NotNil(b)

	av, ok := a.(util.IsValider)
	t.True(ok)
	bv, ok := b.(util.IsValider)
	t.True(ok)

	t.NoError(av.IsValid(t.networkID))
	t.NoError(bv.IsValid(t.networkID))

	ah, ok := a.(hint.Hinter)
	t.True(ok)
	bh, ok := b.(hint.Hinter)
	t.True(ok)

	t.True(ah.Hint().Equal(bh.Hint()))

	aid, ok := a.(HandoverMessage)
	t.True(ok)
	bid, ok := b.(HandoverMessage)
	t.True(ok)
	t.Equal(aid.HandoverID(), bid.HandoverID())

	t.IsType(t.rtype, a, "%T", a)
	t.IsType(t.rtype, b, "%T", b)

	t.NoError(t.compare(a, b))
}

func (t *baseEncodeTestHandoverMessage) Decode(b []byte) interface{} {
	i, err := t.enc.Decode(b)
	t.NoError(err)

	return i
}

func (t *baseEncodeTestHandoverMessage) voteproof() (base.ProposalSignFact, base.INITVoteproof) {
	point := base.RawPoint(32, 44)

	pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, base.RandomAddress(""), valuehash.RandomSHA256(), []util.Hash{valuehash.RandomSHA256()}))
	_ = pr.Sign(base.NewMPrivatekey(), t.networkID)

	sfs := make([]base.BallotSignFact, 2)
	for i := range sfs {
		node := base.RandomLocalNode()
		fact := isaac.NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)
		sf := isaac.NewINITBallotSignFact(fact)
		t.NoError(sf.NodeSign(node.Privatekey(), t.networkID, node.Address()))

		sfs[i] = sf
	}

	ivp := isaac.NewINITVoteproof(point)
	ivp.
		SetMajority(sfs[0].Fact().(base.BallotFact)).
		SetSignFacts(sfs).
		SetThreshold(base.Threshold(100)).
		Finish()

	return pr, ivp
}

func TestHandoverMessageChallengeResponseEncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)
	t.SetT(tt)

	t.rtype = HandoverMessageChallengeResponse{}
	t.create = func() interface{} {
		return newHandoverMessageChallengeResponse(util.UUID().String(), base.NewStagePoint(base.RawPoint(33, 44), base.StageINIT), true, errors.Errorf("showme"))
	}
	t.compare = func(a, b interface{}) error {
		ah, ok := a.(HandoverMessageChallengeResponse)
		if !ok {
			return errors.Errorf("a, not HandoverMessageChallengeResponse")
		}

		bh, ok := b.(HandoverMessageChallengeResponse)
		if !ok {
			return errors.Errorf("b, not HandoverMessageChallengeResponse")
		}

		switch {
		case !ah.point.Equal(bh.point):
			return errors.Errorf("point not matched")
		case ah.ok != bh.ok:
			return errors.Errorf("ok not matched")
		case ah.err == nil && bh.err == nil:
		case ah.err == nil || bh.err == nil:
			return errors.Errorf("error not matched")
		case ah.err.Error() != bh.err.Error():
			return errors.Errorf("error not matched")
		}

		return nil
	}

	suite.Run(tt, t)
}

func TestHandoverMessageFinishEncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)
	t.SetT(tt)

	t.rtype = HandoverMessageFinish{}
	t.create = func() interface{} {
		pr, ivp := t.voteproof()

		return newHandoverMessageFinish(util.UUID().String(), ivp, pr)
	}
	t.compare = func(a, b interface{}) error {
		ah, ok := a.(HandoverMessageFinish)
		if !ok {
			return errors.Errorf("a, not HandoverMessageFinish")
		}

		bh, ok := b.(HandoverMessageFinish)
		if !ok {
			return errors.Errorf("b, not HandoverMessageFinish")
		}

		base.EqualVoteproof(t.Assert(), ah.vp, bh.vp)
		base.EqualProposalSignFact(t.Assert(), ah.pr, bh.pr)

		return nil
	}

	suite.Run(tt, t)
}

func TestHandoverMessageChallengeStagePointncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)
	t.SetT(tt)

	t.rtype = HandoverMessageChallengeStagePoint{}
	t.create = func() interface{} {
		return newHandoverMessageChallengeStagePoint(util.UUID().String(), base.NewStagePoint(base.RawPoint(33, 44), base.StageINIT))
	}
	t.compare = func(a, b interface{}) error {
		ah, ok := a.(HandoverMessageChallengeStagePoint)
		if !ok {
			return errors.Errorf("a, not HandoverMessageChallengeStagePoint")
		}

		bh, ok := b.(HandoverMessageChallengeStagePoint)
		if !ok {
			return errors.Errorf("b, not HandoverMessageChallengeStagePoint")
		}

		if !ah.point.Equal(bh.point) {
			return errors.Errorf("point not matched")
		}

		return nil
	}

	suite.Run(tt, t)
}

func TestHandoverMessageChallengeBlockMapEncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)
	t.SetT(tt)

	t.rtype = HandoverMessageChallengeBlockMap{}
	t.create = func() interface{} {
		m := base.NewDummyManifest(base.Height(22), valuehash.RandomSHA256())
		bm := base.NewDummyBlockMap(m)

		return newHandoverMessageChallengeBlockMap(util.UUID().String(), base.NewStagePoint(base.NewPoint(m.Height(), 44), base.StageINIT), bm)
	}
	t.compare = func(a, b interface{}) error {
		ah, ok := a.(HandoverMessageChallengeBlockMap)
		if !ok {
			return errors.Errorf("a, not HandoverMessageChallengeBlockMap")
		}

		bh, ok := b.(HandoverMessageChallengeBlockMap)
		if !ok {
			return errors.Errorf("b, not HandoverMessageChallengeBlockMap")
		}

		if !ah.point.Equal(bh.point) {
			return errors.Errorf("point not matched")
		}

		base.EqualBlockMap(t.Assert(), ah.m, bh.m)

		return nil
	}

	suite.Run(tt, t)
}

func TestHandoverMessageCancelEncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)
	t.SetT(tt)

	t.rtype = HandoverMessageCancel{}
	t.create = func() interface{} {
		return newHandoverMessageCancel(util.UUID().String())
	}
	t.compare = func(a, b interface{}) error {
		return nil
	}

	suite.Run(tt, t)
}

func TestHandoverMessageDataVoteproofEncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)
	t.SetT(tt)

	t.rtype = HandoverMessageData{}

	t.create = func() interface{} {
		_, ivp := t.voteproof()

		return newHandoverMessageData(util.UUID().String(), HandoverMessageDataTypeVoteproof, ivp)
	}
	t.compare = func(a, b interface{}) error {
		ah, ok := a.(HandoverMessageData)
		if !ok {
			return errors.Errorf("a, not HandoverMessageData")
		}

		bh, ok := b.(HandoverMessageData)
		if !ok {
			return errors.Errorf("b, not HandoverMessageData")
		}

		t.NotNil(ah.Data())
		t.NotNil(bh.Data())

		an, ok := ah.Data().(base.INITVoteproof)
		t.True(ok)
		bn, ok := bh.Data().(base.INITVoteproof)
		t.True(ok)

		base.EqualVoteproof(t.Assert(), an, bn)

		return nil
	}

	suite.Run(tt, t)
}

func TestHandoverMessageDataINITVoteproofEncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)
	t.SetT(tt)

	t.rtype = HandoverMessageData{}

	t.create = func() interface{} {
		pr, ivp := t.voteproof()

		return newHandoverMessageData(util.UUID().String(), HandoverMessageDataTypeINITVoteproof, []interface{}{pr, ivp})
	}
	t.compare = func(a, b interface{}) error {
		ah, ok := a.(HandoverMessageData)
		if !ok {
			return errors.Errorf("a, not HandoverMessageData")
		}

		bh, ok := b.(HandoverMessageData)
		if !ok {
			return errors.Errorf("b, not HandoverMessageData")
		}

		t.NotNil(ah.Data())
		t.NotNil(bh.Data())

		ai, ok := ah.Data().([]interface{})
		t.True(ok)
		t.Equal(2, len(ai))
		bi, ok := bh.Data().([]interface{})
		t.True(ok)
		t.Equal(2, len(bi))

		apr, ok := ai[0].(base.ProposalSignFact)
		t.True(ok)
		bpr, ok := bi[0].(base.ProposalSignFact)
		t.True(ok)

		base.EqualProposalSignFact(t.Assert(), apr, bpr)

		avp, ok := ai[1].(base.INITVoteproof)
		t.True(ok)
		bvp, ok := bi[1].(base.INITVoteproof)
		t.True(ok)

		base.EqualVoteproof(t.Assert(), avp, bvp)

		return nil
	}

	suite.Run(tt, t)
}
