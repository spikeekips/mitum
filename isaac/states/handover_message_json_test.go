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
		{Hint: base.StringAddressHint, Instance: base.StringAddress{}},
		{Hint: base.MPublickeyHint, Instance: base.MPublickey{}},
		{Hint: isaac.INITBallotFactHint, Instance: isaac.INITBallotFact{}},
		{Hint: isaac.INITVoteproofHint, Instance: isaac.INITVoteproof{}},
		{Hint: isaac.INITBallotSignFactHint, Instance: isaac.INITBallotSignFact{}},
		{Hint: base.DummyBlockMapHint, Instance: base.DummyBlockMap{}},
		{Hint: base.DummyManifestHint, Instance: base.DummyManifest{}},
		{Hint: base.DummyNodeHint, Instance: base.BaseNode{}},
		{Hint: HandoverMessageReadyHint, Instance: HandoverMessageReady{}},
		{Hint: HandoverMessageReadyResponseHint, Instance: HandoverMessageReadyResponse{}},
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

func TestHandoverMessageReadyEncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)

	t.rtype = HandoverMessageReady{}
	t.create = func() interface{} {
		return newHandoverMessageReady(util.UUID().String(), base.NewStagePoint(base.RawPoint(33, 44), base.StageINIT))
	}
	t.compare = func(a, b interface{}) error {
		ah, ok := a.(HandoverMessageReady)
		if !ok {
			return errors.Errorf("a, not HandoverMessageReady")
		}

		bh, ok := b.(HandoverMessageReady)
		if !ok {
			return errors.Errorf("b, not HandoverMessageReady")
		}

		switch {
		case !ah.point.Equal(bh.point):
			return errors.Errorf("point not matched")
		}

		return nil
	}

	t.SetT(tt)
	suite.Run(tt, t)
}

func TestHandoverMessageReadyResponseEncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)

	t.rtype = HandoverMessageReadyResponse{}
	t.create = func() interface{} {
		return newHandoverMessageReadyResponse(util.UUID().String(), base.NewStagePoint(base.RawPoint(33, 44), base.StageINIT), true, errors.Errorf("showme"))
	}
	t.compare = func(a, b interface{}) error {
		ah, ok := a.(HandoverMessageReadyResponse)
		if !ok {
			return errors.Errorf("a, not HandoverMessageReadyResponse")
		}

		bh, ok := b.(HandoverMessageReadyResponse)
		if !ok {
			return errors.Errorf("b, not HandoverMessageReadyResponse")
		}

		switch {
		case !ah.point.Equal(bh.point):
			return errors.Errorf("point not matched")
		case ah.ok != ah.ok:
			return errors.Errorf("ok not matched")
		case ah.err.Error() != ah.err.Error():
			return errors.Errorf("error not matched")
		}

		return nil
	}

	t.SetT(tt)
	suite.Run(tt, t)
}

func TestHandoverMessageFinishEncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)

	t.rtype = HandoverMessageFinish{}
	t.create = func() interface{} {
		point := base.RawPoint(32, 44)

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

		return newHandoverMessageFinish(util.UUID().String(), ivp)
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
		return nil
	}

	t.SetT(tt)
	suite.Run(tt, t)
}

func TestHandoverMessageChallengeStagePointncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)

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

	t.SetT(tt)
	suite.Run(tt, t)
}

func TestHandoverMessageChallengeBlockMapEncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)

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

	t.SetT(tt)
	suite.Run(tt, t)
}

func TestHandoverMessageDataEncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)

	t.rtype = HandoverMessageData{}
	t.create = func() interface{} {
		node := base.RandomNode()

		return newHandoverMessageData(util.UUID().String(), node)
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

		an, ok := ah.Data().(base.Node)
		t.True(ok)
		bn, ok := bh.Data().(base.Node)
		t.True(ok)

		t.True(base.IsEqualNode(an, bn))

		return nil
	}

	t.SetT(tt)
	suite.Run(tt, t)
}

func TestHandoverMessageCancelEncode(tt *testing.T) {
	t := new(baseEncodeTestHandoverMessage)

	t.rtype = HandoverMessageCancel{}
	t.create = func() interface{} {
		return newHandoverMessageCancel(util.UUID().String())
	}
	t.compare = func(a, b interface{}) error {
		return nil
	}

	t.SetT(tt)
	suite.Run(tt, t)
}
