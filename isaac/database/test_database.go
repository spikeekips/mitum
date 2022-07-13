//go:build test
// +build test

package isaacdatabase

import (
	"os"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
)

type BaseTestDatabase struct {
	Root string
	Encs *encoder.Encoders
	Enc  encoder.Encoder
}

func (t *BaseTestDatabase) noerror(err error) {
	if err != nil {
		panic(err)
	}
}

func (t *BaseTestDatabase) SetupSuite() {
	t.Encs = encoder.NewEncoders()
	t.Enc = jsonenc.NewEncoder()
	t.noerror(t.Encs.AddHinter(t.Enc))

	t.noerror(t.Enc.AddHinter(base.DummyManifest{}))
	t.noerror(t.Enc.AddHinter(base.DummyBlockMap{}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.NodeHint, Instance: base.BaseNode{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: base.DummyStateValueHint, Instance: base.DummyStateValue{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: base.BaseStateHint, Instance: base.BaseState{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageStateValueHint, Instance: isaac.SuffrageStateValue{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ProposalFactHint, Instance: isaac.ProposalFact{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ProposalSignedFactHint, Instance: isaac.ProposalSignedFact{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.NetworkPolicyStateValueHint, Instance: isaac.NetworkPolicyStateValue{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.NetworkPolicyHint, Instance: isaac.NetworkPolicy{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.FixedSuffrageCandidateLimiterRuleHint, Instance: isaac.FixedSuffrageCandidateLimiterRule{}}))
}

func (t *BaseTestDatabase) SetupTest() {
	t.Root, _ = os.MkdirTemp("", "mitum-test")
}

func (t *BaseTestDatabase) TearDownTest() {
	_ = os.RemoveAll(t.Root)
}

func (t *BaseTestDatabase) NewLeveldbBlockWriteDatabase(height base.Height) *LeveldbBlockWrite {
	st, err := NewLeveldbBlockWrite(height, t.Root, t.Encs, t.Enc)
	t.noerror(err)

	return st
}

func (t *BaseTestDatabase) NewMemLeveldbBlockWriteDatabase(height base.Height) *LeveldbBlockWrite {
	st := leveldbstorage.NewMemWriteStorage()
	return newLeveldbBlockWrite(st, height, t.Encs, t.Enc)
}

func (t *BaseTestDatabase) NewMemLeveldbPermanentDatabase() *LeveldbPermanent {
	st := leveldbstorage.NewMemWriteStorage()
	db, err := newLeveldbPermanent(st, t.Encs, t.Enc)
	t.noerror(err)

	return db
}

func (t *BaseTestDatabase) NewMemLeveldbTempSyncPool() *LeveldbTempSyncPool {
	st := leveldbstorage.NewMemWriteStorage()
	return newLeveldbTempSyncPool(st, t.Encs, t.Enc)
}

func (t *BaseTestDatabase) NewPool() *TempPool {
	st := leveldbstorage.NewMemRWStorage()

	db, err := newTempPool(st, t.Encs, t.Enc)
	t.noerror(err)

	return db
}

func (t *BaseTestDatabase) States(height base.Height, n int) []base.State {
	stts := make([]base.State, n)
	for i := range make([]int, n) {
		v := base.NewDummyStateValue(util.UUID().String())
		stts[i] = base.NewBaseState(
			height,
			util.UUID().String(),
			v,
			valuehash.RandomSHA256(),
			[]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
		)
	}

	return stts
}
