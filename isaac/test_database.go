//go:build test
// +build test

package isaac

import (
	"os"
	"path/filepath"

	"github.com/spikeekips/mitum/base"
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
	t.noerror(t.Enc.AddHinter(base.DummyBlockDataMap{}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: NodeHint, Instance: RemoteNode{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: base.DummyStateValueHint, Instance: base.DummyStateValue{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: base.BaseStateHint, Instance: base.BaseState{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: SuffrageStateValueHint, Instance: SuffrageStateValue{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: ProposalFactHint, Instance: ProposalFact{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: ProposalSignedFactHint, Instance: ProposalSignedFact{}}))
}

func (t *BaseTestDatabase) SetupTest() {
	t.Root = filepath.Join(os.TempDir(), "test-mitum-wo-database-"+util.UUID().String())
}

func (t *BaseTestDatabase) TearDownTest() {
	_ = os.RemoveAll(t.Root)
}

func (t *BaseTestDatabase) NewLeveldbBlockWriteDatabase(height base.Height) *LeveldbBlockWriteDatabase {
	st, err := NewLeveldbBlockWriteDatabase(height, t.Root, t.Encs, t.Enc)
	t.noerror(err)

	return st
}

func (t *BaseTestDatabase) NewMemLeveldbBlockWriteDatabase(height base.Height) *LeveldbBlockWriteDatabase {
	st := leveldbstorage.NewMemWriteStorage()
	return newLeveldbBlockWriteDatabase(st, height, t.Encs, t.Enc)
}

func (t *BaseTestDatabase) NewPool() *TempPoolDatabase {
	st := leveldbstorage.NewMemRWStorage()

	return &TempPoolDatabase{
		baseLeveldbDatabase: newBaseLeveldbDatabase(st, t.Encs, t.Enc),
		st:                  st,
	}
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
			nil,
		)
	}

	return stts
}
