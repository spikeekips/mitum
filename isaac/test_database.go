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
)

type baseTestDatabase struct {
	root string
	encs *encoder.Encoders
	enc  encoder.Encoder
}

func (t *baseTestDatabase) noerror(err error) {
	if err != nil {
		panic(err)
	}
}

func (t *baseTestDatabase) SetupSuite() {
	t.encs = encoder.NewEncoders()
	t.enc = jsonenc.NewEncoder()
	t.noerror(t.encs.AddHinter(t.enc))

	t.noerror(t.enc.AddHinter(base.DummyManifest{}))
	t.noerror(t.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
	t.noerror(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.noerror(t.enc.Add(encoder.DecodeDetail{Hint: NodeHint, Instance: RemoteNode{}}))
	t.noerror(t.enc.Add(encoder.DecodeDetail{Hint: base.DummyStateValueHint, Instance: base.DummyStateValue{}}))
	t.noerror(t.enc.Add(encoder.DecodeDetail{Hint: base.BaseStateHint, Instance: base.BaseState{}}))
	t.noerror(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageStateValueHint, Instance: SuffrageStateValue{}}))
	t.noerror(t.enc.Add(encoder.DecodeDetail{Hint: ProposalFactHint, Instance: ProposalFact{}}))
	t.noerror(t.enc.Add(encoder.DecodeDetail{Hint: ProposalSignedFactHint, Instance: ProposalSignedFact{}}))
}

func (t *baseTestDatabase) SetupTest() {
	t.root = filepath.Join(os.TempDir(), "test-mitum-wo-database-"+util.UUID().String())
}

func (t *baseTestDatabase) TearDownTest() {
	_ = os.RemoveAll(t.root)
}

func (t *baseTestDatabase) newLeveldbBlockWriteDatabase(height base.Height) *LeveldbBlockWriteDatabase {
	st, err := NewLeveldbBlockWriteDatabase(height, t.root, t.encs, t.enc)
	t.noerror(err)

	return st
}

func (t *baseTestDatabase) newMemLeveldbBlockWriteDatabase(height base.Height) *LeveldbBlockWriteDatabase {
	st := leveldbstorage.NewMemWriteStorage()
	return newLeveldbBlockWriteDatabase(st, height, t.encs, t.enc)
}

func (t *baseTestDatabase) newPool() *TempPoolDatabase {
	st := leveldbstorage.NewMemRWStorage()

	return &TempPoolDatabase{
		baseLeveldbDatabase: newBaseLeveldbDatabase(st, t.encs, t.enc),
		st:                  st,
	}
}

func (t *baseTestDatabase) states(height base.Height, n int) []base.State {
	stts := make([]base.State, n)
	for i := range make([]int, n) {
		v := base.NewDummyStateValue(util.UUID().String())
		stts[i] = base.NewBaseState(
			height,
			util.UUID().String(),
			v,
		)
	}

	return stts
}
