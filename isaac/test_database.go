//go:build test
// +build test

package isaac

import (
	"os"
	"path/filepath"

	"github.com/spikeekips/mitum/base"
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

func (t *baseTestDatabase) SetupTest() {
	t.root = filepath.Join(os.TempDir(), "test-mitum-wo-database-"+util.UUID().String())

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
}

func (t *baseTestDatabase) TearDownTest() {
	_ = os.RemoveAll(t.root)
}

func (t *baseTestDatabase) newWO(height base.Height) *TempWODatabase {
	wst, err := newTempWODatabase(height, t.root, t.encs, t.enc)
	t.noerror(err)

	return wst
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
