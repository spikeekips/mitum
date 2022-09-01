//go:build test
// +build test

package isaacdatabase

import (
	"encoding/json"
	"os"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/assert"
)

func (db *LeveldbBlockWrite) DeepClose() error {
	if err := db.Close(); err != nil {
		return err
	}

	if err := db.baseLeveldb.st.RawStorage().Close(); err != nil {
		return err
	}

	return db.baseLeveldb.Close()
}

func (db *TempPool) DeepClose() error {
	if err := db.baseLeveldb.st.RawStorage().Close(); err != nil {
		return err
	}

	return db.Close()
}

var DummySuffrageProofHint = hint.MustNewHint("isaac-dummy-suffrage-proof-v0.0.1")

type DummySuffrageProof struct {
	hint.BaseHinter
	ID string
	ST base.State
	MP base.BlockMap
}

func NewDummySuffrageProof(st base.State) DummySuffrageProof {
	return DummySuffrageProof{
		BaseHinter: hint.NewBaseHinter(DummySuffrageProofHint),
		ID:         util.UUID().String(),
		ST:         st,
		MP:         base.NewDummyBlockMap(base.NewDummyManifest(st.Height(), valuehash.RandomSHA256())),
	}
}

func (proof DummySuffrageProof) IsValid([]byte) error {
	return nil
}

func (proof DummySuffrageProof) Map() base.BlockMap {
	return proof.MP
}

func (proof DummySuffrageProof) State() base.State {
	return proof.ST
}

func (proof DummySuffrageProof) ACCEPTVoteproof() base.ACCEPTVoteproof {
	return nil
}

func (proof DummySuffrageProof) Proof() fixedtree.Proof {
	return fixedtree.Proof{}
}

func (proof DummySuffrageProof) Suffrage() (base.Suffrage, error) {
	return nil, nil
}

func (proof DummySuffrageProof) SuffrageHeight() base.Height {
	return base.NilHeight
}

func (proof DummySuffrageProof) Prove(previousState base.State) error {
	return nil
}

func (proof *DummySuffrageProof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	var u struct {
		ID string
		ST json.RawMessage
		MP json.RawMessage
	}
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	proof.ID = u.ID

	if err := encoder.Decode(enc, u.ST, &proof.ST); err != nil {
		return err
	}

	if err := encoder.Decode(enc, u.MP, &proof.MP); err != nil {
		return err
	}

	return nil
}

func IsEqualDummySuffrageProof(t *assert.Assertions, a, b base.SuffrageProof) {
	ap, ok := a.(DummySuffrageProof)
	t.True(ok)

	bp, ok := b.(DummySuffrageProof)
	t.True(ok)

	t.Equal(ap.ID, bp.ID)
}

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
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageNodeStateValueHint, Instance: isaac.SuffrageNodeStateValue{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageNodesStateValueHint, Instance: isaac.SuffrageNodesStateValue{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ProposalFactHint, Instance: isaac.ProposalFact{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ProposalSignedFactHint, Instance: isaac.ProposalSignedFact{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.NetworkPolicyStateValueHint, Instance: isaac.NetworkPolicyStateValue{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.NetworkPolicyHint, Instance: isaac.NetworkPolicy{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.FixedSuffrageCandidateLimiterRuleHint, Instance: isaac.FixedSuffrageCandidateLimiterRule{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: DummySuffrageProofHint, Instance: DummySuffrageProof{}}))
}

func (t *BaseTestDatabase) SetupTest() {
	t.Root, _ = os.MkdirTemp("", "mitum-test")
}

func (t *BaseTestDatabase) TearDownTest() {
	_ = os.RemoveAll(t.Root)
}

func (t *BaseTestDatabase) NewLeveldbBlockWriteDatabase(height base.Height) *LeveldbBlockWrite {
	mst := leveldbstorage.NewMemStorage()
	return NewLeveldbBlockWrite(height, mst, t.Encs, t.Enc)
}

func (t *BaseTestDatabase) NewPool() *TempPool {
	mst := leveldbstorage.NewMemStorage()

	db, err := newTempPool(mst, t.Encs, t.Enc)
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
