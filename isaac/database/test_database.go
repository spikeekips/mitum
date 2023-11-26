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

	pst, err := db.st()
	if err != nil {
		return err
	}

	if err := pst.RawStorage().Close(); err != nil {
		return err
	}

	return db.baseLeveldb.Close()
}

func (db *TempPool) DeepClose() error {
	pst, err := db.st()
	if err != nil {
		return err
	}

	if err := pst.RawStorage().Close(); err != nil {
		return err
	}

	return db.Close()
}

func (db *TempPool) Clean() error {
	pst, err := db.st()
	if err != nil {
		return err
	}

	batch := pst.NewBatch()
	defer batch.Reset()

	if err := pst.Iter(
		nil,
		func(k []byte, _ []byte) (bool, error) {
			batch.Delete(k)

			return true, nil
		},
		true,
	); err != nil {
		return err
	}

	return pst.Batch(batch, nil)
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

func (proof *DummySuffrageProof) DecodeJSON(b []byte, enc encoder.Encoder) error {
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
	t.Enc = jsonenc.NewEncoder()
	t.Encs = encoder.NewEncoders(t.Enc, t.Enc)

	t.noerror(t.Encs.AddHinter(base.DummyManifest{}))
	t.noerror(t.Encs.AddHinter(base.DummyBlockMap{}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: base.DummyNodeHint, Instance: base.BaseNode{}}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: base.DummyStateValueHint, Instance: base.DummyStateValue{}}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: base.BaseStateHint, Instance: base.BaseState{}}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: isaac.SuffrageNodeStateValueHint, Instance: isaac.SuffrageNodeStateValue{}}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: isaac.SuffrageNodesStateValueHint, Instance: isaac.SuffrageNodesStateValue{}}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: isaac.ProposalFactHint, Instance: isaac.ProposalFact{}}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: isaac.ProposalSignFactHint, Instance: isaac.ProposalSignFact{}}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: isaac.NetworkPolicyStateValueHint, Instance: isaac.NetworkPolicyStateValue{}}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: isaac.NetworkPolicyHint, Instance: isaac.NetworkPolicy{}}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: isaac.FixedSuffrageCandidateLimiterRuleHint, Instance: isaac.FixedSuffrageCandidateLimiterRule{}}))
	t.noerror(t.Encs.AddDetail(encoder.DecodeDetail{Hint: DummySuffrageProofHint, Instance: DummySuffrageProof{}}))
}

func (t *BaseTestDatabase) SetupTest() {
	t.Root, _ = os.MkdirTemp("", "mitum-test")
}

func (t *BaseTestDatabase) TearDownTest() {
	_ = os.RemoveAll(t.Root)
}

func (t *BaseTestDatabase) NewLeveldbPermanentDatabase() *LeveldbPermanent {
	db, err := NewLeveldbPermanent(leveldbstorage.NewMemStorage(), t.Encs, t.Enc, 0)
	if err != nil {
		panic(err)
	}

	return db
}

func (t *BaseTestDatabase) NewLeveldbBlockWriteDatabase(height base.Height) *LeveldbBlockWrite {
	mst := leveldbstorage.NewMemStorage()
	return NewLeveldbBlockWrite(height, mst, t.Encs, t.Enc, 0)
}

func (t *BaseTestDatabase) NewPool() *TempPool {
	mst := leveldbstorage.NewMemStorage()

	db, err := newTempPool(mst, t.Encs, t.Enc, 0)
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
