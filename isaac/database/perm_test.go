package isaacdatabase

import (
	"context"
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/assert"
)

var DummySuffrageProofHint = hint.MustNewHint("dummy-suffrage-proof-v0.0.1")

type DummySuffrageProof struct {
	hint.BaseHinter
	ID string
	ST base.State
}

func NewDummySuffrageProof(st base.State) DummySuffrageProof {
	return DummySuffrageProof{
		BaseHinter: hint.NewBaseHinter(DummySuffrageProofHint),
		ID:         util.UUID().String(),
		ST:         st,
	}
}

func (proof DummySuffrageProof) IsValid([]byte) error {
	return nil
}

func (proof DummySuffrageProof) Map() base.BlockMap {
	return nil
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
	}
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	proof.ID = u.ID

	if err := encoder.Decode(enc, u.ST, &proof.ST); err != nil {
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

type testCommonPermanent struct {
	isaac.BaseTestBallots
	BaseTestDatabase
	newDB     func() isaac.PermanentDatabase
	newFromDB func(isaac.PermanentDatabase) (isaac.PermanentDatabase, error)
	setState  func(isaac.PermanentDatabase, base.State) error
}

func (t *testCommonPermanent) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: DummySuffrageProofHint, Instance: DummySuffrageProof{}}))
}

func (t *testCommonPermanent) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testCommonPermanent) setMap(db isaac.PermanentDatabase, mp base.BlockMap) {
	switch t := db.(type) {
	case *LeveldbPermanent:
		_ = t.mp.SetValue(mp)
	case *RedisPermanent:
		_ = t.mp.SetValue(mp)
	default:
		panic("unknown PermanentDatabase")
	}
}

func (t *testCommonPermanent) setSuffrageProof(db isaac.PermanentDatabase, proof base.SuffrageProof) {
	switch t := db.(type) {
	case *LeveldbPermanent:
		_ = t.proof.SetValue(proof)
	case *RedisPermanent:
		_ = t.proof.SetValue(proof)
	default:
		panic("unknown PermanentDatabase")
	}
}

func (t *testCommonPermanent) setNetworkPolicy(db isaac.PermanentDatabase, policy base.NetworkPolicy) {
	switch t := db.(type) {
	case *LeveldbPermanent:
		_ = t.policy.SetValue(policy)
	case *RedisPermanent:
		_ = t.policy.SetValue(policy)
	default:
		panic("unknown PermanentDatabase")
	}
}

func (t *testCommonPermanent) TestNew() {
	db := t.newDB()
	defer db.Close()

	_ = (interface{})(db).(isaac.PermanentDatabase)
}

func (t *testCommonPermanent) TestLastMap() {
	height := base.Height(33)
	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
	mp := base.NewDummyBlockMap(manifest)

	db := t.newDB()
	defer db.Close()

	t.Run("empty blockmap", func() {
		rm, found, err := db.LastBlockMap()
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
	})

	t.setMap(db, mp)

	t.Run("none-empty blockmap", func() {
		rm, found, err := db.LastBlockMap()
		t.NoError(err)
		t.True(found)

		base.EqualBlockMap(t.Assert(), mp, rm)
	})
}

func (t *testCommonPermanent) TestNetworkPolicy() {
	policy := isaac.DefaultNetworkPolicy()

	db := t.newDB()
	defer db.Close()

	t.Run("empty policy", func() {
		rpolicy := db.LastNetworkPolicy()
		t.Nil(rpolicy)
	})

	t.setNetworkPolicy(db, policy)

	t.Run("none-empty policy", func() {
		rpolicy := db.LastNetworkPolicy()

		base.EqualNetworkPolicy(t.Assert(), policy, rpolicy)
	})
}

func (t *testCommonPermanent) TestLoadEmptyDB() {
	db := t.newDB()
	defer db.Close()
}

func (t *testCommonPermanent) TestLoad() {
	height := base.Height(33)
	_, nodes := t.Locals(3)

	sufst, _ := t.SuffrageState(height, base.Height(66), nodes)
	policy := isaac.DefaultNetworkPolicy()
	policystt, _ := t.NetworkPolicyState(height, policy)

	stts := t.States(height, 3)
	stts = append(stts, sufst, policystt)

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
	mp := base.NewDummyBlockMap(manifest)

	ops := make([]util.Hash, 3)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	proof := NewDummySuffrageProof(sufst)

	wst := t.NewMemLeveldbBlockWriteDatabase(height)
	t.NoError(wst.SetBlockMap(mp))
	t.NoError(wst.SetStates(stts))
	t.NoError(wst.SetOperations(ops))
	t.NoError(wst.SetSuffrageProof(proof))
	t.NoError(wst.Write())

	temp, err := wst.TempDatabase()
	t.NoError(err)

	perm := t.newDB()
	t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

	t.Run("check blockmap in perm", func() {
		nm, found, err := perm.LastBlockMap()
		t.NoError(err)
		t.True(found)
		base.EqualBlockMap(t.Assert(), mp, nm)
	})

	t.Run("check network policy in perm", func() {
		rpolicy := perm.LastNetworkPolicy()
		base.EqualNetworkPolicy(t.Assert(), policy, rpolicy)
	})

	newperm, err := t.newFromDB(perm)
	t.NoError(err)

	t.Run("check blockmap in new perm", func() {
		nm, found, err := newperm.LastBlockMap()
		t.NoError(err)
		t.True(found)
		base.EqualBlockMap(t.Assert(), mp, nm)
	})

	t.Run("check SuffrageProof in new perm", func() {
		nproof, found, err := newperm.LastSuffrageProof()
		t.NoError(err)
		t.True(found)
		IsEqualDummySuffrageProof(t.Assert(), proof, nproof)
	})

	t.Run("check SuffrageProof by block height in new perm", func() {
		nproof, found, err := newperm.SuffrageProofByBlockHeight(height)
		t.NoError(err)
		t.True(found)
		IsEqualDummySuffrageProof(t.Assert(), proof, nproof)
	})
}

func (t *testCommonPermanent) TestMergeTempDatabase() {
	height := base.Height(33)
	_, nodes := t.Locals(3)

	sufst, _ := t.SuffrageState(height, base.Height(66), nodes)

	stts := t.States(height, 3)
	stts = append(stts, sufst)

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
	mp := base.NewDummyBlockMap(manifest)

	ops := make([]util.Hash, 3)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	proof := NewDummySuffrageProof(sufst)

	wst := t.NewMemLeveldbBlockWriteDatabase(height)
	t.NoError(wst.SetBlockMap(mp))
	t.NoError(wst.SetStates(stts))
	t.NoError(wst.SetOperations(ops))
	t.NoError(wst.SetSuffrageProof(proof))
	t.NoError(wst.Write())

	temp, err := wst.TempDatabase()
	t.NoError(err)

	t.Run("check known opertions", func() {
		perm := t.newDB()

		for i := range ops {
			found, err := perm.ExistsKnownOperation(ops[i])
			t.NoError(err)
			t.False(found)
		}

		t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

		for i := range ops {
			found, err := perm.ExistsKnownOperation(ops[i])
			t.NoError(err)
			t.True(found)
		}
	})

	t.Run("check states", func() {
		perm := t.newDB()

		for i := range stts {
			st := stts[i]
			rst, found, err := perm.State(st.Key())
			t.NoError(err)
			t.False(found)
			t.Nil(rst)
		}

		t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

		for i := range ops {
			st := stts[i]

			rst, found, err := perm.State(st.Key())
			t.NoError(err)
			t.True(found)
			t.True(base.IsEqualState(st, rst))
		}
	})

	t.Run("check instate operations", func() {
		perm := t.newDB()

		for i := range stts {
			ops := stts[i].Operations()
			for j := range ops {
				op := ops[j]

				found, err := perm.ExistsInStateOperation(op)
				t.NoError(err)
				t.False(found)

				found, err = perm.ExistsKnownOperation(op)
				t.NoError(err)
				t.False(found)
			}
		}

		t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

		for i := range stts {
			ops := stts[i].Operations()
			for j := range ops {
				op := ops[j]

				found, err := perm.ExistsInStateOperation(op)
				t.NoError(err)
				t.True(found)

				found, err = perm.ExistsKnownOperation(op)
				t.NoError(err)
				t.False(found)
			}
		}
	})

	t.Run("check blockmap", func() {
		perm := t.newDB()

		rm, found, err := perm.BlockMap(height)
		t.NoError(err)
		t.False(found)
		t.Nil(rm)

		t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

		rm, found, err = perm.BlockMap(height)
		t.NoError(err)
		t.True(found)
		t.NotNil(rm)

		base.EqualBlockMap(t.Assert(), mp, rm)
	})

	t.Run("check SuffrageProof", func() {
		perm := t.newDB()

		t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

		nproof, found, err := perm.LastSuffrageProof()
		t.NoError(err)
		t.True(found)
		IsEqualDummySuffrageProof(t.Assert(), proof, nproof)
	})

	t.Run("check SuffrageProof by block height", func() {
		perm := t.newDB()

		t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

		nproof, found, err := perm.SuffrageProofByBlockHeight(height)
		t.NoError(err)
		t.True(found)
		IsEqualDummySuffrageProof(t.Assert(), proof, nproof)
	})
}

func (t *testCommonPermanent) TestClean() {
	proof := NewDummySuffrageProof(nil)

	db := t.newDB()
	defer db.Close()

	t.setSuffrageProof(db, proof)

	t.Run("before clean", func() {
		rproof, found, err := db.LastSuffrageProof()
		t.NoError(err)
		t.True(found)
		t.NotNil(rproof)
	})

	t.Run("clean", func() {
		t.NoError(db.Clean())

		rproof, found, err := db.LastSuffrageProof()
		t.NoError(err)
		t.False(found)
		t.Nil(rproof)
	})
}
