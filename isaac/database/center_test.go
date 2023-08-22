package isaacdatabase

import (
	"context"
	"sort"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

func removeHeight(st *leveldbstorage.Storage, height base.Height) error { //nolint:deadcode,unused //...
	batch := &leveldb.Batch{}
	defer batch.Reset()

	r := &leveldbutil.Range{
		Start: emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, height),
		Limit: emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, height+1),
	}

	if _, err := leveldbstorage.BatchRemove(st, r, 333); err != nil { //nolint:gomnd //...
		return errors.WithMessage(err, "failed to remove height")
	}

	return nil
}

type DummyPermanentDatabase struct {
	closef                      func() error
	suffrageprooff              func(base.Height) (base.SuffrageProof, bool, error)
	suffrageproofbyblockheightf func(base.Height) (base.SuffrageProof, bool, error)
	lastSuffrageprooff          func() (base.SuffrageProof, bool, error)
	statef                      func(key string) (base.State, bool, error)
	statebytesf                 func(key string) (string, []byte, []byte, bool, error)
	existsInStateOperationf     func(facthash util.Hash) (bool, error)
	existsKnownOperationf       func(operationHash util.Hash) (bool, error)
	mapf                        func(height base.Height) (base.BlockMap, bool, error)
	lastMapf                    func() (base.BlockMap, bool, error)
	lastNetworkPolicyf          func() base.NetworkPolicy
	mergeTempDatabasef          func(isaac.TempDatabase) error
}

func (db *DummyPermanentDatabase) Close() error {
	return db.closef()
}

func (db *DummyPermanentDatabase) Clean() error {
	return nil
}

func (db *DummyPermanentDatabase) SuffrageProof(h base.Height) (base.SuffrageProof, bool, error) {
	return db.suffrageprooff(h)
}

func (db *DummyPermanentDatabase) SuffrageProofBytes(h base.Height) (string, []byte, []byte, bool, error) {
	proof, found, err := db.SuffrageProof(h)
	if err != nil || !found {
		return "", nil, nil, found, err
	}

	b, err := util.MarshalJSON(proof)
	if err != nil {
		return "", nil, nil, found, err
	}

	return jsonenc.JSONEncoderHint.String(), NewHashRecordMeta(proof.Map().Manifest().Suffrage()).Bytes(), b, true, nil
}

func (db *DummyPermanentDatabase) SuffrageProofByBlockHeight(h base.Height) (base.SuffrageProof, bool, error) {
	return db.suffrageproofbyblockheightf(h)
}

func (db *DummyPermanentDatabase) LastSuffrageProof() (base.SuffrageProof, bool, error) {
	return db.lastSuffrageprooff()
}

func (db *DummyPermanentDatabase) LastSuffrageProofBytes() (string, []byte, []byte, bool, error) {
	proof, found, err := db.lastSuffrageprooff()
	if err != nil || !found {
		return "", nil, nil, found, err
	}

	b, err := util.MarshalJSON(proof)
	if err != nil {
		return "", nil, nil, found, err
	}

	return jsonenc.JSONEncoderHint.String(), NewHashRecordMeta(proof.Map().Manifest().Suffrage()).Bytes(), b, true, nil
}

func (db *DummyPermanentDatabase) State(key string) (base.State, bool, error) {
	return db.statef(key)
}

func (db *DummyPermanentDatabase) StateBytes(key string) (string, []byte, []byte, bool, error) {
	return db.statebytesf(key)
}

func (db *DummyPermanentDatabase) ExistsInStateOperation(facthash util.Hash) (bool, error) {
	return db.existsInStateOperationf(facthash)
}

func (db *DummyPermanentDatabase) ExistsKnownOperation(operationHash util.Hash) (bool, error) {
	return db.existsKnownOperationf(operationHash)
}

func (db *DummyPermanentDatabase) BlockMap(height base.Height) (base.BlockMap, bool, error) {
	return db.mapf(height)
}

func (db *DummyPermanentDatabase) BlockMapBytes(height base.Height) (string, []byte, []byte, bool, error) {
	m, found, err := db.mapf(height)
	if err != nil || !found {
		return "", nil, nil, found, err
	}

	b, err := util.MarshalJSON(m)
	if err != nil {
		return "", nil, nil, found, err
	}

	return jsonenc.JSONEncoderHint.String(), nil, b, true, nil
}

func (db *DummyPermanentDatabase) LastBlockMap() (base.BlockMap, bool, error) {
	if db.lastMapf == nil {
		return nil, false, nil
	}

	return db.lastMapf()
}

func (db *DummyPermanentDatabase) LastBlockMapBytes() (string, []byte, []byte, bool, error) {
	if db.lastMapf == nil {
		return "", nil, nil, false, nil
	}

	m, found, err := db.lastMapf()
	if err != nil || !found {
		return "", nil, nil, found, err
	}

	b, err := util.MarshalJSON(m)
	if err != nil {
		return "", nil, nil, found, err
	}

	return jsonenc.JSONEncoderHint.String(), NewHashRecordMeta(m.Manifest().Hash()).Bytes(), b, true, nil
}

func (db *DummyPermanentDatabase) LastNetworkPolicy() base.NetworkPolicy {
	if db.lastNetworkPolicyf == nil {
		return nil
	}

	return db.lastNetworkPolicyf()
}

func (db *DummyPermanentDatabase) MergeTempDatabase(_ context.Context, temp isaac.TempDatabase) error {
	return db.mergeTempDatabasef(temp)
}

type testCenterWithPermanent struct {
	isaac.BaseTestBallots
	BaseTestDatabase
}

func (t *testCenterWithPermanent) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testCenterWithPermanent) TestMap() {
	manifest := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
	mp := base.NewDummyBlockMap(manifest)

	perm := &DummyPermanentDatabase{
		mapf: func(height base.Height) (base.BlockMap, bool, error) {
			switch {
			case height == manifest.Height():
				return mp, true, nil
			case height == manifest.Height()+2:
				return nil, false, errors.Errorf("hihihi")
			default:
				return nil, false, nil
			}
		},
	}

	db, err := NewCenter(leveldbstorage.NewMemStorage(), t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	_ = (interface{})(db).(isaac.Database)

	t.Run("found", func() {
		rm, found, err := db.BlockMap(manifest.Height())
		t.NoError(err)
		t.True(found)
		base.EqualBlockMap(t.Assert(), mp, rm)

		enchint, _, body, found, err := db.BlockMapBytes(manifest.Height())
		t.NoError(err)
		t.True(found)
		t.Equal(t.Enc.Hint().String(), enchint)
		t.NotNil(body)
	})

	t.Run("not found", func() {
		rm, found, err := db.BlockMap(manifest.Height() + 1)
		t.NoError(err)
		t.False(found)
		t.Nil(rm)

		_, _, body, found, err := db.BlockMapBytes(manifest.Height() + 1)
		t.NoError(err)
		t.False(found)
		t.Nil(body)
	})

	t.Run("error", func() {
		rm, found, err := db.BlockMap(manifest.Height() + 2)
		t.Error(err)
		t.False(found)
		t.Nil(rm)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testCenterWithPermanent) TestLastMap() {
	manifest := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
	mp := base.NewDummyBlockMap(manifest)

	perm := &DummyPermanentDatabase{}

	db, err := NewCenter(leveldbstorage.NewMemStorage(), t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	t.Run("found", func() {
		perm.lastMapf = func() (base.BlockMap, bool, error) {
			return mp, true, nil
		}

		rm, found, err := db.LastBlockMap()
		t.NoError(err)
		t.True(found)
		base.EqualBlockMap(t.Assert(), mp, rm)
	})

	t.Run("not found", func() {
		perm.lastMapf = func() (base.BlockMap, bool, error) {
			return nil, false, nil
		}

		rm, found, err := db.LastBlockMap()
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
	})

	t.Run("error", func() {
		perm.lastMapf = func() (base.BlockMap, bool, error) {
			return nil, false, errors.Errorf("hihihi")
		}

		rm, found, err := db.LastBlockMap()
		t.Error(err)
		t.False(found)
		t.Nil(rm)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testCenterWithPermanent) TestSuffrageProof() {
	suffrageheight := base.Height(66)

	_, nodes := t.Locals(1)
	sufst, _ := t.SuffrageState(base.Height(77), suffrageheight, nodes)
	proof := NewDummySuffrageProof(sufst)

	perm := &DummyPermanentDatabase{
		suffrageprooff: func(h base.Height) (base.SuffrageProof, bool, error) {
			switch {
			case h == suffrageheight:
				return proof, true, nil
			case h == suffrageheight+2:
				return nil, false, errors.Errorf("hihihi")
			default:
				return nil, false, nil
			}
		},
	}

	db, err := NewCenter(leveldbstorage.NewMemStorage(), t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	t.Run("found SuffrageProof", func() {
		rproof, found, err := db.SuffrageProof(suffrageheight)
		t.NoError(err)
		t.True(found)
		t.Equal(proof, rproof)

		enchint, _, body, found, err := db.SuffrageProofBytes(suffrageheight)
		t.NoError(err)
		t.True(found)
		t.Equal(t.Enc.Hint().String(), enchint)
		t.NotNil(body)
	})

	t.Run("not found SuffrageProof", func() {
		rproof, found, err := db.SuffrageProof(suffrageheight + 1)
		t.NoError(err)
		t.False(found)
		t.Nil(rproof)

		_, _, _, found, err = db.SuffrageProofBytes(suffrageheight + 1)
		t.NoError(err)
		t.False(found)
	})

	t.Run("error SuffrageProof", func() {
		rproof, found, err := db.SuffrageProof(suffrageheight + 2)
		t.Error(err)
		t.False(found)
		t.Nil(rproof)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testCenterWithPermanent) TestSuffrageProofByBlockHeight() {
	height := base.Height(66)

	_, nodes := t.Locals(1)
	sufst, _ := t.SuffrageState(height, base.Height(44), nodes)
	proof := NewDummySuffrageProof(sufst)

	perm := &DummyPermanentDatabase{
		suffrageproofbyblockheightf: func(h base.Height) (base.SuffrageProof, bool, error) {
			switch {
			case h == height:
				return proof, true, nil
			case h == height+2:
				return nil, false, errors.Errorf("hihihi")
			default:
				return nil, false, nil
			}
		},
	}

	db, err := NewCenter(leveldbstorage.NewMemStorage(), t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	t.Run("found SuffrageProof", func() {
		rproof, found, err := db.SuffrageProofByBlockHeight(height)
		t.NoError(err)
		t.True(found)
		t.Equal(proof, rproof)
	})

	t.Run("not found SuffrageProof", func() {
		rproof, found, err := db.SuffrageProofByBlockHeight(height + 1)
		t.NoError(err)
		t.False(found)
		t.Nil(rproof)
	})

	t.Run("error SuffrageProof", func() {
		rproof, found, err := db.SuffrageProofByBlockHeight(height + 2)
		t.Error(err)
		t.False(found)
		t.Nil(rproof)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testCenterWithPermanent) TestLastSuffrageProof() {
	_, nodes := t.Locals(1)
	sufst, _ := t.SuffrageState(base.Height(77), base.Height(66), nodes)
	proof := NewDummySuffrageProof(sufst)

	perm := &DummyPermanentDatabase{}

	db, err := NewCenter(leveldbstorage.NewMemStorage(), t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	t.Run("found", func() {
		perm.lastSuffrageprooff = func() (base.SuffrageProof, bool, error) {
			return proof, true, nil
		}

		rproof, found, err := db.LastSuffrageProof()
		t.NoError(err)
		t.True(found)
		t.Equal(proof, rproof)
	})

	t.Run("not found", func() {
		perm.lastSuffrageprooff = func() (base.SuffrageProof, bool, error) {
			return nil, false, nil
		}

		rproof, found, err := db.LastSuffrageProof()
		t.NoError(err)
		t.False(found)
		t.Nil(rproof)
	})

	t.Run("error", func() {
		perm.lastSuffrageprooff = func() (base.SuffrageProof, bool, error) {
			return nil, false, errors.Errorf("hihihi")
		}

		rproof, found, err := db.LastSuffrageProof()
		t.Error(err)
		t.False(found)
		t.Nil(rproof)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testCenterWithPermanent) TestLastSuffrageProofBytes() {
	_, nodes := t.Locals(1)

	sufst, _ := t.SuffrageState(base.Height(77), base.Height(66), nodes)
	proof := NewDummySuffrageProof(sufst)

	perm := &DummyPermanentDatabase{}

	manifest := base.NewDummyManifest(base.Height(88), valuehash.RandomSHA256())
	mp := base.NewDummyBlockMap(manifest)

	perm.lastMapf = func() (base.BlockMap, bool, error) {
		return mp, true, nil
	}

	db, err := NewCenter(leveldbstorage.NewMemStorage(), t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	t.Run("found", func() {
		perm.lastSuffrageprooff = func() (base.SuffrageProof, bool, error) {
			return proof, true, nil
		}

		enchint, _, body, found, height, err := db.LastSuffrageProofBytes()
		t.NoError(err)
		t.True(found)
		t.Equal(manifest.Height(), height)
		t.Equal(jsonenc.JSONEncoderHint.String(), enchint)

		var rproof base.SuffrageProof

		t.NoError(encoder.Decode(t.Enc, body, &rproof))
		base.EqualSuffrageProof(t.Assert(), proof, rproof)
	})

	t.Run("not found", func() {
		perm.lastSuffrageprooff = func() (base.SuffrageProof, bool, error) {
			return nil, false, nil
		}

		_, _, body, found, height, err := db.LastSuffrageProofBytes()
		t.NoError(err)
		t.False(found)
		t.Equal(base.NilHeight, height)
		t.Empty(body)
	})

	t.Run("error", func() {
		perm.lastSuffrageprooff = func() (base.SuffrageProof, bool, error) {
			return nil, false, errors.Errorf("hihihi")
		}

		_, _, _, found, height, err := db.LastSuffrageProofBytes()
		t.Error(err)
		t.False(found)
		t.Equal(base.NilHeight, height)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testCenterWithPermanent) TestLastNetworkPolicy() {
	perm := &DummyPermanentDatabase{}

	db, err := NewCenter(leveldbstorage.NewMemStorage(), t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	policy := isaac.DefaultNetworkPolicy()

	t.Run("found", func() {
		perm.lastNetworkPolicyf = func() base.NetworkPolicy {
			return policy
		}

		r := db.LastNetworkPolicy()
		base.EqualNetworkPolicy(t.Assert(), policy, r)
	})

	t.Run("not found", func() {
		perm.lastNetworkPolicyf = func() base.NetworkPolicy {
			return nil
		}

		r := db.LastNetworkPolicy()
		t.Nil(r)
	})
}

func (t *testCenterWithPermanent) TestState() {
	st := t.States(base.Height(33), 1)[0]

	errkey := util.UUID().String()
	perm := &DummyPermanentDatabase{
		statef: func(key string) (base.State, bool, error) {
			switch {
			case key == st.Key():
				return st, true, nil
			case key == errkey:
				return nil, false, errors.Errorf("hihihi")
			default:
				return nil, false, nil
			}
		},
	}

	db, err := NewCenter(leveldbstorage.NewMemStorage(), t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	t.Run("found", func() {
		rst, found, err := db.State(st.Key())
		t.NoError(err)
		t.True(found)
		t.True(base.IsEqualState(st, rst))
	})

	t.Run("not found", func() {
		rst, found, err := db.State(util.UUID().String())
		t.NoError(err)
		t.False(found)
		t.Nil(rst)
	})

	t.Run("error", func() {
		rst, found, err := db.State(errkey)
		t.Error(err)
		t.False(found)
		t.Nil(rst)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testCenterWithPermanent) TestExistsInStateOperation() {
	op := valuehash.RandomSHA256()
	errop := valuehash.RandomSHA256()

	perm := &DummyPermanentDatabase{
		existsInStateOperationf: func(facthash util.Hash) (bool, error) {
			switch {
			case facthash.Equal(op):
				return true, nil
			case facthash.Equal(errop):
				return false, errors.Errorf("hihihi")
			default:
				return false, nil
			}
		},
	}

	db, err := NewCenter(leveldbstorage.NewMemStorage(), t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	t.Run("found", func() {
		found, err := db.ExistsInStateOperation(op)
		t.NoError(err)
		t.True(found)
	})

	t.Run("not found", func() {
		found, err := db.ExistsInStateOperation(valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
	})

	t.Run("error", func() {
		found, err := db.ExistsInStateOperation(errop)
		t.Error(err)
		t.False(found)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testCenterWithPermanent) TestExistsKnownOperation() {
	op := valuehash.RandomSHA256()
	errop := valuehash.RandomSHA256()

	perm := &DummyPermanentDatabase{
		existsKnownOperationf: func(operationhash util.Hash) (bool, error) {
			switch {
			case operationhash.Equal(op):
				return true, nil
			case operationhash.Equal(errop):
				return false, errors.Errorf("hihihi")
			default:
				return false, nil
			}
		},
	}

	db, err := NewCenter(leveldbstorage.NewMemStorage(), t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	t.Run("found", func() {
		found, err := db.ExistsKnownOperation(op)
		t.NoError(err)
		t.True(found)
	})

	t.Run("not found", func() {
		found, err := db.ExistsKnownOperation(valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
	})

	t.Run("error", func() {
		found, err := db.ExistsKnownOperation(errop)
		t.Error(err)
		t.False(found)
		t.ErrorContains(err, "hihihi")
	})
}

func TestCenterWithPermanent(t *testing.T) {
	suite.Run(t, new(testCenterWithPermanent))
}

type testCenterBlockWrite struct {
	isaac.BaseTestBallots
	BaseTestDatabase
}

func (t *testCenterBlockWrite) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testCenterBlockWrite) TestMerge() {
	height := base.Height(33)
	_, nodes := t.Locals(3)

	lmanifest := base.NewDummyManifest(height-1, valuehash.RandomSHA256())
	lmp := base.NewDummyBlockMap(lmanifest)
	newstts := t.States(height, 3)
	oldstts := make([]base.State, len(newstts)+1)
	for i := range newstts {
		oldstts[i] = base.NewBaseState(
			newstts[i].Height()-1,
			newstts[i].Key(),
			base.NewDummyStateValue(util.UUID().String()),
			valuehash.RandomSHA256(),
			nil,
		)
	}

	lsufst, lsufsv := t.SuffrageState(height-1, base.Height(66)-1, nodes)
	oldstts[3] = lsufst
	lproof := NewDummySuffrageProof(lsufst)

	perm := &DummyPermanentDatabase{
		mapf: func(base.Height) (base.BlockMap, bool, error) {
			return nil, false, nil
		},
		lastMapf: func() (base.BlockMap, bool, error) {
			return lmp, true, nil
		},
		suffrageprooff: func(suffrageHeight base.Height) (base.SuffrageProof, bool, error) {
			switch {
			case suffrageHeight == lsufsv.Height():
				return lproof, true, nil
			default:
				return nil, false, nil
			}
		},
		lastSuffrageprooff: func() (base.SuffrageProof, bool, error) {
			return lproof, true, nil
		},
		statef: func(key string) (base.State, bool, error) {
			for i := range oldstts {
				st := oldstts[i]
				if st.Key() == key {
					return st, true, nil
				}
			}

			return nil, false, nil
		},
		existsKnownOperationf: func(util.Hash) (bool, error) {
			return false, nil
		},
	}

	st := leveldbstorage.NewMemStorage()
	db, err := NewCenter(st, t.Encs, t.Enc, perm, func(h base.Height) (isaac.BlockWriteDatabase, error) {
		return NewLeveldbBlockWrite(h, st, t.Encs, t.Enc), nil
	})
	t.NoError(err)

	wst, err := db.NewBlockWriteDatabase(height)
	t.NoError(err)
	defer wst.Close()

	sufst, sufsv := t.SuffrageState(height, lsufsv.Height()+1, nodes)
	newstts = append(newstts, sufst)
	proof := NewDummySuffrageProof(sufst)

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())

	ops := make([]util.Hash, 3)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	mp := base.NewDummyBlockMap(manifest)

	t.NoError(wst.SetBlockMap(mp))
	t.NoError(wst.SetStates(newstts))
	t.NoError(wst.SetOperations(ops))
	t.NoError(wst.SetSuffrageProof(proof))
	t.NoError(wst.Write())

	t.Run("check blockmap before merging", func() {
		rm, found, err := db.BlockMap(height)
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
	})

	t.Run("check last blockmap before merging", func() {
		rm, found, err := db.LastBlockMap()
		t.NoError(err)
		t.True(found)

		base.EqualBlockMap(t.Assert(), lmp, rm)
	})

	t.Run("check SuffrageProof before merging", func() {
		rproof, found, err := db.SuffrageProof(sufsv.Height())
		t.NoError(err)
		t.False(found)
		t.Nil(rproof)

		rproof, found, err = db.SuffrageProof(lsufsv.Height())
		t.NoError(err)
		t.True(found)

		t.Equal(lproof, rproof)
	})

	t.Run("check state before merging", func() {
		for i := range newstts {
			st := newstts[i]

			rst, found, err := db.State(st.Key())
			t.NoError(err)
			t.True(found)

			t.True(base.IsEqualState(oldstts[i], rst))
		}
	})

	t.Run("check operations before merging", func() {
		for i := range ops {
			op := ops[i]

			found, err := db.ExistsKnownOperation(op)
			t.NoError(err)
			t.False(found)
		}
	})

	t.NoError(db.MergeBlockWriteDatabase(wst))

	t.Run("check blockmap", func() {
		rm, found, err := db.BlockMap(height)
		t.NoError(err)
		t.True(found)

		base.EqualBlockMap(t.Assert(), mp, rm)
	})

	t.Run("check last blockmap", func() {
		rm, found, err := db.LastBlockMap()
		t.NoError(err)
		t.True(found)

		base.EqualBlockMap(t.Assert(), mp, rm)
	})

	t.Run("check SuffrageProof", func() {
		rproof, found, err := db.SuffrageProof(sufsv.Height())
		t.NoError(err)
		t.True(found)

		t.Equal(proof, rproof)
	})
	t.Run("check last SuffrageProof", func() {
		rproof, found, err := db.LastSuffrageProof()
		t.NoError(err)
		t.True(found)

		t.Equal(proof, rproof)
	})

	t.Run("check state", func() {
		for i := range newstts {
			st := newstts[i]

			rst, found, err := db.State(st.Key())
			t.NoError(err)
			t.True(found)

			t.True(base.IsEqualState(st, rst))
		}
	})
	t.Run("check operation", func() {
		for i := range ops {
			op := ops[i]

			found, err := db.ExistsKnownOperation(op)
			t.NoError(err)
			t.True(found)
		}
	})
}

func (t *testCenterBlockWrite) TestFindState() {
	baseheight := base.Height(33)

	manifest := base.NewDummyManifest(baseheight, valuehash.RandomSHA256())
	mp := base.NewDummyBlockMap(manifest)
	perm := &DummyPermanentDatabase{
		lastMapf: func() (base.BlockMap, bool, error) {
			return mp, true, nil
		},
		statef: func(string) (base.State, bool, error) {
			return nil, false, nil
		},
	}

	st := leveldbstorage.NewMemStorage()
	db, err := NewCenter(st, t.Encs, t.Enc, perm, func(height base.Height) (isaac.BlockWriteDatabase, error) {
		return NewLeveldbBlockWrite(height, st, t.Encs, t.Enc), nil
	})
	t.NoError(err)

	stts := t.States(baseheight, 10)

	var laststts []base.State
	for i := range make([]int, 4) {
		height := baseheight + base.Height(i+1)
		wst, err := db.NewBlockWriteDatabase(height)
		t.NoError(err)
		defer wst.Close()

		manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())

		laststts = make([]base.State, len(stts))
		for i := range stts {
			laststts[i] = base.NewBaseState(
				height,
				stts[i].Key(),
				base.NewDummyStateValue(util.UUID().String()),
				valuehash.RandomSHA256(),
				nil,
			)
		}

		m := base.NewDummyBlockMap(manifest)

		t.NoError(wst.SetBlockMap(m))
		t.NoError(wst.SetStates(laststts))
		t.NoError(wst.Write())

		t.NoError(db.MergeBlockWriteDatabase(wst))
	}

	t.Run("check state", func() {
		for i := range laststts {
			st := laststts[i]

			rst, found, err := db.State(st.Key())
			t.NoError(err)
			t.True(found)

			t.True(base.IsEqualState(st, rst))
		}
	})
}

func (t *testCenterBlockWrite) TestInvalidMerge() {
	height := base.Height(33)

	perm := &DummyPermanentDatabase{
		lastMapf: func() (base.BlockMap, bool, error) {
			m := base.NewDummyManifest(height-1, valuehash.RandomSHA256())
			return base.NewDummyBlockMap(m), true, nil
		},
	}

	st := leveldbstorage.NewMemStorage()
	db, err := NewCenter(st, t.Encs, t.Enc, perm, func(h base.Height) (isaac.BlockWriteDatabase, error) {
		return NewLeveldbBlockWrite(h, st, t.Encs, t.Enc), nil
	})
	t.NoError(err)

	t.Run("wrong height", func() {
		manifest := base.NewDummyManifest(height+1, valuehash.RandomSHA256())

		wst, err := db.NewBlockWriteDatabase(manifest.Height())
		t.NoError(err)
		defer wst.Close()

		mp := base.NewDummyBlockMap(manifest)
		t.NoError(wst.SetBlockMap(mp))
		t.NoError(wst.Write())

		err = db.MergeBlockWriteDatabase(wst)
		t.Error(err)
		t.ErrorContains(err, "merge new TempDatabase")
		t.ErrorContains(err, "wrong height")
	})

	t.Run("not yet written", func() {
		m := base.NewDummyManifest(height, valuehash.RandomSHA256())

		wst, err := db.NewBlockWriteDatabase(m.Height())
		t.NoError(err)
		defer wst.Close()

		err = db.MergeBlockWriteDatabase(wst)
		t.Error(err)
		t.ErrorContains(err, "merge new TempDatabase")
		t.ErrorContains(err, "empty blockmap")
	})
}

func (t *testCenterBlockWrite) TestMergePermanent() {
	baseheight := base.Height(33)

	lmanifest := base.NewDummyManifest(baseheight, valuehash.RandomSHA256())
	lmp := base.NewDummyBlockMap(lmanifest)

	var mergedstt []base.State
	perm := &DummyPermanentDatabase{
		lastMapf: func() (base.BlockMap, bool, error) {
			return lmp, true, nil
		},

		mergeTempDatabasef: func(temp isaac.TempDatabase) error {
			ltemp := temp.(*TempLeveldb)
			t.NoError(ltemp.States(func(st base.State) (bool, error) {
				mergedstt = append(mergedstt, st)

				return true, nil
			}))

			return nil
		},
		statef: func(key string) (base.State, bool, error) {
			for i := range mergedstt {
				st := mergedstt[i]
				if st.Key() == key {
					return st, true, nil
				}
			}

			return nil, false, nil
		},
	}

	st := leveldbstorage.NewMemStorage()
	db, err := NewCenter(st, t.Encs, t.Enc, perm, func(h base.Height) (isaac.BlockWriteDatabase, error) {
		return NewLeveldbBlockWrite(h, st, t.Encs, t.Enc), nil
	})
	t.NoError(err)

	var removeds []isaac.TempDatabase

	for i := range make([]int, 10) {
		height := baseheight + base.Height(i+1)
		manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
		m := base.NewDummyBlockMap(manifest)

		sttss := t.States(height, 2)

		wst, err := db.NewBlockWriteDatabase(height)
		t.NoError(err)
		defer wst.Close()

		t.NoError(wst.SetBlockMap(m))
		t.NoError(wst.SetStates(sttss))
		t.NoError(wst.Write())

		t.NoError(db.MergeBlockWriteDatabase(wst))

		if i < 3 { // 0, 1, 2
			actives := db.activeTemps()

			removeds = append(removeds, actives[0])
		}
	}

	t.Equal(10, len(db.activeTemps()))
	for range make([]int, 3) { // 0, 1, 2
		merged, err := db.mergePermanent(context.TODO())
		t.NoError(err)
		t.True(merged)
		t.NoError(db.cleanRemoved(2))
	}

	t.Equal(7, len(db.activeTemps()))

	t.Run("find merged states in active temps", func() {
		actives := db.activeTemps()
		for i := range mergedstt {
			st := mergedstt[i]
			for j := range actives {
				temp := actives[j]
				_, found, err := temp.State(st.Key())
				t.NoError(err)
				t.False(found)
			}
		}
	})

	t.Run("find merged states in permanent database", func() {
		for i := range mergedstt {
			st := mergedstt[i]

			rst, found, err := perm.State(st.Key())
			t.NoError(err)
			t.True(found)

			t.True(base.IsEqualState(st, rst))
		}
	})

	t.Run("find merged states in database", func() {
		for i := range mergedstt {
			st := mergedstt[i]

			rst, found, err := db.State(st.Key())
			t.NoError(err)
			t.True(found)

			t.True(base.IsEqualState(st, rst))
		}
	})

	removed := removeds[0]
	t.NoError(removed.Remove())

	for i := range db.removed {
		r := db.removed[i]
		t.NotEqual(removed.Height(), r.Height())
	}
}

func TestCenterBlockWrite(t *testing.T) {
	suite.Run(t, new(testCenterBlockWrite))
}

type testCenterLoad struct {
	isaac.BaseTestBallots
	BaseTestDatabase
}

func (t *testCenterLoad) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testCenterLoad) TestLoadTempDatabases() {
	baseheight := base.Height(33)

	basemanifest := base.NewDummyManifest(baseheight, valuehash.RandomSHA256())
	basemp := base.NewDummyBlockMap(basemanifest)
	perm := &DummyPermanentDatabase{
		lastMapf: func() (base.BlockMap, bool, error) {
			return basemp, true, nil
		},
	}

	st := leveldbstorage.NewMemStorage()
	db, err := NewCenter(st, t.Encs, t.Enc, perm, func(height base.Height) (isaac.BlockWriteDatabase, error) {
		return NewLeveldbBlockWrite(height, st, t.Encs, t.Enc), nil
	})
	t.NoError(err)

	created := make([]base.BlockMap, 4)
	for i := range created {
		height := baseheight + base.Height(i+1)
		wst, err := db.NewBlockWriteDatabase(height)
		t.NoError(err)

		manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())

		mp := base.NewDummyBlockMap(manifest)
		t.NoError(wst.SetBlockMap(mp))

		t.NoError(wst.Write())
	}

	// NOTE create again
	for i := range created {
		height := baseheight + base.Height(i+1)
		wst, err := db.NewBlockWriteDatabase(height)
		t.NoError(err)

		manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
		mp := base.NewDummyBlockMap(manifest)
		t.NoError(wst.SetBlockMap(mp))

		t.NoError(wst.Write())

		t.NoError(db.MergeBlockWriteDatabase(wst))

		t.NoError(wst.Close())

		created[i] = mp
	}

	t.T().Log("load temps")

	temps, err := loadTemps(st, basemanifest.Height(), t.Encs, t.Enc)
	t.NoError(err)
	t.Equal(len(created), len(temps))

	sort.Slice(created, func(i, j int) bool {
		return created[i].Manifest().Height() > created[j].Manifest().Height()
	})

	for i := range temps {
		temp := temps[i]
		expected := created[i]

		t.Equal(expected.Manifest().Height(), temp.Height())

		tm, found, err := temp.LastBlockMap()
		t.NoError(err)
		t.True(found)

		base.EqualBlockMap(t.Assert(), expected, tm)
	}

	newdb, err := NewCenter(st, t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	actives := newdb.activeTemps()
	t.Equal(len(temps), len(actives))

	for i := range temps {
		temp := temps[i]
		expected := actives[i]

		t.Equal(expected.Height(), temp.Height())

		em, found, err := expected.LastBlockMap()
		t.NoError(err)
		t.True(found)

		tm, found, err := temp.LastBlockMap()
		t.NoError(err)
		t.True(found)

		base.EqualBlockMap(t.Assert(), em, tm)
	}
}

func (t *testCenterLoad) TestLoadTempDatabasesButMissing() {
	baseheight := base.Height(33)

	basemanifest := base.NewDummyManifest(baseheight, valuehash.RandomSHA256())
	basemp := base.NewDummyBlockMap(basemanifest)
	perm := &DummyPermanentDatabase{
		lastMapf: func() (base.BlockMap, bool, error) {
			return basemp, true, nil
		},
	}

	st := leveldbstorage.NewMemStorage()
	db, err := NewCenter(st, t.Encs, t.Enc, perm, func(height base.Height) (isaac.BlockWriteDatabase, error) {
		return NewLeveldbBlockWrite(height, st, t.Encs, t.Enc), nil
	})
	t.NoError(err)

	created := make([]base.BlockMap, 4)

	for i := range created {
		height := baseheight + base.Height(i+1)

		var wst isaac.BlockWriteDatabase
		var mp base.BlockMap

		for range make([]int, 3) {
			k, err := db.NewBlockWriteDatabase(height)
			t.NoError(err)

			manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
			mp = base.NewDummyBlockMap(manifest)
			t.NoError(k.SetBlockMap(mp))

			t.NoError(k.Write())

			wst = k
		}

		t.NoError(db.MergeBlockWriteDatabase(wst))

		created[i] = mp
	}

	// NOTE delete middle one
	t.NoError(removeHeight(st, baseheight+3))

	rtemps, err := loadTemps(st, basemanifest.Height(), t.Encs, t.Enc)
	t.NoError(err)

	last := rtemps[0]

	lm, found, err := last.LastBlockMap()
	t.NoError(err)
	t.True(found)

	t.Equal(baseheight+2, last.Height())
	t.Equal(baseheight+2, lm.Manifest().Height())

	// NOTE higher data will be removed

	r := leveldbutil.BytesPrefix(leveldbLabelBlockWrite[:])
	r.Start = emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, last.Height()+2)

	var highercount int
	st.Iter(
		r,
		func(key, _ []byte) (bool, error) {
			highercount++

			return true, nil
		},
		true,
	)
	t.True(highercount < 1)
}

func (t *testCenterLoad) TestRemoveBlocks() {
	baseheight := base.Height(33)

	basemanifest := base.NewDummyManifest(baseheight, valuehash.RandomSHA256())
	basemp := base.NewDummyBlockMap(basemanifest)
	perm := &DummyPermanentDatabase{
		lastMapf: func() (base.BlockMap, bool, error) {
			return basemp, true, nil
		},
	}

	st := leveldbstorage.NewMemStorage()
	db, err := NewCenter(st, t.Encs, t.Enc, perm, func(height base.Height) (isaac.BlockWriteDatabase, error) {
		return NewLeveldbBlockWrite(height, st, t.Encs, t.Enc), nil
	})
	t.NoError(err)

	maps := make([]base.BlockMap, 4)
	for i := range maps {
		height := baseheight + base.Height(i+1)

		wst, err := db.NewBlockWriteDatabase(height)
		t.NoError(err)

		manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
		mp := base.NewDummyBlockMap(manifest)
		t.NoError(wst.SetBlockMap(mp))
		t.NoError(wst.Write())
		t.NoError(db.MergeBlockWriteDatabase(wst))

		maps[i] = mp
	}

	t.Equal(4, len(db.activeTemps()))

	t.T().Log("4 temp database merged")

	temps := db.activeTemps()
	top := temps[0].Height()
	bottom := temps[len(temps)-1].Height()

	t.T().Log("top:", top, "bottom:", bottom)

	t.Run("remove over top", func() {
		removed, err := db.RemoveBlocks(top + 1)
		t.NoError(err)
		t.False(removed)
	})

	t.Run("remove under bottom", func() {
		removed, err := db.RemoveBlocks(bottom - 1)
		t.NoError(err)
		t.False(removed)
	})

	t.Run("remove top", func() {
		removed, err := db.RemoveBlocks(top)
		t.NoError(err)
		t.True(removed)

		t.Equal(len(temps)-1, len(db.activeTemps()))

		mp, found, err := db.LastBlockMap()
		t.NoError(err)
		t.True(found)
		base.EqualBlockMap(t.Assert(), maps[2], mp)
	})

	t.Run("remove bottom", func() {
		removed, err := db.RemoveBlocks(bottom)
		t.NoError(err)
		t.True(removed)

		t.Equal(0, len(db.activeTemps()))

		mp, found, err := db.LastBlockMap()
		t.NoError(err)
		t.True(found)
		base.EqualBlockMap(t.Assert(), basemp, mp)
	})
}

func TestCenterLoad(t *testing.T) {
	suite.Run(t, new(testCenterLoad))
}
