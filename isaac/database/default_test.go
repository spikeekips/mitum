package database

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type DummyPermanentDatabase struct {
	closef                  func() error
	suffragef               func(height base.Height) (base.State, bool, error)
	suffrageByHeightf       func(suffrageHeight base.Height) (base.State, bool, error)
	lastSuffragef           func() (base.State, bool, error)
	statef                  func(key string) (base.State, bool, error)
	existsInStateOperationf func(operationFactHash util.Hash) (bool, error)
	existsKnownOperationf   func(operationHash util.Hash) (bool, error)
	mapf                    func(height base.Height) (base.BlockDataMap, bool, error)
	lastMapf                func() (base.BlockDataMap, bool, error)
	lastNetworkPolicyf      func() base.NetworkPolicy
	mergeTempDatabasef      func(isaac.TempDatabase) error
}

func (db *DummyPermanentDatabase) Close() error {
	return db.closef()
}

func (db *DummyPermanentDatabase) Clean() error {
	return nil
}

func (db *DummyPermanentDatabase) SuffrageByHeight(suffrageHeight base.Height) (base.State, bool, error) {
	return db.suffrageByHeightf(suffrageHeight)
}

func (db *DummyPermanentDatabase) Suffrage(height base.Height) (base.State, bool, error) {
	return db.suffragef(height)
}

func (db *DummyPermanentDatabase) LastSuffrage() (base.State, bool, error) {
	return db.lastSuffragef()
}

func (db *DummyPermanentDatabase) State(key string) (base.State, bool, error) {
	return db.statef(key)
}

func (db *DummyPermanentDatabase) ExistsInStateOperation(operationFactHash util.Hash) (bool, error) {
	return db.existsInStateOperationf(operationFactHash)
}

func (db *DummyPermanentDatabase) ExistsKnownOperation(operationHash util.Hash) (bool, error) {
	return db.existsKnownOperationf(operationHash)
}

func (db *DummyPermanentDatabase) Map(height base.Height) (base.BlockDataMap, bool, error) {
	return db.mapf(height)
}

func (db *DummyPermanentDatabase) LastMap() (base.BlockDataMap, bool, error) {
	if db.lastMapf == nil {
		return nil, false, nil
	}

	return db.lastMapf()
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

type testDefaultWithPermanent struct {
	isaac.BaseTestBallots
	BaseTestDatabase
}

func (t *testDefaultWithPermanent) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testDefaultWithPermanent) TestMap() {
	manifest := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
	mp := base.NewDummyBlockDataMap(manifest)

	perm := &DummyPermanentDatabase{
		mapf: func(height base.Height) (base.BlockDataMap, bool, error) {
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

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	_ = (interface{})(db).(isaac.Database)

	t.Run("found", func() {
		rm, found, err := db.Map(manifest.Height())
		t.NoError(err)
		t.True(found)
		base.EqualBlockDataMap(t.Assert(), mp, rm)
	})

	t.Run("not found", func() {
		rm, found, err := db.Map(manifest.Height() + 1)
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
	})

	t.Run("error", func() {
		rm, found, err := db.Map(manifest.Height() + 2)
		t.Error(err)
		t.False(found)
		t.Nil(rm)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testDefaultWithPermanent) TestLastMap() {
	manifest := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
	mp := base.NewDummyBlockDataMap(manifest)

	perm := &DummyPermanentDatabase{}

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	t.Run("found", func() {
		perm.lastMapf = func() (base.BlockDataMap, bool, error) {
			return mp, true, nil
		}

		rm, found, err := db.LastMap()
		t.NoError(err)
		t.True(found)
		base.EqualBlockDataMap(t.Assert(), mp, rm)
	})

	t.Run("not found", func() {
		perm.lastMapf = func() (base.BlockDataMap, bool, error) {
			return nil, false, nil
		}

		rm, found, err := db.LastMap()
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
	})

	t.Run("error", func() {
		perm.lastMapf = func() (base.BlockDataMap, bool, error) {
			return nil, false, errors.Errorf("hihihi")
		}

		rm, found, err := db.LastMap()
		t.Error(err)
		t.False(found)
		t.Nil(rm)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testDefaultWithPermanent) TestSuffrage() {
	_, nodes := t.Locals(3)
	height := base.Height(33)
	suffrageheight := base.Height(66)
	st, sv := t.SuffrageState(height, suffrageheight, nodes)

	perm := &DummyPermanentDatabase{
		suffragef: func(height base.Height) (base.State, bool, error) {
			switch {
			case height == st.Height():
				return st, true, nil
			case height == st.Height()+2:
				return nil, false, errors.Errorf("hihihi")
			default:
				return nil, false, nil
			}
		},
		suffrageByHeightf: func(suffrageHeight base.Height) (base.State, bool, error) {
			switch {
			case suffrageHeight == sv.Height():
				return st, true, nil
			case suffrageHeight == sv.Height()+2:
				return nil, false, errors.Errorf("hihihi")
			default:
				return nil, false, nil
			}
		},
	}

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	t.Run("found SuffrageByHeight", func() {
		rst, found, err := db.SuffrageByHeight(suffrageheight)
		t.NoError(err)
		t.True(found)
		t.True(base.IsEqualState(st, rst))
	})

	t.Run("not found SuffrageByHeight", func() {
		rst, found, err := db.SuffrageByHeight(suffrageheight + 1)
		t.NoError(err)
		t.False(found)
		t.Nil(rst)
	})

	t.Run("error SuffrageByHeight", func() {
		rst, found, err := db.SuffrageByHeight(suffrageheight + 2)
		t.Error(err)
		t.False(found)
		t.Nil(rst)
		t.ErrorContains(err, "hihihi")
	})

	t.Run("found Suffrage", func() {
		rst, found, err := db.Suffrage(height)
		t.NoError(err)
		t.True(found)
		t.True(base.IsEqualState(st, rst))
	})

	t.Run("not found Suffrage", func() {
		rst, found, err := db.Suffrage(height + 1)
		t.NoError(err)
		t.False(found)
		t.Nil(rst)
	})

	t.Run("error Suffrage", func() {
		rst, found, err := db.Suffrage(height + 2)
		t.Error(err)
		t.False(found)
		t.Nil(rst)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testDefaultWithPermanent) TestLastSuffrage() {
	_, nodes := t.Locals(3)
	st, _ := t.SuffrageState(base.Height(33), base.Height(66), nodes)

	perm := &DummyPermanentDatabase{}

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	t.Run("found", func() {
		perm.lastSuffragef = func() (base.State, bool, error) {
			return st, true, nil
		}

		rst, found, err := db.LastSuffrage()
		t.NoError(err)
		t.True(found)
		base.IsEqualState(st, rst)
	})

	t.Run("not found", func() {
		perm.lastSuffragef = func() (base.State, bool, error) {
			return nil, false, nil
		}

		rst, found, err := db.LastSuffrage()
		t.NoError(err)
		t.False(found)
		t.Nil(rst)
	})

	t.Run("error", func() {
		perm.lastSuffragef = func() (base.State, bool, error) {
			return nil, false, errors.Errorf("hihihi")
		}

		rst, found, err := db.LastSuffrage()
		t.Error(err)
		t.False(found)
		t.Nil(rst)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testDefaultWithPermanent) TestLastNetworkPolicy() {
	perm := &DummyPermanentDatabase{}

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, nil)
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

func (t *testDefaultWithPermanent) TestState() {
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

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, nil)
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

func (t *testDefaultWithPermanent) TestExistsInStateOperation() {
	op := valuehash.RandomSHA256()
	errop := valuehash.RandomSHA256()

	perm := &DummyPermanentDatabase{
		existsInStateOperationf: func(operationFactHash util.Hash) (bool, error) {
			switch {
			case operationFactHash.Equal(op):
				return true, nil
			case operationFactHash.Equal(errop):
				return false, errors.Errorf("hihihi")
			default:
				return false, nil
			}
		},
	}

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, nil)
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

func (t *testDefaultWithPermanent) TestExistsKnownOperation() {
	op := valuehash.RandomSHA256()
	errop := valuehash.RandomSHA256()

	perm := &DummyPermanentDatabase{
		existsKnownOperationf: func(operationFactHash util.Hash) (bool, error) {
			switch {
			case operationFactHash.Equal(op):
				return true, nil
			case operationFactHash.Equal(errop):
				return false, errors.Errorf("hihihi")
			default:
				return false, nil
			}
		},
	}

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, nil)
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

func TestDefaultWithPermanent(t *testing.T) {
	suite.Run(t, new(testDefaultWithPermanent))
}

type testDefaultBlockWrite struct {
	isaac.BaseTestBallots
	BaseTestDatabase
}

func (t *testDefaultBlockWrite) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testDefaultBlockWrite) TestMerge() {
	height := base.Height(33)
	_, nodes := t.Locals(3)

	lmanifest := base.NewDummyManifest(height-1, valuehash.RandomSHA256())
	lmp := base.NewDummyBlockDataMap(lmanifest)
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

	lsufstt, lsufsv := t.SuffrageState(height-1, base.Height(66)-1, nodes)
	oldstts[3] = lsufstt

	perm := &DummyPermanentDatabase{
		mapf: func(base.Height) (base.BlockDataMap, bool, error) {
			return nil, false, nil
		},
		lastMapf: func() (base.BlockDataMap, bool, error) {
			return lmp, true, nil
		},
		suffragef: func(height base.Height) (base.State, bool, error) {
			switch {
			case height-1 <= lsufstt.Height():
				return lsufstt, true, nil
			default:
				return nil, false, nil
			}
		},
		suffrageByHeightf: func(suffrageHeight base.Height) (base.State, bool, error) {
			switch {
			case suffrageHeight == lsufsv.Height():
				return lsufstt, true, nil
			default:
				return nil, false, nil
			}
		},
		lastSuffragef: func() (base.State, bool, error) {
			return lsufstt, true, nil
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

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, func(h base.Height) (isaac.BlockWriteDatabase, error) {
		return NewLeveldbBlockWrite(h, filepath.Join(t.Root, h.String()), t.Encs, t.Enc)
	})
	t.NoError(err)

	wst, err := db.NewBlockWriteDatabase(height)
	t.NoError(err)
	defer wst.Close()

	sufstt, sufsv := t.SuffrageState(height, lsufsv.Height()+1, nodes)
	newstts = append(newstts, sufstt)

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())

	ops := make([]util.Hash, 3)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	mp := base.NewDummyBlockDataMap(manifest)

	t.NoError(wst.SetMap(mp))
	t.NoError(wst.SetStates(newstts))
	t.NoError(wst.SetOperations(ops))
	t.NoError(wst.Write())

	t.Run("check blockdatamap before merging", func() {
		rm, found, err := db.Map(height)
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
	})

	t.Run("check last blockdatamap before merging", func() {
		rm, found, err := db.LastMap()
		t.NoError(err)
		t.True(found)

		base.EqualBlockDataMap(t.Assert(), lmp, rm)
	})

	t.Run("check SuffrageByHeight before merging", func() {
		rst, found, err := db.SuffrageByHeight(sufsv.Height())
		t.NoError(err)
		t.False(found)
		t.Nil(rst)

		rst, found, err = db.SuffrageByHeight(lsufsv.Height())
		t.NoError(err)
		t.True(found)

		t.True(base.IsEqualState(lsufstt, rst))
	})

	t.Run("check Suffrage before merging", func() {
		rst, found, err := db.Suffrage(lsufstt.Height())
		t.NoError(err)
		t.True(found)
		t.NotNil(rst)

		t.True(base.IsEqualState(lsufstt, rst))
	})

	t.Run("check last suffrage before merging", func() {
		rst, found, err := db.LastSuffrage()
		t.NoError(err)
		t.True(found)

		t.True(base.IsEqualState(lsufstt, rst))
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

	t.Run("check blockdatamap", func() {
		rm, found, err := db.Map(height)
		t.NoError(err)
		t.True(found)

		base.EqualBlockDataMap(t.Assert(), mp, rm)
	})

	t.Run("check last blockdatamap", func() {
		rm, found, err := db.LastMap()
		t.NoError(err)
		t.True(found)

		base.EqualBlockDataMap(t.Assert(), mp, rm)
	})

	t.Run("check SuffrageByHeight", func() {
		rst, found, err := db.SuffrageByHeight(sufsv.Height())
		t.NoError(err)
		t.True(found)

		t.True(base.IsEqualState(sufstt, rst))
	})
	t.Run("check Suffrage", func() {
		rst, found, err := db.Suffrage(sufstt.Height())
		t.NoError(err)
		t.True(found)

		t.True(base.IsEqualState(sufstt, rst))
	})
	t.Run("check last suffrage", func() {
		rst, found, err := db.LastSuffrage()
		t.NoError(err)
		t.True(found)

		t.True(base.IsEqualState(sufstt, rst))
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

func (t *testDefaultBlockWrite) TestFindState() {
	baseheight := base.Height(33)

	manifest := base.NewDummyManifest(baseheight, valuehash.RandomSHA256())
	mp := base.NewDummyBlockDataMap(manifest)
	perm := &DummyPermanentDatabase{
		lastMapf: func() (base.BlockDataMap, bool, error) {
			return mp, true, nil
		},
		statef: func(string) (base.State, bool, error) {
			return nil, false, nil
		},
	}

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, func(height base.Height) (isaac.BlockWriteDatabase, error) {
		return NewLeveldbBlockWrite(height, filepath.Join(t.Root, height.String()), t.Encs, t.Enc)
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

		m := base.NewDummyBlockDataMap(manifest)

		t.NoError(wst.SetMap(m))
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

func (t *testDefaultBlockWrite) TestInvalidMerge() {
	height := base.Height(33)

	perm := &DummyPermanentDatabase{
		lastMapf: func() (base.BlockDataMap, bool, error) {
			m := base.NewDummyManifest(height-1, valuehash.RandomSHA256())
			return base.NewDummyBlockDataMap(m), true, nil
		},
	}

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, func(h base.Height) (isaac.BlockWriteDatabase, error) {
		return NewLeveldbBlockWrite(h, filepath.Join(t.Root, h.String()), t.Encs, t.Enc)
	})
	t.NoError(err)

	t.Run("wrong height", func() {
		manifest := base.NewDummyManifest(height+1, valuehash.RandomSHA256())

		wst, err := db.NewBlockWriteDatabase(manifest.Height())
		t.NoError(err)
		defer wst.Close()

		mp := base.NewDummyBlockDataMap(manifest)
		t.NoError(wst.SetMap(mp))
		t.NoError(wst.Write())

		err = db.MergeBlockWriteDatabase(wst)
		t.Error(err)
		t.ErrorContains(err, "failed to merge new TempDatabase")
		t.ErrorContains(err, "wrong height")
	})

	t.Run("not yet written", func() {
		m := base.NewDummyManifest(height, valuehash.RandomSHA256())

		wst, err := db.NewBlockWriteDatabase(m.Height())
		t.NoError(err)
		defer wst.Close()

		err = db.MergeBlockWriteDatabase(wst)
		t.Error(err)
		t.ErrorContains(err, "failed to merge new TempDatabase")
		t.ErrorContains(err, "empty blockdatamap")
	})
}

func (t *testDefaultBlockWrite) TestMergePermanent() {
	baseheight := base.Height(33)

	lmanifest := base.NewDummyManifest(baseheight, valuehash.RandomSHA256())
	lmp := base.NewDummyBlockDataMap(lmanifest)

	var mergedstt []base.State
	perm := &DummyPermanentDatabase{
		lastMapf: func() (base.BlockDataMap, bool, error) {
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

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, func(h base.Height) (isaac.BlockWriteDatabase, error) {
		return NewLeveldbBlockWrite(h, filepath.Join(t.Root, h.String()), t.Encs, t.Enc)
	})
	t.NoError(err)

	var removeds []isaac.TempDatabase

	for i := range make([]int, 10) {
		height := baseheight + base.Height(i+1)
		manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
		m := base.NewDummyBlockDataMap(manifest)

		sttss := t.States(height, 2)

		wst, err := db.NewBlockWriteDatabase(height)
		t.NoError(err)
		defer wst.Close()

		t.NoError(wst.SetMap(m))
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
		t.NoError(db.mergePermanent(context.TODO()))
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
	err = removed.Remove()
	t.Error(err)
	t.True(errors.Is(err, os.ErrNotExist))
	t.ErrorContains(err, "failed to remove")

	for i := range db.removed {
		r := db.removed[i]
		t.NotEqual(removed.Height(), r.Height())
	}
}

func TestDefaultBlockWrite(t *testing.T) {
	suite.Run(t, new(testDefaultBlockWrite))
}

type testDefaultLoad struct {
	isaac.BaseTestBallots
	BaseTestDatabase
}

func (t *testDefaultLoad) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()

	t.NoError(os.Mkdir(t.Root, 0o755))
}

func (t *testDefaultLoad) TestNewDirectory() {
	d, err := NewTempDirectory(t.Root, base.Height(33))
	t.NoError(err)
	t.True(strings.HasPrefix(d, t.Root))
}

func (t *testDefaultLoad) TestNewDirectoryWithExistings() {
	height := base.Height(33)
	for i := range make([]int, 33) {
		h := height
		switch {
		case i%2 == 0:
			h--
		default:
			h++
		}

		d, err := NewTempDirectory(t.Root, h)
		t.NoError(err)
		t.NoError(os.Mkdir(d, 0o755))
	}

	for range make([]int, 33) {
		d, err := NewTempDirectory(t.Root, height)
		t.NoError(err)
		t.NoError(os.Mkdir(d, 0o755))
	}

	d, err := NewTempDirectory(t.Root, height)
	t.NoError(err)
	t.NotEmpty(d)
	t.True(strings.HasSuffix(d, fmt.Sprintf("-%d", 33)))

	_, err = os.Stat(d)
	t.Error(err)
	t.True(errors.Is(err, os.ErrNotExist))
}

func (t *testDefaultLoad) TestNewDirectoryWithoutExistingDirectories() {
	height := base.Height(33)

	d, err := NewTempDirectory(t.Root, height)
	t.NoError(err)
	t.NotEmpty(d)
	t.Equal(d, newTempDirectoryName(t.Root, height, 0))
}

func (t *testDefaultLoad) TestNewDirectoryWithUnknownDirectories() {
	height := base.Height(33)
	for range make([]int, 33) {
		prefix := newTempDirectoryPrefixWithHeight(t.Root, height)
		d := prefix + util.UUID().String()
		t.NoError(os.Mkdir(d, 0o755))
	}

	d, err := NewTempDirectory(t.Root, height)
	t.NoError(err)
	t.NotEmpty(d)
	t.Equal(newTempDirectoryName(t.Root, height, 0), d)

	_, err = os.Stat(d)
	t.Error(err)
	t.True(errors.Is(err, os.ErrNotExist))
}

func (t *testDefaultLoad) TestLoadTempDatabases() {
	baseheight := base.Height(33)

	basemanifest := base.NewDummyManifest(baseheight, valuehash.RandomSHA256())
	basemp := base.NewDummyBlockDataMap(basemanifest)
	perm := &DummyPermanentDatabase{
		lastMapf: func() (base.BlockDataMap, bool, error) {
			return basemp, true, nil
		},
	}

	db, err := NewDefault(t.Root, t.Encs, t.Enc, perm, func(height base.Height) (isaac.BlockWriteDatabase, error) {
		f, _ := NewTempDirectory(t.Root, height)
		return NewLeveldbBlockWrite(height, f, t.Encs, t.Enc)
	})
	t.NoError(err)

	created := make([]base.BlockDataMap, 4)
	for i := range created {
		height := baseheight + base.Height(i+1)
		wst, err := db.NewBlockWriteDatabase(height)
		t.NoError(err)

		manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())

		mp := base.NewDummyBlockDataMap(manifest)
		t.NoError(wst.SetMap(mp))

		t.NoError(wst.Write())
	}

	// NOTE create again
	for i := range created {
		height := baseheight + base.Height(i+1)
		wst, err := db.NewBlockWriteDatabase(height)
		t.NoError(err)

		manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
		mp := base.NewDummyBlockDataMap(manifest)
		t.NoError(wst.SetMap(mp))

		t.NoError(wst.Write())

		t.NoError(db.MergeBlockWriteDatabase(wst))

		t.NoError(wst.Close())

		created[i] = mp
	}

	// NOTE create wrong leveldb directory
	for i := range created {
		f, _ := NewTempDirectory(t.Root, baseheight+base.Height(i+1))
		t.NoError(os.Mkdir(f, 0o755))
		_, err := os.Create(filepath.Join(f, util.UUID().String()))
		t.NoError(err)
	}

	temps, err := loadTemps(t.Root, basemanifest.Height(), t.Encs, t.Enc, false)
	t.NoError(err)
	t.Equal(len(created), len(temps))

	sort.Slice(created, func(i, j int) bool {
		return created[i].Manifest().Height() > created[j].Manifest().Height()
	})

	for i := range temps {
		temp := temps[i]
		expected := created[i]

		t.Equal(expected.Manifest().Height(), temp.Height())

		tm, err := temp.Map()
		t.NoError(err)

		base.EqualBlockDataMap(t.Assert(), expected, tm)
	}

	newdb, err := NewDefault(t.Root, t.Encs, t.Enc, perm, nil)
	t.NoError(err)

	actives := newdb.activeTemps()
	t.Equal(len(temps), len(actives))

	for i := range temps {
		temp := temps[i]
		expected := actives[i]

		t.Equal(expected.Height(), temp.Height())

		em, err := expected.Map()
		t.NoError(err)
		tm, err := temp.Map()
		t.NoError(err)

		base.EqualBlockDataMap(t.Assert(), em, tm)
	}
}

func TestDefaultLoad(t *testing.T) {
	suite.Run(t, new(testDefaultLoad))
}
