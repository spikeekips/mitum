package isaac

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type DummyPermanentDatabase struct {
	closef             func() error
	manifestf          func(height base.Height) (base.Manifest, bool, error)
	lastManifestf      func() (base.Manifest, bool, error)
	suffragef          func(height base.Height) (base.State, bool, error)
	suffrageByHeightf  func(suffrageHeight base.Height) (base.State, bool, error)
	lastSuffragef      func() (base.State, bool, error)
	statef             func(key string) (base.State, bool, error)
	existsOperationf   func(operationFactHash util.Hash) (bool, error)
	mergeTempDatabasef func(TempDatabase) error
}

func (db *DummyPermanentDatabase) Close() error {
	return db.closef()
}

func (db *DummyPermanentDatabase) Manifest(height base.Height) (base.Manifest, bool, error) {
	return db.manifestf(height)
}

func (db *DummyPermanentDatabase) LastManifest() (base.Manifest, bool, error) {
	if db.lastManifestf == nil {
		return nil, false, nil
	}

	return db.lastManifestf()
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

func (db *DummyPermanentDatabase) ExistsOperation(operationFactHash util.Hash) (bool, error) {
	return db.existsOperationf(operationFactHash)
}

func (db *DummyPermanentDatabase) MergeTempDatabase(_ context.Context, temp TempDatabase) error {
	return db.mergeTempDatabasef(temp)
}

type testDefaultDatabaseWithPermanent struct {
	baseTestHandler
	baseTestDatabase
}

func (t *testDefaultDatabaseWithPermanent) SetupTest() {
	t.baseTestHandler.SetupTest()
	t.baseTestDatabase.SetupTest()
}

func (t *testDefaultDatabaseWithPermanent) TestManifest() {
	m := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())

	perm := &DummyPermanentDatabase{
		manifestf: func(height base.Height) (base.Manifest, bool, error) {
			switch {
			case height == m.Height():
				return m, true, nil
			case height == m.Height()+2:
				return nil, false, errors.Errorf("hihihi")
			default:
				return nil, false, nil
			}
		},
	}

	db, err := NewDefaultDatabase(t.root, t.encs, t.enc, perm, nil)
	t.NoError(err)

	t.Run("found", func() {
		rm, found, err := db.Manifest(m.Height())
		t.NoError(err)
		t.True(found)
		base.EqualManifest(t.Assert(), m, rm)
	})

	t.Run("not found", func() {
		rm, found, err := db.Manifest(m.Height() + 1)
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
	})

	t.Run("error", func() {
		rm, found, err := db.Manifest(m.Height() + 2)
		t.Error(err)
		t.False(found)
		t.Nil(rm)
		t.Contains(err.Error(), "hihihi")
	})
}

func (t *testDefaultDatabaseWithPermanent) TestLastManifest() {
	m := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())

	perm := &DummyPermanentDatabase{}

	db, err := NewDefaultDatabase(t.root, t.encs, t.enc, perm, nil)
	t.NoError(err)

	t.Run("found", func() {
		perm.lastManifestf = func() (base.Manifest, bool, error) {
			return m, true, nil
		}

		rm, found, err := db.LastManifest()
		t.NoError(err)
		t.True(found)
		base.EqualManifest(t.Assert(), m, rm)
	})

	t.Run("not found", func() {
		perm.lastManifestf = func() (base.Manifest, bool, error) {
			return nil, false, nil
		}

		rm, found, err := db.LastManifest()
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
	})

	t.Run("error", func() {
		perm.lastManifestf = func() (base.Manifest, bool, error) {
			return nil, false, errors.Errorf("hihihi")
		}

		rm, found, err := db.LastManifest()
		t.Error(err)
		t.False(found)
		t.Nil(rm)
		t.Contains(err.Error(), "hihihi")
	})
}

func (t *testDefaultDatabaseWithPermanent) TestSuffrage() {
	_, nodes := t.locals(3)
	height := base.Height(33)
	suffrageheight := base.Height(66)
	st, sv := t.suffrageState(height, suffrageheight, nodes)

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

	db, err := NewDefaultDatabase(t.root, t.encs, t.enc, perm, nil)
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
		t.Contains(err.Error(), "hihihi")
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
		t.Contains(err.Error(), "hihihi")
	})
}

func (t *testDefaultDatabaseWithPermanent) TestLastSuffrage() {
	_, nodes := t.locals(3)
	st, _ := t.suffrageState(base.Height(33), base.Height(66), nodes)

	perm := &DummyPermanentDatabase{}

	db, err := NewDefaultDatabase(t.root, t.encs, t.enc, perm, nil)
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
		t.Contains(err.Error(), "hihihi")
	})
}

func (t *testDefaultDatabaseWithPermanent) TestState() {
	st := t.states(base.Height(33), 1)[0]

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

	db, err := NewDefaultDatabase(t.root, t.encs, t.enc, perm, nil)
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
		t.Contains(err.Error(), "hihihi")
	})
}

func (t *testDefaultDatabaseWithPermanent) TestExistsOperation() {
	op := valuehash.RandomSHA256()
	errop := valuehash.RandomSHA256()

	perm := &DummyPermanentDatabase{
		existsOperationf: func(operationFactHash util.Hash) (bool, error) {
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

	db, err := NewDefaultDatabase(t.root, t.encs, t.enc, perm, nil)
	t.NoError(err)

	t.Run("found", func() {
		found, err := db.ExistsOperation(op)
		t.NoError(err)
		t.True(found)
	})

	t.Run("not found", func() {
		found, err := db.ExistsOperation(valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
	})

	t.Run("error", func() {
		found, err := db.ExistsOperation(errop)
		t.Error(err)
		t.False(found)
		t.Contains(err.Error(), "hihihi")
	})
}

func TestDefaultDatabaseWithPermanent(t *testing.T) {
	suite.Run(t, new(testDefaultDatabaseWithPermanent))
}

type testDefaultDatabaseBlockWrite struct {
	baseTestHandler
	baseTestDatabase
}

func (t *testDefaultDatabaseBlockWrite) SetupTest() {
	t.baseTestHandler.SetupTest()
	t.baseTestDatabase.SetupTest()
}

func (t *testDefaultDatabaseBlockWrite) TestMerge() {
	height := base.Height(33)
	_, nodes := t.locals(3)

	lm := base.NewDummyManifest(height-1, valuehash.RandomSHA256())
	newstts := t.states(height, 3)
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

	lsufstt, lsufsv := t.suffrageState(height-1, base.Height(66)-1, nodes)
	oldstts[3] = lsufstt

	perm := &DummyPermanentDatabase{
		manifestf: func(base.Height) (base.Manifest, bool, error) {
			return nil, false, nil
		},
		lastManifestf: func() (base.Manifest, bool, error) {
			return lm, true, nil
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
		existsOperationf: func(util.Hash) (bool, error) {
			return false, nil
		},
	}

	db, err := NewDefaultDatabase(t.root, t.encs, t.enc, perm, func(h base.Height) (BlockWriteDatabase, error) {
		return NewLeveldbBlockWriteDatabase(h, filepath.Join(t.root, h.String()), t.encs, t.enc)
	})
	t.NoError(err)

	wst, err := db.NewBlockWriteDatabase(height)
	t.NoError(err)
	defer wst.Close()

	sufstt, sufsv := t.suffrageState(height, lsufsv.Height()+1, nodes)
	newstts = append(newstts, sufstt)

	m := base.NewDummyManifest(height, valuehash.RandomSHA256())

	ops := make([]util.Hash, 3)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	t.NoError(wst.SetManifest(m))
	t.NoError(wst.SetStates(newstts))
	t.NoError(wst.SetOperations(ops))
	t.NoError(wst.Write())

	t.Run("check manifest before merging", func() {
		rm, found, err := db.Manifest(height)
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
	})

	t.Run("check last manifest before merging", func() {
		rm, found, err := db.LastManifest()
		t.NoError(err)
		t.True(found)

		base.EqualManifest(t.Assert(), lm, rm)
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

			found, err := db.ExistsOperation(op)
			t.NoError(err)
			t.False(found)
		}
	})

	t.NoError(db.MergeBlockWriteDatabase(wst))

	t.Run("check manifest", func() {
		rm, found, err := db.Manifest(height)
		t.NoError(err)
		t.True(found)

		base.EqualManifest(t.Assert(), m, rm)
	})

	t.Run("check last manifest", func() {
		rm, found, err := db.LastManifest()
		t.NoError(err)
		t.True(found)

		base.EqualManifest(t.Assert(), m, rm)
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

			found, err := db.ExistsOperation(op)
			t.NoError(err)
			t.True(found)
		}
	})
}

func (t *testDefaultDatabaseBlockWrite) TestFindState() {
	baseheight := base.Height(33)

	m := base.NewDummyManifest(baseheight, valuehash.RandomSHA256())
	perm := &DummyPermanentDatabase{
		lastManifestf: func() (base.Manifest, bool, error) {
			return m, true, nil
		},
		statef: func(string) (base.State, bool, error) {
			return nil, false, nil
		},
	}

	db, err := NewDefaultDatabase(t.root, t.encs, t.enc, perm, func(height base.Height) (BlockWriteDatabase, error) {
		return NewLeveldbBlockWriteDatabase(height, filepath.Join(t.root, height.String()), t.encs, t.enc)
	})
	t.NoError(err)

	stts := t.states(baseheight, 10)

	var laststts []base.State
	for i := range make([]int, 4) {
		height := baseheight + base.Height(i+1)
		wst, err := db.NewBlockWriteDatabase(height)
		t.NoError(err)
		defer wst.Close()

		m := base.NewDummyManifest(height, valuehash.RandomSHA256())

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

		t.NoError(wst.SetManifest(m))
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

func (t *testDefaultDatabaseBlockWrite) TestInvalidMerge() {
	height := base.Height(33)

	perm := &DummyPermanentDatabase{
		lastManifestf: func() (base.Manifest, bool, error) {
			return base.NewDummyManifest(height-1, valuehash.RandomSHA256()), true, nil
		},
	}

	db, err := NewDefaultDatabase(t.root, t.encs, t.enc, perm, func(h base.Height) (BlockWriteDatabase, error) {
		return NewLeveldbBlockWriteDatabase(h, filepath.Join(t.root, h.String()), t.encs, t.enc)
	})
	t.NoError(err)

	t.Run("wrong height", func() {
		m := base.NewDummyManifest(height+1, valuehash.RandomSHA256())

		wst, err := db.NewBlockWriteDatabase(m.Height())
		t.NoError(err)
		defer wst.Close()

		t.NoError(wst.SetManifest(m))
		t.NoError(wst.Write())

		err = db.MergeBlockWriteDatabase(wst)
		t.Error(err)
		t.Contains(err.Error(), "failed to merge new TempDatabase")
		t.Contains(err.Error(), "wrong height")
	})

	t.Run("not yet written", func() {
		m := base.NewDummyManifest(height, valuehash.RandomSHA256())

		wst, err := db.NewBlockWriteDatabase(m.Height())
		t.NoError(err)
		defer wst.Close()

		err = db.MergeBlockWriteDatabase(wst)
		t.Error(err)
		t.Contains(err.Error(), "failed to merge new TempDatabase")
		t.Contains(err.Error(), "empty manifest")
	})
}

func (t *testDefaultDatabaseBlockWrite) TestMergePermanent() {
	baseheight := base.Height(33)

	lm := base.NewDummyManifest(baseheight, valuehash.RandomSHA256())

	var mergedstt []base.State
	perm := &DummyPermanentDatabase{
		lastManifestf: func() (base.Manifest, bool, error) {
			return lm, true, nil
		},

		mergeTempDatabasef: func(temp TempDatabase) error {
			ltemp := temp.(*TempLeveldbDatabase)
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

	db, err := NewDefaultDatabase(t.root, t.encs, t.enc, perm, func(h base.Height) (BlockWriteDatabase, error) {
		return NewLeveldbBlockWriteDatabase(h, filepath.Join(t.root, h.String()), t.encs, t.enc)
	})
	t.NoError(err)

	var removeds []TempDatabase

	for i := range make([]int, 10) {
		height := baseheight + base.Height(i+1)
		m := base.NewDummyManifest(height, valuehash.RandomSHA256())

		sttss := t.states(height, 2)

		wst, err := db.NewBlockWriteDatabase(height)
		t.NoError(err)
		defer wst.Close()

		t.NoError(wst.SetManifest(m))
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
		t.NoError(db.cleanRemoved())
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
	t.Contains(err.Error(), "failed to remove")

	for i := range db.removed {
		r := db.removed[i]
		t.NotEqual(removed.Height(), r.Height())
	}
}

func TestDefaultDatabaseBlockWrite(t *testing.T) {
	suite.Run(t, new(testDefaultDatabaseBlockWrite))
}

type testDefaultDatabaseLoad struct {
	baseTestHandler
	baseTestDatabase
}

func (t *testDefaultDatabaseLoad) SetupTest() {
	t.baseTestHandler.SetupTest()
	t.baseTestDatabase.SetupTest()

	t.NoError(os.Mkdir(t.root, 0o755))
}

func (t *testDefaultDatabaseLoad) TestNewDirectory() {
	d, err := newTempDatabaseDirectory(t.root, base.Height(33))
	t.NoError(err)
	t.True(strings.HasPrefix(d, t.root))
}

func (t *testDefaultDatabaseLoad) TestNewDirectoryWithExistings() {
	height := base.Height(33)
	for i := range make([]int, 33) {
		h := height
		switch {
		case i%2 == 0:
			h--
		default:
			h++
		}

		d, err := newTempDatabaseDirectory(t.root, h)
		t.NoError(err)
		t.NoError(os.Mkdir(d, 0o755))
	}

	for range make([]int, 33) {
		d, err := newTempDatabaseDirectory(t.root, height)
		t.NoError(err)
		t.NoError(os.Mkdir(d, 0o755))
	}

	d, err := newTempDatabaseDirectory(t.root, height)
	t.NoError(err)
	t.NotEmpty(d)
	t.True(strings.HasSuffix(d, fmt.Sprintf("-%d", 33)))

	_, err = os.Stat(d)
	t.Error(err)
	t.True(errors.Is(err, os.ErrNotExist))
}

func (t *testDefaultDatabaseLoad) TestNewDirectoryWithoutExistingDirectories() {
	height := base.Height(33)

	d, err := newTempDatabaseDirectory(t.root, height)
	t.NoError(err)
	t.NotEmpty(d)
	t.Equal(d, newTempDatabaseDirectoryName(t.root, height, 0))
}

func (t *testDefaultDatabaseLoad) TestNewDirectoryWithUnknownDirectories() {
	height := base.Height(33)
	for range make([]int, 33) {
		prefix := newTempDatabaseDirectoryPrefixWithHeight(t.root, height)
		d := prefix + util.UUID().String()
		t.NoError(os.Mkdir(d, 0o755))
	}

	d, err := newTempDatabaseDirectory(t.root, height)
	t.NoError(err)
	t.NotEmpty(d)
	t.Equal(newTempDatabaseDirectoryName(t.root, height, 0), d)

	_, err = os.Stat(d)
	t.Error(err)
	t.True(errors.Is(err, os.ErrNotExist))
}

func (t *testDefaultDatabaseLoad) TestLoadTempDatabases() {
	baseheight := base.Height(33)

	basemanifest := base.NewDummyManifest(baseheight, valuehash.RandomSHA256())
	perm := &DummyPermanentDatabase{
		lastManifestf: func() (base.Manifest, bool, error) {
			return basemanifest, true, nil
		},
	}

	db, err := NewDefaultDatabase(t.root, t.encs, t.enc, perm, func(height base.Height) (BlockWriteDatabase, error) {
		f, _ := newTempDatabaseDirectory(t.root, height)
		return NewLeveldbBlockWriteDatabase(height, f, t.encs, t.enc)
	})
	t.NoError(err)

	created := make([]base.Manifest, 4)
	for i := range created {
		height := baseheight + base.Height(i+1)
		wst, err := db.NewBlockWriteDatabase(height)
		t.NoError(err)

		m := base.NewDummyManifest(height, valuehash.RandomSHA256())

		t.NoError(wst.SetManifest(m))
		t.NoError(wst.Write())
	}

	// NOTE create again
	for i := range created {
		height := baseheight + base.Height(i+1)
		wst, err := db.NewBlockWriteDatabase(height)
		t.NoError(err)

		m := base.NewDummyManifest(height, valuehash.RandomSHA256())

		t.NoError(wst.SetManifest(m))
		t.NoError(wst.Write())

		t.NoError(db.MergeBlockWriteDatabase(wst))

		t.NoError(wst.Close())

		created[i] = m
	}

	// NOTE create wrong leveldb directory
	for i := range created {
		f, _ := newTempDatabaseDirectory(t.root, baseheight+base.Height(i+1))
		t.NoError(os.Mkdir(f, 0o755))
		_, err := os.Create(filepath.Join(f, util.UUID().String()))
		t.NoError(err)
	}

	temps, err := loadTempDatabases(t.root, basemanifest.Height(), t.encs, t.enc, false)
	t.NoError(err)
	t.Equal(len(created), len(temps))

	for i := range temps {
		temp := temps[i]
		expected := created[i]

		t.Equal(expected.Height(), temp.Height())

		tm, err := temp.Manifest()
		t.NoError(err)

		base.EqualManifest(t.Assert(), expected, tm)
	}

	newdb, err := NewDefaultDatabase(t.root, t.encs, t.enc, perm, nil)
	t.NoError(err)

	actives := newdb.activeTemps()
	t.Equal(len(temps), len(actives))

	for i := range temps {
		temp := temps[i]
		expected := actives[i]

		t.Equal(expected.Height(), temp.Height())

		em, err := expected.Manifest()
		t.NoError(err)
		tm, err := temp.Manifest()
		t.NoError(err)

		base.EqualManifest(t.Assert(), em, tm)
	}
}

func TestDefaultDatabaseLoad(t *testing.T) {
	suite.Run(t, new(testDefaultDatabaseLoad))
}
