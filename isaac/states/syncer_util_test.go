package isaacstates

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testImportBlocks struct {
	isaacblock.BaseTestLocalBlockFS
	importRoot string
}

func (t *testImportBlocks) SetupTest() {
	t.BaseTestLocalBlockFS.SetupTest()

	t.importRoot, _ = os.MkdirTemp("", "mitum-test-import")
}

func (t *testImportBlocks) TearDownTest() {
	t.BaseTestLocalBlockFS.TearDownTest()

	_ = os.RemoveAll(t.importRoot)
}

func (t *testImportBlocks) prepare(from, to base.Height) *isaacdatabase.Center {
	st := leveldbstorage.NewMemStorage()
	db, err := isaacdatabase.NewCenter(st, t.Encs, t.Enc, t.NewLeveldbPermanentDatabase(),
		func(height base.Height) (isaac.BlockWriteDatabase, error) {
			return isaacdatabase.NewLeveldbBlockWrite(height, st, t.Encs, t.Enc), nil
		},
	)
	t.NoError(err)

	prev := valuehash.RandomSHA256()

	for i := from; i <= to; i++ {
		fs, _, ops, _, sts, _, _ := t.PrepareFS(base.NewPoint(i, 0), prev)

		m, err := fs.Save(context.Background())
		t.NoError(err)

		prev = m.Manifest().Hash()

		bw, err := db.NewBlockWriteDatabase(i)
		t.NoError(err)

		t.NoError(bw.SetBlockMap(m))

		opsh := make([]util.Hash, len(ops))
		for i := range ops {
			opsh[i] = ops[i].Hash()
		}
		t.NoError(bw.SetOperations(opsh))
		t.NoError(bw.SetStates(sts))

		t.NoError(bw.Write())

		t.NoError(db.MergeBlockWriteDatabase(bw))
	}

	t.NoError(db.MergeAllPermanent())

	t.WalkFS(t.Root, "prepared")

	return db
}

func (t *testImportBlocks) localfsReader(root string, height base.Height) (*isaacblock.LocalFSReader, error) {
	return isaacblock.NewLocalFSReaderFromHeight(root, height, t.Enc)
}

func (t *testImportBlocks) TestPrepare() {
	from, to := base.GenesisHeight, base.GenesisHeight+3

	db := t.prepare(from, to)

	t.Run("blockmaps", func() {
		for i := from; i <= to; i++ {
			m, found, err := db.BlockMap(i)
			t.NoError(err)
			t.True(found)
			t.Equal(i, m.Manifest().Height())

			reader, err := t.localfsReader(t.Root, i)
			t.NoError(err)

			rm, found, err := reader.BlockMap()
			t.NoError(err)
			t.True(found)
			t.Equal(i, rm.Manifest().Height())

			base.EqualBlockMap(t.Assert(), m, rm)
		}
	})

	t.Run("last blockmap", func() {
		m, found, err := db.LastBlockMap()
		t.NoError(err)
		t.True(found)
		t.Equal(to, m.Manifest().Height())

		reader, err := t.localfsReader(t.Root, to)
		t.NoError(err)

		rm, found, err := reader.BlockMap()
		t.NoError(err)
		t.True(found)
		t.Equal(to, rm.Manifest().Height())

		base.EqualBlockMap(t.Assert(), m, rm)
	})
}

func (t *testImportBlocks) loadStatesFromLocalFS(root string, height base.Height, item base.BlockMapItem) []base.State {
	reader, err := isaacblock.NewLocalFSReaderFromHeight(root, height, t.Enc)
	t.NoError(err)

	r, found, err := reader.UncompressedReader(base.BlockMapItemTypeStates)
	t.NoError(err)
	t.True(found)

	sts := make([]base.State, item.Num())

	t.NoError(isaacblock.LoadRawItems(r, t.Enc.Decode, func(i uint64, v interface{}) error {
		st, ok := v.(base.State)
		if !ok {
			return errors.Errorf("not State, %T", v)
		}

		if err := st.IsValid(nil); err != nil {
			return err
		}

		sts[i] = st

		return nil
	}))

	return sts
}

func (t *testImportBlocks) loadOperationsFromLocalFS(root string, height base.Height, item base.BlockMapItem) []base.Operation {
	reader, err := isaacblock.NewLocalFSReaderFromHeight(root, height, t.Enc)
	t.NoError(err)

	r, found, err := reader.UncompressedReader(base.BlockMapItemTypeOperations)
	t.NoError(err)
	t.True(found)

	ops := make([]base.Operation, item.Num())

	t.NoError(isaacblock.LoadRawItems(r, t.Enc.Decode, func(i uint64, v interface{}) error {
		op, ok := v.(base.Operation)
		if !ok {
			return errors.Errorf("not Operation, %T", v)
		}

		if err := op.IsValid(nil); err != nil {
			return err
		}

		ops[i] = op

		return nil
	}))

	return ops
}

func (t *testImportBlocks) loadVoteproofsFromLocalFS(root string, height base.Height, item base.BlockMapItem) []base.Voteproof {
	reader, err := isaacblock.NewLocalFSReaderFromHeight(root, height, t.Enc)
	t.NoError(err)

	r, found, err := reader.UncompressedReader(base.BlockMapItemTypeVoteproofs)
	t.NoError(err)
	t.True(found)

	vps := make([]base.Voteproof, item.Num())

	t.NoError(isaacblock.LoadRawItems(r, t.Enc.Decode, func(i uint64, v interface{}) error {
		vp, ok := v.(base.Voteproof)
		if !ok {
			return errors.Errorf("not Voteproof, %T", v)
		}

		if err := vp.IsValid(t.LocalParams.NetworkID()); err != nil {
			return err
		}

		vps[i] = vp

		return nil
	}))

	return vps
}

func (t *testImportBlocks) TestImport() {
	from, to := base.GenesisHeight, base.GenesisHeight+33

	fromdb := t.prepare(from, to)

	st := leveldbstorage.NewMemStorage()
	importdb, err := isaacdatabase.NewCenter(st, t.Encs, t.Enc, t.NewLeveldbPermanentDatabase(),
		func(height base.Height) (isaac.BlockWriteDatabase, error) {
			return isaacdatabase.NewLeveldbBlockWrite(height, st, t.Encs, t.Enc), nil
		},
	)
	t.NoError(err)

	lvps := NewLastVoteproofsHandler()

	t.NoError(ImportBlocks(
		context.Background(),
		from, to,
		3,
		func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			reader, err := isaacblock.NewLocalFSReaderFromHeight(t.Root, height, t.Enc)
			if err != nil {
				return nil, false, err
			}

			m, found, err := reader.BlockMap()

			return m, found, err
		},
		func(_ context.Context, height base.Height, item base.BlockMapItemType) (io.ReadCloser, func() error, bool, error) {
			reader, err := isaacblock.NewLocalFSReaderFromHeight(t.Root, height, t.Enc)
			if err != nil {
				return nil, nil, false, err
			}

			r, found, err := reader.Reader(item)

			return r, func() error { return nil }, found, err
		},
		func(m base.BlockMap) (isaac.BlockImporter, error) {
			bwdb, err := importdb.NewBlockWriteDatabase(m.Manifest().Height())
			if err != nil {
				return nil, err
			}

			return isaacblock.NewBlockImporter(
				t.importRoot,
				t.Encs,
				m,
				bwdb,
				func(context.Context) error {
					return importdb.MergeBlockWriteDatabase(bwdb)
				},
				t.LocalParams.NetworkID(),
			)
		},
		func(reader isaac.BlockReader) error {
			switch v, found, err := reader.Item(base.BlockMapItemTypeVoteproofs); {
			case err != nil:
				return err
			case !found:
				return errors.Errorf("voteproofs not found at last")
			default:
				vps := v.([]base.Voteproof) //nolint:forcetypeassert //...

				_ = lvps.Set(vps[0].(base.INITVoteproof))   //nolint:forcetypeassert //...
				_ = lvps.Set(vps[1].(base.ACCEPTVoteproof)) //nolint:forcetypeassert //...

				return nil
			}
		},
		func(context.Context) error {
			return importdb.MergeAllPermanent()
		},
	))

	t.WalkFS(t.importRoot, "imported")

	t.Run("compare last blockmap from db", func() {
		m, found, err := fromdb.LastBlockMap()
		t.NoError(err)
		t.True(found)

		rm, found, err := importdb.LastBlockMap()
		t.NoError(err)
		t.True(found)

		base.EqualBlockMap(t.Assert(), m, rm)
	})

	t.Run("compare blockmaps from db", func() {
		for i := from; i <= to; i++ {
			m, found, err := fromdb.BlockMap(i)
			t.NoError(err)
			t.True(found)
			t.Equal(i, m.Manifest().Height())

			rm, found, err := importdb.BlockMap(i)
			t.NoError(err)
			t.True(found)

			base.EqualBlockMap(t.Assert(), m, rm)
		}
	})

	t.Run("compare blockmap items from db", func() {
		m, found, err := fromdb.LastBlockMap()
		t.NoError(err)
		t.True(found)

		rm, found, err := importdb.LastBlockMap()
		t.NoError(err)
		t.True(found)

		m.Items(func(item base.BlockMapItem) bool {
			ritem, found := rm.Item(item.Type())
			t.True(found)

			base.EqualBlockMapItem(t.Assert(), item, ritem)

			return true
		})
	})

	t.Run("compare states from db", func() {
		m, found, err := fromdb.BlockMap(to)
		t.NoError(err)
		t.True(found)

		item, found := m.Item(base.BlockMapItemTypeStates)
		t.True(found)

		origsts := t.loadStatesFromLocalFS(t.Root, to, item)

		for i := range origsts {
			st := origsts[i]

			ost, found, err := fromdb.State(st.Key())
			t.NoError(err)
			t.True(found)

			ist, found, err := importdb.State(st.Key())
			t.NoError(err)
			t.True(found)

			t.True(base.IsEqualState(ost, ist))
		}
	})

	t.Run("compare states from localfs", func() {
		for i := from; i <= to; i++ {
			m, found, err := fromdb.BlockMap(i)
			t.NoError(err)
			t.True(found)

			item, found := m.Item(base.BlockMapItemTypeStates)
			t.True(found)

			origsts := t.loadStatesFromLocalFS(t.Root, i, item)
			importedsts := t.loadStatesFromLocalFS(t.importRoot, i, item)

			t.Equal(len(origsts), len(importedsts))

			for i := range origsts {
				t.True(base.IsEqualState(origsts[i], importedsts[i]))
			}
		}
	})

	t.Run("compare operations from localfs", func() {
		for i := from; i <= to; i++ {
			m, found, err := fromdb.BlockMap(i)
			t.NoError(err)
			t.True(found)

			item, found := m.Item(base.BlockMapItemTypeOperations)
			t.True(found)

			origops := t.loadOperationsFromLocalFS(t.Root, i, item)
			importedops := t.loadOperationsFromLocalFS(t.importRoot, i, item)

			t.Equal(len(origops), len(importedops))

			for i := range origops {
				base.EqualOperation(t.Assert(), origops[i], importedops[i])
			}
		}
	})

	t.Run("check operations of state from db", func() {
		for i := from; i <= to; i++ {
			m, found, err := fromdb.BlockMap(i)
			t.NoError(err)
			t.True(found)

			item, found := m.Item(base.BlockMapItemTypeStates)
			t.True(found)

			origsts := t.loadStatesFromLocalFS(t.Root, i, item)

			for i := range origsts {
				st := origsts[i]
				ops := st.Operations()

				for j := range ops {
					found, err = fromdb.ExistsInStateOperation(ops[j])
					t.NoError(err)
					t.True(found)

					found, err = importdb.ExistsInStateOperation(ops[j])
					t.NoError(err)
					t.True(found)
				}
			}
		}
	})

	t.Run("check operations from db", func() {
		for i := from; i <= to; i++ {
			m, found, err := fromdb.BlockMap(i)
			t.NoError(err)
			t.True(found)

			item, found := m.Item(base.BlockMapItemTypeOperations)
			t.True(found)

			origops := t.loadOperationsFromLocalFS(t.Root, i, item)

			for i := range origops {
				op := origops[i]

				found, err = fromdb.ExistsKnownOperation(op.Hash())
				t.NoError(err)
				t.True(found)

				found, err = importdb.ExistsKnownOperation(op.Hash())
				t.NoError(err)
				t.True(found)
			}
		}
	})

	t.Run("compare voteproofs from localfs", func() {
		for i := from; i <= to; i++ {
			m, found, err := fromdb.BlockMap(i)
			t.NoError(err)
			t.True(found)

			item, found := m.Item(base.BlockMapItemTypeVoteproofs)
			t.True(found)

			origvps := t.loadVoteproofsFromLocalFS(t.Root, i, item)
			importvps := t.loadVoteproofsFromLocalFS(t.importRoot, i, item)

			t.Equal(len(origvps), len(importvps))

			for j := range origvps {
				base.EqualVoteproof(t.Assert(), origvps[j], importvps[j])
			}
		}
	})
}

func TestImportBlocks(t *testing.T) {
	suite.Run(t, new(testImportBlocks))
}
