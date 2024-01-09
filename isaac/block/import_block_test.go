package isaacblock

import (
	"context"
	"crypto/sha256"
	"io"
	"os"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testImportBlocks struct {
	BaseTestLocalBlockFS
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
			return isaacdatabase.NewLeveldbBlockWrite(height, st, t.Encs, t.Enc, 0), nil
		},
		nil)
	t.NoError(err)

	prev := valuehash.RandomSHA256()

	for i := from; i <= to; i++ {
		fs, _, ops, _, sts, _, _ := t.PrepareFS(base.NewPoint(i, 0), prev, nil)

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

func (t *testImportBlocks) TestPrepare() {
	from, to := base.GenesisHeight, base.GenesisHeight+3

	db := t.prepare(from, to)

	t.Run("blockmaps", func() {
		for i := from; i <= to; i++ {
			m, found, err := db.BlockMap(i)
			t.NoError(err)
			t.True(found)
			t.Equal(i, m.Manifest().Height())

			rm, found, err := isaac.BlockItemReadersDecode[base.BlockMap](t.Readers.Item, i, base.BlockItemMap, nil)
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

		rm, found, err := isaac.BlockItemReadersDecode[base.BlockMap](t.Readers.Item, to, base.BlockItemMap, nil)
		t.NoError(err)
		t.True(found)

		t.Equal(to, rm.Manifest().Height())

		base.EqualBlockMap(t.Assert(), m, rm)
	})
}

func (t *testImportBlocks) loadStatesFromLocalFS(root string, height base.Height, item base.BlockMapItem) []base.State {
	cw := util.NewHashChecksumWriter(sha256.New())

	count, sts, found, err := isaac.BlockItemReadersDecodeItems[base.State](t.NewReaders(root).Item, height, base.BlockItemStates,
		func(total uint64, index uint64, st base.State) error {
			return st.IsValid(nil)
		},
		func(ir isaac.BlockItemReader) error {
			_, err := ir.Reader().Tee(nil, cw)

			return err
		},
	)
	t.NoError(err)
	t.True(found)
	t.Equal(count, uint64(len(sts)))
	t.Equal(item.Checksum(), cw.Checksum())

	return sts
}

func (t *testImportBlocks) loadOperationsFromLocalFS(root string, height base.Height, item base.BlockMapItem) []base.Operation {
	cw := util.NewHashChecksumWriter(sha256.New())

	count, ops, found, err := isaac.BlockItemReadersDecodeItems[base.Operation](t.NewReaders(root).Item, height, base.BlockItemOperations,
		func(total uint64, index uint64, op base.Operation) error {
			return op.IsValid(nil)
		},
		func(ir isaac.BlockItemReader) error {
			_, err := ir.Reader().Tee(nil, cw)

			return err
		},
	)
	t.NoError(err)
	t.True(found)
	t.Equal(count, uint64(len(ops)))
	t.Equal(item.Checksum(), cw.Checksum())

	return ops
}

func (t *testImportBlocks) loadVoteproofsFromLocalFS(root string, height base.Height, item base.BlockMapItem) [2]base.Voteproof {
	cw := util.NewHashChecksumWriter(sha256.New())

	vps, found, err := isaac.BlockItemReadersDecode[[2]base.Voteproof](t.NewReaders(root).Item, height, base.BlockItemVoteproofs,
		func(ir isaac.BlockItemReader) error {
			_, err := ir.Reader().Tee(nil, cw)

			return err
		},
	)
	t.NoError(err)
	t.True(found)

	t.NoError(vps[0].IsValid(t.LocalParams.NetworkID()))
	t.NoError(vps[1].IsValid(t.LocalParams.NetworkID()))

	t.Equal(item.Checksum(), cw.Checksum())

	return vps
}

func (t *testImportBlocks) TestImport() {
	from, to := base.GenesisHeight, base.GenesisHeight+33

	fromdb := t.prepare(from, to)

	st := leveldbstorage.NewMemStorage()
	importdb, err := isaacdatabase.NewCenter(st, t.Encs, t.Enc, t.NewLeveldbPermanentDatabase(),
		func(height base.Height) (isaac.BlockWriteDatabase, error) {
			return isaacdatabase.NewLeveldbBlockWrite(height, st, t.Encs, t.Enc, 0), nil
		},
		nil)
	t.NoError(err)

	lvps := isaac.NewLastVoteproofsHandler()

	t.NoError(ImportBlocks(
		context.Background(),
		from, to,
		3,
		t.Readers,
		func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			rm, found, err := isaac.BlockItemReadersDecode[base.BlockMap](t.Readers.Item, height, base.BlockItemMap, nil)
			if err != nil {
				return nil, false, err
			}

			return rm, found, nil
		},
		func(_ context.Context, height base.Height, item base.BlockItemType, f func(io.Reader, bool, string) error) error {
			switch _, found, err := t.Readers.Item(height, item, func(ir isaac.BlockItemReader) error {
				return f(ir.Reader(), true, ir.Reader().Format)
			}); {
			case err != nil:
				return err
			case !found:
				return f(nil, false, "")
			default:
				return nil
			}
		},
		func(m base.BlockMap) (isaac.BlockImporter, error) {
			bwdb, err := importdb.NewBlockWriteDatabase(m.Manifest().Height())
			if err != nil {
				return nil, err
			}

			return NewBlockImporter(
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
		func(vps [2]base.Voteproof, found bool) error {
			if !found {
				return util.ErrNotFound.Errorf("last voteproof")
			}

			_ = lvps.Set(vps[0].(base.INITVoteproof))   //nolint:forcetypeassert //...
			_ = lvps.Set(vps[1].(base.ACCEPTVoteproof)) //nolint:forcetypeassert //...

			return nil
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

	t.Run("compare block items from db", func() {
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

		item, found := m.Item(base.BlockItemStates)
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

	t.Run("compare states from local fs", func() {
		for i := from; i <= to; i++ {
			m, found, err := fromdb.BlockMap(i)
			t.NoError(err)
			t.True(found)

			item, found := m.Item(base.BlockItemStates)
			t.True(found)

			origsts := t.loadStatesFromLocalFS(t.Root, i, item)
			importedsts := t.loadStatesFromLocalFS(t.importRoot, i, item)

			t.Equal(len(origsts), len(importedsts))

			for i := range origsts {
				t.True(base.IsEqualState(origsts[i], importedsts[i]))
			}
		}
	})

	t.Run("compare operations from local fs", func() {
		for i := from; i <= to; i++ {
			m, found, err := fromdb.BlockMap(i)
			t.NoError(err)
			t.True(found)

			item, found := m.Item(base.BlockItemOperations)
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

			item, found := m.Item(base.BlockItemStates)
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

			item, found := m.Item(base.BlockItemOperations)
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

	t.Run("compare voteproofs from local fs", func() {
		for i := from; i <= to; i++ {
			m, found, err := fromdb.BlockMap(i)
			t.NoError(err)
			t.True(found)

			item, found := m.Item(base.BlockItemVoteproofs)
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
