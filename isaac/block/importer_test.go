package isaacblock

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testBlockImporter struct {
	BaseTestLocalBlockFS
}

func (t *testBlockImporter) prepare(point base.Point) base.BlockMap {
	fs, _, _, _, _, _, _ := t.PrepareFS(point, nil, nil)
	_, err := fs.Save(context.Background())
	t.NoError(err)

	m, found, err := ReadersDecode[base.BlockMap](t.Readers, point.Height(), base.BlockItemMap, nil)
	t.True(found)
	t.NoError(err)

	return m
}

func (t *testBlockImporter) openFile(root string, it base.BlockItemType) *os.File {
	n, err := DefaultBlockFileName(it, t.Enc.Hint().Type())
	t.NoError(err)

	f, err := os.Open(filepath.Join(root, n))
	t.NoError(err)

	return f
}

func (t *testBlockImporter) TestNew() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, nil, t.LocalParams.NetworkID())
	t.NoError(err)

	_ = (interface{})(im).(isaac.BlockImporter)
}

func (t *testBlockImporter) TestWriteMap() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)

	t.Run("map in localfs", func() {
		f := t.openFile(im.localfs.temp, base.BlockItemMap)
		defer f.Close()

		rbm, found, err := ReadersDecodeFromReader[base.BlockMap](t.Readers, base.BlockItemMap, f, "", nil)
		t.NoError(err)
		t.True(found)

		base.EqualBlockMap(t.Assert(), m, rbm)
	})

	t.Run("map in bwdb", func() {
		tempdb, err := bwdb.TempDatabase()
		t.NoError(err)

		rm, _, err := tempdb.LastBlockMap()
		t.NoError(err)

		base.EqualBlockMap(t.Assert(), m, rm)
	})
}

func (t *testBlockImporter) TestWriteProposal() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	cw := util.NewHashChecksumWriter(sha256.New())

	pr, found, err := ReadersDecode[base.ProposalSignFact](t.Readers, point.Height(), base.BlockItemProposal, func(ir isaac.BlockItemReader) error {
		_, err := ir.Reader().Tee(nil, cw)

		return err
	})
	t.True(found)
	t.NoError(err)

	checksum := cw.Checksum()

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)
	im.batchlimit = 2

	found, err = t.Readers.Item(point.Height(), base.BlockItemProposal, func(ir isaac.BlockItemReader) error {
		return im.WriteItem(base.BlockItemProposal, ir)
	})
	t.True(found)
	t.NoError(err)

	t.Run("in localfs", func() {
		f := t.openFile(im.localfs.temp, base.BlockItemProposal)
		defer f.Close()

		cw := util.NewHashChecksumWriter(sha256.New())
		rpr, found, err := ReadersDecodeFromReader[base.ProposalSignFact](t.Readers, base.BlockItemProposal, f, "gz", func(ir isaac.BlockItemReader) error {
			_, err := ir.Reader().Tee(nil, cw)

			return err
		})
		t.True(found)
		t.NoError(err)

		base.EqualProposalSignFact(t.Assert(), pr, rpr)

		t.Equal(checksum, cw.Checksum())
	})
}

func (t *testBlockImporter) TestWriteOperations() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	cw := util.NewHashChecksumWriter(sha256.New())
	_, ops, found, err := ReadersDecodeItems[base.Operation](t.Readers, point.Height(), base.BlockItemOperations,
		nil,
		func(ir isaac.BlockItemReader) error {
			_, err := ir.Reader().Tee(nil, cw)

			return err
		},
	)
	t.True(found)
	t.NoError(err)

	checksum := cw.Checksum()

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)
	im.batchlimit = 2

	found, err = t.Readers.Item(point.Height(), base.BlockItemOperations, func(ir isaac.BlockItemReader) error {
		return im.WriteItem(base.BlockItemOperations, ir)
	})
	t.True(found)
	t.NoError(err)

	t.Run("in localfs", func() {
		f := t.openFile(im.localfs.temp, base.BlockItemOperations)
		defer f.Close()

		cw := util.NewHashChecksumWriter(sha256.New())
		_, rops, found, err := ReadersDecodeItemsFromReader[base.Operation](t.Readers, base.BlockItemOperations, f, "gz",
			nil,
			func(ir isaac.BlockItemReader) error {
				_, err := ir.Reader().Tee(nil, cw)

				return err
			},
		)
		t.True(found)
		t.NoError(err)

		rchecksum := cw.Checksum()

		t.Equal(len(ops), len(rops))
		for i := range ops {
			base.EqualOperation(t.Assert(), ops[i], rops[i])
		}

		t.Equal(checksum, rchecksum)
	})

	t.Run("in bwdb", func() {
		tempdb, err := bwdb.TempDatabase()
		t.NoError(err)

		for i := range ops {
			found, err := tempdb.ExistsKnownOperation(ops[i].Hash())
			t.NoError(err)
			t.True(found)
		}
	})
}

func (t *testBlockImporter) TestWriteOperationsTree() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	cw := util.NewHashChecksumWriter(sha256.New())

	tr, found, err := ReadersDecode[fixedtree.Tree](t.Readers, point.Height(), base.BlockItemOperationsTree, func(ir isaac.BlockItemReader) error {
		_, err := ir.Reader().Tee(nil, cw)

		return err
	})
	t.True(found)
	t.NoError(err)

	checksum := cw.Checksum()

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)
	im.batchlimit = 2

	found, err = t.Readers.Item(point.Height(), base.BlockItemOperationsTree, func(ir isaac.BlockItemReader) error {
		return im.WriteItem(base.BlockItemOperationsTree, ir)
	})
	t.True(found)
	t.NoError(err)

	t.Run("in localfs", func() {
		f := t.openFile(im.localfs.temp, base.BlockItemOperationsTree)
		defer f.Close()

		var rtr fixedtree.Tree
		cw := util.NewHashChecksumWriter(sha256.New())

		rtr, found, err := ReadersDecodeFromReader[fixedtree.Tree](t.Readers, base.BlockItemOperationsTree, f, "gz", func(ir isaac.BlockItemReader) error {
			_, err := ir.Reader().Tee(nil, cw)

			return err
		})
		t.True(found)
		t.NoError(err)

		t.Equal(checksum, cw.Checksum())

		_ = tr.Traverse(func(index uint64, a fixedtree.Node) (bool, error) {
			b := rtr.Node(index)

			t.True(a.Equal(b))

			return true, nil
		})
	})
}

func (t *testBlockImporter) TestWriteVoteproofs() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	var vps [2]base.Voteproof
	cw := util.NewHashChecksumWriter(sha256.New())

	vps, found, err := ReadersDecode[[2]base.Voteproof](t.Readers, point.Height(), base.BlockItemVoteproofs, func(ir isaac.BlockItemReader) error {
		_, err := ir.Reader().Tee(nil, cw)

		return err
	})
	t.True(found)
	t.NoError(err)

	checksum := cw.Checksum()

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)
	im.batchlimit = 2

	found, err = t.Readers.Item(point.Height(), base.BlockItemVoteproofs, func(ir isaac.BlockItemReader) error {
		return im.WriteItem(base.BlockItemVoteproofs, ir)
	})
	t.True(found)
	t.NoError(err)

	t.Run("in localfs", func() {
		f := t.openFile(im.localfs.temp, base.BlockItemVoteproofs)
		defer f.Close()

		cw := util.NewHashChecksumWriter(sha256.New())

		rvps, found, err := ReadersDecodeFromReader[[2]base.Voteproof](t.Readers, base.BlockItemVoteproofs, f, "", func(ir isaac.BlockItemReader) error {
			_, err := ir.Reader().Tee(nil, cw)

			return err
		})
		t.True(found)
		t.NoError(err)

		t.Equal(checksum, cw.Checksum())
		t.Equal(len(vps), len(rvps))

		base.EqualVoteproof(t.Assert(), vps[0], rvps[0])
		base.EqualVoteproof(t.Assert(), vps[1], rvps[1])
	})
}

func (t *testBlockImporter) TestWriteStates() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	cw := util.NewHashChecksumWriter(sha256.New())

	_, sts, found, err := ReadersDecodeItems[base.State](t.Readers, point.Height(), base.BlockItemStates,
		nil,
		func(ir isaac.BlockItemReader) error {
			_, err := ir.Reader().Tee(nil, cw)

			return err
		},
	)
	t.True(found)
	t.NoError(err)

	checksum := cw.Checksum()

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)
	im.batchlimit = 2

	found, err = t.Readers.Item(point.Height(), base.BlockItemStates, func(ir isaac.BlockItemReader) error {
		return im.WriteItem(base.BlockItemStates, ir)
	})
	t.True(found)
	t.NoError(err)

	t.Run("in localfs", func() {
		f := t.openFile(im.localfs.temp, base.BlockItemStates)
		defer f.Close()

		cw := util.NewHashChecksumWriter(sha256.New())

		_, rsts, found, err := ReadersDecodeItemsFromReader[base.State](t.Readers, base.BlockItemStates, f, "gz",
			nil,
			func(ir isaac.BlockItemReader) error {
				_, err := ir.Reader().Tee(nil, cw)

				return err
			},
		)
		t.True(found)
		t.NoError(err)
		t.Equal(checksum, cw.Checksum())

		t.Equal(len(sts), len(rsts))
		for i := range sts {
			t.True(base.IsEqualState(sts[i], rsts[i]))
		}
	})

	t.Run("in bwdb", func() {
		tempdb, err := bwdb.TempDatabase()
		t.NoError(err)

		for i := range sts {
			ops := sts[i].Operations()

			for j := range ops {
				found, err := tempdb.ExistsInStateOperation(ops[j])
				t.NoError(err)
				t.True(found)
			}
		}
	})
}

func (t *testBlockImporter) TestWriteStatesTree() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	cw := util.NewHashChecksumWriter(sha256.New())
	tr, found, err := ReadersDecode[fixedtree.Tree](t.Readers, point.Height(), base.BlockItemStatesTree, func(ir isaac.BlockItemReader) error {
		_, err := ir.Reader().Tee(nil, cw)

		return err
	})
	t.True(found)
	t.NoError(err)

	checksum := cw.Checksum()

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)
	im.batchlimit = 2

	found, err = t.Readers.Item(point.Height(), base.BlockItemStatesTree, func(ir isaac.BlockItemReader) error {
		return im.WriteItem(base.BlockItemStatesTree, ir)
	})
	t.True(found)
	t.NoError(err)

	t.Run("in localfs", func() {
		f := t.openFile(im.localfs.temp, base.BlockItemStatesTree)
		defer f.Close()

		cw := util.NewHashChecksumWriter(sha256.New())

		rtr, found, err := ReadersDecodeFromReader[fixedtree.Tree](t.Readers, base.BlockItemStatesTree, f, "gz", func(ir isaac.BlockItemReader) error {
			_, err := ir.Reader().Tee(nil, cw)

			return err
		})
		t.True(found)
		t.NoError(err)
		t.Equal(checksum, cw.Checksum())

		_ = tr.Traverse(func(index uint64, a fixedtree.Node) (bool, error) {
			b := rtr.Node(index)

			t.True(a.Equal(b))

			return true, nil
		})
	})
}

func (t *testBlockImporter) TestSave() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	newroot := filepath.Join(t.Root, "save")

	im, err := NewBlockImporter(newroot, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)

	m.Items(func(item base.BlockMapItem) bool {
		found, err := t.Readers.Item(point.Height(), item.Type(), func(ir isaac.BlockItemReader) error {
			return im.WriteItem(item.Type(), ir)
		})
		t.True(found)
		t.NoError(err, "failed: %q", item.Type())

		return true
	})

	t.Run("no files in new directory", func() {
		m.Items(func(item base.BlockMapItem) bool {
			found, err := t.NewReaders(newroot).Item(point.Height(), item.Type(), func(ir isaac.BlockItemReader) error { return nil })
			t.NoError(err)
			t.False(found)

			return true
		})
	})

	_, err = im.Save(context.Background())
	t.NoError(err)

	t.PrintFS(newroot)

	t.Run("check files in new directory", func() {
		newreaders := t.NewReaders(newroot)

		m.Items(func(item base.BlockMapItem) bool {
			cw := util.NewHashChecksumWriter(sha256.New())

			found, err := newreaders.Item(point.Height(), item.Type(), func(ir isaac.BlockItemReader) error {
				if _, err := ir.Reader().Tee(nil, cw); err != nil {
					return err
				}

				_, err := ir.Decode()

				return err
			})
			t.NoError(err)
			t.True(found)

			t.Equal(item.Checksum(), cw.Checksum())

			return true
		})
	})

	t.Run("check files file", func() {
		fpath := filepath.Join(filepath.Dir(newroot), filepath.Dir(HeightDirectory(point.Height())), base.BlockItemFilesName(point.Height()))
		f, err := os.Open(fpath)
		t.NoError(err)

		var bfiles base.BlockItemFiles
		t.NoError(encoder.DecodeReader(t.Enc, f, &bfiles))

		check := func(t base.BlockItemType) bool {
			_, found := bfiles.Item(t)

			return found
		}

		t.True(check(base.BlockItemMap))
		t.True(check(base.BlockItemProposal))
		t.True(check(base.BlockItemVoteproofs))
		t.True(check(base.BlockItemOperations))
		t.True(check(base.BlockItemOperationsTree))
		t.True(check(base.BlockItemStatesTree))
		t.True(check(base.BlockItemStatesTree))
	})
}

func (t *testBlockImporter) TestCancelImport() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)
	t.PrintFS(t.Root)

	newroot := filepath.Join(t.Root, "save")

	t.Run("cancel before save", func() {
		bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
		defer bwdb.DeepClose()

		im, err := NewBlockImporter(newroot, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
		t.NoError(err)

		t.NoError(im.CancelImport(context.Background()))
	})

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(newroot, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)

	m.Items(func(item base.BlockMapItem) bool {
		found, err := t.Readers.Item(point.Height(), item.Type(), func(ir isaac.BlockItemReader) error {
			return im.WriteItem(item.Type(), ir)
		})
		t.True(found, "not found, %q", item.Type())
		t.NoError(err, "failed, %q", item.Type())

		return true
	})

	t.Run("cancel after save", func() {
		t.PrintFS(newroot, "temp files")

		_, err = im.Save(context.Background())
		t.NoError(err)

		t.PrintFS(newroot, "after saved")

		t.NoError(im.CancelImport(context.Background()))

		t.T().Log("check directory; it should be empty")
		t.PrintFS(newroot)

		_, err = os.Stat(im.localfs.temp)
		t.True(os.IsNotExist(err))
	})

	t.Run("cancel again", func() {
		t.NoError(im.CancelImport(context.Background()))
		t.NoError(im.CancelImport(context.Background()))
	})
}

func TestBlockImporter(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testBlockImporter))
}
