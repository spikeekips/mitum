package isaacblock

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
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

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	m, found, err := reader.BlockMap()
	t.NoError(err)
	t.True(found)

	return m
}

func (t *testBlockImporter) copyBlockItemFiles(root, temp string, height base.Height) {
	p := filepath.Join(BlockItemFilesPath(root, height))
	s, err := os.Open(p)
	t.NoError(err)

	p = filepath.Join(filepath.Dir(temp), base.BlockItemFilesName(height))
	d, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	t.NoError(err)

	io.Copy(d, s)

	s.Close()
	d.Close()
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

	t.copyBlockItemFiles(im.localfs.root, im.localfs.temp, m.Manifest().Height())

	reader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
	t.NoError(err)

	t.Run("map in localfs", func() {
		rm, found, err := reader.BlockMap()
		t.NoError(err)
		t.True(found)

		base.EqualBlockMap(t.Assert(), m, rm)
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

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)
	im.batchlimit = 2

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	i, found, err := reader.Item(base.BlockItemProposal)
	t.NoError(err)
	t.True(found)

	pr := i.(base.ProposalSignFact)

	r, found, err := reader.Reader(base.BlockItemProposal)
	t.NoError(err)
	t.True(found)

	var checksum string
	{
		cr, found, err := reader.ChecksumReader(base.BlockItemProposal)
		t.NoError(err)
		t.True(found)

		checksum = cr.Checksum()
	}

	dr, err := util.NewCompressedReader(r, "gz", nil)
	t.NoError(err)

	t.NoError(im.WriteItem(base.BlockItemProposal, dr))

	t.copyBlockItemFiles(im.localfs.root, im.localfs.temp, m.Manifest().Height())

	t.Run("in localfs", func() {
		tempreader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
		t.NoError(err)

		i, found, err := tempreader.Item(base.BlockItemProposal)
		t.NoError(err)
		t.True(found)

		rpr, ok := i.(base.ProposalSignFact)
		t.True(ok)

		base.EqualProposalSignFact(t.Assert(), pr, rpr)

		cr, found, err := tempreader.ChecksumReader(base.BlockItemProposal)
		t.NoError(err)
		t.True(found)

		t.Equal(checksum, cr.Checksum())
	})
}

func (t *testBlockImporter) TestWriteOperations() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)
	im.batchlimit = 2

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	i, found, err := reader.Item(base.BlockItemOperations)
	t.NoError(err)
	t.True(found)

	ops := i.([]base.Operation)

	r, found, err := reader.Reader(base.BlockItemOperations)
	t.NoError(err)
	t.True(found)

	var checksum string
	{
		cr, found, err := reader.ChecksumReader(base.BlockItemOperations)
		t.NoError(err)
		t.True(found)

		checksum = cr.Checksum()
	}

	dr, err := util.NewCompressedReader(r, "gz", nil)
	t.NoError(err)

	t.NoError(im.WriteItem(base.BlockItemOperations, dr))

	t.copyBlockItemFiles(im.localfs.root, im.localfs.temp, m.Manifest().Height())

	t.Run("in localfs", func() {
		tempreader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
		t.NoError(err)

		i, found, err := tempreader.Item(base.BlockItemOperations)
		t.NoError(err)
		t.True(found)

		rops, ok := i.([]base.Operation)
		t.True(ok)

		t.Equal(len(ops), len(rops))
		for i := range ops {
			base.EqualOperation(t.Assert(), ops[i], rops[i])
		}

		cr, found, err := tempreader.ChecksumReader(base.BlockItemOperations)
		t.NoError(err)
		t.True(found)

		t.Equal(checksum, cr.Checksum())
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

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)
	im.batchlimit = 2

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	i, found, err := reader.Item(base.BlockItemOperationsTree)
	t.NoError(err)
	t.True(found)

	tr := i.(fixedtree.Tree)

	r, found, err := reader.Reader(base.BlockItemOperationsTree)
	t.NoError(err)
	t.True(found)

	var checksum string
	{
		cr, found, err := reader.ChecksumReader(base.BlockItemOperationsTree)
		t.NoError(err)
		t.True(found)

		checksum = cr.Checksum()
	}

	dr, err := util.NewCompressedReader(r, "gz", nil)
	t.NoError(err)

	t.NoError(im.WriteItem(base.BlockItemOperationsTree, dr))

	t.copyBlockItemFiles(im.localfs.root, im.localfs.temp, m.Manifest().Height())

	t.Run("in localfs", func() {
		tempreader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
		t.NoError(err)

		i, found, err := tempreader.Item(base.BlockItemOperationsTree)
		t.NoError(err)
		t.True(found)

		rtr, ok := i.(fixedtree.Tree)
		t.True(ok)

		cr, found, err := tempreader.ChecksumReader(base.BlockItemOperationsTree)
		t.NoError(err)
		t.True(found)

		t.Equal(checksum, cr.Checksum())

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

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)
	im.batchlimit = 2

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	i, found, err := reader.Item(base.BlockItemVoteproofs)
	t.NoError(err)
	t.True(found)

	vps := i.([2]base.Voteproof)

	r, found, err := reader.Reader(base.BlockItemVoteproofs)
	t.NoError(err)
	t.True(found)

	var checksum string
	{
		cr, found, err := reader.ChecksumReader(base.BlockItemVoteproofs)
		t.NoError(err)
		t.True(found)

		checksum = cr.Checksum()
	}

	dr, err := util.NewCompressedReader(r, "", nil)
	t.NoError(err)

	t.NoError(im.WriteItem(base.BlockItemVoteproofs, dr))

	t.copyBlockItemFiles(im.localfs.root, im.localfs.temp, m.Manifest().Height())

	t.Run("in localfs", func() {
		tempreader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
		t.NoError(err)

		i, found, err := tempreader.Item(base.BlockItemVoteproofs)
		t.NoError(err)
		t.True(found)

		rvps, ok := i.([2]base.Voteproof)
		t.True(ok)

		cr, found, err := tempreader.ChecksumReader(base.BlockItemVoteproofs)
		t.NoError(err)
		t.True(found)

		t.Equal(checksum, cr.Checksum())
		t.Equal(len(vps), len(rvps))

		base.EqualVoteproof(t.Assert(), vps[0], rvps[0])
		base.EqualVoteproof(t.Assert(), vps[1], rvps[1])
	})
}

func (t *testBlockImporter) TestWriteStates() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)
	im.batchlimit = 2

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	i, found, err := reader.Item(base.BlockItemStates)
	t.NoError(err)
	t.True(found)

	sts := i.([]base.State)

	r, found, err := reader.Reader(base.BlockItemStates)
	t.NoError(err)
	t.True(found)

	var checksum string
	{
		cr, found, err := reader.ChecksumReader(base.BlockItemStates)
		t.NoError(err)
		t.True(found)

		checksum = cr.Checksum()
	}

	dr, err := util.NewCompressedReader(r, "gz", nil)
	t.NoError(err)

	t.NoError(im.WriteItem(base.BlockItemStates, dr))

	t.copyBlockItemFiles(im.localfs.root, im.localfs.temp, m.Manifest().Height())

	t.Run("in localfs", func() {
		tempreader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
		t.NoError(err)

		i, found, err := tempreader.Item(base.BlockItemStates)
		t.NoError(err)
		t.True(found)

		rsts, ok := i.([]base.State)
		t.True(ok)

		t.Equal(len(sts), len(rsts))
		for i := range sts {
			t.True(base.IsEqualState(sts[i], rsts[i]))
		}

		cr, found, err := tempreader.ChecksumReader(base.BlockItemStates)
		t.NoError(err)
		t.True(found)

		t.Equal(checksum, cr.Checksum())
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

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)
	im.batchlimit = 2

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	i, found, err := reader.Item(base.BlockItemStatesTree)
	t.NoError(err)
	t.True(found)

	tr := i.(fixedtree.Tree)

	r, found, err := reader.Reader(base.BlockItemStatesTree)
	t.NoError(err)
	t.True(found)

	var checksum string
	{
		cr, found, err := reader.ChecksumReader(base.BlockItemStatesTree)
		t.NoError(err)
		t.True(found)

		checksum = cr.Checksum()
	}

	dr, err := util.NewCompressedReader(r, "gz", nil)
	t.NoError(err)

	t.NoError(im.WriteItem(base.BlockItemStatesTree, dr))

	t.copyBlockItemFiles(im.localfs.root, im.localfs.temp, m.Manifest().Height())

	t.Run("in localfs", func() {
		tempreader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
		t.NoError(err)

		i, found, err := tempreader.Item(base.BlockItemStatesTree)
		t.NoError(err)
		t.True(found)

		rtr, ok := i.(fixedtree.Tree)
		t.True(ok)

		cr, found, err := tempreader.ChecksumReader(base.BlockItemStatesTree)
		t.NoError(err)
		t.True(found)

		t.Equal(checksum, cr.Checksum())

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

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	bwdb := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.DeepClose()

	newroot := filepath.Join(t.Root, "save")

	im, err := NewBlockImporter(newroot, t.Encs, m, bwdb, func(context.Context) error { return nil }, t.LocalParams.NetworkID())
	t.NoError(err)

	m.Items(func(item base.BlockMapItem) bool {
		r, found, err := reader.Reader(item.Type())
		t.NoError(err)
		t.True(found)

		compressformat := ""
		if isCompressedBlockMapItemType(item.Type()) {
			compressformat = "gz"
		}

		dr, err := util.NewCompressedReader(r, compressformat, nil)
		t.NoError(err)

		t.NoError(im.WriteItem(item.Type(), dr), "failed: %q", item.Type())

		return true
	})

	t.Run("no files in new directory", func() {
		_, err := NewLocalFSReaderFromHeight(newroot, point.Height(), t.Enc)
		t.Error(err)
		t.True(errors.Is(err, util.ErrNotFound))
	})

	_, err = im.Save(context.Background())
	t.NoError(err)

	t.PrintFS(newroot)

	t.Run("check files in new directory", func() {
		newreader, err := NewLocalFSReaderFromHeight(newroot, point.Height(), t.Enc)
		t.NoError(err)

		m.Items(func(item base.BlockMapItem) bool {
			r, found, err := newreader.ChecksumReader(item.Type())
			t.NoError(err)
			t.True(found)

			t.Equal(item.Checksum(), r.Checksum())

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

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

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
		r, found, err := reader.Reader(item.Type())
		t.NoError(err)
		t.True(found)

		compressformat := ""
		if isCompressedBlockMapItemType(item.Type()) {
			compressformat = "gz"
		}

		dr, err := util.NewCompressedReader(r, compressformat, nil)
		t.NoError(err)

		t.NoError(im.WriteItem(item.Type(), dr), "failed: %q", item.Type())

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
