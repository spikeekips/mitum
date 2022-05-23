package isaacblock

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testBlockImporter struct {
	testBaseLocalBlockFS
}

func (t *testBlockImporter) prepare(point base.Point) base.BlockMap {
	fs, _, _, _, _, _, _ := t.preparefs(point)
	_, err := fs.Save(context.Background())
	t.NoError(err)

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	m, found, err := reader.Map()
	t.NoError(err)
	t.True(found)

	return m
}

func (t *testBlockImporter) TestNew() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	bwdb := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.Close()

	permdb := t.NewMemLeveldbPermanentDatabase()
	defer permdb.Close()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, permdb)
	t.NoError(err)

	_ = (interface{})(im).(isaac.BlockImporter)
}

func (t *testBlockImporter) TestWriteMap() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	bwdb := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.Close()

	permdb := t.NewMemLeveldbPermanentDatabase()
	defer permdb.Close()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, permdb)
	t.NoError(err)

	reader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
	t.NoError(err)

	t.Run("map in localfs", func() {
		rm, found, err := reader.Map()
		t.NoError(err)
		t.True(found)

		base.EqualBlockMap(t.Assert(), m, rm)
	})

	t.Run("map in bwdb", func() {
		tempdb, err := bwdb.TempDatabase()
		t.NoError(err)

		rm, err := tempdb.Map()
		t.NoError(err)

		base.EqualBlockMap(t.Assert(), m, rm)
	})
}

func (t *testBlockImporter) TestWriteProposal() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	bwdb := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.Close()

	permdb := t.NewMemLeveldbPermanentDatabase()
	defer permdb.Close()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, permdb)
	t.NoError(err)
	im.batchlimit = 2

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	i, found, err := reader.Item(base.BlockMapItemTypeProposal)
	t.NoError(err)
	t.True(found)

	pr := i.(base.ProposalSignedFact)

	r, found, err := reader.Reader(base.BlockMapItemTypeProposal)
	t.NoError(err)
	t.True(found)

	var checksum string
	{
		cr, found, err := reader.ChecksumReader(base.BlockMapItemTypeProposal)
		t.NoError(err)
		t.True(found)

		checksum = cr.Checksum()
	}

	t.NoError(im.WriteItem(base.BlockMapItemTypeProposal, r))

	t.Run("in localfs", func() {
		tempreader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
		t.NoError(err)

		i, found, err := tempreader.Item(base.BlockMapItemTypeProposal)
		t.NoError(err)
		t.True(found)

		rpr, ok := i.(base.ProposalSignedFact)
		t.True(ok)

		base.EqualProposalSignedFact(t.Assert(), pr, rpr)

		cr, found, err := tempreader.ChecksumReader(base.BlockMapItemTypeProposal)
		t.NoError(err)
		t.True(found)

		t.Equal(checksum, cr.Checksum())
	})
}

func (t *testBlockImporter) TestWriteOperations() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	bwdb := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.Close()

	permdb := t.NewMemLeveldbPermanentDatabase()
	defer permdb.Close()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, permdb)
	t.NoError(err)
	im.batchlimit = 2

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	i, found, err := reader.Item(base.BlockMapItemTypeOperations)
	t.NoError(err)
	t.True(found)

	ops := i.([]base.Operation)

	r, found, err := reader.Reader(base.BlockMapItemTypeOperations)
	t.NoError(err)
	t.True(found)

	var checksum string
	{
		cr, found, err := reader.ChecksumReader(base.BlockMapItemTypeOperations)
		t.NoError(err)
		t.True(found)

		checksum = cr.Checksum()
	}

	t.NoError(im.WriteItem(base.BlockMapItemTypeOperations, r))

	t.Run("in localfs", func() {
		tempreader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
		t.NoError(err)

		i, found, err := tempreader.Item(base.BlockMapItemTypeOperations)
		t.NoError(err)
		t.True(found)

		rops, ok := i.([]base.Operation)
		t.True(ok)

		t.Equal(len(ops), len(rops))
		for i := range ops {
			base.EqualOperation(t.Assert(), ops[i], rops[i])
		}

		cr, found, err := tempreader.ChecksumReader(base.BlockMapItemTypeOperations)
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

	bwdb := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.Close()

	permdb := t.NewMemLeveldbPermanentDatabase()
	defer permdb.Close()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, permdb)
	t.NoError(err)
	im.batchlimit = 2

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	i, found, err := reader.Item(base.BlockMapItemTypeOperationsTree)
	t.NoError(err)
	t.True(found)

	tr := i.(fixedtree.Tree)

	r, found, err := reader.Reader(base.BlockMapItemTypeOperationsTree)
	t.NoError(err)
	t.True(found)

	var checksum string
	{
		cr, found, err := reader.ChecksumReader(base.BlockMapItemTypeOperationsTree)
		t.NoError(err)
		t.True(found)

		checksum = cr.Checksum()
	}

	t.NoError(im.WriteItem(base.BlockMapItemTypeOperationsTree, r))

	t.Run("in localfs", func() {
		tempreader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
		t.NoError(err)

		i, found, err := tempreader.Item(base.BlockMapItemTypeOperationsTree)
		t.NoError(err)
		t.True(found)

		rtr, ok := i.(fixedtree.Tree)
		t.True(ok)

		cr, found, err := tempreader.ChecksumReader(base.BlockMapItemTypeOperationsTree)
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

	bwdb := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.Close()

	permdb := t.NewMemLeveldbPermanentDatabase()
	defer permdb.Close()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, permdb)
	t.NoError(err)
	im.batchlimit = 2

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	i, found, err := reader.Item(base.BlockMapItemTypeVoteproofs)
	t.NoError(err)
	t.True(found)

	vps := i.([]base.Voteproof)

	r, found, err := reader.Reader(base.BlockMapItemTypeVoteproofs)
	t.NoError(err)
	t.True(found)

	var checksum string
	{
		cr, found, err := reader.ChecksumReader(base.BlockMapItemTypeVoteproofs)
		t.NoError(err)
		t.True(found)

		checksum = cr.Checksum()
	}

	t.NoError(im.WriteItem(base.BlockMapItemTypeVoteproofs, r))

	t.Run("in localfs", func() {
		tempreader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
		t.NoError(err)

		i, found, err := tempreader.Item(base.BlockMapItemTypeVoteproofs)
		t.NoError(err)
		t.True(found)

		rvps, ok := i.([]base.Voteproof)
		t.True(ok)

		cr, found, err := tempreader.ChecksumReader(base.BlockMapItemTypeVoteproofs)
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

	bwdb := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.Close()

	permdb := t.NewMemLeveldbPermanentDatabase()
	defer permdb.Close()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, permdb)
	t.NoError(err)
	im.batchlimit = 2

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	i, found, err := reader.Item(base.BlockMapItemTypeStates)
	t.NoError(err)
	t.True(found)

	sts := i.([]base.State)

	r, found, err := reader.Reader(base.BlockMapItemTypeStates)
	t.NoError(err)
	t.True(found)

	var checksum string
	{
		cr, found, err := reader.ChecksumReader(base.BlockMapItemTypeStates)
		t.NoError(err)
		t.True(found)

		checksum = cr.Checksum()
	}

	t.NoError(im.WriteItem(base.BlockMapItemTypeStates, r))

	t.Run("in localfs", func() {
		tempreader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
		t.NoError(err)

		i, found, err := tempreader.Item(base.BlockMapItemTypeStates)
		t.NoError(err)
		t.True(found)

		rsts, ok := i.([]base.State)
		t.True(ok)

		t.Equal(len(sts), len(rsts))
		for i := range sts {
			t.True(base.IsEqualState(sts[i], rsts[i]))
		}

		cr, found, err := tempreader.ChecksumReader(base.BlockMapItemTypeStates)
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

	bwdb := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.Close()

	permdb := t.NewMemLeveldbPermanentDatabase()
	defer permdb.Close()

	im, err := NewBlockImporter(t.Root, t.Encs, m, bwdb, permdb)
	t.NoError(err)
	im.batchlimit = 2

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	i, found, err := reader.Item(base.BlockMapItemTypeStatesTree)
	t.NoError(err)
	t.True(found)

	tr := i.(fixedtree.Tree)

	r, found, err := reader.Reader(base.BlockMapItemTypeStatesTree)
	t.NoError(err)
	t.True(found)

	var checksum string
	{
		cr, found, err := reader.ChecksumReader(base.BlockMapItemTypeStatesTree)
		t.NoError(err)
		t.True(found)

		checksum = cr.Checksum()
	}

	t.NoError(im.WriteItem(base.BlockMapItemTypeStatesTree, r))

	t.Run("in localfs", func() {
		tempreader, err := NewLocalFSReader(im.localfs.temp, t.Enc)
		t.NoError(err)

		i, found, err := tempreader.Item(base.BlockMapItemTypeStatesTree)
		t.NoError(err)
		t.True(found)

		rtr, ok := i.(fixedtree.Tree)
		t.True(ok)

		cr, found, err := tempreader.ChecksumReader(base.BlockMapItemTypeStatesTree)
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

	bwdb := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.Close()

	newroot := filepath.Join(t.Root, "save")
	permdb := t.NewMemLeveldbPermanentDatabase()
	defer permdb.Close()

	im, err := NewBlockImporter(newroot, t.Encs, m, bwdb, permdb)
	t.NoError(err)

	m.Items(func(item base.BlockMapItem) bool {
		r, found, err := reader.Reader(item.Type())
		t.NoError(err)
		t.True(found)

		t.NoError(im.WriteItem(item.Type(), r), "failed: %q", item.Type())

		return true
	})

	t.Run("no files in new directory", func() {
		_, err := NewLocalFSReaderFromHeight(newroot, point.Height(), t.Enc)
		t.Error(err)
		t.True(errors.Is(err, os.ErrNotExist))
		t.ErrorContains(err, "invalid root directory")
	})

	t.NoError(im.Save(context.Background()))

	t.walkDirectory(newroot)

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

	t.Run("merge", func() {
		t.NoError(im.Merge(context.Background()))

		i, found, err := reader.Item(base.BlockMapItemTypeOperations)
		t.NoError(err)
		t.True(found)

		ops := i.([]base.Operation)

		i, found, err = reader.Item(base.BlockMapItemTypeStates)
		t.NoError(err)
		t.True(found)

		sts := i.([]base.State)

		for i := range ops {
			found, err := permdb.ExistsKnownOperation(ops[i].Hash())
			t.NoError(err)
			t.True(found)
		}

		for i := range sts {
			st, found, err := permdb.State(sts[i].Key())
			t.NoError(err)
			t.True(found)
			t.NotNil(st)
		}
	})
}

func (t *testBlockImporter) TestCancelImport() {
	point := base.RawPoint(33, 44)
	m := t.prepare(point)

	reader, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	newroot := filepath.Join(t.Root, "save")

	t.Run("cancel before save", func() {
		bwdb := t.NewMemLeveldbBlockWriteDatabase(point.Height())
		defer bwdb.Close()

		permdb := t.NewMemLeveldbPermanentDatabase()
		defer permdb.Close()

		im, err := NewBlockImporter(newroot, t.Encs, m, bwdb, permdb)
		t.NoError(err)

		t.NoError(im.CancelImport(context.Background()))
	})

	bwdb := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer bwdb.Close()

	permdb := t.NewMemLeveldbPermanentDatabase()
	defer permdb.Close()

	im, err := NewBlockImporter(newroot, t.Encs, m, bwdb, permdb)
	t.NoError(err)

	m.Items(func(item base.BlockMapItem) bool {
		r, found, err := reader.Reader(item.Type())
		t.NoError(err)
		t.True(found)

		t.NoError(im.WriteItem(item.Type(), r), "failed: %q", item.Type())

		return true
	})

	t.Run("cancel after save", func() {
		t.walkDirectory(newroot, "temp files")

		t.NoError(im.Save(context.Background()))

		t.walkDirectory(newroot, "after saved")

		t.NoError(im.CancelImport(context.Background()))

		t.T().Log("check directory; it should be empty")
		t.walkDirectory(newroot)

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
