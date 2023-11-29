package isaacblock

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"golang.org/x/sync/semaphore"
)

type DummyBlockFSWriter struct {
	setProposalf        func(context.Context, base.ProposalSignFact) error
	setOperationf       func(context.Context, uint64, uint64, base.Operation) error
	setOperationsTreef  func(context.Context, *fixedtree.Writer) error
	setStatef           func(context.Context, uint64, uint64, base.State) error
	setStatesTreef      func(context.Context, *fixedtree.Writer) (fixedtree.Tree, error)
	setManifestf        func(context.Context, base.Manifest) error
	setINITVoteprooff   func(context.Context, base.INITVoteproof) error
	setACCEPTVoteprooff func(context.Context, base.ACCEPTVoteproof) error
	savef               func(context.Context) (base.BlockMap, error)
	cancelf             func() error
}

func (w *DummyBlockFSWriter) SetProposal(ctx context.Context, pr base.ProposalSignFact) error {
	if w.setProposalf != nil {
		return w.setProposalf(ctx, pr)
	}
	return nil
}

func (w *DummyBlockFSWriter) SetOperation(ctx context.Context, total, index uint64, op base.Operation) error {
	if w.setOperationf != nil {
		return w.setOperationf(ctx, total, index, op)
	}
	return nil
}

func (w *DummyBlockFSWriter) SetOperationsTree(ctx context.Context, tw *fixedtree.Writer) error {
	if w.setOperationsTreef != nil {
		return w.setOperationsTreef(ctx, tw)
	}
	return nil
}

func (w *DummyBlockFSWriter) SetState(ctx context.Context, total, index uint64, st base.State) error {
	if w.setStatef != nil {
		return w.setStatef(ctx, total, index, st)
	}
	return nil
}

func (w *DummyBlockFSWriter) SetStatesTree(ctx context.Context, tw *fixedtree.Writer) (tr fixedtree.Tree, _ error) {
	if w.setStatesTreef != nil {
		return w.setStatesTreef(ctx, tw)
	}
	return tr, nil
}

func (w *DummyBlockFSWriter) SetManifest(ctx context.Context, m base.Manifest) error {
	if w.setManifestf != nil {
		return w.setManifestf(ctx, m)
	}
	return nil
}

func (w *DummyBlockFSWriter) SetINITVoteproof(ctx context.Context, vp base.INITVoteproof) error {
	if w.setINITVoteprooff != nil {
		return w.setINITVoteprooff(ctx, vp)
	}
	return nil
}

func (w *DummyBlockFSWriter) SetACCEPTVoteproof(ctx context.Context, vp base.ACCEPTVoteproof) error {
	if w.setACCEPTVoteprooff != nil {
		return w.setACCEPTVoteprooff(ctx, vp)
	}
	return nil
}

func (w *DummyBlockFSWriter) Save(ctx context.Context) (base.BlockMap, error) {
	if w.savef != nil {
		return w.savef(ctx)
	}
	return nil, nil
}

func (w *DummyBlockFSWriter) Cancel() error {
	if w.cancelf != nil {
		return w.cancelf()
	}
	return nil
}

type testLocalFSWriter struct {
	BaseTestLocalBlockFS
}

func (t *testLocalFSWriter) findTempFile(temp string, d base.BlockItemType, islist bool) (string, io.Reader, error) {
	fname, err := BlockFileName(d, t.Enc.Hint().Type().String())
	t.NoError(err)

	fpath := filepath.Join(temp, fname)
	f, err := os.Open(fpath)
	if err != nil {
		return fpath, nil, err
	}

	return fpath, f, nil
}

func (t *testLocalFSWriter) TestNew() {
	fs, err := NewLocalFSWriter(t.Root, base.Height(33), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
	t.NoError(err)

	_ = (interface{})(fs).(FSWriter)

	t.T().Logf("root directory: %q", fs.root)
	t.T().Logf("root base directory: %q", fs.heightbase)
	t.T().Logf("temp directory: %q", fs.temp)
}

func (t *testLocalFSWriter) TestSetManifest() {
	point := base.RawPoint(33, 44)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
	t.NoError(err)

	t.Nil(fs.m.Manifest())

	t.NoError(fs.SetManifest(context.Background(), manifest))

	base.EqualManifest(t.Assert(), manifest, fs.m.Manifest())
}

func (t *testLocalFSWriter) TestSetProposal() {
	point := base.RawPoint(33, 44)
	pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}))
	_ = pr.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())

	fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
	t.NoError(err)

	t.NoError(fs.SetProposal(context.Background(), pr))

	fpath, f, err := t.findTempFile(fs.temp, base.BlockItemProposal, false)
	t.NoError(err)
	t.T().Log("temp file:", fpath)
	t.NotNil(f)

	// NOTE find header
	var head []byte

	{
		gf, _ := util.NewSafeGzipReadCloser(f)
		br := bufio.NewReader(gf)
		i, err := br.ReadBytes('\n')
		t.NoError(err)
		head = i
	}

	item, found := fs.m.Item(base.BlockItemProposal)
	t.True(found)
	t.NoError(item.IsValid(nil))

	// NOTE compare checksum
	t.Run("compare checksum", func() {
		buf := bytes.NewBuffer(head)
		t.NoError(t.Enc.StreamEncoder(buf).Encode(pr))

		checksum := util.SHA256Checksum(buf.Bytes())

		t.Equal(checksum, item.Checksum())
	})
}

func (t *testLocalFSWriter) TestSave() {
	point := base.RawPoint(33, 44)
	pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}))
	_ = pr.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())

	fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
	t.NoError(err)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	t.NoError(fs.SetManifest(context.Background(), manifest))

	t.NoError(fs.SetProposal(context.Background(), pr))
	ivp, avp := t.Voteproofs(point)
	t.NoError(fs.SetINITVoteproof(context.Background(), ivp))
	t.NoError(fs.SetACCEPTVoteproof(context.Background(), avp))

	newroot := filepath.Join(fs.root, fs.heightbase)

	m, err := fs.Save(context.Background())
	t.NoError(err)
	t.NotNil(m)

	{
		t.PrintFS(newroot)

		b, _ := util.MarshalJSONIndent(m)
		t.T().Log("blockmap:", string(b))
	}

	b, _ := util.MarshalJSONIndent(m)
	t.T().Log("blockmap:", string(b))

	t.Run("operations(tree) should be empty in map", func() {
		_, found := m.Item(base.BlockItemOperations)
		t.False(found)
		_, found = m.Item(base.BlockItemOperationsTree)
		t.False(found)
	})

	t.Run("states(tree) should be empty in map", func() {
		_, found := m.Item(base.BlockItemStates)
		t.False(found)
		_, found = m.Item(base.BlockItemStatesTree)
		t.False(found)
	})

	checkfile := func(d base.BlockItemType) {
		fname, err := BlockFileName(d, t.Enc.Hint().Type().String())
		t.NoError(err)
		fi, err := os.Stat(filepath.Join(newroot, fname))
		t.NoError(err)
		t.False(fi.IsDir())
	}

	t.Run("check save directory", func() {
		fi, err := os.Stat(newroot)
		t.NoError(err)
		t.True(fi.IsDir())

		checkfile(base.BlockItemProposal)
		checkfile(base.BlockItemVoteproofs)
	})

	t.Run("check map file", func() {
		fname, err := BlockFileName(base.BlockItemMap, t.Enc.Hint().Type().String())
		t.NoError(err)

		fpath := filepath.Join(newroot, fname)
		f, err := os.Open(fpath)
		t.NoError(err)

		var br io.Reader = f

		switch i, _, _, err := readBaseHeader(br); {
		case err != nil:
			t.NoError(err)
		default:
			br = i
		}

		b, err := io.ReadAll(br)
		t.NoError(err)

		hinter, err := t.Enc.Decode(b)
		t.NoError(err)

		um, ok := hinter.(base.BlockMap)
		t.True(ok)

		base.EqualBlockMap(t.Assert(), m, um)
	})

	t.Run("check files file", func() {
		fpath := filepath.Join(filepath.Dir(newroot), base.BlockItemFilesName(point.Height()))
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
		t.False(check(base.BlockItemOperations))
		t.False(check(base.BlockItemOperationsTree))
		t.False(check(base.BlockItemStatesTree))
		t.False(check(base.BlockItemStatesTree))
	})
}

func (t *testLocalFSWriter) TestSaveAgain() {
	point := base.RawPoint(33, 44)
	pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}))
	_ = pr.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())

	fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
	t.NoError(err)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	t.NoError(fs.SetManifest(context.Background(), manifest))

	t.NoError(fs.SetProposal(context.Background(), pr))
	ivp, avp := t.Voteproofs(point)
	t.NoError(fs.SetINITVoteproof(context.Background(), ivp))
	t.NoError(fs.SetACCEPTVoteproof(context.Background(), avp))

	m, err := fs.Save(context.Background())
	t.NoError(err)
	t.NotNil(m)

	t.Run("save again", func() {
		fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
		t.NoError(err)

		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
		t.NoError(fs.SetManifest(context.Background(), manifest))

		t.NoError(fs.SetProposal(context.Background(), pr))
		ivp, avp := t.Voteproofs(point)
		t.NoError(fs.SetINITVoteproof(context.Background(), ivp))
		t.NoError(fs.SetACCEPTVoteproof(context.Background(), avp))

		m, err := fs.Save(context.Background())
		t.Error(err)
		t.Nil(m)
		t.ErrorContains(err, "height directory already exists")
	})
}

func (t *testLocalFSWriter) TestCancel() {
	point := base.RawPoint(33, 44)
	pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}))
	_ = pr.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())

	fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
	t.NoError(err)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	t.NoError(fs.SetManifest(context.Background(), manifest))

	t.NoError(fs.SetProposal(context.Background(), pr))
	ivp, avp := t.Voteproofs(point)
	t.NoError(fs.SetINITVoteproof(context.Background(), ivp))
	t.NoError(fs.SetACCEPTVoteproof(context.Background(), avp))

	t.NoError(fs.Cancel())

	t.Run("check temp directory", func() {
		fi, err := os.Stat(fs.temp)
		t.True(os.IsNotExist(err))
		t.Nil(fi)
	})
}

func (t *testLocalFSWriter) TestSetACCEPTVoteproof() {
	point := base.RawPoint(33, 44)

	ivp, avp := t.Voteproofs(point)
	t.Run("both", func() {
		fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
		t.NoError(err)

		t.NoError(fs.SetINITVoteproof(context.Background(), ivp))
		t.NoError(fs.SetACCEPTVoteproof(context.Background(), avp))

		fpath, f, err := t.findTempFile(fs.temp, base.BlockItemVoteproofs, false)
		t.NoError(err)
		t.T().Log("temp file:", fpath)
		t.NotNil(f)

		item, found := fs.m.Item(base.BlockItemVoteproofs)
		t.True(found)
		t.NoError(item.IsValid(nil))
	})

	t.Run("without init", func() {
		fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
		t.NoError(err)

		t.NoError(fs.SetACCEPTVoteproof(context.Background(), avp))

		fpath, f, err := t.findTempFile(fs.temp, base.BlockItemVoteproofs, false)
		t.Error(err)
		t.T().Log("temp file:", fpath)
		t.Nil(f)

		item, found := fs.m.Item(base.BlockItemVoteproofs)
		t.False(found)
		t.Nil(item)
	})

	t.Run("without accept", func() {
		fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
		t.NoError(err)

		t.NoError(fs.SetINITVoteproof(context.Background(), ivp))

		fpath, f, err := t.findTempFile(fs.temp, base.BlockItemVoteproofs, false)
		t.Error(err)
		t.T().Log("temp file:", fpath)
		t.Nil(f)

		item, found := fs.m.Item(base.BlockItemVoteproofs)
		t.False(found)
		t.Nil(item)
	})
}

func (t *testLocalFSWriter) TestSetOperations() {
	point := base.RawPoint(33, 44)

	fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
	t.NoError(err)

	ops := make([]base.Operation, 33)
	opstreeg, err := fixedtree.NewWriter(base.OperationFixedtreeHint, 33)
	t.NoError(err)
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, _ := isaac.NewDummyOperation(fact, t.Local.Privatekey(), t.LocalParams.NetworkID())
		ops[i] = op

		node := base.NewInStateOperationFixedtreeNode(op.Fact().Hash(), "")

		t.NoError(opstreeg.Add(uint64(i), node))
	}

	ctx := context.Background()
	sem := semaphore.NewWeighted(int64(len(ops)))

	for i := range ops {
		if err := sem.Acquire(ctx, 1); err != nil {
			panic(err)
		}

		i := uint64(i)
		op := ops[i]
		go func() {
			defer sem.Release(1)

			if err := fs.SetOperation(context.Background(), uint64(len(ops)), i, op); err != nil {
				panic(err)
			}
		}()
	}

	if err := sem.Acquire(ctx, int64(len(ops))); err != nil {
		panic(err)
	}

	t.NoError(fs.SetOperationsTree(ctx, opstreeg))

	t.Run("operations file", func() {
		fpath, f, err := t.findTempFile(fs.temp, base.BlockItemOperations, true)
		t.NoError(err)
		t.T().Log("temp file:", fpath)
		t.NotNil(f)

		item, found := fs.m.Item(base.BlockItemOperations)
		t.True(found)
		t.NoError(item.IsValid(nil))

		// NOTE compare checksum
		g, err := util.NewSafeGzipReadCloser(f)
		t.NoError(err)
		b, err := io.ReadAll(g)
		t.NoError(err)

		t.Equal(item.Checksum(), util.SHA256Checksum(b))
	})

	t.Run("operations tree file", func() {
		fpath, f, err := t.findTempFile(fs.temp, base.BlockItemOperationsTree, true)
		t.NoError(err)
		t.T().Log("temp file:", fpath)
		t.NotNil(f)

		item, found := fs.m.Item(base.BlockItemOperationsTree)
		t.True(found)
		t.NoError(item.IsValid(nil))

		// NOTE compare checksum
		g, err := util.NewSafeGzipReadCloser(f)
		t.NoError(err)
		b, err := io.ReadAll(g)
		t.NoError(err)

		t.Equal(item.Checksum(), util.SHA256Checksum(b))
	})
}

func (t *testLocalFSWriter) TestSetStates() {
	point := base.RawPoint(33, 44)

	fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
	t.NoError(err)

	stts := make([]base.State, 33)
	sttstreeg, err := fixedtree.NewWriter(base.StateFixedtreeHint, 33)
	t.NoError(err)
	for i := range stts {
		key := util.UUID().String()
		stts[i] = base.NewBaseState(
			point.Height(),
			key,
			base.NewDummyStateValue(util.UUID().String()),
			valuehash.RandomSHA256(),
			nil,
		)
		node := fixedtree.NewBaseNode(key)
		t.NoError(sttstreeg.Add(uint64(i), node))
	}

	ctx := context.Background()
	sem := semaphore.NewWeighted(int64(len(stts)))

	for i := range stts {
		if err := sem.Acquire(ctx, 1); err != nil {
			panic(err)
		}

		i := uint64(i)
		st := stts[i]
		go func() {
			defer sem.Release(1)

			if err := fs.SetState(context.Background(), uint64(len(stts)), i, st); err != nil {
				panic(err)
			}
		}()
	}

	if err := sem.Acquire(ctx, int64(len(stts))); err != nil {
		panic(err)
	}

	_, err = fs.SetStatesTree(ctx, sttstreeg)
	t.NoError(err)

	t.Run("states file", func() {
		fpath, f, err := t.findTempFile(fs.temp, base.BlockItemStates, true)
		t.NoError(err)
		t.T().Log("temp file:", fpath)
		t.NotNil(f)

		item, found := fs.m.Item(base.BlockItemStates)
		t.True(found)
		t.NoError(item.IsValid(nil))

		// NOTE compare checksum
		g, err := util.NewSafeGzipReadCloser(f)
		t.NoError(err)
		b, err := io.ReadAll(g)
		t.NoError(err)

		t.Equal(item.Checksum(), util.SHA256Checksum(b))
	})

	t.Run("states tree file", func() {
		fpath, f, err := t.findTempFile(fs.temp, base.BlockItemStatesTree, true)
		t.NoError(err)
		t.T().Log("temp file:", fpath)
		t.NotNil(f)

		item, found := fs.m.Item(base.BlockItemStatesTree)
		t.True(found)
		t.NoError(item.IsValid(nil))

		// NOTE compare checksum
		g, err := util.NewSafeGzipReadCloser(f)
		t.NoError(err)
		b, err := io.ReadAll(g)
		t.NoError(err)

		t.Equal(item.Checksum(), util.SHA256Checksum(b))
	})
}

func (t *testLocalFSWriter) TestRemove() {
	save := func(height int64) error {
		point := base.RawPoint(height, 44)
		pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}))
		_ = pr.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())

		fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Enc, t.Local, t.LocalParams.NetworkID())
		if err != nil {
			return err
		}

		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
		if err := fs.SetManifest(context.Background(), manifest); err != nil {
			return err
		}

		if err := fs.SetProposal(context.Background(), pr); err != nil {
			return err
		}

		ivp, avp := t.Voteproofs(point)
		if err := fs.SetINITVoteproof(context.Background(), ivp); err != nil {
			return err
		}
		if err := fs.SetACCEPTVoteproof(context.Background(), avp); err != nil {
			return err
		}

		if _, err := fs.Save(context.Background()); err != nil {
			return err
		}

		return nil
	}

	var top, bottom int64 = 33, 30

	for i := bottom; i <= top; i++ {
		t.NoError(save(i))
	}

	t.PrintFS(t.Root)

	t.Run("remove over top", func() {
		removed, err := RemoveBlocksFromLocalFS(t.Root, base.Height(top+1))
		t.NoError(err)
		t.False(removed)
	})

	t.Run("remove under genesis", func() {
		removed, err := RemoveBlocksFromLocalFS(t.Root, base.NilHeight)
		t.NoError(err)
		t.False(removed)
	})

	t.Run("remove top", func() {
		removed, err := RemoveBlocksFromLocalFS(t.Root, base.Height(top))
		t.NoError(err)
		t.True(removed)

		_, err = NewLocalFSReaderFromHeight(t.Root, base.Height(top), t.Enc)
		t.Error(err)
		t.True(errors.Is(err, util.ErrNotFound))

		for i := bottom; i < top; i++ {
			_, err = NewLocalFSReaderFromHeight(t.Root, base.Height(i), t.Enc)
			t.NoError(err)
		}
	})

	t.Run("remove bottom", func() {
		removed, err := RemoveBlocksFromLocalFS(t.Root, base.Height(bottom))
		t.NoError(err)
		t.True(removed)

		_, err = NewLocalFSReaderFromHeight(t.Root, base.Height(bottom), t.Enc)
		t.Error(err)
		t.True(errors.Is(err, util.ErrNotFound))

		for i := bottom; i < top; i++ {
			_, err = NewLocalFSReaderFromHeight(t.Root, base.Height(i), t.Enc)
			t.True(errors.Is(err, util.ErrNotFound))
		}
	})
}

func TestLocalFSWriter(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testLocalFSWriter))
}

type testHeightDirectory struct {
	suite.Suite
	root string
}

func (t *testHeightDirectory) SetupTest() {
	t.root, _ = os.MkdirTemp("", "mitum-test")
}

func (t *testHeightDirectory) TearDownTest() {
	os.RemoveAll(t.root)
}

func (t *testHeightDirectory) prepareFS(root string, heights []uint64, others []string) {
	for i := range heights {
		t.NoError(os.MkdirAll(
			filepath.Join(
				root,
				HeightDirectory(base.Height(int64(heights[i]))),
			),
			0o700,
		))
	}

	for i := range others {
		t.NoError(os.MkdirAll(filepath.Join(root, others[i]), 0o700))
	}
}

func (t *testHeightDirectory) walk(root string) {
	t.NoError(filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		switch {
		case err != nil:
			return err
		case !info.IsDir():
			return nil
		}

		t.T().Log(" >", path)

		return nil
	}))
}

func (t *testHeightDirectory) TestFindHighest() {
	cases := []struct {
		name    string
		heights []uint64
		others  []string
		r       uint64
	}{
		{"same level", []uint64{0, 1, 2, 3}, nil, 3},
		{
			"max",
			[]uint64{0, 1, 2, uint64(math.MaxInt64)},
			nil,
			uint64(math.MaxInt64),
		},
		{"zero level", []uint64{0}, nil, 0},
		{"different level #0", []uint64{0, 1, 2, 3333333}, nil, 3333333},
		{"different level #1", []uint64{0, 1, 2, 1333333, 3333333, 3333334, 3333339}, nil, 3333339},
		{"different level #2", []uint64{0, 1, 2, 1333333, 3333333, 9333333333}, nil, 9333333333},
		{
			"not height directory #0",
			[]uint64{0, 1, 2, 1333333, 3333333, 9333333333},
			[]string{
				"tem",
				"900000000000000000",
			},
			9333333333,
		},
		{
			"not height directory #1",
			[]uint64{0, 1, 2, 1333333, 3333333, 9333333333},
			[]string{
				"000/000/000/009/43a/433",
			},
			9333333333,
		},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				root := filepath.Join(t.root, util.UUID().String())
				t.prepareFS(root, c.heights, c.others)

				t.walk(root)

				d, found, err := FindHighestDirectory(root)
				t.NoError(err, "%d: %v", i, c.name)
				t.True(found, "%d: %v", i, c.name)
				t.NotEmpty(d, "%d: %v", i, c.name)

				rel, err := filepath.Rel(root, d)
				t.NoError(err, "%d: %v", i, c.name)

				rh, err := HeightFromDirectory(rel)
				t.NoError(err, "%d: %v", i, c.name)

				t.Equal(c.r, uint64(rh.Int64()), "%d: %v; %v != %v", i, c.name, c.r, rh.Int64())
			},
		)
	}
}

func TestHeightDirectory(t *testing.T) {
	suite.Run(t, new(testHeightDirectory))
}
