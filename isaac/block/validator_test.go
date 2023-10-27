package isaacblock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type baseTestValidateBlock struct {
	BaseTestLocalBlockFS
	ImportedRoot string
}

func (t *baseTestValidateBlock) SetupTest() {
	t.BaseTestLocalBlockFS.SetupTest()

	t.ImportedRoot, _ = os.MkdirTemp("", "mitum-test-imported")
}

func (t *baseTestValidateBlock) TearDownTest() {
	t.BaseTestLocalBlockFS.TearDownTest()

	_ = os.RemoveAll(t.ImportedRoot)
}

func (t *baseTestValidateBlock) buildLocalFS(name string, top base.Height) string {
	var prev util.Hash

	for height := base.GenesisHeight; height <= top; height++ {
		fs, _, _, _, _, _, _ := t.PrepareFS(base.NewPoint(height, 0), prev, nil)
		m, err := fs.Save(context.Background())
		t.NoError(err)

		prev = m.Manifest().Hash()
	}

	root := filepath.Join(t.ImportedRoot, name)
	t.NoError(os.Rename(t.Root, root))
	t.NoError(os.Mkdir(t.Root, 0o700))

	return root
}

func (t *baseTestValidateBlock) buildBlocks(name string, top base.Height) (
	string,
	isaac.Database,
) {
	st := leveldbstorage.NewMemStorage()
	db, err := isaacdatabase.NewCenter(st, t.Encs, t.Enc, t.NewLeveldbPermanentDatabase(),
		func(height base.Height) (isaac.BlockWriteDatabase, error) {
			return isaacdatabase.NewLeveldbBlockWrite(height, st, t.Encs, t.Enc, 0), nil
		},
	)
	t.NoError(err)

	var prev util.Hash

	for height := base.GenesisHeight; height <= top; height++ {
		fs, _, ops, _, sts, _, _ := t.PrepareFS(base.NewPoint(height, 0), prev, nil)
		m, err := fs.Save(context.Background())
		t.NoError(err)

		bw, err := db.NewBlockWriteDatabase(height)
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

		prev = m.Manifest().Hash()
	}

	root := filepath.Join(t.ImportedRoot, name)
	t.NoError(os.Rename(t.Root, root))
	t.NoError(os.Mkdir(t.Root, 0o700))

	return root, db
}

type testValidateLastBlocks struct {
	baseTestValidateBlock
}

func (t *testValidateLastBlocks) TestOK() {
	a, db := t.buildBlocks("no0", 3)

	t.WalkFS(a)

	t.Run("ok", func() {
		t.NoError(ValidateLastBlocks(a, t.Encs, t.Enc, db, t.LocalParams.NetworkID()))
	})

	t.Run("wrong localfs root", func() {
		err := ValidateLastBlocks("/tmp", t.Encs, t.Enc, db, t.LocalParams.NetworkID())
		t.Error(err)
	})
}

func (t *testValidateLastBlocks) TestLastBlockMapNotFound() {
	t.Run("not found in database", func() {
		a, _ := t.buildBlocks("no0", 3)

		st := leveldbstorage.NewMemStorage()
		db, err := isaacdatabase.NewCenter(st, t.Encs, t.Enc, t.NewLeveldbPermanentDatabase(), nil)
		t.NoError(err)

		err = ValidateLastBlocks(a, t.Encs, t.Enc, db, t.LocalParams.NetworkID())
		t.Error(err)
		t.True(errors.Is(err, ErrLastBlockMapOnlyInLocalFS))
	})

	t.Run("not found in localfs", func() {
		_, db := t.buildBlocks("no1", 3)

		temp, _ := os.MkdirTemp("", "mitum-test-imported")
		defer os.RemoveAll(temp)

		err := ValidateLastBlocks(temp, t.Encs, t.Enc, db, t.LocalParams.NetworkID())
		t.Error(err)
		t.True(errors.Is(err, ErrLastBlockMapOnlyInDatabase))
	})
}

func (t *testValidateLastBlocks) TestDifferentHeight_BlockMapNotFoundInDatabase() {
	a, db := t.buildBlocks("no0", 3)

	removed, err := db.RemoveBlocks(3)
	t.NoError(err)
	t.True(removed)

	err = ValidateLastBlocks(a, t.Encs, t.Enc, db, t.LocalParams.NetworkID())
	t.Error(err)

	var derr *ErrValidatedDifferentHeightBlockMaps

	t.True(errors.As(err, &derr))
	t.Equal(base.Height(2), derr.DatabaseHeight())
	t.Equal(base.Height(3), derr.LocalFSHeight())
}

func (t *testValidateLastBlocks) TestDifferentHash() {
	a, db := t.buildBlocks("no0", 3)

	t.T().Log("override block of height, 3")
	fs, _, _, _, _, _, _ := t.PrepareFS(base.NewPoint(3, 0), nil, nil)
	_, err := fs.Save(context.Background())
	t.NoError(err)

	removed, err := RemoveBlocksFromLocalFS(a, 3)
	t.NoError(err)
	t.True(removed)

	lastheightdirectory := filepath.Join(a, HeightDirectory(3))

	t.NoError(os.Rename(filepath.Join(t.Root, HeightDirectory(3)), lastheightdirectory))

	err = ValidateLastBlocks(a, t.Encs, t.Enc, db, t.LocalParams.NetworkID())
	t.Error(err)
	t.ErrorContains(err, "different manifest hash")
}

func TestValidateLastBlocks(t *testing.T) {
	suite.Run(t, new(testValidateLastBlocks))
}

type testValidateAllBlockMapsFromLocalFS struct {
	baseTestValidateBlock
}

func (t *testValidateAllBlockMapsFromLocalFS) TestOK() {
	a := t.buildLocalFS("no0", 3)

	t.NoError(ValidateAllBlockMapsFromLocalFS(a, t.Enc, 3, t.LocalParams.NetworkID()))
}

func (t *testValidateAllBlockMapsFromLocalFS) TestNotFound() {
	a := t.buildLocalFS("no0", 3)

	t.Run("empty", func() {
		err := ValidateAllBlockMapsFromLocalFS(util.UUID().String(), t.Enc, 3, t.LocalParams.NetworkID())
		t.Error(err)
		t.True(errors.Is(err, os.ErrNotExist))
	})

	t.Run("last not found", func() {
		err := ValidateAllBlockMapsFromLocalFS(a, t.Enc, 4, t.LocalParams.NetworkID())
		t.Error(err)
		t.True(errors.Is(err, os.ErrNotExist))
	})

	t.Run("missing", func() {
		removed, err := RemoveBlocksFromLocalFS(a, 2)
		t.NoError(err)
		t.True(removed)

		err = ValidateAllBlockMapsFromLocalFS(a, t.Enc, 3, t.LocalParams.NetworkID())
		t.Error(err)
		t.True(errors.Is(err, os.ErrNotExist))
	})
}

func (t *testValidateAllBlockMapsFromLocalFS) TestWrong() {
	t.Run("wrong manifest", func() {
		a := t.buildLocalFS("no0", 3)

		removed, err := RemoveBlockFromLocalFS(a, 2)
		t.NoError(err)
		t.True(removed)

		fs, _, _, _, _, _, _ := t.PrepareFS(base.NewPoint(2, 0), nil, nil)
		_, err = fs.Save(context.Background())
		t.NoError(err)

		lastheightdirectory := filepath.Join(a, HeightDirectory(2))

		t.NoError(os.Rename(filepath.Join(t.Root, HeightDirectory(2)), lastheightdirectory))

		err = ValidateAllBlockMapsFromLocalFS(a, t.Enc, 3, t.LocalParams.NetworkID())
		t.Error(err)
		t.ErrorContains(err, "previous does not match")
	})

	t.Run("wrong height", func() {
		a := t.buildLocalFS("no1", 3)

		removed, err := RemoveBlockFromLocalFS(a, 2)
		t.NoError(err)
		t.True(removed)

		fs, _, _, _, _, _, _ := t.PrepareFS(base.NewPoint(1, 0), nil, nil)
		_, err = fs.Save(context.Background())
		t.NoError(err)

		lastheightdirectory := filepath.Join(a, HeightDirectory(2))

		t.NoError(os.Rename(filepath.Join(t.Root, HeightDirectory(1)), lastheightdirectory))

		err = ValidateAllBlockMapsFromLocalFS(a, t.Enc, 3, t.LocalParams.NetworkID())
		t.Error(err)
		t.ErrorContains(err, "different height blockmaps")
	})
}

func TestValidateAllBlockMapsFromLocalFS(t *testing.T) {
	suite.Run(t, new(testValidateAllBlockMapsFromLocalFS))
}
