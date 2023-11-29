package isaacblock

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testLocalFSReader struct {
	BaseTestLocalBlockFS
}

func (t *testLocalFSReader) TestNew() {
	ctx := context.Background()

	point := base.RawPoint(33, 44)
	fs, _, _, _, _, _, _ := t.PrepareFS(point, nil, nil)

	root := filepath.Join(fs.root, fs.heightbase)

	_, err := fs.Save(ctx)
	t.NoError(err)

	r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)
	t.NotNil(r)

	_ = (interface{})(r).(isaac.BlockReader)

	t.Equal(root, r.root)
}

func (t *testLocalFSReader) TestMap() {
	point := base.RawPoint(33, 44)
	fs, _, _, _, _, _, _ := t.PrepareFS(point, nil, nil)
	_, err := fs.Save(context.Background())
	t.NoError(err)

	r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	m, found, err := r.BlockMap()
	t.NoError(err)
	t.True(found)

	t.NoError(m.IsValid(t.LocalParams.NetworkID()))
}

func (t *testLocalFSReader) TestReader() {
	point := base.RawPoint(33, 44)
	fs, _, _, _, _, _, _ := t.PrepareFS(point, nil, nil)
	m, err := fs.Save(context.Background())
	t.NoError(err)

	{
		t.PrintFS(fs.root)

		b, _ := util.MarshalJSONIndent(m)
		t.T().Log("blockmap:", string(b))
	}

	r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	t.Run("unknown", func() {
		f, found, err := r.Reader(base.BlockItemType("findme"))
		t.Error(err)
		t.False(found)
		t.Nil(f)

		t.ErrorContains(err, "unknown block map item type")
	})

	t.Run("all knowns", func() {
		types := []base.BlockItemType{
			base.BlockItemProposal,
			base.BlockItemOperations,
			base.BlockItemOperationsTree,
			base.BlockItemStates,
			base.BlockItemStatesTree,
			base.BlockItemVoteproofs,
		}

		for i := range types {
			f, found, err := r.Reader(types[i])
			t.NoError(err, "type: %q", types[i])
			t.True(found, "type: %q", types[i])
			t.NotNil(f, "type: %q", types[i])
		}
	})

	t.Run("known and found", func() {
		f, found, err := r.UncompressedReader(base.BlockItemProposal)
		t.NoError(err)
		t.True(found)
		defer f.Close()

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

		_ = hinter.(base.ProposalSignFact)
	})

	t.Run("known, but not found", func() {
		// NOTE remove
		fname, _ := BlockFileName(base.BlockItemOperations, t.Enc.Hint().Type().String())
		t.NoError(os.Remove(filepath.Join(r.root, fname)))

		f, found, err := r.Reader(base.BlockItemOperations)
		t.NoError(err)
		t.False(found)
		t.Nil(f)
	})
}

func (t *testLocalFSReader) TestChecksumReader() {
	point := base.RawPoint(33, 44)
	fs, _, _, _, _, _, _ := t.PrepareFS(point, nil, nil)
	m, err := fs.Save(context.Background())
	t.NoError(err)

	{
		t.PrintFS(t.Root)

		b, _ := util.MarshalJSONIndent(m)
		t.T().Log("blockmap:", string(b))
	}

	r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	t.Run("unknown", func() {
		f, found, err := r.ChecksumReader(base.BlockItemType("findme"))
		t.Error(err)
		t.False(found)
		t.Nil(f)

		t.ErrorContains(err, "unknown block map item type")
	})

	t.Run("all knowns", func() {
		types := []base.BlockItemType{
			base.BlockItemProposal,
			base.BlockItemOperations,
			base.BlockItemOperationsTree,
			base.BlockItemStates,
			base.BlockItemStatesTree,
			base.BlockItemVoteproofs,
		}

		for i := range types {
			f, found, err := r.ChecksumReader(types[i])
			t.NoError(err, "type: %q", types[i])
			t.True(found, "type: %q", types[i])
			t.NotNil(f, "type: %q", types[i])
		}
	})

	t.Run("known and found", func() {
		f, found, err := r.ChecksumReader(base.BlockItemProposal)
		t.NoError(err)
		t.True(found)
		defer f.Close()

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

		_ = hinter.(base.ProposalSignFact)
	})

	t.Run("known, but not found", func() {
		// NOTE remove
		fname, _ := BlockFileName(base.BlockItemOperations, t.Enc.Hint().Type().String())
		t.NoError(os.Remove(filepath.Join(r.root, fname)))

		f, found, err := r.ChecksumReader(base.BlockItemOperations)
		t.NoError(err)
		t.False(found)
		t.Nil(f)
	})
}

func (t *testLocalFSReader) TestItem() {
	point := base.RawPoint(33, 44)
	fs, pr, ops, opstree, stts, sttstree, vps := t.PrepareFS(point, nil, nil)

	_, err := fs.Save(context.Background())
	t.NoError(err)

	r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	t.Run("unknown", func() {
		f, found, err := r.Item(base.BlockItemType("findme"))
		t.NoError(err)
		t.False(found)
		t.Nil(f)
	})

	t.Run("all knowns", func() {
		types := []base.BlockItemType{
			base.BlockItemProposal,
			base.BlockItemOperations,
			base.BlockItemOperationsTree,
			base.BlockItemStates,
			base.BlockItemStatesTree,
			base.BlockItemVoteproofs,
		}

		for i := range types {
			v, found, err := r.Item(types[i])
			t.NoError(err, "type: %q", types[i])
			t.True(found, "type: %q", types[i])
			t.NotNil(v, "type: %q", types[i])
		}
	})

	t.Run("proposal", func() {
		v, found, err := r.Item(base.BlockItemProposal)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		upr, ok := v.(base.ProposalSignFact)
		t.True(ok)

		base.EqualProposalSignFact(t.Assert(), pr, upr)
	})

	t.Run("operations", func() {
		v, found, err := r.Item(base.BlockItemOperations)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		uops, ok := v.([]base.Operation)
		t.True(ok)

		t.Equal(len(ops), len(uops))

		sort.Slice(ops, func(i, j int) bool {
			return bytes.Compare(ops[i].Fact().Hash().Bytes(), ops[j].Fact().Hash().Bytes()) > 0
		})
		sort.Slice(uops, func(i, j int) bool {
			return bytes.Compare(uops[i].Fact().Hash().Bytes(), uops[j].Fact().Hash().Bytes()) > 0
		})

		for i := range ops {
			a := ops[i]
			b := uops[i]

			base.EqualOperation(t.Assert(), a, b)
		}
	})

	t.Run("operations tree", func() {
		v, found, err := r.Item(base.BlockItemOperationsTree)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		uopstree, ok := v.(fixedtree.Tree)
		t.True(ok)

		t.NoError(opstree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
			if i := uopstree.Node(index); i == nil {
				return false, util.ErrNotFound.Errorf("node not found")
			} else if !n.Equal(i) {
				return false, errors.Errorf("not equal")
			}

			return true, nil
		}))
	})

	t.Run("states", func() {
		v, found, err := r.Item(base.BlockItemStates)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		ustts, ok := v.([]base.State)
		t.True(ok)

		t.Equal(len(stts), len(ustts))

		sort.Slice(stts, func(i, j int) bool {
			return strings.Compare(stts[i].Key(), stts[j].Key()) > 0
		})
		sort.Slice(ustts, func(i, j int) bool {
			return strings.Compare(ustts[i].Key(), ustts[j].Key()) > 0
		})

		for i := range stts {
			a := stts[i]
			b := ustts[i]

			t.True(base.IsEqualState(a, b))
		}
	})

	t.Run("states tree", func() {
		v, found, err := r.Item(base.BlockItemStatesTree)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		usttstree, ok := v.(fixedtree.Tree)
		t.True(ok)

		t.NoError(sttstree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
			if i := usttstree.Node(index); i == nil {
				return false, util.ErrNotFound.Errorf("node not found")
			} else if !n.Equal(i) {
				return false, errors.Errorf("not equal")
			}

			return true, nil
		}))
	})

	t.Run("voteproofs", func() {
		v, found, err := r.Item(base.BlockItemVoteproofs)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		uvps, ok := v.([2]base.Voteproof)
		t.True(ok)
		t.Equal(2, len(uvps))

		base.EqualVoteproof(t.Assert(), vps[0], uvps[0])
		base.EqualVoteproof(t.Assert(), vps[1], uvps[1])
	})
}

func (t *testLocalFSReader) TestWrongChecksum() {
	point := base.RawPoint(33, 44)
	fs, pr, _, _, _, _, _ := t.PrepareFS(point, nil, nil)

	_, err := fs.Save(context.Background())
	t.NoError(err)

	t.PrintFS(t.Root)

	var root string
	t.Run("load proposal before modifying", func() {
		r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
		t.NoError(err)

		root = r.root

		v, found, err := r.Item(base.BlockItemProposal)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		upr, ok := v.(base.ProposalSignFact)
		t.True(ok)

		base.EqualProposalSignFact(t.Assert(), pr, upr)
	})

	// NOTE modify proposal.json
	i, _ := BlockFileName(base.BlockItemProposal, t.Enc.Hint().Type().String())
	path := filepath.Join(root, i)

	t.T().Log("file:", path)

	f, err := os.Open(path)
	t.NoError(err)

	gf, err := util.NewSafeGzipReadCloser(f)
	t.NoError(err)

	b, err := io.ReadAll(gf)
	t.NoError(err)
	gf.Close()

	b = append(b, '\n')
	nf, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	t.NoError(err)

	gnf, _ := util.NewGzipWriter(nf, gzip.BestSpeed)

	_, err = gnf.Write(b)
	t.NoError(err)
	gnf.Close()

	t.Run("load proposal after modifying", func() {
		r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
		t.NoError(err)

		v, found, err := r.Item(base.BlockItemProposal)
		t.Error(err)
		t.ErrorContains(err, "checksum mismatch")
		t.True(found)
		t.NotNil(v)
	})
}

func TestLocalFSReader(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testLocalFSReader))
}
