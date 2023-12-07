package isaacblock

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/stretchr/testify/suite"
)

func isListBlockItemType(t base.BlockItemType) bool {
	switch t {
	case base.BlockItemOperations,
		base.BlockItemOperationsTree,
		base.BlockItemStates,
		base.BlockItemStatesTree,
		base.BlockItemVoteproofs:
		return true
	default:
		return false
	}
}

type testItemReader struct {
	BaseTestLocalBlockFS
}

func (t *testItemReader) prepare() (fixedtree.Tree, fixedtree.Tree) {
	fs, _, _, opstree, _, sttstree, _ := t.PrepareFS(base.RawPoint(33, 0), nil, nil)
	_, err := fs.Save(context.Background())
	t.NoError(err)

	return opstree, sttstree
}

func (t *testItemReader) pick(it base.BlockItemType) *util.CompressedReader {
	var compressFormat string
	if isCompressedBlockItemType(it) {
		compressFormat = "gz"
	}

	f := t.openFile(it)

	cr, err := util.NewCompressedReader(f, compressFormat, nil)
	t.NoError(err)

	return cr
}

func (t *testItemReader) openFile(it base.BlockItemType) *os.File {
	n, err := DefaultBlockFileName(it, t.Enc.Hint().Type())
	t.NoError(err)

	f, err := os.Open(filepath.Join(t.Root, isaac.BlockHeightDirectory(33), n))
	t.NoError(err)

	return f
}

func (t *testItemReader) loadItem(it base.BlockItemType) (u interface{}) {
	var f io.ReadCloser

	f = t.openFile(it)
	defer f.Close()

	if isCompressedBlockItemType(it) {
		i, err := gzip.NewReader(f)
		t.NoError(err)
		f = i
	}

	br, _, err := isaac.ReadBlockItemFileHeader(f)
	t.NoError(err)

	if isListBlockItemType(it) {
		var n []interface{}

		_, err := isaac.BlockItemDecodeLineItems(br, t.Enc.Decode, func(index uint64, v interface{}) error {
			n = append(n, v)

			return nil
		})
		t.NoError(err)

		return n
	}

	t.NoError(encoder.DecodeReader(t.Enc, br, &u))
	return u
}

func (t *testItemReader) TestDecode() {
	opstree, sttstree := t.prepare()

	t.Run("blockmap", func() {
		cr := t.pick(base.BlockItemMap)

		f := NewDefaultItemReaderFunc(3)
		reader, err := f(base.BlockItemMap, t.Enc, cr)
		t.NoError(err)

		t.Equal(base.BlockItemMap, reader.Type())
		t.Equal("", reader.Reader().Format)

		v, err := reader.Decode()
		t.NoError(err)

		i, ok := v.(base.BlockMap)
		t.True(ok)

		j, ok := t.loadItem(base.BlockItemMap).(base.BlockMap)
		t.True(ok)

		base.IsEqualBlockMap(j, i)
	})

	t.Run("proposal", func() {
		cr := t.pick(base.BlockItemProposal)

		f := NewDefaultItemReaderFunc(3)
		reader, err := f(base.BlockItemProposal, t.Enc, cr)
		t.NoError(err)

		t.Equal(base.BlockItemProposal, reader.Type())
		t.Equal("gz", reader.Reader().Format)

		v, err := reader.Decode()
		t.NoError(err)
		i, ok := v.(base.ProposalSignFact)
		t.True(ok)

		j, ok := t.loadItem(base.BlockItemProposal).(base.ProposalSignFact)
		t.True(ok)

		base.EqualProposalSignFact(t.Assert(), j, i)
	})

	t.Run("operations", func() {
		cr := t.pick(base.BlockItemOperations)

		f := NewDefaultItemReaderFunc(3)
		reader, err := f(base.BlockItemOperations, t.Enc, cr)
		t.NoError(err)

		t.Equal(base.BlockItemOperations, reader.Type())
		t.Equal("gz", reader.Reader().Format)

		v, err := reader.Decode()
		t.NoError(err)

		a, ok := v.([]interface{})
		t.True(ok)

		b, ok := t.loadItem(base.BlockItemOperations).([]interface{})
		t.True(ok)

		t.Equal(len(b), len(a))

		for i := range a {
			ao, ok := a[i].(base.Operation)
			t.True(ok)
			bo, ok := b[i].(base.Operation)
			t.True(ok)

			base.EqualOperation(t.Assert(), bo, ao)
		}
	})

	t.Run("states", func() {
		cr := t.pick(base.BlockItemStates)

		f := NewDefaultItemReaderFunc(3)
		reader, err := f(base.BlockItemStates, t.Enc, cr)
		t.NoError(err)

		t.Equal(base.BlockItemStates, reader.Type())
		t.Equal("gz", reader.Reader().Format)

		v, err := reader.Decode()
		t.NoError(err)

		a, ok := v.([]interface{})
		t.True(ok)

		b, ok := t.loadItem(base.BlockItemStates).([]interface{})
		t.True(ok)

		t.Equal(len(b), len(a))

		for i := range a {
			ao, ok := a[i].(base.State)
			t.True(ok)
			bo, ok := b[i].(base.State)
			t.True(ok)

			t.True(base.IsEqualState(bo, ao))
		}
	})

	t.Run("operations_tree", func() {
		cr := t.pick(base.BlockItemOperationsTree)

		f := NewDefaultItemReaderFunc(3)
		reader, err := f(base.BlockItemOperationsTree, t.Enc, cr)
		t.NoError(err)

		t.Equal(base.BlockItemOperationsTree, reader.Type())
		t.Equal("gz", reader.Reader().Format)

		v, err := reader.Decode()
		t.NoError(err)

		atr, ok := v.(fixedtree.Tree)
		t.True(ok)

		atr.Traverse(func(index uint64, a fixedtree.Node) (bool, error) {
			b := opstree.Node(index)

			t.True(a.Equal(b))

			return true, nil
		})
	})

	t.Run("states_tree", func() {
		cr := t.pick(base.BlockItemStatesTree)

		f := NewDefaultItemReaderFunc(3)
		reader, err := f(base.BlockItemStatesTree, t.Enc, cr)
		t.NoError(err)

		t.Equal(base.BlockItemStatesTree, reader.Type())
		t.Equal("gz", reader.Reader().Format)

		v, err := reader.Decode()
		t.NoError(err)

		atr, ok := v.(fixedtree.Tree)
		t.True(ok)

		atr.Traverse(func(index uint64, a fixedtree.Node) (bool, error) {
			b := sttstree.Node(index)

			t.True(a.Equal(b))

			return true, nil
		})
	})

	t.Run("voteproofs", func() {
		cr := t.pick(base.BlockItemVoteproofs)

		f := NewDefaultItemReaderFunc(3)
		reader, err := f(base.BlockItemVoteproofs, t.Enc, cr)
		t.NoError(err)

		t.Equal(base.BlockItemVoteproofs, reader.Type())
		t.Equal("", reader.Reader().Format)

		v, err := reader.Decode()
		t.NoError(err)

		a, ok := v.([2]base.Voteproof)
		t.True(ok)

		b, ok := t.loadItem(base.BlockItemVoteproofs).([]interface{})
		t.True(ok)

		t.Equal(len(a), len(b))

		for i := range a {
			base.EqualVoteproof(t.Assert(), a[i], b[i].(base.Voteproof))
		}
	})
}

func (t *testItemReader) TestTee() {
	t.prepare()

	cr := t.pick(base.BlockItemMap)

	f := NewDefaultItemReaderFunc(3)
	reader, err := f(base.BlockItemMap, t.Enc, cr)
	t.NoError(err)

	buf := bytes.NewBuffer(nil)
	_, err = reader.Reader().Tee(buf, nil)
	t.NoError(err)

	t.Equal(base.BlockItemMap, reader.Type())
	t.Equal("", reader.Reader().Format)

	v, err := reader.Decode()
	t.NoError(err)

	i, ok := v.(base.BlockMap)
	t.True(ok)

	j, ok := t.loadItem(base.BlockItemMap).(base.BlockMap)
	t.True(ok)

	base.IsEqualBlockMap(j, i)

	of := t.openFile(base.BlockItemMap)
	defer of.Close()

	b, err := io.ReadAll(of)
	t.NoError(err)

	t.Equal(b, buf.Bytes())
}

func (t *testItemReader) TestDecodeItems() {
	t.prepare()

	t.Run("operations", func() {
		cr := t.pick(base.BlockItemOperations)

		f := NewDefaultItemReaderFunc(3)
		reader, err := f(base.BlockItemOperations, t.Enc, cr)
		t.NoError(err)

		t.Equal(base.BlockItemOperations, reader.Type())
		t.Equal("gz", reader.Reader().Format)

		var ops []base.Operation
		var once sync.Once
		count, err := reader.DecodeItems(func(total, index uint64, v interface{}) error {
			once.Do(func() {
				ops = make([]base.Operation, total)
			})

			ops[index] = v.(base.Operation)

			return nil
		})
		t.NoError(err)

		ops = ops[:count]

		b, ok := t.loadItem(base.BlockItemOperations).([]interface{})
		t.True(ok)

		t.Equal(len(b), len(ops))

		for i := range ops {
			bo, ok := b[i].(base.Operation)
			t.True(ok)

			base.EqualOperation(t.Assert(), bo, ops[i])
		}
	})

	t.Run("operations; bigger count", func() {
		{ // update count header
			f := t.openFile(base.BlockItemOperations)
			var u countItemsHeader

			gr, err := gzip.NewReader(f)
			t.NoError(err)

			br, err := isaac.LoadBlockItemFileHeader(gr, &u)
			t.NoError(err)

			t.T().Log("count:", u.Count)

			u.Count = u.Count * 2

			fname, err := DefaultBlockFileName(base.BlockItemOperations, t.Enc.Hint().Type())
			t.NoError(err)

			cf, err := os.OpenFile(filepath.Join(t.Root, isaac.BlockHeightDirectory(33), fname), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
			t.NoError(err)

			gw := gzip.NewWriter(cf)

			t.NoError(writeBaseHeader(gw, u))
			_, err = io.Copy(gw, br)
			t.NoError(err)

			t.NoError(gw.Close())
			t.NoError(cf.Close())
			t.NoError(f.Close())
		}

		cr := t.pick(base.BlockItemOperations)

		f := NewDefaultItemReaderFunc(3)
		reader, err := f(base.BlockItemOperations, t.Enc, cr)
		t.NoError(err)

		t.Equal(base.BlockItemOperations, reader.Type())
		t.Equal("gz", reader.Reader().Format)

		var ops []base.Operation
		var once sync.Once
		count, err := reader.DecodeItems(func(total, index uint64, v interface{}) error {
			once.Do(func() {
				t.T().Log("total:", total)
				ops = make([]base.Operation, total)
			})

			ops[index] = v.(base.Operation)

			return nil
		})
		t.NoError(err)
		t.T().Log("count:", count)

		ops = ops[:count]

		b, ok := t.loadItem(base.BlockItemOperations).([]interface{})
		t.True(ok)

		t.Equal(len(b), len(ops))

		for i := range ops {
			bo, ok := b[i].(base.Operation)
			t.True(ok)

			base.EqualOperation(t.Assert(), bo, ops[i])
		}
	})
}

func TestItemReader(t *testing.T) {
	suite.Run(t, new(testItemReader))
}
