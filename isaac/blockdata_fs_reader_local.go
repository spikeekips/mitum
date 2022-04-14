package isaac

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/tree"
)

type LocalBlockDataFSReader struct {
	root     string
	enc      encoder.Encoder
	mapl     *util.Locked
	readersl *util.LockedMap
	itemsl   *util.LockedMap
}

func NewLocalBlockDataFSReader(
	root string,
	height base.Height,
	enc encoder.Encoder,
) (*LocalBlockDataFSReader, error) {
	e := util.StringErrorFunc("failed to NewLocalBlockDataFSReader")

	newroot, err := findValidBlockDataFSDirectory(root, height, enc)
	if err != nil {
		return nil, e(err, "")
	}

	return &LocalBlockDataFSReader{
		root:     newroot,
		enc:      enc,
		mapl:     util.EmptyLocked(),
		readersl: util.NewLockedMap(),
		itemsl:   util.NewLockedMap(),
	}, nil
}

func (r *LocalBlockDataFSReader) Map() (base.BlockDataMap, bool, error) {
	i, err := r.mapl.Get(func() (interface{}, error) {
		var b []byte
		switch f, err := os.Open(filepath.Join(r.root, blockDataFSMapFilename(r.enc))); {
		case err != nil:
			return nil, err
		default:
			defer func() {
				_ = f.Close()
			}()

			i, err := io.ReadAll(f)
			if err != nil {
				return nil, err
			}

			b = i
		}

		hinter, err := r.enc.Decode(b)
		if err != nil {
			return nil, err
		}

		um, ok := hinter.(base.BlockDataMap)
		if !ok {
			return nil, errors.Errorf("not blockdatamap, %T", hinter)
		}

		return um, nil
	})

	e := util.StringErrorFunc("failed to load blockdatamap")
	switch {
	case err == nil:
		return i.(base.BlockDataMap), true, nil
	case os.IsNotExist(err):
		return nil, false, nil
	default:
		return nil, false, e(err, "")
	}
}

func (r *LocalBlockDataFSReader) Reader(t base.BlockDataType) (io.ReadCloser, bool, error) {
	e := util.StringErrorFunc("failed to make reader, %q", t)

	var fpath string
	switch i, err := BlockDataFileName(t, r.enc); {
	case err != nil:
		return nil, false, e(err, "")
	default:
		fpath = filepath.Join(r.root, i)
	}

	i, _, _ := r.readersl.Get(t, func() (interface{}, error) {
		switch fi, err := os.Stat(fpath); {
		case err != nil:
			return err, nil // nolint:nilerr
		case fi.IsDir():
			return errors.Errorf("not normal file; directory"), nil
		default:
			return nil, nil
		}
	})

	if i != nil {
		switch err, ok := i.(error); {
		case !ok:
			return nil, false, nil
		case os.IsNotExist(err):
			return nil, false, nil
		default:
			return nil, false, e(err, "")
		}
	}

	var f io.ReadCloser

	rawf, err := os.Open(filepath.Clean(fpath))
	if err == nil {
		switch {
		case isCompressedBlockDataType(t):
			i, eerr := util.NewGzipReader(rawf)
			if eerr != nil {
				err = eerr
			}

			f = i
		default:
			f = rawf
		}
	}

	_ = r.readersl.SetValue(t, err)

	switch {
	case err == nil:
		return f, true, nil
	case os.IsNotExist(err):
		return nil, false, nil
	default:
		return nil, false, e(err, "")
	}
}

func (r *LocalBlockDataFSReader) Item(t base.BlockDataType) (interface{}, bool, error) {
	i, _, _ := r.itemsl.Get(t, func() (interface{}, error) {
		j, found, err := r.item(t)

		return [3]interface{}{j, found, err}, nil
	})

	l := i.([3]interface{})

	var err error
	if l[2] != nil {
		err = l[2].(error)
	}

	return l[0], l[1].(bool), err
}

func (r *LocalBlockDataFSReader) item(t base.BlockDataType) (interface{}, bool, error) {
	e := util.StringErrorFunc("failed to load item, %q", t)

	var item base.BlockDataMapItem
	switch m, found, err := r.Map(); {
	case err != nil || !found:
		return nil, found, e(err, "")
	default:
		if item, found = m.Item(t); !found {
			return nil, false, nil
		}
	}

	f, found, err := r.Reader(t)
	switch {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		defer func() {
			_ = f.Close()
		}()
	}

	if !isListBlockDataType(t) {
		i, eerr := r.loadItem(f)
		return i, true, eerr
	}

	i, eerr := r.loadItems(item, f)
	return i, true, eerr
}

func (r *LocalBlockDataFSReader) loadItem(f io.Reader) (interface{}, error) {
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	hinter, err := r.enc.Decode(b)
	switch {
	case err != nil:
		return nil, errors.Wrap(err, "")
	default:
		return hinter, nil
	}
}

func (r *LocalBlockDataFSReader) loadRawItems(f io.Reader, callback func(interface{}) (bool, error)) error {
	br := bufio.NewReader(f)

end:
	for {
		b, err := br.ReadBytes('\n')
		if len(b) > 0 {
			hinter, eerr := r.enc.Decode(b)
			if err != nil {
				return errors.Wrap(eerr, "")
			}

			switch keep, eerr := callback(hinter); {
			case eerr != nil:
				return errors.Wrap(err, "")
			case !keep:
				break end
			}
		}

		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			break end
		default:
			return errors.Wrap(err, "")
		}
	}

	return nil
}

func (r *LocalBlockDataFSReader) loadItems(item base.BlockDataMapItem, f io.Reader) (interface{}, error) {
	switch item.Type() {
	case base.BlockDataTypeOperations:
		return r.loadOperations(item, f)
	case base.BlockDataTypeOperationsTree:
		return r.loadOperationsTree(item, f)
	case base.BlockDataTypeStates:
		return r.loadStates(item, f)
	case base.BlockDataTypeStatesTree:
		return r.loadStatesTree(item, f)
	case base.BlockDataTypeVoteproofs:
		return r.loadVoteproofs(item, f)
	default:
		return nil, errors.Errorf("unsupported list items, %q", item.Type())
	}
}

func (r *LocalBlockDataFSReader) loadOperations(item base.BlockDataMapItem, f io.Reader) ([]base.Operation, error) {
	if item.Num() < 1 {
		return nil, nil
	}

	ops := make([]base.Operation, item.Num())

	i := -1
	if err := r.loadRawItems(f, func(v interface{}) (bool, error) {
		i++

		op, ok := v.(base.Operation)
		if !ok {
			return false, errors.Errorf("not Operation, %T", v)
		}

		ops[i] = op

		return true, nil
	}); err != nil {
		return nil, errors.Wrap(err, "")
	}

	return ops, nil
}

func (r *LocalBlockDataFSReader) loadOperationsTree(item base.BlockDataMapItem, f io.Reader) (tree.FixedTree, error) {
	tr, err := r.loadTree(item, f, func(i interface{}) (tree.FixedTreeNode, error) {
		node, ok := i.(base.OperationFixedTreeNode)
		if !ok {
			return nil, errors.Errorf("not OperationFixedTreeNode, %T", i)
		}

		return node, nil
	})
	if err != nil {
		return tree.FixedTree{}, errors.Wrap(err, "failed to load OperationsTree")
	}

	return tr, nil
}

func (r *LocalBlockDataFSReader) loadStates(item base.BlockDataMapItem, f io.Reader) ([]base.State, error) {
	if item.Num() < 1 {
		return nil, nil
	}

	ops := make([]base.State, item.Num())

	i := -1
	if err := r.loadRawItems(f, func(v interface{}) (bool, error) {
		i++

		op, ok := v.(base.State)
		if !ok {
			return false, errors.Errorf("not State, %T", v)
		}

		ops[i] = op

		return true, nil
	}); err != nil {
		return nil, errors.Wrap(err, "")
	}

	return ops, nil
}

func (r *LocalBlockDataFSReader) loadStatesTree(item base.BlockDataMapItem, f io.Reader) (tree.FixedTree, error) {
	tr, err := r.loadTree(item, f, func(i interface{}) (tree.FixedTreeNode, error) {
		node, ok := i.(base.StateFixedTreeNode)
		if !ok {
			return nil, errors.Errorf("not StateFixedTreeNode, %T", i)
		}

		return node, nil
	})
	if err != nil {
		return tree.FixedTree{}, errors.Wrap(err, "failed to load StatesTree")
	}

	return tr, nil
}

func (r *LocalBlockDataFSReader) loadVoteproofs(item base.BlockDataMapItem, f io.Reader) ([]base.Voteproof, error) {
	if item.Num() < 1 {
		return nil, nil
	}

	e := util.StringErrorFunc("failed to load voteproofs")

	vps := make([]base.Voteproof, 2)

	if err := r.loadRawItems(f, func(v interface{}) (bool, error) {
		switch t := v.(type) {
		case base.INITVoteproof:
			vps[0] = t
		case base.ACCEPTVoteproof:
			vps[1] = t
		default:
			return false, errors.Errorf("not Operation, %T", v)
		}

		return true, nil
	}); err != nil {
		return nil, e(err, "")
	}

	if vps[0] == nil || vps[1] == nil {
		return nil, e(nil, "missing")
	}

	return vps, nil
}

func (r *LocalBlockDataFSReader) loadTree(
	item base.BlockDataMapItem,
	f io.Reader,
	callback func(interface{}) (tree.FixedTreeNode, error),
) (tree.FixedTree, error) {
	if item.Num() < 1 {
		return tree.FixedTree{}, nil
	}

	e := util.StringErrorFunc("failed to load tree")

	tg := tree.NewFixedTreeGenerator(uint64(item.Num()))

	worker := util.NewErrgroupWorker(context.Background(), math.MaxInt32)
	defer worker.Close()

	if err := r.loadRawItems(f, func(v interface{}) (bool, error) {
		if err := worker.NewJob(func(context.Context, uint64) error {
			node, err := callback(v)
			if err != nil {
				return errors.Wrap(err, "")
			}

			if err := tg.Add(node); err != nil {
				return errors.Wrap(err, "")
			}

			return nil
		}); err != nil {
			return false, errors.Wrap(err, "")
		}

		return true, nil
	}); err != nil {
		return tree.FixedTree{}, e(err, "")
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return tree.FixedTree{}, e(err, "")
	}

	switch tr, err := tg.Tree(); {
	case err != nil:
		return tree.FixedTree{}, e(err, "")
	default:
		return tr, nil
	}
}

func findValidBlockDataFSDirectory(root string, height base.Height, enc encoder.Encoder) (string, error) {
	e := util.StringErrorFunc("valid block data fs directory not found")

	matches, err := filepath.Glob(filepath.Join(root, HeightDirectory(height)) + "/*")
	switch {
	case err != nil:
		return "", e(err, "")
	case len(matches) < 1:
		return "", e(nil, "no matches")
	}

	sort.Slice(matches, func(i, j int) bool {
		return strings.Compare(matches[i], matches[j]) > 0
	})

	var found string
	for i := range matches {
		s := matches[i]
		if checkValidBlockDataFSDirectory(s, enc) {
			found = s

			break
		}
	}

	if len(found) < 1 {
		return "", e(nil, "not found")
	}

	return found, nil
}

func checkValidBlockDataFSDirectory(s string, enc encoder.Encoder) bool {
	switch fi, err := os.Stat(filepath.Join(s, blockDataFSMapFilename(enc))); {
	case err != nil:
		return false
	case fi.IsDir():
		return false
	default:
		return true
	}
}

func blockDataFSMapFilename(enc encoder.Encoder) string {
	return fmt.Sprintf("%s%s", blockDataMapFilename, fileExtFromEncoder(enc))
}
