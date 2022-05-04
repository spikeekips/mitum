package isaacblock

import (
	"bufio"
	"context"
	"crypto/sha256"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/tree"
)

type LocalFSReader struct {
	root     string
	enc      encoder.Encoder
	mapl     *util.Locked
	readersl *util.LockedMap
	itemsl   *util.LockedMap
}

func NewLocalFSReader(
	baseroot string,
	height base.Height,
	enc encoder.Encoder,
) (*LocalFSReader, error) {
	e := util.StringErrorFunc("failed to NewLocalFSReader")

	heightroot := filepath.Join(baseroot, HeightDirectory(height))
	switch fi, err := os.Stat(filepath.Join(heightroot, blockFSMapFilename(enc))); {
	case err != nil:
		return nil, e(err, "invalid root directory")
	case fi.IsDir():
		return nil, e(nil, "map file is directory")
	}

	return &LocalFSReader{
		root:     heightroot,
		enc:      enc,
		mapl:     util.EmptyLocked(),
		readersl: util.NewLockedMap(),
		itemsl:   util.NewLockedMap(),
	}, nil
}

func (r *LocalFSReader) Map() (base.BlockMap, bool, error) {
	i, err := r.mapl.Get(func() (interface{}, error) {
		var b []byte
		switch f, err := os.Open(filepath.Join(r.root, blockFSMapFilename(r.enc))); {
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

		um, ok := hinter.(base.BlockMap)
		if !ok {
			return nil, errors.Errorf("not blockmap, %T", hinter)
		}

		return um, nil
	})

	e := util.StringErrorFunc("failed to load blockmap")
	switch {
	case err == nil:
		return i.(base.BlockMap), true, nil
	case os.IsNotExist(err):
		return nil, false, nil
	default:
		return nil, false, e(err, "")
	}
}

func (r *LocalFSReader) Reader(t base.BlockMapItemType) (util.ChecksumReader, bool, error) {
	e := util.StringErrorFunc("failed to make reader, %q", t)

	var fpath string
	switch i, err := BlockFileName(t, r.enc); {
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

	var f util.ChecksumReader

	rawf, err := os.Open(filepath.Clean(fpath))
	if err == nil {
		cr := util.NewHashChecksumReader(rawf, sha256.New())
		switch {
		case isCompressedBlockMapItemType(t):
			gr, eerr := util.NewGzipReader(cr)
			if eerr != nil {
				err = eerr
			}

			f = util.NewDummyChecksumReader(gr, cr)
		default:
			f = cr
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

func (r *LocalFSReader) Item(t base.BlockMapItemType) (interface{}, bool, error) {
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

func (r *LocalFSReader) item(t base.BlockMapItemType) (interface{}, bool, error) {
	e := util.StringErrorFunc("failed to load item, %q", t)

	var item base.BlockMapItem
	switch m, found, err := r.Map(); {
	case err != nil || !found:
		return nil, found, e(err, "")
	default:
		if item, found = m.Item(t); !found {
			return nil, false, nil
		}
	}

	var f util.ChecksumReader
	switch i, found, err := r.Reader(t); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		defer func() {
			_ = f.Close()
		}()

		f = i
	}

	var i interface{}
	var err error
	switch {
	case !isListBlockMapItemType(t):
		i, err = r.loadItem(f)
	default:
		i, err = r.loadItems(item, f)
	}

	switch {
	case err != nil:
		return i, true, e(err, "")
	case item.Checksum() != f.Checksum():
		return i, true, e(nil, "checksum mismatch; item=%q != file=%q", item.Checksum(), f.Checksum())
	default:
		return i, true, err
	}
}

func (r *LocalFSReader) loadItem(f io.Reader) (interface{}, error) {
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

func (r *LocalFSReader) loadRawItems(f io.Reader, callback func(interface{}) (bool, error)) error {
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

func (r *LocalFSReader) loadItems(item base.BlockMapItem, f io.Reader) (interface{}, error) {
	switch item.Type() {
	case base.BlockMapItemTypeOperations:
		return r.loadOperations(item, f)
	case base.BlockMapItemTypeOperationsTree:
		return r.loadOperationsTree(item, f)
	case base.BlockMapItemTypeStates:
		return r.loadStates(item, f)
	case base.BlockMapItemTypeStatesTree:
		return r.loadStatesTree(item, f)
	case base.BlockMapItemTypeVoteproofs:
		return r.loadVoteproofs(item, f)
	default:
		return nil, errors.Errorf("unsupported list items, %q", item.Type())
	}
}

func (r *LocalFSReader) loadOperations(item base.BlockMapItem, f io.Reader) ([]base.Operation, error) {
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

func (r *LocalFSReader) loadOperationsTree(item base.BlockMapItem, f io.Reader) (tree.Fixedtree, error) {
	tr, err := r.loadTree(item, f, func(i interface{}) (tree.FixedtreeNode, error) {
		node, ok := i.(base.OperationFixedtreeNode)
		if !ok {
			return nil, errors.Errorf("not OperationFixedtreeNode, %T", i)
		}

		return node, nil
	})
	if err != nil {
		return tree.Fixedtree{}, errors.Wrap(err, "failed to load OperationsTree")
	}

	return tr, nil
}

func (r *LocalFSReader) loadStates(item base.BlockMapItem, f io.Reader) ([]base.State, error) {
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

func (r *LocalFSReader) loadStatesTree(item base.BlockMapItem, f io.Reader) (tree.Fixedtree, error) {
	tr, err := r.loadTree(item, f, func(i interface{}) (tree.FixedtreeNode, error) {
		node, ok := i.(base.StateFixedtreeNode)
		if !ok {
			return nil, errors.Errorf("not StateFixedtreeNode, %T", i)
		}

		return node, nil
	})
	if err != nil {
		return tree.Fixedtree{}, errors.Wrap(err, "failed to load StatesTree")
	}

	return tr, nil
}

func (r *LocalFSReader) loadVoteproofs(item base.BlockMapItem, f io.Reader) ([]base.Voteproof, error) {
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

func (r *LocalFSReader) loadTree(
	item base.BlockMapItem,
	f io.Reader,
	callback func(interface{}) (tree.FixedtreeNode, error),
) (tree.Fixedtree, error) {
	if item.Num() < 1 {
		return tree.Fixedtree{}, nil
	}

	e := util.StringErrorFunc("failed to load tree")

	tg := tree.NewFixedtreeGenerator(item.Num())

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
		return tree.Fixedtree{}, e(err, "")
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return tree.Fixedtree{}, e(err, "")
	}

	switch tr, err := tg.Tree(); {
	case err != nil:
		return tree.Fixedtree{}, e(err, "")
	default:
		return tr, nil
	}
}
