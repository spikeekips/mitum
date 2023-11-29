package isaacblock

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
)

type LocalFSReader struct {
	enc      encoder.Encoder
	bm       base.BlockMap
	bfiles   base.BlockItemFiles
	readersl *util.ShardedMap[base.BlockItemType, error]
	itemsl   *util.ShardedMap[base.BlockItemType, [3]interface{}]
	root     string
}

func NewLocalFSReader(root string, enc encoder.Encoder) (*LocalFSReader, error) {
	readersl, _ := util.NewShardedMap[base.BlockItemType, error](6, nil)        //nolint:gomnd //...
	itemsl, _ := util.NewShardedMap[base.BlockItemType, [3]interface{}](6, nil) //nolint:gomnd //...

	r := &LocalFSReader{
		root:     root,
		enc:      enc,
		readersl: readersl,
		itemsl:   itemsl,
	}

	switch i, found, err := r.loadBlockMap(); {
	case err != nil:
		return nil, err
	case !found:
		return nil, util.ErrNotFound.Errorf("blockmap")
	default:
		r.bm = i
	}

	switch i, found, err := r.loadBlockItemFiles(); {
	case err != nil:
		return nil, err
	case !found:
		return nil, util.ErrNotFound.Errorf("block item files")
	default:
		r.bfiles = i
	}

	return r, nil
}

func NewLocalFSReaderFromHeight(baseroot string, height base.Height, enc encoder.Encoder) (*LocalFSReader, error) {
	return NewLocalFSReader(filepath.Join(baseroot, HeightDirectory(height)), enc)
}

func (r *LocalFSReader) Close() error {
	r.readersl.Close()
	r.itemsl.Close()

	return nil
}

func (r *LocalFSReader) BlockMap() (base.BlockMap, bool, error) {
	return r.bm, true, nil
}

func (r *LocalFSReader) BlockItemFiles() (base.BlockItemFiles, bool, error) {
	return r.bfiles, true, nil
}

func (r *LocalFSReader) Reader(t base.BlockItemType) (io.ReadCloser, bool, error) {
	e := util.StringError("make reader, %q", t)

	var fpath string

	switch i, err := BlockFileName(t, r.enc.Hint().Type().String()); {
	case err != nil:
		return nil, false, e.Wrap(err)
	default:
		fpath = filepath.Join(r.root, i)
	}

	switch err := r.readersl.GetOrCreate(
		t,
		func(i error, _ bool) error {
			return i
		},
		func() (error, error) {
			switch fi, err := os.Stat(fpath); {
			case err != nil:
				return err, nil //nolint:nilerr,wrapcheck //...
			case fi.IsDir():
				return errors.Errorf("not normal file; directory"), nil
			default:
				return nil, nil
			}
		},
	); {
	case err == nil:
	case os.IsNotExist(err):
		return nil, false, nil
	default:
		return nil, false, e.Wrap(err)
	}

	f, err := os.Open(filepath.Clean(fpath))

	_ = r.readersl.SetValue(t, err)

	switch {
	case err == nil:
		return f, true, nil
	case os.IsNotExist(err):
		return nil, false, nil
	default:
		return nil, false, e.Wrap(err)
	}
}

func (r *LocalFSReader) UncompressedReader(t base.BlockItemType) (io.ReadCloser, bool, error) {
	var f io.ReadCloser

	switch i, found, err := r.Reader(t); {
	case err != nil, !found:
		return nil, found, err
	default:
		f = i
	}

	if isCompressedBlockMapItemType(t) {
		switch i, err := util.NewSafeGzipReadCloser(f); {
		case err != nil:
			return nil, false, err
		default:
			f = i
		}
	}

	return util.NewHashChecksumReader(f, sha256.New()), true, nil
}

func (r *LocalFSReader) ChecksumReader(t base.BlockItemType) (util.ChecksumReader, bool, error) {
	e := util.StringError("make reader, %q", t)

	var fpath string

	switch i, err := BlockFileName(t, r.enc.Hint().Type().String()); {
	case err != nil:
		return nil, false, e.Wrap(err)
	default:
		fpath = filepath.Join(r.root, i)
	}

	switch err := r.readersl.GetOrCreate(
		t,
		func(i error, _ bool) error {
			return i
		},
		func() (error, error) {
			switch fi, err := os.Stat(fpath); {
			case err != nil:
				return err, nil //nolint:nilerr,wrapcheck //...
			case fi.IsDir():
				return errors.Errorf("not normal file; directory"), nil
			default:
				return nil, nil
			}
		},
	); {
	case err == nil:
	case os.IsNotExist(err):
		return nil, false, nil
	default:
		return nil, false, e.Wrap(err)
	}

	switch i, err := func() (io.ReadCloser, error) {
		var f io.ReadCloser

		switch i, err := os.Open(filepath.Clean(fpath)); {
		case err != nil:
			return nil, err //nolint:wrapcheck //...
		default:
			f = i
		}

		if isCompressedBlockMapItemType(t) {
			switch i, err := util.NewSafeGzipReadCloser(f); {
			case err != nil:
				return nil, err
			default:
				f = i
			}
		}

		return f, nil
	}(); {
	case os.IsNotExist(err):
		_ = r.readersl.SetValue(t, err)

		return nil, false, nil
	case err != nil:
		_ = r.readersl.SetValue(t, err)

		return nil, false, e.Wrap(err)
	default:
		_ = r.readersl.SetValue(t, nil)

		return util.NewHashChecksumReader(i, sha256.New()), true, nil
	}
}

func (r *LocalFSReader) Item(t base.BlockItemType) (item interface{}, found bool, _ error) {
	err := r.itemsl.GetOrCreate(
		t,
		func(i [3]interface{}, _ bool) error {
			item = i[0]
			found = i[1].(bool) //nolint:forcetypeassert //...

			var err error

			if i[2] != nil {
				err = i[2].(error) //nolint:forcetypeassert //...
			}

			return err
		},
		func() ([3]interface{}, error) {
			i, j, k := r.item(t)

			return [3]interface{}{i, j, k}, nil
		},
	)

	return item, found, err
}

func (r *LocalFSReader) Items(f func(base.BlockMapItem, interface{}, bool, error) bool) error {
	r.bm.Items(func(item base.BlockMapItem) bool {
		i, found, err := r.Item(item.Type())

		return f(item, i, found, err)
	})

	return nil
}

func (r *LocalFSReader) loadBlockMap() (base.BlockMap, bool, error) {
	var f io.ReadCloser

	switch i, found, err := r.Reader(base.BlockItemMap); {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	default:
		defer func() {
			_ = i.Close()
		}()

		f = i
	}

	switch i, err := r.loadItem(f); {
	case err != nil:
		return nil, true, err
	default:
		bm, ok := i.(base.BlockMap)
		if !ok {
			return nil, true, errors.Errorf("expected blockmap, but %T", i)
		}

		return bm, true, nil
	}
}

func (r *LocalFSReader) loadBlockItemFiles() (base.BlockItemFiles, bool, error) {
	var f io.ReadCloser

	fpath := filepath.Join(filepath.Dir(r.root), base.BlockItemFilesName(r.bm.Manifest().Height()))

	switch i, err := os.Open(filepath.Clean(fpath)); {
	case os.IsNotExist(err):
		return nil, false, nil
	case err != nil:
		return nil, false, errors.WithStack(err)
	default:
		defer func() {
			_ = i.Close()
		}()

		f = i
	}

	var u base.BlockItemFiles
	if err := encoder.DecodeReader(r.enc, f, &u); err != nil {
		return nil, true, err
	}

	return u, true, nil
}

func (r *LocalFSReader) item(t base.BlockItemType) (interface{}, bool, error) {
	e := util.StringError("load item, %q", t)

	var item base.BlockMapItem

	switch i, found := r.bm.Item(t); {
	case !found:
		return nil, false, nil
	default:
		item = i
	}

	var f util.ChecksumReader

	switch i, found, err := r.ChecksumReader(t); {
	case err != nil:
		return nil, false, e.Wrap(err)
	case !found:
		return nil, false, nil
	default:
		defer func() {
			_ = i.Close()
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
		return i, true, e.Wrap(err)
	case item.Checksum() != f.Checksum():
		return i, true, e.Errorf("checksum mismatch; item=%q != file=%q", item.Checksum(), f.Checksum())
	default:
		return i, true, err
	}
}

func (r *LocalFSReader) loadItem(f io.Reader) (u interface{}, _ error) {
	br := f

	switch i, _, _, err := readBaseHeader(br); {
	case err != nil:
		return nil, err
	default:
		br = i
	}

	if err := encoder.DecodeReader(r.enc, br, &u); err != nil {
		return nil, err
	}

	return u, nil
}

func (r *LocalFSReader) loadItems(item base.BlockMapItem, f io.Reader) (interface{}, error) {
	switch item.Type() {
	case base.BlockItemOperations:
		return r.loadOperations(f)
	case base.BlockItemOperationsTree:
		return r.loadOperationsTree(f)
	case base.BlockItemStates:
		return r.loadStates(f)
	case base.BlockItemStatesTree:
		return r.loadStatesTree(f)
	case base.BlockItemVoteproofs:
		return r.loadVoteproofs(f)
	default:
		return nil, errors.Errorf("unsupported list items, %q", item.Type())
	}
}

func (r *LocalFSReader) loadOperations(f io.Reader) ([]base.Operation, error) {
	var ops []base.Operation
	var count uint64
	br := f

	switch i, _, _, j, err := readCountHeader(br); {
	case err != nil:
		return nil, err
	case j < 1:
		return nil, nil
	default:
		br = i
		count = j
		ops = make([]base.Operation, count)
	}

	var last uint64

	if err := LoadRawItemsWithWorker(br, count, r.enc.Decode, func(index uint64, v interface{}) error {
		op, ok := v.(base.Operation)
		if !ok {
			return errors.Errorf("not Operation, %T", v)
		}

		ops[index] = op

		atomic.AddUint64(&last, 1)

		return nil
	},
	); err != nil {
		return nil, err
	}

	switch i := atomic.LoadUint64(&last); {
	case i < 1:
		return nil, nil
	default:
		return ops[:i], nil
	}
}

func (r *LocalFSReader) loadOperationsTree(f io.Reader) (fixedtree.Tree, error) {
	switch i, err := r.loadTree(f); {
	case err != nil:
		return fixedtree.Tree{}, errors.Wrap(err, "load OperationsTree")
	default:
		return i, nil
	}
}

func (r *LocalFSReader) loadStates(f io.Reader) ([]base.State, error) {
	var count uint64
	var sts []base.State
	br := f

	switch i, _, _, j, err := readCountHeader(br); {
	case err != nil:
		return nil, err
	case j < 1:
		return nil, nil
	default:
		count = j
		sts = make([]base.State, count)
		br = i
	}

	if err := LoadRawItemsWithWorker(br, count, r.enc.Decode, func(index uint64, v interface{}) error {
		st, ok := v.(base.State)
		if !ok {
			return errors.Errorf("expected State, but %T", v)
		}

		sts[index] = st

		return nil
	}); err != nil {
		return nil, err
	}

	return sts, nil
}

func (r *LocalFSReader) loadStatesTree(f io.Reader) (fixedtree.Tree, error) {
	switch i, err := r.loadTree(f); {
	case err != nil:
		return fixedtree.Tree{}, errors.Wrap(err, "load StatesTree")
	default:
		return i, nil
	}
}

func (r *LocalFSReader) loadTree(f io.Reader) (fixedtree.Tree, error) {
	br := f
	var count uint64
	var treehint hint.Hint

	switch i, _, _, j, k, err := readTreeHeader(br); {
	case err != nil:
		return fixedtree.Tree{}, err
	case j < 1:
		return fixedtree.Tree{}, nil
	default:
		br = i
		count = j
		treehint = k
	}

	return LoadTree(r.enc, count, treehint, br, func(i interface{}) (fixedtree.Node, error) {
		node, ok := i.(fixedtree.Node)
		if !ok {
			return nil, errors.Errorf("not fixedtree.Node, %T", i)
		}

		return node, nil
	})
}

func (r *LocalFSReader) loadVoteproofs(f io.Reader) (vps [2]base.Voteproof, _ error) {
	br := f

	switch i, _, _, err := readBaseHeader(br); {
	case err != nil:
		return vps, err
	default:
		br = i
	}

	switch i, err := LoadVoteproofsFromReader(br, r.enc.Decode); {
	case err != nil:
		return vps, err
	default:
		return i, nil
	}
}

func LoadVoteproofsFromReader(
	r io.Reader,
	decode func([]byte) (interface{}, error),
) (vps [2]base.Voteproof, _ error) {
	e := util.StringError("load voteproofs")

	if err := LoadRawItemsWithWorker(r, 2, decode, func(i uint64, v interface{}) error { //nolint:gomnd //...
		switch t := v.(type) {
		case base.INITVoteproof:
			vps[0] = t
		case base.ACCEPTVoteproof:
			vps[1] = t
		default:
			return errors.Errorf("not voteproof, %T", v)
		}

		return nil
	}); err != nil {
		return vps, e.Wrap(err)
	}

	if vps[0] == nil || vps[1] == nil {
		return vps, e.Errorf("missing")
	}

	return vps, nil
}
