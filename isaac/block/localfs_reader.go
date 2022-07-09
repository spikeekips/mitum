package isaacblock

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type LocalFSReader struct {
	enc      encoder.Encoder
	mapl     *util.Locked
	readersl *util.LockedMap
	itemsl   *util.LockedMap
	root     string
}

func NewLocalFSReader(root string, enc encoder.Encoder) (*LocalFSReader, error) {
	e := util.StringErrorFunc("failed to NewLocalFSReader")

	switch fi, err := os.Stat(filepath.Join(root, blockFSMapFilename(enc.Hint().Type().String()))); {
	case err != nil:
		return nil, e(err, "invalid root directory")
	case fi.IsDir():
		return nil, e(nil, "map file is directory")
	}

	return &LocalFSReader{
		root:     root,
		enc:      enc,
		mapl:     util.EmptyLocked(),
		readersl: util.NewLockedMap(),
		itemsl:   util.NewLockedMap(),
	}, nil
}

func NewLocalFSReaderFromHeight(baseroot string, height base.Height, enc encoder.Encoder) (*LocalFSReader, error) {
	return NewLocalFSReader(filepath.Join(baseroot, HeightDirectory(height)), enc)
}

func (r *LocalFSReader) Close() error {
	r.mapl = nil
	r.readersl = nil
	r.itemsl = nil

	return nil
}

func (r *LocalFSReader) BlockMap() (base.BlockMap, bool, error) {
	i, err := r.mapl.Get(func() (interface{}, error) {
		var b []byte

		switch f, err := os.Open(filepath.Join(r.root, blockFSMapFilename(r.enc.Hint().Type().String()))); {
		case err != nil:
			return nil, errors.WithStack(err)
		default:
			defer func() {
				_ = f.Close()
			}()

			i, err := io.ReadAll(f)
			if err != nil {
				return nil, errors.WithStack(err)
			}

			b = i
		}

		var um base.BlockMap
		if err := encoder.Decode(r.enc, b, &um); err != nil {
			return nil, err
		}

		return um, nil
	})

	e := util.StringErrorFunc("failed to load blockmap")

	switch {
	case err == nil:
		return i.(base.BlockMap), true, nil //nolint:forcetypeassert //...
	case os.IsNotExist(err):
		return nil, false, nil
	default:
		return nil, false, e(err, "")
	}
}

func (r *LocalFSReader) Reader(t base.BlockMapItemType) (io.ReadCloser, bool, error) {
	e := util.StringErrorFunc("failed to make reader, %q", t)

	var fpath string

	switch i, err := BlockFileName(t, r.enc.Hint().Type().String()); {
	case err != nil:
		return nil, false, e(err, "")
	default:
		fpath = filepath.Join(r.root, i)
	}

	i, _, _ := r.readersl.Get(t, func() (interface{}, error) {
		switch fi, err := os.Stat(fpath); {
		case err != nil:
			return err, nil //nolint:nilerr,wrapcheck //...
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

	f, err := os.Open(filepath.Clean(fpath))
	if err == nil {
		return f, true, nil
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

func (r *LocalFSReader) ChecksumReader(t base.BlockMapItemType) (util.ChecksumReader, bool, error) {
	e := util.StringErrorFunc("failed to make reader, %q", t)

	var fpath string

	switch i, err := BlockFileName(t, r.enc.Hint().Type().String()); {
	case err != nil:
		return nil, false, e(err, "")
	default:
		fpath = filepath.Join(r.root, i)
	}

	i, _, _ := r.readersl.Get(t, func() (interface{}, error) {
		switch fi, err := os.Stat(fpath); {
		case err != nil:
			return err, nil //nolint:nilerr,wrapcheck //...
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

func (r *LocalFSReader) Item(t base.BlockMapItemType) (item interface{}, found bool, _ error) {
	i, _, _ := r.itemsl.Get(t, func() (interface{}, error) {
		j, found, err := r.item(t)

		return [3]interface{}{j, found, err}, nil
	})

	l := i.([3]interface{}) //nolint:forcetypeassert //...

	var err error
	if l[2] != nil {
		err = l[2].(error) //nolint:forcetypeassert //...
	}

	return l[0], l[1].(bool), err //nolint:forcetypeassert //...
}

func (r *LocalFSReader) Items(f func(base.BlockMapItem, interface{}, bool, error) bool) error {
	var m base.BlockMap

	switch i, found, err := r.BlockMap(); {
	case err != nil:
		return err
	case !found:
		return util.ErrNotFound.Errorf("BlockMap not found")
	default:
		m = i
	}

	m.Items(func(item base.BlockMapItem) bool {
		i, found, err := r.Item(item.Type())

		return f(item, i, found, err)
	})

	return nil
}

func (r *LocalFSReader) item(t base.BlockMapItemType) (interface{}, bool, error) {
	e := util.StringErrorFunc("failed to load item, %q", t)

	var item base.BlockMapItem

	switch m, found, err := r.BlockMap(); {
	case err != nil || !found:
		return nil, found, e(err, "")
	default:
		if item, found = m.Item(t); !found {
			return nil, false, nil
		}
	}

	var f util.ChecksumReader

	switch i, found, err := r.ChecksumReader(t); {
	case err != nil:
		return nil, false, e(err, "")
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
		i, err = r.loadItem(item, f)
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

func (r *LocalFSReader) loadItem(item base.BlockMapItem, f io.Reader) (interface{}, error) {
	switch item.Type() {
	case base.BlockMapItemTypeProposal:
		var u base.ProposalSignedFact
		if err := encoder.DecodeReader(r.enc, f, &u); err != nil {
			return nil, err
		}

		return u, nil
	default:
		return nil, errors.Errorf("unsupported list items, %q", item.Type())
	}
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

func (r *LocalFSReader) loadOperations( //nolint:dupl //...
	item base.BlockMapItem, f io.Reader,
) ([]base.Operation, error) {
	if item.Num() < 1 {
		return nil, nil
	}

	ops := make([]base.Operation, item.Num())

	if err := LoadRawItemsWithWorker(f, item.Num(), r.enc.Decode, func(index uint64, v interface{}) error {
		op, ok := v.(base.Operation)
		if !ok {
			return errors.Errorf("not Operation, %T", v)
		}

		ops[index] = op

		return nil
	},
	); err != nil {
		return nil, err
	}

	return ops, nil
}

func (r *LocalFSReader) loadOperationsTree(item base.BlockMapItem, f io.Reader) (fixedtree.Tree, error) {
	tr, err := LoadTree(r.enc, item, f, func(i interface{}) (fixedtree.Node, error) {
		node, ok := i.(base.OperationFixedtreeNode)
		if !ok {
			return nil, errors.Errorf("not OperationFixedtreeNode, %T", i)
		}

		return node, nil
	})
	if err != nil {
		return fixedtree.Tree{}, errors.Wrap(err, "failed to load OperationsTree")
	}

	return tr, nil
}

func (r *LocalFSReader) loadStates( //nolint:dupl //...
	item base.BlockMapItem, f io.Reader,
) ([]base.State, error) {
	if item.Num() < 1 {
		return nil, nil
	}

	sts := make([]base.State, item.Num())

	if err := LoadRawItemsWithWorker(f, item.Num(), r.enc.Decode, func(index uint64, v interface{}) error {
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

func (r *LocalFSReader) loadStatesTree(item base.BlockMapItem, f io.Reader) (fixedtree.Tree, error) {
	tr, err := LoadTree(r.enc, item, f, func(i interface{}) (fixedtree.Node, error) {
		node, ok := i.(fixedtree.Node)
		if !ok {
			return nil, errors.Errorf("not StateFixedtreeNode, %T", i)
		}

		return node, nil
	})
	if err != nil {
		return fixedtree.Tree{}, errors.Wrap(err, "failed to load StatesTree")
	}

	return tr, nil
}

func (r *LocalFSReader) loadVoteproofs(item base.BlockMapItem, f io.Reader) ([]base.Voteproof, error) {
	if item.Num() < 1 {
		return nil, nil
	}

	vps, err := LoadVoteproofsFromReader(f, r.enc.Decode)
	if err != nil {
		return nil, err
	}

	return vps, nil
}

func LoadVoteproofsFromReader(
	r io.Reader,
	decode func([]byte) (interface{}, error),
) ([]base.Voteproof, error) {
	e := util.StringErrorFunc("failed to load voteproofs")

	vps := make([]base.Voteproof, 2)

	if err := LoadRawItemsWithWorker(r, 2, decode, func(_ uint64, v interface{}) error { //nolint:gomnd //...
		switch t := v.(type) {
		case base.INITVoteproof:
			vps[0] = t
		case base.ACCEPTVoteproof:
			vps[1] = t
		default:
			return errors.Errorf("not Operation, %T", v)
		}

		return nil
	}); err != nil {
		return nil, e(err, "")
	}

	if vps[0] == nil || vps[1] == nil {
		return nil, e(nil, "missing")
	}

	return vps, nil
}
