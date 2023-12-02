package isaacblock

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
	"golang.org/x/sync/singleflight"
)

type NewItemReaderFunc func(
	base.BlockItemType,
	encoder.Encoder,
	*util.CompressedReader,
) (isaac.BlockItemReader, error)

type Readers struct {
	*hint.CompatibleSet[NewItemReaderFunc]
	encs             *encoder.Encoders
	bfilescache      *util.GCache[base.Height, base.BlockItemFiles]
	decompressReader util.DecompressReaderFunc
	bfilessg         singleflight.Group
	root             string
}

func NewReaders(
	root string,
	encs *encoder.Encoders,
	decompressReader util.DecompressReaderFunc,
) *Readers {
	if decompressReader == nil {
		decompressReader = util.DefaultDecompressReaderFunc //revive:disable-line:modifies-parameter
	}

	return &Readers{
		CompatibleSet:    hint.NewCompatibleSet[NewItemReaderFunc](8), //nolint:gomnd //...
		root:             root,
		encs:             encs,
		decompressReader: decompressReader,
		bfilescache:      util.NewLFUGCache[base.Height, base.BlockItemFiles](1 << 9), //nolint:gomnd //...
	}
}

func (rs *Readers) Add(writerhint hint.Hint, v NewItemReaderFunc) error {
	return rs.CompatibleSet.Add(writerhint, v)
}

func (rs *Readers) Item(
	height base.Height,
	t base.BlockItemType,
	callback func(isaac.BlockItemReader) error,
) (bool, error) {
	var bfile base.BlockItemFile

	switch i, found, err := rs.ItemFile(height, t); {
	case err != nil, !found:
		return found, err
	default:
		bfile = i
	}

	var f *os.File

	switch i, found, err := rs.readFile(height, bfile); {
	case err != nil, !found:
		return found, err
	default:
		defer func() {
			_ = i.Close()
		}()

		f = i
	}

	var readerf NewItemReaderFunc
	var enc encoder.Encoder

	switch i, e, found, err := rs.findBlockReader(bfile, f); {
	case err != nil, !found:
		return found, err
	default:
		readerf = i
		enc = e
	}

	var cr *util.CompressedReader

	switch i, err := util.NewCompressedReader(f, bfile.CompressFormat(), rs.decompressReader); {
	case err != nil:
		return true, err
	default:
		defer func() {
			_ = i.Close()
		}()

		cr = i
	}

	switch i, err := readerf(t, enc, cr); {
	case err != nil:
		return true, err
	default:
		return true, callback(i)
	}
}

func (rs *Readers) ItemFiles(height base.Height) (base.BlockItemFiles, bool, error) {
	i, err, _ := util.SingleflightDo[[2]interface{}](
		&rs.bfilessg,
		height.String(),
		func() (ii [2]interface{}, _ error) {
			if i, found := rs.bfilescache.Get(height); found {
				return [2]interface{}{i, found}, nil
			}

			switch i, found, err := LoadBlockItemFilesPath(rs.root, height, rs.encs.JSON()); {
			case err != nil:
				return ii, err
			default:
				if found {
					rs.bfilescache.Set(height, i, 0)
				}

				return [2]interface{}{i, found}, nil
			}
		},
	)

	switch {
	case err != nil:
		return nil, false, err
	case !i[1].(bool): //nolint:forcetypeassert //...
		return nil, false, nil
	default:
		return i[0].(base.BlockItemFiles), true, nil //nolint:forcetypeassert //...
	}
}

func (rs *Readers) ItemFile(height base.Height, t base.BlockItemType) (base.BlockItemFile, bool, error) {
	switch bfiles, found, err := rs.ItemFiles(height); {
	case err != nil, !found:
		return nil, found, err
	default:
		i, found := bfiles.Item(t)

		return i, found, nil
	}
}

func (rs *Readers) readFile(height base.Height, bfile base.BlockItemFile) (*os.File, bool, error) {
	var p string

	switch u := bfile.URI(); u.Scheme {
	case LocalFSBlockItemScheme:
		p = filepath.Join(rs.root, HeightDirectory(height), u.Path)
	case "file":
		p = u.Path
	default:
		return nil, false, nil
	}

	switch i, err := os.Open(p); {
	case os.IsNotExist(err):
		return nil, false, nil
	case err != nil:
		return nil, false, errors.WithStack(err)
	default:
		return i, true, nil
	}
}

func (rs *Readers) findBlockReader(bfile base.BlockItemFile, f *os.File) (
	NewItemReaderFunc,
	encoder.Encoder,
	bool,
	error,
) {
	var dr io.Reader

	switch i, err := rs.decompressReader(bfile.CompressFormat()); {
	case err != nil:
		return nil, nil, false, err
	default:
		j, err := i(f)
		if err != nil {
			return nil, nil, false, err
		}

		dr = j
	}

	switch _, writerhint, enchint, err := loadBaseHeader(dr); {
	case errors.Is(err, io.EOF):
		return nil, nil, false, nil
	case err != nil:
		return nil, nil, false, err
	default:
		if _, err := f.Seek(0, 0); err != nil {
			return nil, nil, false, errors.WithStack(err)
		}

		r, found := rs.Find(writerhint)
		if !found {
			return nil, nil, false, nil
		}

		e, found := rs.encs.Find(enchint)
		if !found {
			return nil, nil, false, nil
		}

		return r, e, true, nil
	}
}

func LoadTree(
	enc encoder.Encoder,
	count uint64,
	treehint hint.Hint,
	f io.Reader,
	callback func(interface{}) (fixedtree.Node, error),
) (tr fixedtree.Tree, err error) {
	if count < 1 {
		return tr, nil
	}

	e := util.StringError("load tree")

	br := bufio.NewReader(f)

	nodes := make([]fixedtree.Node, count)
	if tr, err = fixedtree.NewTree(treehint, nodes); err != nil {
		return tr, e.Wrap(err)
	}

	if err := DecodeLineItemsWithWorker(
		br,
		count,
		func(b []byte) (interface{}, error) {
			return unmarshalIndexedTreeNode(enc, b, treehint)
		},
		func(_ uint64, v interface{}) error {
			in := v.(indexedTreeNode) //nolint:forcetypeassert //...
			n, err := callback(in.Node)
			if err != nil {
				return err
			}

			return tr.Set(in.Index, n)
		},
	); err != nil {
		return tr, e.Wrap(err)
	}

	return tr, nil
}

func DecodeLineItems(
	f io.Reader,
	decode func([]byte) (interface{}, error),
	callback func(uint64, interface{}) error,
) error {
	var br *bufio.Reader
	if i, ok := f.(*bufio.Reader); ok {
		br = i
	} else {
		br = bufio.NewReader(f)
	}

	var index uint64
end:
	for {
		b, err := br.ReadBytes('\n')

		switch {
		case err != nil && !errors.Is(err, io.EOF):
			return errors.WithStack(err)
		case len(b) < 1,
			bytes.HasPrefix(b, []byte("# ")):
		default:
			v, eerr := decode(b)
			if eerr != nil {
				return errors.WithStack(eerr)
			}

			if eerr := callback(index, v); eerr != nil {
				return eerr
			}

			index++
		}

		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			break end
		default:
			return errors.WithStack(err)
		}
	}

	return nil
}

func DecodeLineItemsWithWorker(
	f io.Reader,
	num uint64,
	decode func([]byte) (interface{}, error),
	callback func(uint64, interface{}) error,
) error {
	worker, err := util.NewErrgroupWorker(context.Background(), int64(num))
	if err != nil {
		return err
	}

	defer worker.Close()

	if err := DecodeLineItems(f, decode, func(index uint64, v interface{}) error {
		return worker.NewJob(func(ctx context.Context, _ uint64) error {
			return callback(index, v)
		})
	}); err != nil {
		return err
	}

	worker.Done()

	return worker.Wait()
}

func LoadTreeHint(br *bufio.Reader) (ht hint.Hint, _ error) {
end:
	for {
		s, err := br.ReadString('\n')

		switch {
		case err != nil:
			return ht, errors.WithStack(err)
		case len(s) < 1:
			continue end
		}

		ht, err = hint.ParseHint(s)
		if err != nil {
			return ht, errors.Wrap(err, "load tree hint")
		}

		if err := ht.IsValid(nil); err != nil {
			return ht, errors.Wrap(err, "load tree hint")
		}

		return ht, nil
	}
}

func LoadBlockItemFilesPath(
	root string,
	height base.Height,
	jsonenc encoder.Encoder,
) (bfiles base.BlockItemFiles, found bool, _ error) {
	switch i, err := os.Open(BlockItemFilesPath(root, height)); {
	case os.IsNotExist(err):
		return nil, false, nil
	case err != nil:
		return nil, false, errors.WithStack(err)
	default:
		defer func() {
			_ = i.Close()
		}()

		if err := encoder.DecodeReader(jsonenc, i, &bfiles); err != nil {
			return nil, false, err
		}

		return bfiles, true, nil
	}
}
