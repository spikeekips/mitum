package isaacblock

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"golang.org/x/sync/singleflight"
)

type ItemReaderFunc func(
	base.Height,
	base.BlockItemType,
	func(isaac.BlockItemReader) error,
) (bool, error)

type NewItemReaderFunc func(
	base.BlockItemType,
	encoder.Encoder,
	*util.CompressedReader,
) (isaac.BlockItemReader, error)

func ReadersDecode[T any](
	readers *Readers,
	height base.Height,
	t base.BlockItemType,
	f func(ir isaac.BlockItemReader) error,
) (target T, _ bool, _ error) {
	irf := readersDecodeFunc[T](f)

	switch found, err := readers.Item(height, t, func(ir isaac.BlockItemReader) error {
		switch i, err := irf(ir); {
		case err != nil:
			return err
		default:
			target = i

			return nil
		}
	}); {
	case err != nil, !found:
		return target, found, err
	default:
		return target, true, nil
	}
}

func ReadersDecodeItems[T any](
	readers *Readers,
	height base.Height,
	t base.BlockItemType,
	item func(uint64, uint64, T) error,
	f func(ir isaac.BlockItemReader) error,
) (count uint64, l []T, found bool, _ error) {
	irf, countf := readersDecodeItemsFuncs(item, f)

	switch found, err := readers.Item(height, t, func(ir isaac.BlockItemReader) error {
		return irf(ir)
	}); {
	case err != nil, !found:
		return 0, nil, found, err
	default:
		count, l = countf()

		return count, l, true, nil
	}
}

func ReadersDecodeFromReader[T any](
	readers *Readers,
	t base.BlockItemType,
	r io.Reader,
	compressFormat string,
	f func(ir isaac.BlockItemReader) error,
) (target T, _ bool, _ error) {
	irf := readersDecodeFunc[T](f)

	switch found, err := readers.ItemFromReader(t, r, compressFormat, func(ir isaac.BlockItemReader) error {
		switch i, err := irf(ir); {
		case err != nil:
			return err
		default:
			target = i

			return nil
		}
	}); {
	case err != nil, !found:
		return target, found, err
	default:
		return target, true, nil
	}
}

func ReadersDecodeItemsFromReader[T any](
	readers *Readers,
	t base.BlockItemType,
	r io.Reader,
	compressFormat string,
	item func(uint64, uint64, T) error,
	f func(ir isaac.BlockItemReader) error,
) (count uint64, l []T, found bool, _ error) {
	irf, countf := readersDecodeItemsFuncs(item, f)

	switch found, err := readers.ItemFromReader(t, r, compressFormat, func(ir isaac.BlockItemReader) error {
		return irf(ir)
	}); {
	case err != nil, !found:
		return 0, nil, found, err
	default:
		count, l = countf()

		return count, l, true, nil
	}
}

func readersDecodeFunc[T any](
	f func(ir isaac.BlockItemReader) error,
) func(isaac.BlockItemReader) (T, error) {
	if f == nil {
		f = func(ir isaac.BlockItemReader) error { return nil } //revive:disable-line:modifies-parameter
	}

	return func(ir isaac.BlockItemReader) (target T, _ error) {
		if err := f(ir); err != nil {
			return target, err
		}

		switch i, err := ir.Decode(); {
		case err != nil:
			return target, err
		default:
			v, ok := i.(T)
			if !ok {
				return target, errors.Errorf("expected %T, but %T", target, i)
			}

			return v, nil
		}
	}
}

func readersDecodeItemsFuncs[T any](
	item func(uint64, uint64, T) error,
	f func(ir isaac.BlockItemReader) error,
) (
	func(ir isaac.BlockItemReader) error,
	func() (uint64, []T),
) {
	if item == nil {
		item = func(uint64, uint64, T) error { return nil } //revive:disable-line:modifies-parameter
	}

	if f == nil {
		f = func(ir isaac.BlockItemReader) error { return nil } //revive:disable-line:modifies-parameter
	}

	var l []T
	var once sync.Once
	var count uint64

	return func(ir isaac.BlockItemReader) error {
			if err := f(ir); err != nil {
				return err
			}

			switch i, err := ir.DecodeItems(func(total, index uint64, v interface{}) error {
				i, ok := v.(T)
				if !ok {
					var target T

					return errors.Errorf("expected %T, but %T", target, v)
				}

				if err := item(total, index, i); err != nil {
					return err
				}

				once.Do(func() {
					l = make([]T, total)
				})

				l[index] = i

				return nil
			}); {
			case err != nil:
				return err
			default:
				count = i

				return nil
			}
		},
		func() (uint64, []T) {
			return count, l[:count]
		}
}

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

func (rs *Readers) Root() string {
	return rs.root
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

	switch f, found, err := rs.readFile(height, bfile); {
	case err != nil, !found:
		return found, err
	default:
		defer func() {
			_ = f.Close()
		}()

		return rs.itemFromReader(t, f, bfile.CompressFormat(), callback)
	}
}

func (rs *Readers) ItemFromReader(
	t base.BlockItemType,
	r io.Reader,
	compressFormat string,
	callback func(isaac.BlockItemReader) error,
) (bool, error) {
	return rs.itemFromReader(t, r, compressFormat, callback)
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

func (rs *Readers) findBlockReader(f io.Reader, compressFormat string) (
	NewItemReaderFunc,
	encoder.Encoder,
	bool,
	error,
) {
	var dr io.Reader

	switch i, err := rs.decompressReader(compressFormat); {
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

func (rs *Readers) itemFromReader(
	t base.BlockItemType,
	r io.Reader,
	compressFormat string,
	callback func(isaac.BlockItemReader) error,
) (bool, error) {
	var br io.Reader
	var resetf func() error

	switch rt := r.(type) {
	case io.Seeker:
		br = r
		resetf = func() error {
			_, err := rt.Seek(0, 0)
			return errors.WithStack(err)
		}
	default:
		i := util.NewBufferedResetReader(r)
		defer func() {
			_ = i.Close()
		}()

		resetf = func() error {
			i.Reset()

			return nil
		}

		br = i
	}

	var readerf NewItemReaderFunc
	var enc encoder.Encoder

	switch i, e, found, err := rs.findBlockReader(br, compressFormat); {
	case err != nil, !found:
		return found, err
	default:
		if err := resetf(); err != nil {
			return false, err
		}

		readerf = i
		enc = e
	}

	var cr *util.CompressedReader

	switch i, err := util.NewCompressedReader(br, compressFormat, rs.decompressReader); {
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

func DecodeLineItems(
	f io.Reader,
	decode func([]byte) (interface{}, error),
	callback func(uint64, interface{}) error,
) (count uint64, _ error) {
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
			return 0, errors.WithStack(err)
		case len(b) < 1,
			bytes.HasPrefix(b, []byte("# ")):
		default:
			v, eerr := decode(b)
			if eerr != nil {
				return 0, errors.WithStack(eerr)
			}

			if eerr := callback(index, v); eerr != nil {
				return 0, eerr
			}

			index++
		}

		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			break end
		default:
			return 0, errors.WithStack(err)
		}
	}

	return index, nil
}

func DecodeLineItemsWithWorker(
	f io.Reader,
	num uint64,
	decode func([]byte) (interface{}, error),
	callback func(uint64, interface{}) error,
) (count uint64, _ error) {
	worker, err := util.NewErrgroupWorker(context.Background(), int64(num))
	if err != nil {
		return 0, err
	}

	defer worker.Close()

	switch i, err := DecodeLineItems(f, decode, func(index uint64, v interface{}) error {
		return worker.NewJob(func(ctx context.Context, _ uint64) error {
			return callback(index, v)
		})
	}); {
	case err != nil:
		return 0, err
	default:
		worker.Done()

		return i, worker.Wait()
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
