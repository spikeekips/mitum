package isaac

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
	"golang.org/x/sync/singleflight"
)

var (
	LocalFSBlockItemScheme     = "local"
	BlockDirectoryHeightFormat = "%021s"
)

type BlockItemReaderCallbackFunc func(BlockItemReader) error

type BlockItemReaderFunc func(
	base.Height,
	base.BlockItemType,
	BlockItemReaderCallbackFunc,
) (bool, error)

type NewBlockItemReaderFunc func(
	base.BlockItemType,
	encoder.Encoder,
	*util.CompressedReader,
) (BlockItemReader, error)

type BlockItemReadersItemFunc func(
	base.Height,
	base.BlockItemType,
	BlockItemReaderCallbackFunc,
) (base.BlockItemFile, bool, error)

type BlockItemReadersFromReaderFunc func(
	_ base.BlockItemType,
	_ io.Reader,
	compressFormat string,
	_ BlockItemReaderCallbackFunc,
) error

func BlockItemReadersDecode[T any](
	itemf BlockItemReadersItemFunc,
	height base.Height,
	t base.BlockItemType,
	f func(ir BlockItemReader) error,
) (target T, _ bool, _ error) {
	irf := blockItemReadersDecodeFunc[T](f)

	switch _, found, err := itemf(height, t, func(ir BlockItemReader) error {
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

func BlockItemReadersDecodeItems[T any](
	itemf BlockItemReadersItemFunc,
	height base.Height,
	t base.BlockItemType,
	item func(uint64, uint64, T) error,
	f func(ir BlockItemReader) error,
) (count uint64, l []T, found bool, _ error) {
	irf, countf := blockItemReadersDecodeItemsFuncs(item, f)

	switch _, found, err := itemf(height, t, func(ir BlockItemReader) error {
		return irf(ir)
	}); {
	case err != nil, !found:
		return 0, nil, found, err
	default:
		count, l = countf()

		return count, l, true, nil
	}
}

func BlockItemReadersDecodeFromReader[T any](
	itemf BlockItemReadersFromReaderFunc,
	t base.BlockItemType,
	r io.Reader,
	compressFormat string,
	f func(ir BlockItemReader) error,
) (target T, _ error) {
	irf := blockItemReadersDecodeFunc[T](f)

	if err := itemf(t, r, compressFormat, func(ir BlockItemReader) error {
		switch i, err := irf(ir); {
		case err != nil:
			return err
		default:
			target = i

			return nil
		}
	}); err != nil {
		return target, err
	}

	return target, nil
}

func BlockItemReadersDecodeItemsFromReader[T any](
	itemf BlockItemReadersFromReaderFunc,
	t base.BlockItemType,
	r io.Reader,
	compressFormat string,
	item func(uint64, uint64, T) error,
	f func(ir BlockItemReader) error,
) (count uint64, l []T, _ error) {
	irf, countf := blockItemReadersDecodeItemsFuncs(item, f)

	if err := itemf(t, r, compressFormat, func(ir BlockItemReader) error {
		return irf(ir)
	}); err != nil {
		return 0, nil, err
	}

	count, l = countf()

	return count, l, nil
}

func blockItemReadersDecodeFunc[T any](
	f func(ir BlockItemReader) error,
) func(BlockItemReader) (T, error) {
	if f == nil {
		f = func(ir BlockItemReader) error { return nil } //revive:disable-line:modifies-parameter
	}

	return func(ir BlockItemReader) (target T, _ error) {
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

func blockItemReadersDecodeItemsFuncs[T any](
	item func(uint64, uint64, T) error,
	f func(ir BlockItemReader) error,
) (
	func(ir BlockItemReader) error,
	func() (uint64, []T),
) {
	if item == nil {
		item = func(uint64, uint64, T) error { return nil } //revive:disable-line:modifies-parameter
	}

	if f == nil {
		f = func(ir BlockItemReader) error { return nil } //revive:disable-line:modifies-parameter
	}

	var l []T
	var once sync.Once
	var count uint64

	return func(ir BlockItemReader) error {
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

type BlockItemReadersArgs struct {
	DecompressReaderFunc       util.DecompressReaderFunc
	LoadEmptyHeightsFunc       func() ([]base.Height, error)
	AddEmptyHeightFunc         func(base.Height) error
	CancelAddedEmptyHeightFunc func(base.Height) error
	RemoveEmptyAfter           time.Duration
	RemoveEmptyInterval        time.Duration
}

func NewBlockItemReadersArgs() *BlockItemReadersArgs {
	return &BlockItemReadersArgs{
		DecompressReaderFunc:       util.DefaultDecompressReaderFunc,
		LoadEmptyHeightsFunc:       func() ([]base.Height, error) { return nil, nil },
		AddEmptyHeightFunc:         func(base.Height) error { return nil },
		CancelAddedEmptyHeightFunc: func(base.Height) error { return nil },
		RemoveEmptyAfter:           time.Minute,
		RemoveEmptyInterval:        time.Minute,
	}
}

type BlockItemReaders struct {
	*hint.CompatibleSet[NewBlockItemReaderFunc]
	args *BlockItemReadersArgs
	*util.ContextDaemon
	*logging.Logging
	encs             *encoder.Encoders
	bfilescache      *util.GCache[base.Height, base.BlockItemFiles]
	emptyHeightsLock util.LockedMap[base.Height, time.Time]
	bfilessg         singleflight.Group
	root             string
}

func NewBlockItemReaders(
	root string,
	encs *encoder.Encoders,
	args *BlockItemReadersArgs,
) *BlockItemReaders {
	if args == nil {
		args = NewBlockItemReadersArgs() //revive:disable-line:modifies-parameter
	}

	emptyHeightDirectoryLock, _ := util.NewShardedMap[base.Height, time.Time](1<<5, nil) //nolint:gomnd //...

	readers := &BlockItemReaders{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "block-item-readers")
		}),
		CompatibleSet:    hint.NewCompatibleSet[NewBlockItemReaderFunc](8), //nolint:gomnd //...
		root:             root,
		encs:             encs,
		bfilescache:      util.NewLFUGCache[base.Height, base.BlockItemFiles](1 << 9), //nolint:gomnd //...
		args:             args,
		emptyHeightsLock: emptyHeightDirectoryLock,
	}

	_, _ = readers.loadAndRemoveEmptyHeightDirectories()

	readers.ContextDaemon = util.NewContextDaemon(readers.start)

	return readers
}

func (rs *BlockItemReaders) Root() string {
	return rs.root
}

func (rs *BlockItemReaders) Add(writerhint hint.Hint, v NewBlockItemReaderFunc) error {
	return rs.CompatibleSet.Add(writerhint, v)
}

func (rs *BlockItemReaders) Reader(
	height base.Height,
	t base.BlockItemType,
	callback func(_ io.Reader, compressFormat string) error,
) (bool, error) {
	var bfile base.BlockItemFile

	switch i, found, err := rs.ItemFile(height, t); {
	case err != nil, !found:
		return found, errors.WithMessage(err, t.String())
	default:
		bfile = i
	}

	switch i, found, err := rs.readFile(height, bfile); {
	case err != nil:
		return false, errors.WithMessage(err, t.String())
	case !found:
		return false, util.ErrNotFound.Errorf("item file, %q", t)
	default:
		defer func() {
			_ = i.Close()
		}()

		if err := callback(i, bfile.CompressFormat()); err != nil {
			return true, err
		}

		return true, nil
	}
}

func (rs *BlockItemReaders) Item(
	height base.Height,
	t base.BlockItemType,
	callback BlockItemReaderCallbackFunc,
) (base.BlockItemFile, bool, error) {
	var bfile base.BlockItemFile

	switch i, found, err := rs.ItemFile(height, t); {
	case err != nil, !found:
		return nil, found, errors.WithMessage(err, t.String())
	case i.URI().Scheme != LocalFSBlockItemScheme:
		return i, false, nil
	default:
		bfile = i
	}

	var f *os.File

	switch i, found, err := rs.readFile(height, bfile); {
	case err != nil:
		return bfile, false, errors.WithMessage(err, t.String())
	case !found:
		return bfile, false, util.ErrNotFound.Errorf("item file, %q", t)
	default:
		defer func() {
			_ = i.Close()
		}()

		f = i
	}

	if err := rs.itemFromReader(t, f, bfile.CompressFormat(), callback); err != nil {
		return bfile, false, errors.WithMessage(err, t.String())
	}

	return bfile, true, nil
}

func (rs *BlockItemReaders) ItemFromReader(
	t base.BlockItemType,
	r io.Reader,
	compressFormat string,
	callback BlockItemReaderCallbackFunc,
) error {
	return rs.itemFromReader(t, r, compressFormat, callback)
}

func (rs *BlockItemReaders) ItemFiles(height base.Height) (base.BlockItemFiles, bool, error) {
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

func (rs *BlockItemReaders) ItemFilesReader(
	height base.Height,
	f func(io.Reader) error,
) (bool, error) {
	switch i, err := os.Open(BlockItemFilesPath(rs.root, height)); {
	case os.IsNotExist(err):
		return false, nil
	case err != nil:
		return false, errors.WithStack(err)
	default:
		defer func() {
			_ = i.Close()
		}()

		if f == nil {
			return true, nil
		}

		return true, f(i)
	}
}

func (rs *BlockItemReaders) WriteItemFiles(height base.Height, b []byte) (updated bool, _ error) {
	_, _, _, err := rs.emptyHeightsLock.SetOrRemove(
		height,
		func(_ time.Time, found bool) (t time.Time, _ bool, _ error) {
			switch bfiles, bupdated, isempty, err := rs.writeItemFiles(height, b); {
			case err != nil:
				return t, found, err
			case !bupdated:
				return t, false, util.ErrLockedSetIgnore
			default:
				updated = true

				rs.bfilescache.Set(height, bfiles, 0)

				if isempty { // NOTE no files in local, remove height directory (in queue)
					if err := rs.args.AddEmptyHeightFunc(height); err != nil {
						return t, found, err
					}

					return time.Now(), false, nil
				}

				if err := rs.args.CancelAddedEmptyHeightFunc(height); err != nil {
					return t, found, err
				}

				return t, true, nil
			}
		},
	)

	return updated, err
}

func (rs *BlockItemReaders) writeItemFiles(height base.Height, b []byte) (
	_ base.BlockItemFiles,
	updated bool,
	isempty bool,
	_ error,
) {
	var oldbfiles base.BlockItemFiles

	switch i, found, err := rs.ItemFiles(height); {
	case err != nil, !found:
		return nil, false, false, err
	default:
		oldbfiles = i
	}

	var newbfiles base.BlockItemFiles

	switch err := encoder.Decode(rs.encs.JSON(), b, &newbfiles); {
	case err != nil:
		return nil, false, false, err
	default:
		if err := newbfiles.IsValid(nil); err != nil {
			return nil, false, false, err
		}
	}

	oldHasLocal, newHasLocal := rs.isInLocalFS(oldbfiles), rs.isInLocalFS(newbfiles)

	// NOTE compare with old
	for i := range oldbfiles.Items() {
		if _, found := newbfiles.Item(i); !found {
			return nil, false, false, errors.Errorf("item, %q not found", i)
		}
	}

	switch i, err := os.CreateTemp("", "blockitemfiles"); {
	case err != nil:
		return nil, true, false, errors.WithStack(err)
	default:
		if _, err := i.Write(b); err != nil {
			_ = i.Close()

			return nil, true, false, errors.WithStack(err)
		}

		if err := i.Close(); err != nil {
			return nil, true, false, errors.WithStack(err)
		}

		if err := os.Rename(i.Name(), BlockItemFilesPath(rs.root, height)); err != nil {
			return nil, true, false, errors.WithStack(err)
		}

		return newbfiles, true, oldHasLocal && !newHasLocal, nil
	}
}

func (rs *BlockItemReaders) ItemFile(height base.Height, t base.BlockItemType) (base.BlockItemFile, bool, error) {
	switch bfiles, found, err := rs.ItemFiles(height); {
	case err != nil, !found:
		return nil, found, err
	default:
		i, found := bfiles.Item(t)

		return i, found, nil
	}
}

func (rs *BlockItemReaders) readFile(height base.Height, bfile base.BlockItemFile) (*os.File, bool, error) {
	var p string

	switch u := bfile.URI(); u.Scheme {
	case LocalFSBlockItemScheme:
		p = filepath.Join(rs.root, BlockHeightDirectory(height), u.Path)
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

func (rs *BlockItemReaders) findBlockReader(f io.Reader, compressFormat string) (
	NewBlockItemReaderFunc,
	encoder.Encoder,
	error,
) {
	var dr io.Reader

	switch i, err := rs.args.DecompressReaderFunc(compressFormat); {
	case err != nil:
		return nil, nil, err
	default:
		j, err := i(f)
		if err != nil {
			return nil, nil, err
		}

		dr = j
	}

	switch _, writerhint, enchint, err := LoadBlockItemFileBaseHeader(dr); {
	case errors.Is(err, io.EOF):
		return nil, nil, util.ErrNotFound.Errorf("header")
	case err != nil:
		return nil, nil, err
	default:
		r, found := rs.Find(writerhint)
		if !found {
			return nil, nil, util.ErrNotFound.Errorf("writer")
		}

		e, found := rs.encs.Find(enchint)
		if !found {
			return nil, nil, util.ErrNotFound.Errorf("encoder, %q", enchint)
		}

		return r, e, nil
	}
}

func (rs *BlockItemReaders) itemFromReader(
	t base.BlockItemType,
	r io.Reader,
	compressFormat string,
	callback BlockItemReaderCallbackFunc,
) error {
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

	var readerf NewBlockItemReaderFunc
	var enc encoder.Encoder

	switch i, e, err := rs.findBlockReader(br, compressFormat); {
	case err != nil:
		return err
	default:
		if err := resetf(); err != nil {
			return err
		}

		readerf = i
		enc = e
	}

	var cr *util.CompressedReader

	switch i, err := util.NewCompressedReader(br, compressFormat, rs.args.DecompressReaderFunc); {
	case err != nil:
		return err
	default:
		defer func() {
			_ = i.Close()
		}()

		cr = i
	}

	switch i, err := readerf(t, enc, cr); {
	case err != nil:
		return err
	default:
		return callback(i)
	}
}

func (rs *BlockItemReaders) loadAndRemoveEmptyHeightDirectories() (removed uint64, _ error) {
	var heights []base.Height

	switch i, err := rs.args.LoadEmptyHeightsFunc(); {
	case err != nil:
		return 0, err
	case len(i) < 1:
		return 0, nil
	default:
		heights = i
	}

	for i := range heights {
		height := heights[i]

		ok, err := rs.removeEmptyHeightDirectory(height)
		if err != nil {
			rs.Log().Error().Err(err).Interface("height", height).Msg("failed to remove empty height")

			return removed, err
		}

		if ok {
			if err := rs.args.CancelAddedEmptyHeightFunc(height); err != nil {
				return 0, err
			}

			removed++
		}
	}

	return removed, nil
}

func (rs *BlockItemReaders) removeEmptyHeightsLock() (removed uint64, _ error) {
	var heights []base.Height

	_ = rs.emptyHeightsLock.Traverse(func(height base.Height, inserted time.Time) bool {
		if time.Since(inserted) < rs.args.RemoveEmptyAfter {
			return true
		}

		heights = append(heights, height)

		return true
	})

	for i := range heights {
		ok, err := rs.removeEmptyHeightLock(heights[i])
		if err != nil {
			rs.Log().Error().Err(err).Interface("height", heights[i]).Msg("failed to remove empty height")

			return removed, err
		}

		if ok {
			removed++
		}
	}

	return removed, nil
}

func (rs *BlockItemReaders) removeEmptyHeightLock(height base.Height) (bool, error) {
	return rs.emptyHeightsLock.Remove(height, func(inserted time.Time, found bool) error {
		switch {
		case !found:
			return util.ErrLockedSetIgnore
		case time.Since(inserted) < rs.args.RemoveEmptyAfter:
			return util.ErrLockedSetIgnore
		}

		switch removed, err := rs.removeEmptyHeightDirectory(height); {
		case err != nil:
			return err
		case !removed:
			return util.ErrLockedSetIgnore
		default:
			return nil
		}
	})
}

func (rs *BlockItemReaders) removeEmptyHeightDirectory(height base.Height) (bool, error) {
	switch i, found, err := rs.ItemFiles(height); {
	case err != nil, !found:
		return true, err
	case rs.isInLocalFS(i): // NOTE if local item files found, the height directory will be ignored.
		return true, nil
	}

	d := filepath.Join(rs.root, BlockHeightDirectory(height))

	switch i, err := os.Stat(d); {
	case errors.Is(err, os.ErrNotExist):
		return true, nil
	case err != nil:
		return false, errors.WithMessagef(err, d)
	case !i.IsDir():
		return true, nil
	}

	if err := os.RemoveAll(d); err != nil {
		return false, errors.WithMessagef(err, "remove %q", d)
	}

	return true, nil
}

func (rs *BlockItemReaders) start(ctx context.Context) error {
	ticker := time.NewTicker(rs.args.RemoveEmptyInterval)
	defer ticker.Stop()

end:
	for {
		select {
		case <-ctx.Done():
			break end
		case <-ticker.C:
			switch removed, err := rs.removeEmptyHeightsLock(); {
			case err != nil:
				rs.Log().Error().Err(err).Msg("remove empty height directories")
			case removed > 0:
				rs.Log().Debug().Uint64("removed", removed).Msg("empty height directories removed")
			}
		}
	}

	return nil
}

func (*BlockItemReaders) isInLocalFS(f base.BlockItemFiles) bool {
	m := f.Items()

	for i := range m {
		if m[i].URI().Scheme == LocalFSBlockItemScheme {
			return true
		}
	}

	return false
}

func BlockItemDecodeLineItems(
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

func BlockItemDecodeLineItemsWithWorker(
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

	switch i, err := BlockItemDecodeLineItems(f, decode, func(index uint64, v interface{}) error {
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

func BlockHeightDirectory(height base.Height) string {
	h := height.String()
	if height < 0 {
		h = strings.ReplaceAll(h, "-", "_")
	}

	p := fmt.Sprintf(BlockDirectoryHeightFormat, h)

	sl := make([]string, 7)
	var i int

	for {
		e := (i * 3) + 3 //nolint:gomnd //...
		if e > len(p) {
			e = len(p)
		}

		s := p[i*3 : e]
		if len(s) < 1 {
			break
		}

		sl[i] = s

		if len(s) < 3 { //nolint:gomnd //...
			break
		}

		i++
	}

	return "/" + strings.Join(sl, "/")
}

type BlockItemFileBaseItemsHeader struct {
	Writer  hint.Hint `json:"writer"`
	Encoder hint.Hint `json:"encoder"`
}

func LoadBlockItemFileBaseHeader(f io.Reader) (
	_ *bufio.Reader,
	writer, enc hint.Hint,
	_ error,
) {
	var u BlockItemFileBaseItemsHeader

	switch i, err := LoadBlockItemFileHeader(f, &u); {
	case err != nil:
		return nil, writer, enc, err
	default:
		return i, u.Writer, u.Encoder, nil
	}
}

func ReadBlockItemFileHeader(f io.Reader) (br *bufio.Reader, _ []byte, _ error) {
	if i, ok := f.(*bufio.Reader); ok {
		br = i
	} else {
		br = bufio.NewReader(f)
	}

	for {
		var iseof bool
		var b []byte

		switch i, err := br.ReadBytes('\n'); {
		case errors.Is(err, io.EOF):
			iseof = true
		case err != nil:
			return br, nil, errors.WithStack(err)
		default:
			b = i
		}

		if !bytes.HasPrefix(b, []byte("# ")) {
			if iseof {
				return br, nil, io.EOF
			}

			continue
		}

		return br, b[2:], nil
	}
}

func LoadBlockItemFileHeader(f io.Reader, v interface{}) (br *bufio.Reader, _ error) {
	switch br, b, err := ReadBlockItemFileHeader(f); {
	case err != nil:
		return br, err
	default:
		if err := util.UnmarshalJSON(b, v); err != nil {
			return br, err
		}

		return br, nil
	}
}

func BlockItemFilesPath(root string, height base.Height) string {
	return filepath.Join(
		filepath.Dir(filepath.Join(root, BlockHeightDirectory(height))),
		base.BlockItemFilesName(height),
	)
}

type RemotesBlockItemReadFunc func(
	_ context.Context,
	uri url.URL,
	compressFormat string,
	callback func(_ io.Reader, compressFormat string) error,
) (known, found bool, _ error)

type RemoteBlockItemReadFunc func(context.Context, url.URL, func(io.Reader) error) (bool, error)

func NewDefaultRemotesBlockItemReadFunc(insecure bool) RemotesBlockItemReadFunc {
	httpf := HTTPBlockItemReadFunc(insecure)

	return func(
		ctx context.Context,
		uri url.URL,
		compressFormat string,
		callback func(io.Reader, string) error,
	) (bool, bool, error) {
		switch uri.Scheme {
		case LocalFSBlockItemScheme:
			return true, false, nil
		case "http", "https":
			found, err := httpf(ctx, uri, func(r io.Reader) error {
				return callback(r, compressFormat)
			})

			return true, found, err
		default:
			return false, false, nil
		}
	}
}

func HTTPBlockItemReadFunc(insecure bool) RemoteBlockItemReadFunc {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: insecure,
			},
		},
	}

	return func(ctx context.Context, uri url.URL, callback func(io.Reader) error) (bool, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri.String(), nil)
		if err != nil {
			return false, errors.WithStack(err)
		}

		res, err := client.Do(req)
		if err != nil {
			return false, nil // NOTE ignore error
		}

		defer func() {
			_ = res.Body.Close()
		}()

		if res.StatusCode != http.StatusOK {
			return false, nil
		}

		return true, callback(res.Body)
	}
}

func IsInLocalBlockItemFile(uri url.URL) bool {
	switch uri.Scheme {
	case LocalFSBlockItemScheme, "file":
		return true
	default:
		return false
	}
}

func BlockItemReadersItemFuncWithRemote(
	readers *BlockItemReaders,
	fromRemotes RemotesBlockItemReadFunc,
	callback func(itemfile base.BlockItemFile, ir BlockItemReader, callback BlockItemReaderCallbackFunc) error,
) func(context.Context) BlockItemReadersItemFunc {
	if callback == nil {
		callback = func( //revive:disable-line:modifies-parameter
			_ base.BlockItemFile, ir BlockItemReader, f BlockItemReaderCallbackFunc,
		) error {
			return f(ir)
		}
	}

	return func(ctx context.Context) BlockItemReadersItemFunc {
		return func(
			height base.Height,
			t base.BlockItemType,
			f BlockItemReaderCallbackFunc,
		) (base.BlockItemFile, bool, error) {
			var itf base.BlockItemFile

			switch i, found, err := readers.ItemFile(height, t); {
			case err != nil, !found:
				return nil, found, err
			case IsInLocalBlockItemFile(i.URI()):
				return readers.Item(height, t, func(ir BlockItemReader) error {
					return callback(i, ir, f)
				})
			default:
				itf = i
			}

			switch known, found, err := fromRemotes(ctx, itf.URI(), itf.CompressFormat(),
				func(r io.Reader, compressFormat string) error {
					return readers.ItemFromReader(t, r, compressFormat,
						func(ir BlockItemReader) error {
							return callback(itf, ir, f)
						},
					)
				}); {
			case err != nil, !found:
				return nil, found, err
			case !known:
				return nil, found, errors.Errorf("unknown remote item file, %v", itf.URI())
			default:
				return itf, true, nil
			}
		}
	}
}
