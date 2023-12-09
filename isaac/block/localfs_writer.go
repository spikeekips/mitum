package isaacblock

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
)

var LocalFSWriterHint = hint.MustNewHint("block-localfs-writer-v0.0.1")

var (
	rHeightDirectory = regexp.MustCompile(`^[\d]{3}$`)
	rBlockItemFiles  = regexp.MustCompile(`^([0-9][0-9]*)\.json$`)
)

var (
	blockFilenames = map[base.BlockItemType]string{
		base.BlockItemMap:            "map",
		base.BlockItemProposal:       "proposal",
		base.BlockItemOperations:     "operations",
		base.BlockItemOperationsTree: "operations_tree",
		base.BlockItemStates:         "states",
		base.BlockItemStatesTree:     "states_tree",
		base.BlockItemVoteproofs:     "voteproofs",
	}
	BlockTempDirectoryPrefix = "temp"
)

type LocalFSWriter struct {
	vps        [2]base.Voteproof
	local      base.LocalNode
	opsf       util.ChecksumWriter
	stsf       util.ChecksumWriter
	bfiles     *isaac.BlockItemFilesMaker
	enc        encoder.Encoder
	root       string
	id         string
	heightbase string
	temp       string
	m          BlockMap
	networkID  base.NetworkID
	hint.BaseHinter
	lenops           uint64
	height           base.Height
	saved            bool
	opsHeaderOnce    sync.Once
	statesHeaderOnce sync.Once
	sync.Mutex
}

func NewLocalFSWriter(
	root string,
	height base.Height,
	jsonenc, enc encoder.Encoder,
	local base.LocalNode,
	networkID base.NetworkID,
) (*LocalFSWriter, error) {
	e := util.StringError("create LocalFSWriter")

	abs, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return nil, e.Wrap(err)
	}

	switch fi, err := os.Stat(abs); {
	case err != nil:
		return nil, e.Wrap(err)
	case !fi.IsDir():
		return nil, e.Errorf("root is not directory")
	}

	id := util.ULID().String()
	temp := filepath.Join(abs, BlockTempDirectoryPrefix, fmt.Sprintf("%d-%s", height, id))

	if err := os.MkdirAll(temp, 0o700); err != nil {
		return nil, e.WithMessage(err, "create temp directory")
	}

	w := &LocalFSWriter{
		BaseHinter: hint.NewBaseHinter(LocalFSWriterHint),
		id:         id,
		root:       abs,
		height:     height,
		enc:        enc,
		local:      local,
		networkID:  networkID,
		heightbase: isaac.BlockHeightDirectory(height),
		temp:       temp,
		m:          NewBlockMap(),
		bfiles:     isaac.NewBlockItemFilesMaker(jsonenc),
	}

	switch f, err := w.newChecksumWriter(base.BlockItemOperations); {
	case err != nil:
		return nil, e.WithMessage(err, "create operations file")
	default:
		w.opsf = f
	}

	switch f, err := w.newChecksumWriter(base.BlockItemStates); {
	case err != nil:
		return nil, e.WithMessage(err, "create states file")
	default:
		w.stsf = f
	}

	return w, nil
}

func (w *LocalFSWriter) SetProposal(_ context.Context, pr base.ProposalSignFact) error {
	if err := w.writeItem(base.BlockItemProposal, pr); err != nil {
		return errors.Wrap(err, "set proposal in fs writer")
	}

	return nil
}

func (w *LocalFSWriter) SetOperation(_ context.Context, total, _ uint64, op base.Operation) error {
	w.opsHeaderOnce.Do(func() {
		// NOTE the total of operations is not exact number of operations; it is
		// maximum number.
		_ = writeCountHeader(w.opsf, LocalFSWriterHint, w.enc.Hint(), total)
	})

	if err := w.appendfile(w.opsf, op); err != nil {
		return errors.Wrap(err, "set operation")
	}

	atomic.AddUint64(&w.lenops, 1)

	return nil
}

func (w *LocalFSWriter) SetOperationsTree(ctx context.Context, tw *fixedtree.Writer) error {
	if _, err := w.setTree(
		ctx,
		base.BlockItemOperationsTree,
		tw,
		func(ctx context.Context, _ uint64) error {
			_ = w.opsf.Close()

			if l := atomic.LoadUint64(&w.lenops); l > 0 {
				if err := w.m.SetItem(NewBlockMapItem(
					base.BlockItemOperations,
					w.opsf.Checksum(),
				)); err != nil {
					return errors.Wrap(err, "set operations")
				}
			}

			return nil
		},
	); err != nil {
		return errors.Wrap(err, "set operations tree")
	}

	return nil
}

func (w *LocalFSWriter) SetState(_ context.Context, total, _ uint64, st base.State) error {
	w.statesHeaderOnce.Do(func() {
		_ = writeCountHeader(w.stsf, LocalFSWriterHint, w.enc.Hint(), total)
	})

	if err := w.appendfile(w.stsf, st); err != nil {
		return errors.Wrap(err, "set state")
	}

	return nil
}

func (w *LocalFSWriter) SetStatesTree(ctx context.Context, tw *fixedtree.Writer) (tr fixedtree.Tree, err error) {
	tr, err = w.setTree(
		ctx,
		base.BlockItemStatesTree,
		tw,
		func(ctx context.Context, _ uint64) error {
			_ = w.stsf.Close()

			if eerr := w.m.SetItem(NewBlockMapItem(
				base.BlockItemStates,
				w.stsf.Checksum(),
			)); eerr != nil {
				return errors.Wrap(eerr, "set states")
			}

			return nil
		},
	)
	if err != nil {
		return tr, errors.Wrap(err, "set states tree")
	}

	return tr, nil
}

func (w *LocalFSWriter) SetManifest(_ context.Context, m base.Manifest) error {
	w.m.SetManifest(m)

	return nil
}

func (w *LocalFSWriter) SetINITVoteproof(_ context.Context, vp base.INITVoteproof) error {
	w.Lock()
	defer w.Unlock()

	w.vps[0] = vp
	if w.vps[1] == nil {
		return nil
	}

	if err := w.saveVoteproofs(); err != nil {
		return errors.Wrap(err, "set init voteproof in fs writer")
	}

	return nil
}

func (w *LocalFSWriter) SetACCEPTVoteproof(_ context.Context, vp base.ACCEPTVoteproof) error {
	w.Lock()
	defer w.Unlock()

	w.vps[1] = vp
	if w.vps[0] == nil {
		return nil
	}

	if err := w.saveVoteproofs(); err != nil {
		return errors.Wrap(err, "set accept voteproof in fs writer")
	}

	return nil
}

func (w *LocalFSWriter) saveVoteproofs() error {
	if _, found := w.m.Item(base.BlockItemVoteproofs); found {
		return nil
	}

	e := util.StringError("save voteproofs ")

	f, err := w.newChecksumWriter(base.BlockItemVoteproofs)
	if err != nil {
		return e.Wrap(err)
	}

	defer func() {
		_ = f.Close()
	}()

	if err := writeBaseHeader(f,
		isaac.BlockItemFileBaseItemsHeader{Writer: LocalFSWriterHint, Encoder: w.enc.Hint()},
	); err != nil {
		return e.Wrap(err)
	}

	for i := range w.vps {
		if err := w.appendfile(f, w.vps[i]); err != nil {
			return e.Wrap(err)
		}
	}

	if err := w.m.SetItem(NewBlockMapItem(
		base.BlockItemVoteproofs,
		f.Checksum(),
	)); err != nil {
		return e.Wrap(err)
	}

	_ = w.bfiles.SetItem(base.BlockItemVoteproofs, isaac.NewLocalFSBlockItemFile(f.Name(), ""))

	return nil
}

func (w *LocalFSWriter) Save(ctx context.Context) (base.BlockMap, error) {
	w.Lock()
	defer w.Unlock()

	if w.saved {
		return w.m, nil
	}

	heightdirectory := filepath.Join(w.root, w.heightbase)

	// NOTE check height directory
	switch _, err := os.Stat(heightdirectory); {
	case err == nil:
		return nil, errors.Errorf("save fs writer; height directory already exists")
	case os.IsNotExist(err):
	default:
		return nil, errors.Errorf("save fs writer; check height directory")
	}

	switch m, err := w.save(ctx, heightdirectory); {
	case err != nil:
		_ = os.RemoveAll(heightdirectory)

		return nil, errors.WithMessage(err, "save fs writer")
	default:
		return m, nil
	}
}

func (w *LocalFSWriter) save(_ context.Context, heightdirectory string) (base.BlockMap, error) {
	if w.opsf != nil {
		_ = w.opsf.Close()

		switch item, found := w.m.Item(base.BlockItemOperations); {
		case !found || item == nil:
			// NOTE remove empty operations file
			_ = os.Remove(filepath.Join(w.temp, w.opsf.Name()))
		default:
			_ = w.bfiles.SetItem(base.BlockItemOperations, isaac.NewLocalFSBlockItemFile(w.opsf.Name(), ""))
		}
	}

	if w.stsf != nil {
		_ = w.stsf.Close()

		switch item, found := w.m.Item(base.BlockItemStates); {
		case !found || item == nil:
			// NOTE remove empty states file
			_ = os.Remove(filepath.Join(w.temp, w.stsf.Name()))
		default:
			_ = w.bfiles.SetItem(base.BlockItemStates, isaac.NewLocalFSBlockItemFile(w.stsf.Name(), ""))
		}
	}

	if item, found := w.m.Item(base.BlockItemVoteproofs); !found || item == nil {
		return nil, errors.Errorf("empty voteproofs")
	}

	if err := w.saveMap(); err != nil {
		return nil, err
	}

	switch err := os.MkdirAll(filepath.Dir(heightdirectory), 0o700); {
	case err == nil:
	case os.IsExist(err):
	default:
		return nil, errors.WithMessage(err, "create height parent directory")
	}

	if err := os.Rename(w.temp, heightdirectory); err != nil {
		return nil, errors.WithStack(err)
	}

	m := w.m

	if err := w.bfiles.Save(isaac.BlockItemFilesPath(w.root, w.height)); err != nil {
		return nil, err
	}

	if err := w.close(); err != nil {
		return nil, err
	}

	w.saved = true

	return m, nil
}

func (w *LocalFSWriter) Cancel() error {
	w.Lock()
	defer w.Unlock()

	if w.opsf != nil {
		_ = w.opsf.Close()
	}

	if w.stsf != nil {
		_ = w.stsf.Close()
	}

	e := util.StringError("cancel fs writer")
	if err := os.RemoveAll(w.temp); err != nil {
		return e.WithMessage(err, "remove temp directory")
	}

	return w.close()
}

func (w *LocalFSWriter) close() error {
	w.vps = [2]base.Voteproof{}
	w.local = nil
	w.opsf = nil
	w.stsf = nil
	w.enc = nil
	w.root = ""
	w.id = ""
	w.heightbase = ""
	w.temp = ""
	w.m = BlockMap{}
	w.lenops = 0

	return nil
}

func (w *LocalFSWriter) setTree(
	ctx context.Context,
	treetype base.BlockItemType,
	tw *fixedtree.Writer,
	newjob util.ContextWorkerCallback,
) (tr fixedtree.Tree, _ error) {
	e := util.StringError("set tree, %q", treetype)

	worker, err := util.NewErrgroupWorker(ctx, math.MaxInt8)
	if err != nil {
		return tr, e.Wrap(err)
	}

	defer worker.Close()

	tf, err := w.newChecksumWriter(treetype)
	if err != nil {
		return tr, e.WithMessage(err, "create tree file, %q", treetype)
	}

	defer func() {
		_ = tf.Close()
	}()

	if err := writeTreeHeader(tf, LocalFSWriterHint, w.enc.Hint(), uint64(tw.Len()), tw.Hint()); err != nil {
		return tr, e.Wrap(err)
	}

	if newjob != nil {
		if err := worker.NewJob(newjob); err != nil {
			return tr, e.Wrap(err)
		}
	}

	if err := tw.Write(func(index uint64, n fixedtree.Node) error {
		return worker.NewJob(func(ctx context.Context, _ uint64) error {
			b, err := marshalIndexedTreeNode(w.enc, index, n)
			if err != nil {
				return err
			}

			return w.writefile(tf, append(b, '\n'))
		})
	}); err != nil {
		return tr, e.Wrap(err)
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return tr, e.Wrap(err)
	}

	_ = tf.Close()

	switch i, err := tw.Tree(); {
	case err != nil:
		return tr, e.Wrap(err)
	default:
		tr = i
	}

	if err := w.m.SetItem(NewBlockMapItem(treetype, tf.Checksum())); err != nil {
		return tr, e.Wrap(err)
	}

	_ = w.bfiles.SetItem(treetype, isaac.NewLocalFSBlockItemFile(tf.Name(), ""))

	return tr, nil
}

func (w *LocalFSWriter) saveMap() error {
	e := util.StringError("filed to save map")

	// NOTE sign blockmap by local node
	if err := w.m.Sign(w.local.Address(), w.local.Privatekey(), w.networkID); err != nil {
		return e.Wrap(err)
	}

	if err := w.writeItem(base.BlockItemMap, w.m); err != nil {
		return errors.Wrap(err, "blockmap in fs writer")
	}

	return nil
}

func (w *LocalFSWriter) filename(t base.BlockItemType) (filename string, temppath string, err error) {
	f, err := DefaultBlockFileName(t, w.enc.Hint().Type())
	if err != nil {
		return "", "", err
	}

	return f, filepath.Join(w.temp, f), nil
}

func (w *LocalFSWriter) writeItem(t base.BlockItemType, i interface{}) error {
	cw, err := w.newChecksumWriter(t)
	if err != nil {
		return err
	}

	defer func() {
		_ = cw.Close()
	}()

	if err := writeBaseHeader(cw,
		isaac.BlockItemFileBaseItemsHeader{Writer: LocalFSWriterHint, Encoder: w.enc.Hint()},
	); err != nil {
		return err
	}

	if err := w.writefileonce(cw, i); err != nil {
		return err
	}

	_ = cw.Close()

	_ = w.bfiles.SetItem(t, isaac.NewLocalFSBlockItemFile(cw.Name(), ""))

	if t != base.BlockItemMap {
		return w.m.SetItem(NewBlockMapItem(t, cw.Checksum()))
	}

	return nil
}

func (w *LocalFSWriter) writefileonce(f io.Writer, i interface{}) error {
	return w.enc.StreamEncoder(f).Encode(i)
}

func (w *LocalFSWriter) appendfile(f io.Writer, i interface{}) error {
	b, err := w.enc.Marshal(i)
	if err != nil {
		return err
	}

	return w.writefile(f, append(b, '\n'))
}

func (*LocalFSWriter) writefile(f io.Writer, b []byte) error {
	_, err := f.Write(b)

	return errors.WithStack(err)
}

func (w *LocalFSWriter) newChecksumWriter(t base.BlockItemType) (util.ChecksumWriter, error) {
	fname, temppath, _ := w.filename(t)

	var f io.WriteCloser

	switch i, err := os.OpenFile(temppath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600); { //nolint:gosec //...
	case err != nil:
		return nil, errors.Wrapf(err, "open file, %q", temppath)
	default:
		f = i
	}

	if isCompressedBlockItemType(t) {
		switch i, err := util.NewGzipWriter(f, gzip.BestSpeed); {
		case err != nil:
			return nil, err
		default:
			f = i
		}
	}

	return util.NewHashChecksumWriterWithWriter(fname, f, sha256.New()), nil
}

func HeightFromDirectory(s string) (base.Height, error) {
	hs := strings.Replace(s, "/", "", -1)

	h, err := base.ParseHeightString(hs)
	if err != nil {
		return base.NilHeight, errors.WithMessage(err, "wrong directory for height")
	}

	return h, nil
}

func FindHighestDirectory(root string) (_ base.Height, directory string, found bool, _ error) {
	switch h, f, err := FindHighestBlockItemFiles(root, 0); {
	case err != nil:
		return -1, "", false, err
	case len(f) < 1, h < 0:
		return -1, "", false, nil
	default:
		return base.Height(h), filepath.Join(root, isaac.BlockHeightDirectory(base.Height(h))), true, nil
	}
}

func FindHighestBlockItemFiles(p string, depth int) (height int64, lastpath string, _ error) {
	switch {
	case depth > 6: //nolint:gomnd //...
		return -1, "", nil
	case depth == 6: //nolint:gomnd //...
		return findHighestBlockItemFilesInDirectory(p)
	}

	var last string

	switch i, err := os.ReadDir(p); {
	case err != nil:
		return -1, "", errors.WithStack(err)
	case len(i) < 1:
		return -1, "", nil
	default:
		files := util.FilterSlice(i, func(i os.DirEntry) bool {
			return i.IsDir() && rHeightDirectory.MatchString(i.Name())
		})

		if len(files) < 1 {
			return -1, "", nil
		}

		if len(files) > 1 {
			sort.Slice(files, func(i, j int) bool {
				return strings.Compare(files[i].Name(), files[j].Name()) > 0
			})
		}

		last = files[0].Name()
	}

	lastpath = filepath.Join(p, last)

	switch i, j, err := FindHighestBlockItemFiles(lastpath, depth+1); {
	case err != nil:
		return -1, "", err
	case len(j) > 0:
		return i, j, nil
	default:
		return -1, lastpath, nil
	}
}

func findHighestBlockItemFilesInDirectory(p string) (int64, string, error) {
	var files []os.DirEntry

	switch i, err := os.ReadDir(p); {
	case err != nil:
		return -1, "", errors.WithStack(err)
	case len(i) < 1:
		return -1, "", nil
	default:
		files = util.FilterSlice(i, func(i os.DirEntry) bool {
			return !i.IsDir() && rBlockItemFiles.MatchString(i.Name())
		})

		if len(files) < 1 {
			return -1, "", nil
		}
	}

	var last string
	var highest *int64

	for i := range files {
		f := files[i]

		switch j := rBlockItemFiles.FindStringSubmatch(f.Name()); {
		case len(j) < 2:
			continue
		default:
			u, err := strconv.ParseInt(j[1], 10, 64)
			if err != nil {
				continue
			}

			if highest == nil || u > *highest {
				highest = &u

				last = f.Name()
			}
		}
	}

	if highest == nil {
		return -1, "", nil
	}

	return *highest, filepath.Join(p, last), nil
}

func BlockFileName(t base.BlockItemType, hinttype hint.Type, compressFormat string) (string, error) {
	name, found := blockFilenames[t]
	if !found {
		return "", errors.Errorf("unknown block map item type, %q", t)
	}

	ext, _ := encoder.EncodersExtension(hinttype)

	if len(compressFormat) > 0 {
		ext += "." + compressFormat
	}

	return fmt.Sprintf("%s.%s", name, ext), nil
}

func DefaultBlockFileName(t base.BlockItemType, hinttype hint.Type) (string, error) {
	var compressFormat string

	if isCompressedBlockItemType(t) {
		compressFormat = "gz"
	}

	return BlockFileName(t, hinttype, compressFormat)
}

func CleanBlockTempDirectory(root string) error {
	d := filepath.Join(filepath.Clean(root), BlockTempDirectoryPrefix)
	if err := os.RemoveAll(d); err != nil {
		return errors.Wrap(err, "remove block temp directory")
	}

	return nil
}

func RemoveBlockFromLocalFS(root string, height base.Height) (bool, error) {
	heightdirectory := filepath.Join(root, isaac.BlockHeightDirectory(height))

	if err := os.Remove(isaac.BlockItemFilesPath(root, height)); err != nil {
		return false, errors.WithMessagef(err, "files.json")
	}

	switch _, err := os.Stat(heightdirectory); {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return false, errors.WithMessagef(err, "check height directory, %q", heightdirectory)
	default:
		if err := os.RemoveAll(heightdirectory); err != nil {
			return false, errors.WithMessagef(err, "remove %q", heightdirectory)
		}
	}

	return true, nil
}

func RemoveBlocksFromLocalFS(root string, height base.Height) (bool, error) {
	switch {
	case height < base.GenesisHeight:
		return false, nil
	case height < base.GenesisHeight+1:
		if err := os.RemoveAll(root); err != nil {
			return false, errors.WithMessage(err, "clean localfs")
		}

		return true, nil
	}

	var top base.Height

	switch h, _, found, err := FindHighestDirectory(root); {
	case err != nil:
		return false, err
	case !found:
		return false, nil
	case h < height:
		return false, nil
	default:
		top = h
	}

	for i := top; i >= height; i-- {
		if removed, err := RemoveBlockFromLocalFS(root, i); err != nil {
			return removed, err
		}
	}

	return true, nil
}

func isCompressedBlockItemType(t base.BlockItemType) bool {
	switch t {
	case base.BlockItemProposal,
		base.BlockItemOperations,
		base.BlockItemOperationsTree,
		base.BlockItemStates,
		base.BlockItemStatesTree:
		return true
	default:
		return false
	}
}

func marshalIndexedTreeNode(enc encoder.Encoder, index uint64, n fixedtree.Node) ([]byte, error) {
	b, err := enc.Marshal(n)
	if err != nil {
		return nil, err
	}

	return util.ConcatBytesSlice([]byte(fmt.Sprintf("%d,", index)), b), nil
}

type indexedTreeNode struct {
	Node  fixedtree.Node
	Index uint64
}

func unmarshalIndexedTreeNode(enc encoder.Encoder, b []byte, ht hint.Hint) (in indexedTreeNode, _ error) {
	e := util.StringError("unmarshal indexed tree node")

	bf := bytes.NewBuffer(b)
	defer bf.Reset()

	switch i, err := bf.ReadBytes(','); {
	case err != nil:
		return in, e.Wrap(err)
	case len(i) < 2:
		return in, e.Errorf("find index string")
	default:
		index, err := strconv.ParseUint(string(i[:len(i)-1]), 10, 64)
		if err != nil {
			return in, e.Wrap(err)
		}

		in.Index = index
	}

	left, err := io.ReadAll(bf)
	if err != nil {
		return in, e.Wrap(err)
	}

	switch i, err := enc.DecodeWithHint(left, ht); {
	case err != nil:
		return in, err
	default:
		j, ok := i.(fixedtree.Node)
		if !ok {
			return in, errors.Errorf("expected fixedtree.Node, but %T", i)
		}

		in.Node = j

		return in, nil
	}
}

func writeBaseHeader(f io.Writer, hs interface{}) error {
	switch b, err := util.MarshalJSON(hs); {
	case err != nil:
		return err
	default:
		var s strings.Builder
		_, _ = s.WriteString("# ")
		_, _ = s.WriteString(string(b))
		_, _ = s.WriteString("\n")

		if _, err := f.Write([]byte(s.String())); err != nil {
			return errors.WithStack(err)
		}

		return nil
	}
}

type countItemsHeader struct {
	isaac.BlockItemFileBaseItemsHeader
	Count uint64 `json:"count"`
}

type treeItemsHeader struct {
	Tree hint.Hint `json:"tree"`
	countItemsHeader
}

func writeCountHeader(f io.Writer, writer, enc hint.Hint, count uint64) error {
	return writeBaseHeader(f, countItemsHeader{
		BlockItemFileBaseItemsHeader: isaac.BlockItemFileBaseItemsHeader{Writer: writer, Encoder: enc},
		Count:                        count,
	})
}

func writeTreeHeader(f io.Writer, writer, enc hint.Hint, count uint64, tree hint.Hint) error {
	return writeBaseHeader(f, treeItemsHeader{
		countItemsHeader: countItemsHeader{
			BlockItemFileBaseItemsHeader: isaac.BlockItemFileBaseItemsHeader{Writer: writer, Encoder: enc},
			Count:                        count,
		},
		Tree: tree,
	})
}
