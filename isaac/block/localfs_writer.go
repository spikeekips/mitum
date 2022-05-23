package isaacblock

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
)

var LocalFSWriterHint = hint.MustNewHint("local-block-fs-writer-v0.0.1")

var (
	blockMapFilename = "map"
	blockFilenames   = map[base.BlockMapItemType]string{
		base.BlockMapItemTypeProposal:       "proposal",
		base.BlockMapItemTypeOperations:     "operations",
		base.BlockMapItemTypeOperationsTree: "operations_tree",
		base.BlockMapItemTypeStates:         "states",
		base.BlockMapItemTypeStatesTree:     "states_tree",
		base.BlockMapItemTypeVoteproofs:     "voteproofs",
	}
	BlockTempDirectoryPrefix = "temp"
)

var ulid = util.NewULID()

type LocalFSWriter struct {
	vps        [2]base.Voteproof
	local      base.LocalNode
	opsf       util.ChecksumWriter
	stsf       util.ChecksumWriter
	enc        encoder.Encoder
	saved      *util.Locked
	root       string
	id         string
	heightbase string
	temp       string
	m          BlockMap
	networkID  base.NetworkID
	hint.BaseHinter
	lenops uint64
	height base.Height
	sync.Mutex
}

func NewLocalFSWriter(
	root string,
	height base.Height,
	enc encoder.Encoder,
	local base.LocalNode,
	networkID base.NetworkID,
) (*LocalFSWriter, error) {
	e := util.StringErrorFunc("failed to create LocalFSWriter")

	abs, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return nil, e(err, "")
	}

	switch fi, err := os.Stat(abs); {
	case err != nil:
		return nil, e(err, "")
	case !fi.IsDir():
		return nil, e(nil, "root is not directory")
	}

	id := ulid.New().String()
	temp := filepath.Join(abs, BlockTempDirectoryPrefix, fmt.Sprintf("%d-%s", height, id))

	if err := os.MkdirAll(temp, 0o700); err != nil {
		return nil, e(err, "failed to create temp directory")
	}

	w := &LocalFSWriter{
		BaseHinter: hint.NewBaseHinter(LocalFSWriterHint),
		id:         id,
		root:       abs,
		height:     height,
		enc:        enc,
		local:      local,
		networkID:  networkID,
		heightbase: HeightDirectory(height),
		temp:       temp,
		m:          NewBlockMap(LocalFSWriterHint, enc.Hint()),
		saved:      util.EmptyLocked(),
	}

	switch f, err := w.newChecksumWriter(base.BlockMapItemTypeOperations); {
	case err != nil:
		return nil, e(err, "failed to create operations file")
	default:
		w.opsf = f
	}

	switch f, err := w.newChecksumWriter(base.BlockMapItemTypeStates); {
	case err != nil:
		return nil, e(err, "failed to create states file")
	default:
		w.stsf = f
	}

	return w, nil
}

func (w *LocalFSWriter) SetProposal(_ context.Context, pr base.ProposalSignedFact) error {
	if err := w.writeItem(base.BlockMapItemTypeProposal, pr); err != nil {
		return errors.Wrap(err, "failed to set proposal in fs writer")
	}

	return nil
}

func (w *LocalFSWriter) SetOperation(_ context.Context, _ uint64, op base.Operation) error {
	if err := w.appendfile(w.opsf, op); err != nil {
		return errors.Wrap(err, "failed to set operation")
	}

	atomic.AddUint64(&w.lenops, 1)

	return nil
}

func (w *LocalFSWriter) SetOperationsTree(ctx context.Context, tw *fixedtree.Writer) error {
	if err := w.setTree(
		ctx,
		base.BlockMapItemTypeOperationsTree,
		tw,
		func(ctx context.Context, _ uint64) error {
			_ = w.opsf.Close()

			if err := w.m.SetItem(NewLocalBlockMapItem(
				base.BlockMapItemTypeOperations,
				w.opsf.Checksum(),
				atomic.LoadUint64(&w.lenops),
			)); err != nil {
				return errors.Wrap(err, "failed to set operations")
			}

			return nil
		},
	); err != nil {
		return errors.Wrap(err, "failed to set operations tree")
	}

	return nil
}

func (w *LocalFSWriter) SetState(_ context.Context, _ uint64, st base.State) error {
	if err := w.appendfile(w.stsf, st); err != nil {
		return errors.Wrap(err, "failed to set state")
	}

	return nil
}

func (w *LocalFSWriter) SetStatesTree(ctx context.Context, tw *fixedtree.Writer) error {
	if err := w.setTree(
		ctx,
		base.BlockMapItemTypeStatesTree,
		tw,
		func(ctx context.Context, _ uint64) error {
			_ = w.stsf.Close()

			if err := w.m.SetItem(NewLocalBlockMapItem(
				base.BlockMapItemTypeStates,
				w.stsf.Checksum(),
				uint64(tw.Len()),
			)); err != nil {
				return errors.Wrap(err, "failed to set states")
			}

			return nil
		},
	); err != nil {
		return errors.Wrap(err, "failed to set states tree")
	}

	return nil
}

func (w *LocalFSWriter) SetManifest(_ context.Context, m base.Manifest) error {
	w.m.SetManifest(m)

	return nil
}

func (w *LocalFSWriter) SetINITVoteproof(_ context.Context, vp base.INITVoteproof) error {
	w.vps[0] = vp
	if w.vps[1] == nil {
		return nil
	}

	if err := w.saveVoteproofs(); err != nil {
		return errors.Wrap(err, "failed to set init voteproof in fs writer")
	}

	return nil
}

func (w *LocalFSWriter) SetACCEPTVoteproof(_ context.Context, vp base.ACCEPTVoteproof) error {
	w.vps[1] = vp
	if w.vps[0] == nil {
		return nil
	}

	if err := w.saveVoteproofs(); err != nil {
		return errors.Wrap(err, "failed to set accept voteproof in fs writer")
	}

	return nil
}

func (w *LocalFSWriter) saveVoteproofs() error {
	if _, found := w.m.Item(base.BlockMapItemTypeVoteproofs); found {
		return nil
	}

	e := util.StringErrorFunc("failed to save voteproofs ")

	f, err := w.newChecksumWriter(base.BlockMapItemTypeVoteproofs)
	if err != nil {
		return e(err, "")
	}

	defer func() {
		_ = f.Close()
	}()

	for i := range w.vps {
		if err := w.appendfile(f, w.vps[i]); err != nil {
			return e(err, "")
		}
	}

	if err := w.m.SetItem(NewLocalBlockMapItem(
		base.BlockMapItemTypeVoteproofs,
		f.Checksum(),
		1,
	)); err != nil {
		return e(err, "")
	}

	return nil
}

func (w *LocalFSWriter) Save(ctx context.Context) (base.BlockMap, error) {
	if i, _ := w.saved.Value(); i != nil {
		return w.m, nil
	}

	m, err := w.save(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	_ = w.saved.SetValue(true)

	return m, nil
}

func (w *LocalFSWriter) save(context.Context) (base.BlockMap, error) {
	w.Lock()
	defer w.Unlock()

	e := util.StringErrorFunc("failed to save fs writer")

	heightdirectory := filepath.Join(w.root, w.heightbase)

	// NOTE check height directory
	switch _, err := os.Stat(heightdirectory); {
	case err == nil:
		return nil, e(nil, "height directory already exists")
	case os.IsNotExist(err):
	default:
		return nil, e(err, "failed to check height directory")
	}

	if w.opsf != nil {
		_ = w.opsf.Close()

		if item, found := w.m.Item(base.BlockMapItemTypeOperations); !found || item == nil {
			// NOTE remove empty operations file
			_ = os.Remove(filepath.Join(w.temp, w.opsf.Name()))
		}
	}

	if w.stsf != nil {
		_ = w.stsf.Close()

		if item, found := w.m.Item(base.BlockMapItemTypeStates); !found || item == nil {
			// NOTE remove empty states file
			_ = os.Remove(filepath.Join(w.temp, w.stsf.Name()))
		}
	}

	if item, found := w.m.Item(base.BlockMapItemTypeVoteproofs); !found || item == nil {
		return nil, e(nil, "empty voteproofs")
	}

	if err := w.saveMap(); err != nil {
		return nil, e(err, "")
	}

	switch err := os.MkdirAll(filepath.Dir(heightdirectory), 0o700); {
	case err == nil:
	case os.IsExist(err):
	case err != nil:
		return nil, e(err, "failed to create height parent directory")
	}

	if err := os.Rename(w.temp, heightdirectory); err != nil {
		return nil, e(err, "")
	}

	return w.m, nil
}

func (w *LocalFSWriter) Cancel() error {
	w.Lock()
	defer w.Unlock()

	if w.opsf != nil {
		_ = w.opsf.Close()
		w.opsf = nil
	}

	if w.stsf != nil {
		_ = w.stsf.Close()
		w.stsf = nil
	}

	w.lenops = 0

	e := util.StringErrorFunc("failed to cancel fs writer")
	if err := os.RemoveAll(w.temp); err != nil {
		return e(err, "failed to remove temp directory")
	}

	return nil
}

func (w *LocalFSWriter) setTree(
	ctx context.Context,
	treetype base.BlockMapItemType,
	tw *fixedtree.Writer,
	newjob util.ContextWorkerCallback,
) error {
	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

	e := util.StringErrorFunc("failed to set tree, %q", treetype)

	tf, err := w.newChecksumWriter(treetype)
	if err != nil {
		return e(err, "failed to create tree file, %q", treetype)
	}

	defer func() {
		_ = tf.Close()
	}()

	if err := w.writefile(tf, append(tw.Hint().Bytes(), '\n')); err != nil {
		return e(err, "")
	}

	if newjob != nil {
		if err := worker.NewJob(newjob); err != nil {
			return e(err, "")
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
		return e(err, "")
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	_ = tf.Close()

	if err := w.m.SetItem(NewLocalBlockMapItem(treetype, tf.Checksum(), uint64(tw.Len()))); err != nil {
		return e(err, "")
	}

	return nil
}

func (w *LocalFSWriter) saveMap() error {
	e := util.StringErrorFunc("filed to save map")

	// NOTE sign blockmap by local node
	if err := w.m.Sign(w.local.Address(), w.local.Privatekey(), w.networkID); err != nil {
		return e(err, "")
	}

	// NOTE save blockmap
	f, err := os.OpenFile(
		filepath.Join(w.temp, blockFSMapFilename(w.enc.Hint().Type().String())),
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0o600,
	)
	if err != nil {
		return e(err, "failed to create map file")
	}

	if err := w.writefileonce(f, w.m); err != nil {
		return e(err, "")
	}

	return nil
}

func (w *LocalFSWriter) filename(t base.BlockMapItemType) (filename string, temppath string, err error) {
	f, err := BlockFileName(t, w.enc.Hint().Type().String())
	if err != nil {
		return "", "", errors.Wrap(err, "")
	}

	return f, filepath.Join(w.temp, f), nil
}

func (w *LocalFSWriter) writeItem(t base.BlockMapItemType, i interface{}) error {
	cw, err := w.newChecksumWriter(t)
	if err != nil {
		return errors.Wrap(err, "")
	}

	defer func() {
		_ = cw.Close()
	}()

	if err := w.writefileonce(cw, i); err != nil {
		return errors.Wrap(err, "")
	}

	_ = cw.Close()

	if err := w.m.SetItem(NewLocalBlockMapItem(
		t,
		cw.Checksum(),
		1,
	)); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (w *LocalFSWriter) writefileonce(f io.Writer, i interface{}) error {
	b, err := w.enc.Marshal(i)
	if err != nil {
		return errors.Wrap(err, "")
	}

	return w.writefile(f, b)
}

func (w *LocalFSWriter) appendfile(f io.Writer, i interface{}) error {
	b, err := w.enc.Marshal(i)
	if err != nil {
		return errors.Wrap(err, "")
	}

	return w.writefile(f, append(b, '\n'))
}

func (*LocalFSWriter) writefile(f io.Writer, b []byte) error {
	if _, err := f.Write(b); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (w *LocalFSWriter) newChecksumWriter(t base.BlockMapItemType) (util.ChecksumWriter, error) {
	fname, temppath, _ := w.filename(t)

	switch f, err := os.OpenFile(temppath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600); { //nolint:gosec //...
	case err != nil:
		return nil, errors.Wrapf(err, "failed to open file, %q", temppath)
	default:
		var cw util.ChecksumWriter
		cw = util.NewHashChecksumWriter(fname, f, sha256.New())

		if isCompressedBlockMapItemType(t) {
			cw = util.NewDummyChecksumWriter(util.NewGzipWriter(cw), cw)
		}

		return cw, nil
	}
}

func HeightDirectory(height base.Height) string {
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

func BlockFileName(t base.BlockMapItemType, hinttype string) (string, error) {
	name, found := blockFilenames[t]
	if !found {
		return "", errors.Errorf("unknown block map item type, %q", t)
	}

	ext := fileExtFromEncoder(hinttype)
	if isListBlockMapItemType(t) {
		ext = listFileExtFromEncoder(hinttype)
	}

	if isCompressedBlockMapItemType(t) {
		ext += ".gz"
	}

	return fmt.Sprintf("%s%s", name, ext), nil
}

func fileExtFromEncoder(hinttype string) string {
	switch {
	case strings.Contains(strings.ToLower(hinttype), "json"):
		return ".json"
	default:
		return ".b" // NOTE means b(ytes)
	}
}

func listFileExtFromEncoder(hinttype string) string {
	switch {
	case strings.Contains(strings.ToLower(hinttype), "json"):
		return ".ndjson"
	default:
		return ".blist"
	}
}

func isListBlockMapItemType(t base.BlockMapItemType) bool {
	switch t {
	case base.BlockMapItemTypeOperations,
		base.BlockMapItemTypeOperationsTree,
		base.BlockMapItemTypeStates,
		base.BlockMapItemTypeStatesTree,
		base.BlockMapItemTypeVoteproofs:
		return true
	default:
		return false
	}
}

func isCompressedBlockMapItemType(t base.BlockMapItemType) bool {
	switch t {
	case base.BlockMapItemTypeOperations,
		base.BlockMapItemTypeOperationsTree,
		base.BlockMapItemTypeStates,
		base.BlockMapItemTypeStatesTree:
		return true
	default:
		return false
	}
}

func blockFSMapFilename(hinttype string) string {
	return fmt.Sprintf("%s%s", blockMapFilename, fileExtFromEncoder(hinttype))
}

func CleanBlockTempDirectory(root string) error {
	d := filepath.Join(filepath.Clean(root), BlockTempDirectoryPrefix)
	if err := os.RemoveAll(d); err != nil {
		return errors.Wrap(err, "failed to remove block temp directory")
	}

	return nil
}

func marshalIndexedTreeNode(enc encoder.Encoder, index uint64, n fixedtree.Node) ([]byte, error) {
	b, err := enc.Marshal(n)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	return util.ConcatBytesSlice([]byte(fmt.Sprintf("%d,", index)), b), nil
}

type indexedTreeNode struct {
	Node  fixedtree.Node
	Index uint64
}

func unmarshalIndexedTreeNode(enc encoder.Encoder, b []byte, ht hint.Hint) (in indexedTreeNode, _ error) {
	e := util.StringErrorFunc("failed to unmarshal indexed tree node")

	bf := bytes.NewBuffer(b)

	switch i, err := bf.ReadBytes(','); {
	case err != nil:
		return in, e(err, "")
	case len(i) < 2: //nolint:gomnd //...
		return in, e(nil, "failed to find index string")
	default:
		index, err := strconv.ParseUint(string(i[:len(i)-1]), 10, 64)
		if err != nil {
			return in, e(err, "")
		}

		in.Index = index
	}

	left, err := io.ReadAll(bf)
	if err != nil {
		return in, e(err, "")
	}

	switch i, err := enc.DecodeWithHint(left, ht); {
	case err != nil:
		return in, errors.Wrap(err, "")
	case i == nil:
		return in, errors.Errorf("empty node")
	default:
		j, ok := i.(fixedtree.Node)
		if !ok {
			return in, errors.Errorf("expected fixedtree.Node, but %T", i)
		}

		in.Node = j

		return in, nil
	}
}
