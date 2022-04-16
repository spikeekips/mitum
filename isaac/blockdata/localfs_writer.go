package blockdata

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/tree"
)

var LocalFSWriterHint = hint.MustNewHint("local-blockdata-fs-writer-v0.0.1")

var (
	blockDataMapFilename = "map"
	blockDataFilenames   = map[base.BlockDataType]string{
		base.BlockDataTypeProposal:       "proposal",
		base.BlockDataTypeOperations:     "operations",
		base.BlockDataTypeOperationsTree: "operations_tree",
		base.BlockDataTypeStates:         "states",
		base.BlockDataTypeStatesTree:     "states_tree",
		base.BlockDataTypeVoteproofs:     "voteproofs",
	}
)

var ulid = util.NewULID()

type LocalFSWriter struct {
	sync.Mutex
	hint.BaseHinter
	id         string
	root       string
	height     base.Height
	enc        encoder.Encoder
	local      base.LocalNode
	networkID  base.NetworkID
	heightbase string
	temp       string
	m          BlockDataMap
	vps        [2]base.Voteproof
	lenops     int64
	opsf       util.ChecksumWriter
	stsf       util.ChecksumWriter
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
	temp := filepath.Join(abs, fmt.Sprintf("%d-%s", height, id))
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
		m:          NewBlockDataMap(LocalFSWriterHint, enc.Hint()),
	}

	switch f, err := w.newChecksumWriter(base.BlockDataTypeOperations); {
	case err != nil:
		return nil, e(err, "failed to create operations file")
	default:
		w.opsf = f
	}

	switch f, err := w.newChecksumWriter(base.BlockDataTypeStates); {
	case err != nil:
		return nil, e(err, "failed to create states file")
	default:
		w.stsf = f
	}

	return w, nil
}

func (w *LocalFSWriter) SetProposal(_ context.Context, pr base.ProposalSignedFact) error {
	if err := w.writeItem(base.BlockDataTypeProposal, pr); err != nil {
		return errors.Wrap(err, "failed to set proposal in fs writer")
	}

	return nil
}

func (w *LocalFSWriter) SetOperation(_ context.Context, _ int, op base.Operation) error {
	if err := w.appendfile(w.opsf, op); err != nil {
		return errors.Wrap(err, "failed to set operation")
	}

	atomic.AddInt64(&w.lenops, 1)

	return nil
}

func (w *LocalFSWriter) SetOperationsTree(ctx context.Context, tr tree.FixedTree) error {
	if err := w.setTree(
		ctx,
		tr,
		base.BlockDataTypeOperationsTree,
		func(ctx context.Context, _ uint64) error {
			_ = w.opsf.Close()

			if err := w.m.SetItem(NewLocalBlockDataMapItem(
				base.BlockDataTypeOperations,
				filepath.Join(w.heightbase, w.opsf.Name()),
				w.opsf.Checksum(),
				atomic.LoadInt64(&w.lenops),
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

func (w *LocalFSWriter) SetState(_ context.Context, _ int, st base.State) error {
	if err := w.appendfile(w.stsf, st); err != nil {
		return errors.Wrap(err, "failed to set state")
	}

	return nil
}

func (w *LocalFSWriter) SetStatesTree(ctx context.Context, tr tree.FixedTree) error {
	if err := w.setTree(
		ctx,
		tr,
		base.BlockDataTypeStatesTree,
		func(ctx context.Context, _ uint64) error {
			_ = w.stsf.Close()

			if err := w.m.SetItem(NewLocalBlockDataMapItem(
				base.BlockDataTypeStates,
				filepath.Join(w.heightbase, w.stsf.Name()),
				w.stsf.Checksum(),
				int64(tr.Len()),
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
		return errors.Wrap(err, "failed to set voteproofs in fs writer")
	}

	return nil
}

func (w *LocalFSWriter) SetACCEPTVoteproof(_ context.Context, vp base.ACCEPTVoteproof) error {
	w.vps[1] = vp
	if w.vps[0] == nil {
		return nil
	}

	if err := w.saveVoteproofs(); err != nil {
		return errors.Wrap(err, "failed to set voteproofs in fs writer")
	}

	return nil
}

func (w *LocalFSWriter) saveVoteproofs() error {
	e := util.StringErrorFunc("failed to save voteproofs ")

	f, err := w.newChecksumWriter(base.BlockDataTypeVoteproofs)
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

	if err := w.m.SetItem(NewLocalBlockDataMapItem(
		base.BlockDataTypeVoteproofs,
		filepath.Join(w.heightbase, f.Name()),
		f.Checksum(),
		1,
	)); err != nil {
		return e(err, "")
	}

	return nil
}

func (w *LocalFSWriter) Save(_ context.Context) (base.BlockDataMap, error) {
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

		if item, found := w.m.Item(base.BlockDataTypeOperations); !found || item == nil {
			_ = os.Remove(filepath.Join(w.temp, w.opsf.Name())) // NOTE remove empty operations file
		}
	}

	if w.stsf != nil {
		_ = w.stsf.Close()

		if item, found := w.m.Item(base.BlockDataTypeStates); !found || item == nil {
			_ = os.Remove(filepath.Join(w.temp, w.stsf.Name())) // NOTE remove empty states file
		}
	}

	if item, found := w.m.Item(base.BlockDataTypeVoteproofs); !found || item == nil {
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
	tr tree.FixedTree,
	treetype base.BlockDataType,
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

	if newjob != nil {
		if err := worker.NewJob(newjob); err != nil {
			return e(err, "")
		}
	}

	go func() {
		defer worker.Done()

		_ = tr.Traverse(func(node tree.FixedTreeNode) (bool, error) {
			n := node
			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				if err := w.appendfile(tf, n); err != nil {
					return errors.Wrap(err, "failed to write fixed tree node")
				}

				return nil
			}); err != nil {
				return false, errors.Wrap(err, "")
			}

			return true, nil
		})
	}()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	_ = tf.Close()

	if err := w.m.SetItem(NewLocalBlockDataMapItem(
		treetype,
		filepath.Join(w.heightbase, tf.Name()), tf.Checksum(), int64(tr.Len())),
	); err != nil {
		return e(err, "")
	}

	return nil
}

func (w *LocalFSWriter) saveMap() error {
	e := util.StringErrorFunc("filed to save map")

	// NOTE sign blockdatamap by local node
	if err := w.m.Sign(w.local.Address(), w.local.Privatekey(), w.networkID); err != nil {
		return e(err, "")
	}

	// NOTE save blockdatamap
	f, err := os.OpenFile(
		filepath.Join(w.temp, blockDataFSMapFilename(w.enc)),
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

func (w *LocalFSWriter) filename(t base.BlockDataType) (filename string, temppath string, err error) {
	f, err := BlockDataFileName(t, w.enc)
	if err != nil {
		return "", "", errors.Wrap(err, "")
	}

	return f, filepath.Join(w.temp, f), nil
}

func (w *LocalFSWriter) writeItem(t base.BlockDataType, i interface{}) error {
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

	if err := w.m.SetItem(NewLocalBlockDataMapItem(
		t,
		filepath.Join(w.heightbase, cw.Name()),
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

func (w *LocalFSWriter) newChecksumWriter(t base.BlockDataType) (util.ChecksumWriter, error) {
	fname, temppath, _ := w.filename(t)
	switch f, err := os.OpenFile(temppath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600); { // nolint:gosec
	case err != nil:
		return nil, errors.Wrap(err, "")
	default:
		var cw util.ChecksumWriter
		cw = util.NewHashChecksumWriter(fname, f, sha256.New())
		if isCompressedBlockDataType(t) {
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
		e := (i * 3) + 3
		if e > len(p) {
			e = len(p)
		}

		s := p[i*3 : e]
		if len(s) < 1 {
			break
		}

		sl[i] = s

		if len(s) < 3 {
			break
		}

		i++
	}

	return "/" + strings.Join(sl, "/")
}

func BlockDataFileName(t base.BlockDataType, enc encoder.Encoder) (string, error) {
	name, found := blockDataFilenames[t]
	if !found {
		return "", errors.Errorf("unknown block data type, %q", t)
	}

	ext := fileExtFromEncoder(enc)
	if isListBlockDataType(t) {
		ext = listFileExtFromEncoder(enc)
	}

	if isCompressedBlockDataType(t) {
		ext += ".gz"
	}

	return fmt.Sprintf("%s%s", name, ext), nil
}

func fileExtFromEncoder(enc encoder.Encoder) string {
	switch {
	case strings.Contains(strings.ToLower(enc.Hint().Type().String()), "json"):
		return ".json"
	default:
		return ".b" // NOTE means b(ytes)
	}
}

func listFileExtFromEncoder(enc encoder.Encoder) string {
	switch {
	case strings.Contains(strings.ToLower(enc.Hint().Type().String()), "json"):
		return ".ndjson"
	default:
		return ".blist"
	}
}

func isListBlockDataType(t base.BlockDataType) bool {
	switch t {
	case base.BlockDataTypeOperations,
		base.BlockDataTypeOperationsTree,
		base.BlockDataTypeStates,
		base.BlockDataTypeStatesTree,
		base.BlockDataTypeVoteproofs:
		return true
	default:
		return false
	}
}

func isCompressedBlockDataType(t base.BlockDataType) bool {
	switch t {
	case base.BlockDataTypeOperations,
		base.BlockDataTypeOperationsTree,
		base.BlockDataTypeStates,
		base.BlockDataTypeStatesTree:
		return true
	default:
		return false
	}
}

func blockDataFSMapFilename(enc encoder.Encoder) string {
	return fmt.Sprintf("%s%s", blockDataMapFilename, fileExtFromEncoder(enc))
}
