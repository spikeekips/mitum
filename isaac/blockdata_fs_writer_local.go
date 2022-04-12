package isaac

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/tree"
)

var LocalBlockDataFSWriterHint = hint.MustNewHint("local-blockdata-fs-writer-v0.0.1")

var blockDataFilenames = map[base.BlockDataType]string{
	base.BlockDataTypeProposal:       "proposal",
	base.BlockDataTypeOperations:     "operations",
	base.BlockDataTypeOperationsTree: "operations_tree",
	base.BlockDataTypeStates:         "states",
	base.BlockDataTypeStatesTree:     "states_tree",
	base.BlockDataTypeVoteproofs:     "voteproofs",
}

var ulid = util.NewULID()

type LocalBlockDataFSWriter struct {
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
	opsf       *util.ChecksumWriter
	stsf       *util.ChecksumWriter
}

func NewLocalBlockDataFSWriter(
	root string,
	height base.Height,
	enc encoder.Encoder,
	local base.LocalNode,
	networkID base.NetworkID,
) (*LocalBlockDataFSWriter, error) {
	e := util.StringErrorFunc("failed to create LocalBlockDataFSWriter")
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
	if err := os.Mkdir(temp, 0o700); err != nil {
		return nil, e(err, "failed to create temp directory")
	}

	heightbase := HeightDirectory(height)
	if err := os.MkdirAll(filepath.Join(abs, heightbase), 0o700); err != nil {
		return nil, e(err, "failed to create temp directory")
	}

	w := &LocalBlockDataFSWriter{
		BaseHinter: hint.NewBaseHinter(LocalBlockDataFSWriterHint),
		id:         id,
		root:       abs,
		height:     height,
		enc:        enc,
		local:      local,
		networkID:  networkID,
		heightbase: heightbase,
		temp:       temp,
		m:          NewBlockDataMap(LocalBlockDataFSWriterHint, enc.Hint()),
	}

	switch f, err := w.newChecksumWriter(base.BlockDataTypeOperations, true); {
	case err != nil:
		return nil, e(err, "failed to create operations file")
	default:
		w.opsf = f
	}

	switch f, err := w.newChecksumWriter(base.BlockDataTypeStates, true); {
	case err != nil:
		return nil, e(err, "failed to create states file")
	default:
		w.stsf = f
	}

	return w, nil
}

func (w *LocalBlockDataFSWriter) SetProposal(ctx context.Context, pr base.ProposalSignedFact) error {
	if err := w.writeItem(base.BlockDataTypeProposal, pr); err != nil {
		return errors.Wrap(err, "failed to set proposal in fs writer")
	}

	return nil
}

func (w *LocalBlockDataFSWriter) SetOperation(ctx context.Context, index int, op base.Operation) error {
	if err := w.writefile(w.opsf, op, true); err != nil {
		return errors.Wrap(err, "failed to set operation")
	}

	return nil
}

func (w *LocalBlockDataFSWriter) SetOperationsTree(ctx context.Context, tr tree.FixedTree) error {
	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

	e := util.StringErrorFunc("failed to set operations tree")

	tf, err := w.newChecksumWriter(base.BlockDataTypeOperationsTree, true)
	if err != nil {
		return e(err, "failed to create operations tree file")
	}
	defer func() {
		_ = tf.Close()
	}()

	if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
		_ = w.opsf.Close()

		if err := w.m.SetItem(NewLocalBlockDataMapItem(base.BlockDataTypeOperations, filepath.Join(w.savedir(), w.opsf.Name()), w.opsf.Checksum())); err != nil {
			return errors.Wrap(err, "failed to set operations")
		}

		return nil
	}); err != nil {
		return e(err, "")
	}

	go func() {
		defer worker.Done()

		_ = tr.Traverse(func(node tree.FixedTreeNode) (bool, error) {
			n := node
			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				if err := w.writefile(tf, n, true); err != nil {
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

	if err := w.m.SetItem(NewLocalBlockDataMapItem(base.BlockDataTypeOperationsTree, filepath.Join(w.savedir(), tf.Name()), tf.Checksum())); err != nil {
		return errors.Wrap(err, "failed to set operations tree")
	}

	return nil
}

func (w *LocalBlockDataFSWriter) SetState(ctx context.Context, index int /* BLOCK remove */, st base.State) error {
	if err := w.writefile(w.stsf, st, true); err != nil {
		return errors.Wrap(err, "failed to set state")
	}

	return nil
}

func (w *LocalBlockDataFSWriter) SetStatesTree(ctx context.Context, tr tree.FixedTree) error {
	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

	e := util.StringErrorFunc("failed to set states tree")

	tf, err := w.newChecksumWriter(base.BlockDataTypeStatesTree, true)
	if err != nil {
		return e(err, "failed to create states tree file")
	}
	defer func() {
		_ = tf.Close()
	}()

	if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
		_ = w.opsf.Close()

		if err := w.m.SetItem(NewLocalBlockDataMapItem(base.BlockDataTypeStates, filepath.Join(w.savedir(), w.stsf.Name()), w.stsf.Checksum())); err != nil {
			return errors.Wrap(err, "failed to set states")
		}

		return nil
	}); err != nil {
		return e(err, "")
	}

	go func() {
		defer worker.Done()

		_ = tr.Traverse(func(node tree.FixedTreeNode) (bool, error) {
			n := node
			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				if err := w.writefile(tf, n, true); err != nil {
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

	if err := w.m.SetItem(NewLocalBlockDataMapItem(base.BlockDataTypeStatesTree, filepath.Join(w.savedir(), tf.Name()), tf.Checksum())); err != nil {
		return errors.Wrap(err, "failed to set states tree")
	}

	return nil
}

func (w *LocalBlockDataFSWriter) SetManifest(_ context.Context, m base.Manifest) error {
	w.m.SetManifest(m)

	return nil
}

func (w *LocalBlockDataFSWriter) SetINITVoteproof(_ context.Context, vp base.INITVoteproof) error {
	w.vps[0] = vp
	if w.vps[1] == nil {
		return nil
	}

	if err := w.writeItem(base.BlockDataTypeVoteproofs, w.vps); err != nil {
		return errors.Wrap(err, "failed to set voteproofs in fs writer")
	}

	return nil
}

func (w *LocalBlockDataFSWriter) SetACCEPTVoteproof(_ context.Context, vp base.ACCEPTVoteproof) error {
	w.vps[1] = vp
	if w.vps[0] == nil {
		return nil
	}

	if err := w.writeItem(base.BlockDataTypeVoteproofs, w.vps); err != nil {
		return errors.Wrap(err, "failed to set voteproofs in fs writer")
	}

	return nil
}

func (w *LocalBlockDataFSWriter) Save(ctx context.Context) (base.BlockDataMap, error) {
	w.Lock()
	defer w.Unlock()

	if w.opsf != nil {
		_ = w.opsf.Close()

		if item, found := w.m.Item(base.BlockDataTypeOperations); !found || item == nil {
			_ = os.Remove(filepath.Join(w.temp, w.opsf.Name())) // NOTE remove empty operations file
		}
	}

	e := util.StringErrorFunc("failed to save fs writer")
	if item, found := w.m.Item(base.BlockDataTypeVoteproofs); !found || item == nil {
		return nil, e(nil, "empty voteproofs")
	}

	if err := w.m.Sign(w.local.Address(), w.local.Privatekey(), w.networkID); err != nil {
		return nil, e(err, "")
	}

	if err := os.Rename(w.temp, filepath.Join(w.root, w.savedir())); err != nil {
		return nil, e(err, "")
	}

	return w.m, nil
}

func (w *LocalBlockDataFSWriter) Cancel() error {
	w.Lock()
	defer w.Unlock()

	if w.opsf != nil {
		_ = w.opsf.Close()
		w.opsf = nil
	}

	e := util.StringErrorFunc("failed to cancel fs writer")
	if err := os.RemoveAll(w.temp); err != nil {
		return e(err, "failed to remove temp directory")
	}

	return nil
}

func (w *LocalBlockDataFSWriter) savedir() string {
	return filepath.Join(w.heightbase, w.id)
}

func (w *LocalBlockDataFSWriter) filename(t base.BlockDataType, islist bool) (filename string, temppath string, err error) {
	f, err := BlockDataFileName(t, FileExtFromEncoder(w.enc, islist))
	if err != nil {
		return "", "", errors.Wrap(err, "")
	}

	return f, filepath.Join(w.temp, f), nil
}

func (w *LocalBlockDataFSWriter) writeItem(t base.BlockDataType, i interface{}) error {
	fname, temppath, err := w.filename(t, false)
	if err != nil {
		return errors.Wrap(err, "")
	}

	f, err := os.OpenFile(temppath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return errors.Wrap(err, "")
	}

	cw := util.NewChecksumWriter(fname, f)
	defer func() {
		_ = cw.Close()
	}()

	if err := w.writefile(cw, i, false); err != nil {
		return errors.Wrap(err, "")
	}

	_ = cw.Close()

	if err := w.m.SetItem(NewLocalBlockDataMapItem(t, filepath.Join(w.savedir(), fname), cw.Checksum())); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (w *LocalBlockDataFSWriter) writefile(f io.Writer, i interface{}, newline bool) error {
	b, err := w.enc.Marshal(i)
	if err != nil {
		return errors.Wrap(err, "")
	}

	if newline {
		b = append(b, '\n')
	}

	if _, err := f.Write(b); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (w *LocalBlockDataFSWriter) newChecksumWriter(t base.BlockDataType, islist bool) (*util.ChecksumWriter, error) {
	fname, temppath, _ := w.filename(t, islist)
	switch f, err := os.OpenFile(temppath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644); {
	case err != nil:
		return nil, errors.Wrap(err, "")
	default:
		return util.NewChecksumWriter(fname, f), nil
	}
}

func BlockDataFileName(t base.BlockDataType, ext string) (string, error) {
	name, found := blockDataFilenames[t]
	if !found {
		return "", errors.Errorf("unknown block data type, %q", t)
	}

	if len(ext) < 1 {
		return name, nil
	}

	return fmt.Sprintf("%s%s", name, ext), nil
}

func FileExtFromEncoder(enc encoder.Encoder, islist bool) string {
	switch {
	case strings.Contains(strings.ToLower(enc.Hint().Type().String()), "json"):
		if islist {
			return ".ndjson"
		}

		return ".json"
	default:
		if islist {
			return ".blist"
		}

		return ".b" // NOTE means b(lock)
	}
}
