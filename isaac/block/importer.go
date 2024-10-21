package isaacblock

import (
	"context"
	"crypto/sha256"
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type (
	ImportBlocksBlockMapFunc  func(context.Context, base.Height) (base.BlockMap, bool, error)
	ImportBlocksBlockItemFunc func(
		context.Context, base.Height, base.BlockItemType, func(_ io.Reader, found bool, compressFormat string) error,
	) error
)

type BlockImporter struct {
	m                        base.BlockMap
	encs                     *encoder.Encoders
	bwdb                     isaac.BlockWriteDatabase
	sufst                    base.State
	localfs                  *LocalFSImporter
	finisheds                *util.ShardedMap[base.BlockItemType, bool]
	mergeBlockWriteDatabasef func(context.Context) error
	root                     string
	networkID                base.NetworkID
	statestree               fixedtree.Tree
	batchlimit               uint64
}

func NewBlockImporter(
	root string,
	encs *encoder.Encoders,
	m base.BlockMap,
	bwdb isaac.BlockWriteDatabase,
	mergeBlockWriteDatabasef func(context.Context) error,
	networkID base.NetworkID,
) (*BlockImporter, error) {
	e := util.StringError("BlockImporter")

	localfs, err := NewLocalFSImporter(root, encs.JSON(), encs.Default(), m)
	if err != nil {
		return nil, e.Wrap(err)
	}

	finisheds, _ := util.NewShardedMap[base.BlockItemType, bool](6, nil) //nolint:mnd //...

	im := &BlockImporter{
		root:                     root,
		m:                        m,
		encs:                     encs,
		localfs:                  localfs,
		bwdb:                     bwdb,
		networkID:                networkID,
		finisheds:                finisheds,
		batchlimit:               333, //nolint:mnd // enough big size
		mergeBlockWriteDatabasef: mergeBlockWriteDatabasef,
	}

	if err := im.WriteMap(m); err != nil {
		return nil, e.Wrap(err)
	}

	return im, nil
}

func (im *BlockImporter) WriteMap(m base.BlockMap) error {
	e := util.StringError("write BlockMap")

	im.m = m

	m.Items(func(item base.BlockMapItem) bool {
		_ = im.finisheds.SetValue(item.Type(), false)

		return true
	})

	// NOTE save map
	if err := im.localfs.WriteMap(m); err != nil {
		return e.WithMessage(err, "write BlockMap")
	}

	if err := im.bwdb.SetBlockMap(m); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (im *BlockImporter) WriteItem(t base.BlockItemType, ir isaac.BlockItemReader) error {
	e := util.StringError("write item")

	if err := im.importItem(t, ir); err != nil {
		return e.WithMessage(err, "import item, %q", t)
	}

	_ = im.finisheds.SetValue(t, true)

	return nil
}

func (im *BlockImporter) Save(context.Context) (func(context.Context) error, error) {
	e := util.StringError("save")

	if !im.isfinished() {
		return nil, e.Errorf("not yet finished")
	}

	if im.sufst != nil {
		proof, err := im.statestree.Proof(im.sufst.Hash().String())
		if err != nil {
			return nil, e.WithMessage(err, "make proof of suffrage state")
		}

		sufproof := NewSuffrageProof(im.m, im.sufst, proof)

		if err := im.bwdb.SetSuffrageProof(sufproof); err != nil {
			return nil, e.Wrap(err)
		}
	}

	if err := im.bwdb.Write(); err != nil {
		return nil, e.Wrap(err)
	}

	if err := im.localfs.Save(); err != nil {
		return nil, e.Wrap(err)
	}

	return func(ctx context.Context) error {
		return im.mergeBlockWriteDatabasef(ctx)
	}, nil
}

func (im *BlockImporter) CancelImport(context.Context) error {
	e := util.StringError("cancel")

	if err := im.bwdb.Cancel(); err != nil {
		return e.Wrap(err)
	}

	if err := im.localfs.Cancel(); err != nil {
		return e.Wrap(err)
	}

	im.finisheds.Close()

	return nil
}

func (im *BlockImporter) importItem(t base.BlockItemType, ir isaac.BlockItemReader) error {
	item, found := im.m.Item(t)
	if !found {
		return nil
	}

	var cw util.ChecksumWriter

	switch w, err := im.localfs.WriteItem(t, ir.Encoder().Hint(), ir.Reader().Format); {
	case err != nil:
		return err
	default:
		defer func() {
			_ = w.Close()
		}()

		cw = util.NewHashChecksumWriter(sha256.New())
		defer func() {
			_ = cw.Close()
		}()

		if _, err := ir.Reader().Tee(w, cw); err != nil {
			return err
		}
	}

	if err := func() error {
		switch t {
		case base.BlockItemStatesTree:
			return im.importStatesTree(ir)
		case base.BlockItemStates:
			return im.importStates(ir)
		case base.BlockItemOperations:
			return im.importOperations(ir)
		case base.BlockItemVoteproofs:
			return im.importVoteproofs(ir)
		default:
			return im.importOther(ir)
		}
	}(); err != nil {
		return err
	}

	if cw.Checksum() != item.Checksum() {
		return errors.Errorf("checksum does not match, expected=%q != %q", item.Checksum(), cw.Checksum())
	}

	return nil
}

func (im *BlockImporter) isfinished() bool {
	var notyet bool
	im.m.Items(func(item base.BlockMapItem) bool {
		switch finished, found := im.finisheds.Value(item.Type()); {
		case !found:
			notyet = true

			return false
		case !finished:
			notyet = true

			return false
		default:
			return true
		}
	})

	return !notyet
}

func (im *BlockImporter) importOperations(ir isaac.BlockItemReader) error {
	validate := func(op base.Operation) error {
		return op.IsValid(im.networkID)
	}
	if im.m.Manifest().Height() == base.GenesisHeight {
		validate = func(op base.Operation) error {
			return base.IsValidGenesisOperation(op, im.networkID, im.m.Signer())
		}
	}

	var once sync.Once
	var ops []base.Operation
	var ophs []util.Hash

	switch i, err := ir.DecodeItems(
		func(total, index uint64, v interface{}) error {
			once.Do(func() {
				ops = make([]base.Operation, total)
				ophs = make([]util.Hash, total)
			})

			switch op, err := util.AssertInterfaceValue[base.Operation](v); {
			case err != nil:
				return err
			default:
				if err := validate(op); err != nil {
					return err
				}

				ops[index] = op
				ophs[index] = op.Hash()

				return nil
			}
		},
	); {
	case err != nil:
		return err
	default:
		ops = ops[:i]

		return im.bwdb.SetOperations(ophs[:len(ops)])
	}
}

func (im *BlockImporter) importStates(ir isaac.BlockItemReader) error {
	var once sync.Once
	var sts []base.State

	switch i, err := ir.DecodeItems(
		func(total, index uint64, v interface{}) error {
			once.Do(func() {
				sts = make([]base.State, total)
			})

			switch st, err := util.AssertInterfaceValue[base.State](v); {
			case err != nil:
				return err
			default:
				if err := st.IsValid(nil); err != nil {
					return err
				}

				if base.IsSuffrageNodesState(st) {
					im.sufst = st
				}

				sts[index] = st

				return nil
			}
		},
	); {
	case err != nil:
		return err
	default:
		sts = sts[:i]

		return im.bwdb.SetStates(sts)
	}
}

func (im *BlockImporter) importStatesTree(ir isaac.BlockItemReader) error {
	switch v, err := ir.Decode(); {
	case err != nil:
		return errors.WithMessage(err, "states tree")
	default:
		return util.SetInterfaceValue(v, &im.statestree)
	}
}

func (im *BlockImporter) importVoteproofs(ir isaac.BlockItemReader) error {
	switch v, err := ir.Decode(); {
	case err != nil:
		return errors.WithMessage(err, "voteproofs")
	default:
		switch vps, err := util.AssertInterfaceValue[[2]base.Voteproof](v); {
		case err != nil:
			return err
		default:
			for i := range vps {
				if err := vps[i].IsValid(im.networkID); err != nil {
					return err
				}
			}

			if err := base.IsValidVoteproofsWithManifest(vps, im.m.Manifest()); err != nil {
				return err
			}

			return nil
		}
	}
}

func (*BlockImporter) importOther(ir isaac.BlockItemReader) error {
	if err := ir.Reader().Exaust(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
