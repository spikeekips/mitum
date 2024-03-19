package isaacblock

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
)

type FSWriter interface {
	SetProposal(context.Context, base.ProposalSignFact) error
	SetOperation(_ context.Context, total, index uint64, _ base.Operation) error
	SetOperationsTree(context.Context, fixedtree.Tree) error
	SetState(_ context.Context, total, index uint64, _ base.State) error
	SetStatesTree(context.Context, fixedtree.Tree) error
	SetManifest(context.Context, base.Manifest) error
	SetINITVoteproof(context.Context, base.INITVoteproof) error
	SetACCEPTVoteproof(context.Context, base.ACCEPTVoteproof) error
	Save(context.Context) (base.BlockMap, error)
	Cancel() error
}

type Writer struct {
	*logging.Logging
	manifest      base.Manifest
	proposal      base.ProposalSignFact
	opstree       fixedtree.Tree
	db            isaac.BlockWriteDatabase
	fswriter      FSWriter
	mergeDatabase func(isaac.BlockWriteDatabase) error
	saveWorker    func(bool) *util.ErrgroupWorker
	opstreeg      *fixedtree.Writer
	getStateFunc  base.GetStateFunc
	statesMerger  StatesMerger
	ststree       fixedtree.Tree
	workersize    int64
	sync.RWMutex
}

func NewWriter(
	proposal base.ProposalSignFact,
	getStateFunc base.GetStateFunc,
	db isaac.BlockWriteDatabase,
	mergeDatabase func(isaac.BlockWriteDatabase) error,
	fswriter FSWriter,
	workersize int64,
) *Writer {
	statesMerger := NewDefaultStatesMerger( //revive:disable-line:modifies-parameter
		proposal.ProposalFact().Point().Height(),
		getStateFunc,
		workersize,
	)

	return &Writer{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "block-writer")
		}),
		proposal:      proposal,
		getStateFunc:  getStateFunc,
		db:            db,
		mergeDatabase: mergeDatabase,
		fswriter:      fswriter,
		workersize:    workersize,
		statesMerger:  statesMerger,
		saveWorker: func() func(bool) *util.ErrgroupWorker {
			var ew *util.ErrgroupWorker
			var saveWorkerOnce sync.Once

			return func(create bool) *util.ErrgroupWorker {
				if !create {
					return ew
				}

				saveWorkerOnce.Do(func() {
					ew, _ = util.NewErrgroupWorker(context.Background(), workersize)
				})

				return ew
			}
		}(),
	}
}

func (w *Writer) SetOperationsSize(n uint64) {
	w.Lock()
	defer w.Unlock()

	opstreeg, err := fixedtree.NewWriter(base.OperationFixedtreeHint, n)
	if err != nil {
		return
	}

	w.opstreeg = opstreeg
}

func (w *Writer) SetProcessResult( // revive:disable-line:flag-parameter
	_ context.Context,
	index uint64,
	op, facthash util.Hash,
	instate bool,
	errorreason base.OperationProcessReasonError,
) error {
	e := util.StringError("set operation")

	if op != nil {
		if err := w.saveWorker(true).NewJob(func(context.Context, uint64) error {
			return w.db.SetOperations([]util.Hash{op})
		}); err != nil {
			return e.Wrap(err)
		}
	}

	var msg string
	if errorreason != nil {
		msg = errorreason.Msg()
	}

	var node base.OperationFixedtreeNode
	if instate {
		node = base.NewInStateOperationFixedtreeNode(facthash, msg)
	} else {
		node = base.NewNotInStateOperationFixedtreeNode(facthash, msg)
	}

	if err := w.opstreeg.Add(index, node); err != nil {
		return e.WithMessage(err, "set operation")
	}

	return nil
}

func (w *Writer) SetStates(
	ctx context.Context, index uint64, states []base.StateMergeValue, operation base.Operation,
) error {
	e := util.StringError("set states")

	if w.proposal == nil {
		return e.Errorf("not yet written")
	}

	if err := w.statesMerger.SetStates(ctx, index, states, operation.Fact().Hash()); err != nil {
		return e.Wrap(err)
	}

	if err := w.saveWorker(true).NewJob(func(ctx context.Context, _ uint64) error {
		return w.fswriter.SetOperation(ctx, uint64(w.opstreeg.Len()), index, operation)
	}); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (w *Writer) closeStateValues(
	ctx context.Context,
	catchState func(base.State),
) error {
	defer logging.TimeElapsed()(w.Log().Debug(), "close state values")

	if w.statesMerger.Len() < 1 {
		return nil
	}

	e := util.StringError("close state values")

	var tg *fixedtree.Writer

	switch i, err := w.statesMergerClose(ctx, catchState); {
	case err != nil:
		return e.Wrap(err)
	default:
		tg = i
	}

	switch tr, err := tg.Tree(); {
	case err != nil:
		return e.Wrap(err)
	default:
		w.ststree = tr

		return nil
	}
}

func (w *Writer) statesMergerClose(
	ctx context.Context,
	catchState func(base.State),
) (tg *fixedtree.Writer, _ error) {
	defer logging.TimeElapsed()(w.Log().Debug(), "close states merger")

	if err := w.statesMerger.CloseStates(
		ctx,
		func(keyscount uint64) error {
			if keyscount < 1 {
				return nil
			}

			switch i, err := fixedtree.NewWriter(base.StateFixedtreeHint, keyscount); {
			case err != nil:
				return err
			default:
				tg = i

				return nil
			}
		},
		func(st base.State, total, index uint64) error {
			catchState(st)

			if _, ok := st.(base.StateValueMerger); ok {
				return errors.Errorf("expect pure State, not StateValueMerger, %T", st)
			}

			if err := tg.Add(index, fixedtree.NewBaseNode(st.Hash().String())); err != nil {
				return err
			}

			return w.saveWorker(true).NewJob(func(ctx context.Context, _ uint64) error {
				if err := w.fswriter.SetState(ctx, total, index, st); err != nil {
					return err
				}

				return w.db.SetStates([]base.State{st})
			})
		},
	); err != nil {
		return nil, err
	}

	return tg, nil
}

func (w *Writer) Manifest(ctx context.Context, previous base.Manifest) (base.Manifest, error) {
	w.Lock()
	defer w.Unlock()

	e := util.StringError("make manifest")

	if w.proposal == nil || (previous == nil && w.proposal.Point().Height() > base.GenesisHeight) {
		return nil, e.Errorf("not yet written")
	}

	var opstreeroot util.Hash

	if w.opstreeg != nil {
		switch tr, err := w.opstreeg.Tree(); {
		case err != nil:
			return nil, e.Wrap(err)
		default:
			w.opstree = tr

			if w.opstree.Len() > 0 {
				opstreeroot = tr.Root()
			}
		}
	}

	var suffrage, previousHash util.Hash

	if w.proposal.Point().Height() > base.GenesisHeight {
		suffrage = previous.Suffrage()
		previousHash = previous.Hash()
	}

	if err := w.closeStateValues(ctx, func(st base.State) {
		if st.Key() == isaac.SuffrageStateKey {
			suffrage = st.Hash()
		}
	}); err != nil {
		return nil, e.Wrap(err)
	}

	var ststreeroot util.Hash
	if w.ststree.Len() > 0 {
		ststreeroot = w.ststree.Root()
	}

	if w.manifest == nil {
		w.manifest = isaac.NewManifest(
			w.proposal.Point().Height(),
			previousHash,
			w.proposal.Fact().Hash(),
			opstreeroot,
			ststreeroot,
			suffrage,
			w.proposal.ProposalFact().ProposedAt(),
		)

		if err := w.saveWorker(true).NewJob(func(ctx context.Context, _ uint64) error {
			return w.fswriter.SetManifest(ctx, w.manifest)
		}); err != nil {
			return nil, e.Wrap(err)
		}
	}

	return w.manifest, nil
}

func (w *Writer) SetINITVoteproof(_ context.Context, vp base.INITVoteproof) error {
	if err := w.saveWorker(true).NewJob(func(ctx context.Context, _ uint64) error {
		return w.fswriter.SetINITVoteproof(ctx, vp)
	}); err != nil {
		return errors.Wrap(err, "set init voteproof")
	}

	return nil
}

func (w *Writer) SetACCEPTVoteproof(_ context.Context, vp base.ACCEPTVoteproof) error {
	if err := w.saveWorker(true).NewJob(func(ctx context.Context, _ uint64) error {
		return w.fswriter.SetACCEPTVoteproof(ctx, vp)
	}); err != nil {
		return errors.Wrap(err, "set accept voteproof")
	}

	return nil
}

func (w *Writer) Save(ctx context.Context) (base.BlockMap, error) {
	w.Lock()
	defer w.Unlock()

	e := util.StringError("save")

	if err := w.waitSaveWorker(ctx); err != nil {
		return nil, e.Wrap(err)
	}

	var m base.BlockMap

	switch i, err := w.fswriter.Save(ctx); {
	case err != nil:
		return nil, e.Wrap(err)
	default:
		if err := w.db.SetBlockMap(i); err != nil {
			return nil, e.Wrap(err)
		}

		m = i
	}

	if st := w.db.SuffrageState(); st != nil {
		// NOTE save suffrageproof
		proof, err := w.ststree.Proof(st.Hash().String())
		if err != nil {
			return nil, e.WithMessage(err, "make proof of suffrage state")
		}

		sufproof := NewSuffrageProof(m, st, proof)

		if err := w.db.SetSuffrageProof(sufproof); err != nil {
			return nil, e.Wrap(err)
		}
	}

	if err := w.mergeDatabase(w.db); err != nil {
		return nil, e.Wrap(err)
	}

	if err := w.close(); err != nil {
		return nil, e.Wrap(err)
	}

	return m, nil
}

func (w *Writer) Cancel() error {
	w.Lock()
	defer w.Unlock()

	e := util.StringError("cancel Writer")

	if ew := w.saveWorker(false); ew != nil {
		ew.Close()
	}

	if err := w.fswriter.Cancel(); err != nil {
		return e.Wrap(err)
	}

	if err := w.db.Cancel(); err != nil {
		return e.Wrap(err)
	}

	return w.close()
}

func (w *Writer) close() error {
	w.manifest = nil
	w.proposal = nil
	w.db = nil
	w.fswriter = nil
	w.mergeDatabase = nil
	w.opstreeg = nil
	w.getStateFunc = nil
	_ = w.statesMerger.Close()
	w.ststree = fixedtree.Tree{}

	return nil
}

func (w *Writer) waitSaveWorker(ctx context.Context) error {
	saveWorker := w.saveWorker(false)
	if saveWorker == nil {
		return nil
	}

	defer saveWorker.Close()

	if err := w.saveWorker(true).NewJob(func(ctx context.Context, _ uint64) error {
		return w.fswriter.SetProposal(ctx, w.proposal)
	}); err != nil {
		return errors.Wrap(err, "set proposal")
	}

	saveWorker.Done()

	if err := saveWorker.Wait(); err != nil {
		return err
	}

	if err := w.db.Write(); err != nil {
		return err
	}

	if w.opstree.Len() > 0 {
		if err := w.fswriter.SetOperationsTree(ctx, w.opstree); err != nil {
			return err
		}
	}

	if w.ststree.Len() > 0 {
		if err := w.fswriter.SetStatesTree(ctx, w.ststree); err != nil {
			return err
		}
	}

	return nil
}
