package isaacblock

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/localtime"
)

type FSWriter interface {
	SetProposal(context.Context, base.ProposalSignedFact) error
	SetOperation(context.Context, uint64, base.Operation) error
	SetOperationsTree(context.Context, *fixedtree.Writer) error
	SetState(context.Context, uint64, base.State) error
	SetStatesTree(context.Context, *fixedtree.Writer) error
	SetManifest(context.Context, base.Manifest) error
	SetINITVoteproof(context.Context, base.INITVoteproof) error
	SetACCEPTVoteproof(context.Context, base.ACCEPTVoteproof) error
	Save(context.Context) (base.BlockMap, error)
	Cancel() error
}

type Writer struct {
	manifest      base.Manifest
	proposal      base.ProposalSignedFact
	opstreeroot   util.Hash
	db            isaac.BlockWriteDatabase
	fswriter      FSWriter
	ststreeroot   util.Hash
	mergeDatabase func(isaac.BlockWriteDatabase) error
	opstreeg      *fixedtree.Writer
	getStateFunc  base.GetStateFunc
	states        *util.LockedMap
	sync.RWMutex
}

func NewWriter(
	proposal base.ProposalSignedFact,
	getStateFunc base.GetStateFunc,
	db isaac.BlockWriteDatabase,
	mergeDatabase func(isaac.BlockWriteDatabase) error,
	fswriter FSWriter,
) *Writer {
	return &Writer{
		proposal:      proposal,
		getStateFunc:  getStateFunc,
		db:            db,
		mergeDatabase: mergeDatabase,
		fswriter:      fswriter,
		states:        util.NewLockedMap(),
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
	e := util.StringErrorFunc("failed to set operation")

	if op != nil {
		if err := w.db.SetOperations([]util.Hash{op}); err != nil {
			return e(err, "")
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
		return e(err, "failed to set operation")
	}

	return nil
}

func (w *Writer) SetStates(
	ctx context.Context, index uint64, states []base.StateMergeValue, operation base.Operation,
) error {
	e := util.StringErrorFunc("failed to set states")

	if w.proposal == nil {
		return e(nil, "not yet written")
	}

	for i := range states {
		if err := w.SetState(ctx, states[i], operation); err != nil {
			return e(err, "")
		}
	}

	if err := w.fswriter.SetOperation(ctx, index, operation); err != nil {
		return e(err, "")
	}

	return nil
}

func (w *Writer) SetState(_ context.Context, stv base.StateMergeValue, operation base.Operation) error {
	e := util.StringErrorFunc("failed to set state")

	j, _, err := w.states.Get(stv.Key(), func() (interface{}, error) {
		var st base.State

		switch j, found, err := w.getStateFunc(stv.Key()); {
		case err != nil:
			return nil, err
		case found:
			st = j
		}

		return stv.Merger(w.proposal.Point().Height(), st), nil
	})
	if err != nil {
		return e(err, "")
	}

	if err := j.(base.StateValueMerger).Merge( //nolint:forcetypeassert //...
		stv.Value(), []util.Hash{operation.Fact().Hash()}); err != nil {
		return e(err, "failed to merge")
	}

	return nil
}

func (w *Writer) closeStateValues(ctx context.Context) error {
	if w.states.Len() < 1 {
		return nil
	}

	e := util.StringErrorFunc("failed to close state values")

	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

	if w.opstreeg != nil {
		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			if err := w.fswriter.SetOperationsTree(ctx, w.opstreeg); err != nil {
				return errors.Wrap(err, "")
			}

			w.opstreeroot = w.opstreeg.Root()

			return nil
		}); err != nil {
			return e(err, "")
		}
	}

	sortedkeys := make([]string, w.states.Len())
	{
		var i int
		w.states.Traverse(func(k, _ interface{}) bool {
			sortedkeys[i] = k.(string) //nolint:forcetypeassert //...
			i++

			return true
		})
	}

	sort.Slice(sortedkeys, func(i, j int) bool {
		return strings.Compare(sortedkeys[i], sortedkeys[j]) < 0
	})

	tg, err := fixedtree.NewWriter(base.StateFixedtreeHint, uint64(w.states.Len()))
	if err != nil {
		return e(err, "")
	}

	states := make([]base.State, w.states.Len())

	go func() {
		defer worker.Done()

		for i := range sortedkeys {
			v, _ := w.states.Value(sortedkeys[i])
			st := v.(base.State) //nolint:forcetypeassert //...

			index := uint64(i)

			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				nst, err := w.closeStateValue(tg, st, index)
				if err != nil {
					return err
				}

				if err := w.fswriter.SetState(ctx, index, nst); err != nil {
					return errors.Wrap(err, "")
				}

				if err := w.db.SetStates([]base.State{nst}); err != nil {
					return errors.Wrap(err, "")
				}

				states[index] = nst

				return nil
			}); err != nil {
				return
			}
		}
	}()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	if err := w.saveStates(ctx, tg, states); err != nil {
		return e(err, "")
	}

	if err := w.db.Write(); err != nil {
		return e(err, "")
	}

	return nil
}

func (*Writer) closeStateValue(
	tg *fixedtree.Writer, st base.State, index uint64,
) (base.State, error) {
	stm, ok := st.(base.StateValueMerger)
	if !ok {
		return st, tg.Add(index, fixedtree.NewBaseNode(st.Hash().String()))
	}

	if err := stm.Close(); err != nil {
		return nil, errors.Wrap(err, "")
	}

	return stm, tg.Add(index, fixedtree.NewBaseNode(stm.Hash().String()))
}

func (w *Writer) saveStates(
	ctx context.Context, tg *fixedtree.Writer, states []base.State,
) error {
	if err := util.RunErrgroupWorkerByJobs(
		ctx,
		func(ctx context.Context, _ uint64) error {
			return w.db.SetStates(states) //nolint:wrapcheck //...
		},
		func(ctx context.Context, _ uint64) error {
			if err := w.fswriter.SetStatesTree(ctx, tg); err != nil {
				return errors.Wrap(err, "")
			}

			w.ststreeroot = tg.Root()

			return nil
		},
	); err != nil {
		return errors.Wrap(err, "failed to set states tree")
	}

	return nil
}

func (w *Writer) Manifest(ctx context.Context, previous base.Manifest) (base.Manifest, error) {
	w.Lock()
	defer w.Unlock()

	e := util.StringErrorFunc("failed to make manifest")

	if w.proposal == nil || (previous == nil && w.proposal.Point().Height() > base.GenesisHeight) {
		return nil, e(nil, "not yet written")
	}

	if err := w.setProposal(ctx); err != nil {
		return nil, e(err, "")
	}

	if err := w.closeStateValues(ctx); err != nil {
		return nil, e(err, "")
	}

	var suffrage, previousHash util.Hash
	if w.proposal.Point().Height() > base.GenesisHeight {
		suffrage = previous.Suffrage()
		previousHash = previous.Hash()
	}

	if i := w.db.SuffrageState(); i != nil {
		suffrage = i.Hash()
	}

	if w.manifest == nil {
		w.manifest = isaac.NewManifest(
			w.proposal.Point().Height(),
			previousHash,
			w.proposal.Fact().Hash(),
			w.opstreeroot,
			w.ststreeroot,
			suffrage,
			localtime.UTCNow(),
		)

		if err := w.fswriter.SetManifest(ctx, w.manifest); err != nil {
			return nil, e(err, "")
		}
	}

	return w.manifest, nil
}

func (w *Writer) SetINITVoteproof(ctx context.Context, vp base.INITVoteproof) error {
	if err := w.fswriter.SetINITVoteproof(ctx, vp); err != nil {
		return errors.Wrap(err, "failed to set init voteproof")
	}

	return nil
}

func (w *Writer) SetACCEPTVoteproof(ctx context.Context, vp base.ACCEPTVoteproof) error {
	if err := w.fswriter.SetACCEPTVoteproof(ctx, vp); err != nil {
		return errors.Wrap(err, "failed to set accept voteproof")
	}

	return nil
}

func (w *Writer) Save(ctx context.Context) (base.BlockMap, error) {
	w.Lock()
	defer w.Unlock()

	e := util.StringErrorFunc("failed to save")

	var m base.BlockMap

	switch i, err := w.fswriter.Save(ctx); {
	case err != nil:
		return nil, e(err, "")
	default:
		if err := w.db.SetMap(i); err != nil {
			return nil, e(err, "")
		}

		m = i
	}

	if err := w.mergeDatabase(w.db); err != nil {
		return nil, e(err, "")
	}

	return m, nil
}

func (w *Writer) Cancel() error {
	w.Lock()
	defer w.Unlock()

	e := util.StringErrorFunc("failed to cancel Writer")
	if err := w.fswriter.Cancel(); err != nil {
		return e(err, "")
	}

	if err := w.db.Cancel(); err != nil {
		return e(err, "")
	}

	return nil
}

func (w *Writer) setProposal(ctx context.Context) error {
	if err := w.fswriter.SetProposal(ctx, w.proposal); err != nil {
		return errors.Wrap(err, "failed to set proposal")
	}

	return nil
}
