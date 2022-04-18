package blockdata

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
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/tree"
	"github.com/spikeekips/mitum/util/valuehash"
)

type FSWriter interface {
	SetProposal(context.Context, base.ProposalSignedFact) error
	SetOperation(context.Context, int, base.Operation) error
	SetOperationsTree(context.Context, tree.FixedTree) error
	SetState(context.Context, int, base.State) error
	SetStatesTree(context.Context, tree.FixedTree) error
	SetManifest(context.Context, base.Manifest) error
	SetINITVoteproof(context.Context, base.INITVoteproof) error
	SetACCEPTVoteproof(context.Context, base.ACCEPTVoteproof) error
	Save(context.Context) (base.BlockDataMap, error)
	Cancel() error
}

type Writer struct {
	sync.RWMutex
	proposal      base.ProposalSignedFact
	getStateFunc  base.GetStateFunc
	db            isaac.BlockWriteDatabase
	mergeDatabase func(isaac.BlockWriteDatabase) error
	fswriter      FSWriter
	manifest      base.Manifest
	opstreeg      *tree.FixedTreeGenerator
	opstree       tree.FixedTree
	ststree       tree.FixedTree
	states        *util.LockedMap
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
		opstree:       tree.EmptyFixedTree(),
		ststree:       tree.EmptyFixedTree(),
		states:        util.NewLockedMap(),
	}
}

func (w *Writer) SetOperationsSize(n uint64) {
	w.Lock()
	defer w.Unlock()

	if n < 1 {
		return
	}

	w.opstreeg = tree.NewFixedTreeGenerator(n)
}

func (w *Writer) SetProcessResult(
	_ context.Context, index int, facthash util.Hash, instate bool, errorreason base.OperationProcessReasonError,
) error {
	e := util.StringErrorFunc("failed to set operation")
	if err := w.db.SetOperations([]util.Hash{facthash}); err != nil {
		return e(err, "")
	}

	var msg string
	if errorreason != nil {
		msg = errorreason.Msg()
	}

	node := base.NewOperationFixedTreeNode(
		uint64(index),
		facthash,
		instate,
		msg,
	)
	if err := w.opstreeg.Add(node); err != nil {
		return e(err, "failed to set operation")
	}

	return nil
}

func (w *Writer) SetStates(
	ctx context.Context, index int, states []base.StateMergeValue, operation base.Operation,
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

func (w *Writer) SetState(
	ctx context.Context, stv base.StateMergeValue, operation base.Operation,
) error {
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

	if err := j.(base.StateValueMerger).Merge(stv.Value(), []util.Hash{operation.Fact().Hash()}); err != nil {
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
			i, err := w.opstreeg.Tree()
			if err != nil {
				return err
			}

			if err := w.fswriter.SetOperationsTree(ctx, i); err != nil {
				return errors.Wrap(err, "")
			}

			w.opstree = i

			return nil
		}); err != nil {
			return e(err, "")
		}
	}

	sortedkeys := make([]string, w.states.Len())
	{
		var i int
		w.states.Traverse(func(k, _ interface{}) bool {
			sortedkeys[i] = k.(string)
			i++

			return true
		})
	}

	sort.Slice(sortedkeys, func(i, j int) bool {
		return strings.Compare(sortedkeys[i], sortedkeys[j]) < 0
	})

	tg := tree.NewFixedTreeGenerator(uint64(w.states.Len()))
	states := make([]base.State, w.states.Len())

	go func() {
		defer worker.Done()

		for i := range sortedkeys {
			v, _ := w.states.Value(sortedkeys[i])
			st := v.(base.State)

			index := i
			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				nst, err := w.closeStateValue(tg, st, index)
				if err != nil {
					return err
				}

				if err := w.fswriter.SetState(ctx, index, nst); err != nil {
					return err
				}

				if err := w.db.SetStates([]base.State{nst}); err != nil {
					return err
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
	tg *tree.FixedTreeGenerator, st base.State, index int,
) (base.State, error) {
	stm, ok := st.(base.StateValueMerger)
	if !ok {
		return st, tg.Add(base.NewStateFixedTreeNode(uint64(index), []byte(st.Key())))
	}

	if err := stm.Close(); err != nil {
		return nil, err
	}

	return stm, tg.Add(base.NewStateFixedTreeNode(uint64(index), []byte(stm.Key())))
}

func (w *Writer) saveStates(
	ctx context.Context, tg *tree.FixedTreeGenerator, states []base.State,
) error {
	e := util.StringErrorFunc("failed to set states tree")

	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

	if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
		return w.setProposal(ctx)
	}); err != nil {
		return e(err, "")
	}

	if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
		return w.db.SetStates(states)
	}); err != nil {
		return e(err, "")
	}

	if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
		switch i, err := tg.Tree(); {
		case err != nil:
			return err
		default:
			if err := w.fswriter.SetStatesTree(ctx, i); err != nil {
				return errors.Wrap(err, "")
			}

			w.ststree = i

			return nil
		}
	}); err != nil {
		return e(err, "")
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	return nil
}

func (w *Writer) Manifest(ctx context.Context, previous base.Manifest) (base.Manifest, error) {
	w.Lock()
	defer w.Unlock()

	e := util.StringErrorFunc("failed to make manifest")

	if w.proposal == nil || (w.proposal.Point().Height() > base.GenesisHeight && previous == nil) {
		return nil, e(nil, "not yet written")
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
			valuehash.NewHashFromBytes(w.opstree.Root()),
			valuehash.NewHashFromBytes(w.ststree.Root()),
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

func (w *Writer) Save(ctx context.Context) error {
	w.Lock()
	defer w.Unlock()

	e := util.StringErrorFunc("failed to save")

	switch m, err := w.fswriter.Save(ctx); {
	case err != nil:
		return e(err, "")
	default:
		if err := w.db.SetMap(m); err != nil {
			return e(err, "")
		}
	}

	if err := w.mergeDatabase(w.db); err != nil {
		return e(err, "")
	}

	return nil
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
