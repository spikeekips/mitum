package isaac

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/tree"
	"github.com/spikeekips/mitum/util/valuehash"
)

type BlockDataWriter interface {
	SetProposal(context.Context, base.ProposalSignedFact) error
	SetOperationsSize(collected uint64, valids uint64)
	SetOperation(
		_ context.Context,
		index int,
		facthash util.Hash,
		instate bool,
		errorreason base.OperationProcessReasonError,
	) error
	SetStates(_ context.Context, index int, states []base.State, operation base.Operation) error
	Manifest(_ context.Context, previous base.Manifest) (base.Manifest, error)
	SetINITVoteproof(context.Context, base.INITVoteproof) error
	SetACCEPTVoteproof(context.Context, base.ACCEPTVoteproof) error
	Save(context.Context) error
	Cancel() error
}

type BlockDataFSWriter interface {
	SetProposal(context.Context, base.ProposalSignedFact) error
	SetOperation(context.Context, int, base.Operation) error
	SetOperationsTree(context.Context, tree.FixedTree) error
	SetState(context.Context, int, base.State) error
	SetStatesTree(context.Context, tree.FixedTree) error
	SetManifest(context.Context, base.Manifest) error
	SetINITVoteproof(context.Context, base.INITVoteproof) error
	SetACCEPTVoteproof(context.Context, base.ACCEPTVoteproof) error
	Map(context.Context) (base.BlockDataMap, error)
	Cancel() error
}

type DefaultBlockDataWriter struct {
	sync.RWMutex
	mergeDatabase func(BlockWriteDatabase) error
	db            BlockWriteDatabase
	fswriter      BlockDataFSWriter
	proposal      base.ProposalSignedFact
	manifest      base.Manifest
	opstreeg      *tree.FixedTreeGenerator
	opstree       tree.FixedTree
	ststree       tree.FixedTree
	states        *util.LockedMap
	suffrage      base.SuffrageStateValue
}

func NewDefaultBlockDataWriter(
	db BlockWriteDatabase,
	mergeDatabase func(BlockWriteDatabase) error,
	fswriter BlockDataFSWriter,
) *DefaultBlockDataWriter {
	return &DefaultBlockDataWriter{
		db:            db,
		mergeDatabase: mergeDatabase,
		fswriter:      fswriter,
		opstree:       tree.EmptyFixedTree(),
		ststree:       tree.EmptyFixedTree(),
		states:        util.NewLockedMap(),
	}
}

func (w *DefaultBlockDataWriter) SetProposal(ctx context.Context, proposal base.ProposalSignedFact) error {
	if err := w.fswriter.SetProposal(ctx, proposal); err != nil {
		return errors.Wrap(err, "failed to set proposal")
	}

	w.proposal = proposal

	return nil
}

func (w *DefaultBlockDataWriter) SetOperationsSize(_, valids uint64) {
	w.Lock()
	defer w.Unlock()

	if valids < 1 {
		return
	}

	w.opstreeg = tree.NewFixedTreeGenerator(valids)
}

func (w *DefaultBlockDataWriter) SetOperation(
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

func (w *DefaultBlockDataWriter) SetStates(
	ctx context.Context, index int, states []base.State, operation base.Operation,
) error {
	e := util.StringErrorFunc("failed to set states")

	if w.proposal == nil {
		return e(nil, "not yet written")
	}

	for i := range states {
		st := states[i]

		j, found, _ := w.states.Get(st.Key(), func() (interface{}, error) {
			return st.Merger(w.proposal.Point().Height()), nil
		})
		if found {
			merger := j.(base.StateValueMerger)

			if err := merger.Merge(st.Value(), []util.Hash{operation.Fact().Hash()}); err != nil {
				return e(err, "failed to merge")
			}
		}
	}

	if err := w.fswriter.SetOperation(ctx, index, operation); err != nil {
		return e(err, "")
	}

	return nil
}

func (w *DefaultBlockDataWriter) closeStateValues(ctx context.Context) error {
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

func (*DefaultBlockDataWriter) closeStateValue(
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

func (w *DefaultBlockDataWriter) saveStates(
	ctx context.Context, tg *tree.FixedTreeGenerator, states []base.State,
) error {
	e := util.StringErrorFunc("failed to set states tree")

	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

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

func (w *DefaultBlockDataWriter) Manifest(ctx context.Context, previous base.Manifest) (base.Manifest, error) {
	w.Lock()
	defer w.Unlock()

	e := util.StringErrorFunc("failed to make manifest")

	if previous == nil || w.proposal == nil {
		return nil, e(nil, "not yet written")
	}

	if err := w.closeStateValues(ctx); err != nil {
		return nil, e(err, "")
	}

	suffrage := previous.Suffrage()
	if i := w.db.SuffrageState(); i != nil {
		suffrage = i.Hash()
	}

	if w.manifest == nil {

		w.manifest = NewManifest(
			w.proposal.Point().Height(),
			previous.Hash(),
			w.proposal.Fact().Hash(),
			valuehash.NewBytes(w.opstree.Root()),
			valuehash.NewBytes(w.ststree.Root()),
			suffrage,
			localtime.UTCNow(),
		)

		if err := w.fswriter.SetManifest(ctx, w.manifest); err != nil {
			return nil, e(err, "")
		}

		if err := w.db.SetManifest(w.manifest); err != nil {
			return nil, e(err, "")
		}
	}

	return w.manifest, nil
}

func (w *DefaultBlockDataWriter) SetINITVoteproof(ctx context.Context, vp base.INITVoteproof) error {
	if err := w.fswriter.SetINITVoteproof(ctx, vp); err != nil {
		return errors.Wrap(err, "failed to set init voteproof")
	}

	return nil
}

func (w *DefaultBlockDataWriter) SetACCEPTVoteproof(ctx context.Context, vp base.ACCEPTVoteproof) error {
	if err := w.fswriter.SetACCEPTVoteproof(ctx, vp); err != nil {
		return errors.Wrap(err, "failed to set accept voteproof")
	}

	return nil
}

func (w *DefaultBlockDataWriter) Save(ctx context.Context) error {
	// BLOCK save voteproof
	// BLOCK save states in states tree
	// BLOCK save block data map

	e := util.StringErrorFunc("failed to save")
	switch m, err := w.fswriter.Map(ctx); {
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

func (w *DefaultBlockDataWriter) Cancel() error {
	e := util.StringErrorFunc("failed to cancel DefaultBlockDataWriter")
	if err := w.fswriter.Cancel(); err != nil {
		return e(err, "")
	}

	if err := w.db.Cancel(); err != nil {
		return e(err, "")
	}

	return nil
}
