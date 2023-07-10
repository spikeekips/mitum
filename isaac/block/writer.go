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
)

type FSWriter interface {
	SetProposal(context.Context, base.ProposalSignFact) error
	SetOperation(context.Context, uint64, base.Operation) error
	SetOperationsTree(context.Context, *fixedtree.Writer) error
	SetState(context.Context, uint64, base.State) error
	SetStatesTree(context.Context, *fixedtree.Writer) (fixedtree.Tree, error)
	SetManifest(context.Context, base.Manifest) error
	SetINITVoteproof(context.Context, base.INITVoteproof) error
	SetACCEPTVoteproof(context.Context, base.ACCEPTVoteproof) error
	Save(context.Context) (base.BlockMap, error)
	Cancel() error
}

type Writer struct {
	manifest      base.Manifest
	proposal      base.ProposalSignFact
	opstreeroot   util.Hash
	db            isaac.BlockWriteDatabase
	fswriter      FSWriter
	avp           base.ACCEPTVoteproof
	mergeDatabase func(isaac.BlockWriteDatabase) error
	opstreeg      *fixedtree.Writer
	getStateFunc  base.GetStateFunc
	states        *util.ShardedMap[string, base.StateValueMerger]
	ststree       fixedtree.Tree
	sync.RWMutex
}

func NewWriter(
	proposal base.ProposalSignFact,
	getStateFunc base.GetStateFunc,
	db isaac.BlockWriteDatabase,
	mergeDatabase func(isaac.BlockWriteDatabase) error,
	fswriter FSWriter,
) *Writer {
	states, _ := util.NewShardedMap[string, base.StateValueMerger](1 << 9) //nolint:gomnd //...

	return &Writer{
		proposal:      proposal,
		getStateFunc:  getStateFunc,
		db:            db,
		mergeDatabase: mergeDatabase,
		fswriter:      fswriter,
		states:        states,
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
		if err := w.db.SetOperations([]util.Hash{op}); err != nil {
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

	for i := range states {
		if err := w.SetState(ctx, states[i], operation); err != nil {
			return e.Wrap(err)
		}
	}

	if err := w.fswriter.SetOperation(ctx, index, operation); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (w *Writer) SetState(_ context.Context, stv base.StateMergeValue, operation base.Operation) error {
	e := util.StringError("set state")

	j, _, err := w.states.GetOrCreate(stv.Key(), func() (base.StateValueMerger, error) {
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
		return e.Wrap(err)
	}

	if err := j.Merge(stv.Value(), []util.Hash{operation.Fact().Hash()}); err != nil {
		return e.WithMessage(err, "merge")
	}

	return nil
}

func (w *Writer) closeStateValues(ctx context.Context) error {
	if w.states.Len() < 1 {
		return nil
	}

	e := util.StringError("close state values")

	worker, err := util.NewErrgroupWorker(ctx, math.MaxInt8)
	if err != nil {
		return e.Wrap(err)
	}

	defer worker.Close()

	var sortedkeys []string
	w.states.Traverse(func(k string, _ base.StateValueMerger) bool {
		sortedkeys = append(sortedkeys, k)

		return true
	})

	if len(sortedkeys) > 0 {
		sort.Slice(sortedkeys, func(i, j int) bool {
			return strings.Compare(sortedkeys[i], sortedkeys[j]) < 0
		})
	}

	tg, err := fixedtree.NewWriter(base.StateFixedtreeHint, uint64(len(sortedkeys)))
	if err != nil {
		return e.Wrap(err)
	}

	states := make([]base.State, len(sortedkeys))

	go func() {
		defer worker.Done()

		for i := range sortedkeys {
			st, _ := w.states.Value(sortedkeys[i])

			index := uint64(i)

			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				nst, err := w.closeStateValue(tg, st, index)

				switch {
				case err == nil:
				case errors.Is(err, isaac.ErrIgnoreStateValue):
					return nil
				default:
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
		return e.Wrap(err)
	}

	if err := w.saveStates(ctx, tg, states); err != nil {
		return e.Wrap(err)
	}

	if err := w.db.Write(); err != nil {
		return e.Wrap(err)
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
		return nil, err
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
			i, err := w.fswriter.SetStatesTree(ctx, tg)
			if err != nil {
				return err
			}

			w.ststree = i

			return nil
		},
	); err != nil {
		return errors.Wrap(err, "set states tree")
	}

	return nil
}

func (w *Writer) Manifest(ctx context.Context, previous base.Manifest) (base.Manifest, error) {
	w.Lock()
	defer w.Unlock()

	e := util.StringError("make manifest")

	if w.proposal == nil || (previous == nil && w.proposal.Point().Height() > base.GenesisHeight) {
		return nil, e.Errorf("not yet written")
	}

	if err := w.setProposal(ctx); err != nil {
		return nil, e.Wrap(err)
	}

	if w.opstreeg != nil {
		if err := w.fswriter.SetOperationsTree(ctx, w.opstreeg); err != nil {
			return nil, e.Wrap(err)
		}

		w.opstreeroot = w.opstreeg.Root()
	}

	if err := w.closeStateValues(ctx); err != nil {
		return nil, e.Wrap(err)
	}

	var suffrage, previousHash util.Hash
	if w.proposal.Point().Height() > base.GenesisHeight {
		suffrage = previous.Suffrage()
		previousHash = previous.Hash()
	}

	if st := w.db.SuffrageState(); st != nil {
		suffrage = st.Hash()
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
			w.opstreeroot,
			ststreeroot,
			suffrage,
			w.proposal.ProposalFact().ProposedAt(),
		)

		if err := w.fswriter.SetManifest(ctx, w.manifest); err != nil {
			return nil, e.Wrap(err)
		}
	}

	return w.manifest, nil
}

func (w *Writer) SetINITVoteproof(ctx context.Context, vp base.INITVoteproof) error {
	if err := w.fswriter.SetINITVoteproof(ctx, vp); err != nil {
		return errors.Wrap(err, "set init voteproof")
	}

	return nil
}

func (w *Writer) SetACCEPTVoteproof(ctx context.Context, vp base.ACCEPTVoteproof) error {
	if err := w.fswriter.SetACCEPTVoteproof(ctx, vp); err != nil {
		return errors.Wrap(err, "set accept voteproof")
	}

	w.avp = vp

	return nil
}

func (w *Writer) Save(ctx context.Context) (base.BlockMap, error) {
	w.Lock()
	defer w.Unlock()

	e := util.StringError("save")

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

		sufproof := NewSuffrageProof(m, st, proof, w.avp)

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
	w.opstreeroot = nil
	w.db = nil
	w.fswriter = nil
	w.avp = nil
	w.mergeDatabase = nil
	w.opstreeg = nil
	w.getStateFunc = nil
	w.states.Close()
	w.ststree = fixedtree.Tree{}

	return nil
}

func (w *Writer) setProposal(ctx context.Context) error {
	if err := w.fswriter.SetProposal(ctx, w.proposal); err != nil {
		return errors.Wrap(err, "set proposal")
	}

	return nil
}
