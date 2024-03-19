package isaacblock

import (
	"context"
	"sort"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type StatesMerger interface {
	SetStates(_ context.Context, opindex uint64, _ []base.StateMergeValue, operationfacthash util.Hash) error
	CloseStates(
		_ context.Context,
		beforeClose func(keyscount uint64) error,
		oneState func(newState base.State, total, index uint64) error,
	) error
	Len() int
	Close() error
}

type DefaultStatesMerger struct {
	getStateFunc base.GetStateFunc
	stvmmap      *util.ShardedMap[string, base.StateValueMerger]
	height       base.Height
	workersize   int64
}

func NewDefaultStatesMerger(
	height base.Height,
	getStateFunc base.GetStateFunc,
	workersize int64,
) *DefaultStatesMerger {
	stvmmap, _ := util.NewShardedMap[string, base.StateValueMerger](1<<9, nil) //nolint:gomnd //...

	return &DefaultStatesMerger{
		height:       height,
		stvmmap:      stvmmap,
		getStateFunc: getStateFunc,
		workersize:   workersize,
	}
}

func (sm *DefaultStatesMerger) SetStates(
	ctx context.Context,
	index uint64,
	stvms []base.StateMergeValue,
	operation util.Hash,
) error {
	for i := range stvms {
		if err := sm.setState(ctx, stvms[i], index, operation); err != nil {
			return err
		}
	}

	return nil
}

func (sm *DefaultStatesMerger) CloseStates(
	ctx context.Context,
	beforeClose func(keyscount uint64) error,
	oneState func(newState base.State, total, index uint64) error,
) error {
	if sm.stvmmap.Len() < 1 {
		return beforeClose(0)
	}

	sortedkeys := sm.sortStateKeys()

	total := uint64(len(sortedkeys))

	if err := beforeClose(total); err != nil {
		return err
	}

	worker, err := util.NewErrgroupWorker(ctx, sm.workersize)
	if err != nil {
		return err
	}

	defer worker.Close()

	go func() {
		defer worker.Done()

		for i := range sortedkeys {
			stvm, _ := sm.stvmmap.Value(sortedkeys[i])

			index := uint64(i)

			if err := worker.NewJob(func(context.Context, uint64) error {
				switch newst, err := stvm.CloseValue(); {
				case newst == nil, errors.Is(err, base.ErrIgnoreStateValue):
					return nil
				case err != nil:
					return err
				default:
					return oneState(newst, total, index)
				}
			}); err != nil {
				break
			}
		}
	}()

	return worker.Wait()
}

func (sm *DefaultStatesMerger) Len() int {
	return sm.stvmmap.Len()
}

func (sm *DefaultStatesMerger) Close() error {
	if sm.stvmmap.Len() < 1 {
		return nil
	}

	worker, err := util.NewErrgroupWorker(context.Background(), sm.workersize)
	if err != nil {
		return err
	}

	defer worker.Close()

	go func() {
		defer worker.Done()

		sm.stvmmap.Traverse(func(_ string, merger base.StateValueMerger) bool {
			return worker.NewJob(func(context.Context, uint64) error {
				_ = merger.Close()

				return nil
			}) == nil
		})
	}()

	_ = worker.Wait()

	sm.stvmmap.Close()

	return nil
}

func (sm *DefaultStatesMerger) setState(
	_ context.Context,
	stvm base.StateMergeValue,
	_ uint64,
	operation util.Hash,
) error {
	return sm.stvmmap.GetOrCreate(
		stvm.Key(),
		func(i base.StateValueMerger, _ bool) error {
			return errors.WithMessage(
				i.Merge(stvm.Value(), operation),
				"merge",
			)
		},
		func() (base.StateValueMerger, error) {
			var st base.State

			switch j, found, err := sm.getStateFunc(stvm.Key()); {
			case err != nil:
				return nil, err
			case found:
				st = j
			}

			return stvm.Merger(sm.height, st), nil
		},
	)
}

func (sm *DefaultStatesMerger) sortStateKeys() []string {
	var sortedkeys []string
	sm.stvmmap.Traverse(func(k string, _ base.StateValueMerger) bool {
		sortedkeys = append(sortedkeys, k)

		return true
	})

	if len(sortedkeys) > 0 {
		sort.Strings(sortedkeys)
	}

	return sortedkeys
}
