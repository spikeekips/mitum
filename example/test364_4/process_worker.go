package main

import (
	"context"
	"slices"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type (
	broadcastStateMergeValueInsertFunc func(worker string, opindex uint64, _ []StateMergeValue) error
	broadcastStateMergeValueFuncs      func(_ context.Context, worker string) (
		workerInitialInsertOpFunc,
		func() error,
		error,
	)
)

func newStateMergeValueExtractorGetOperationFunc(
	f stateMergeValueExtractorGetOperationFunc,
	networkID base.NetworkID,
) stateMergeValueExtractorGetOperationFunc {
	return func(ctx context.Context, opindex uint64, oph, fact util.Hash) (Operation, bool, error) {
		var op Operation

		switch o, found, err := f(ctx, opindex, oph, fact); {
		case err != nil, !found:
			return nil, found, err
		default:
			op = o
		}

		if err := op.IsValid(networkID); err != nil {
			return nil, true, err
		}

		// NOTE check hashes are correct
		switch {
		case !op.Hash().Equal(oph):
			return nil, true, util.ErrInvalid.Errorf("operation hash not match")
		case !op.Fact().Hash().Equal(fact):
			return nil, true, util.ErrInvalid.Errorf("fact hash not match")
		}

		return op, true, nil
	}
}

func newBroadcastStateMergeValuesFuncs(
	ctx context.Context,
	local string,
	workers []string, // NOTE local not included
	broadcastStateMergeValuesFuncs broadcastStateMergeValueFuncs,
	checkWorkerError func(worker string, _ error) error,
) (
	broadcastStateMergeValueInsertFunc,
	func() error,
	error,
) {
	var broadcastJw, doneJw JobWorker

	switch i, err := NewBaseJobWorker(ctx, int64(len(workers))); {
	case err != nil:
		return nil, nil, err
	default:
		broadcastJw = i
	}

	switch i, err := NewBaseJobWorker(ctx, int64(len(workers))); {
	case err != nil:
		return nil, nil, err
	default:
		doneJw = i
	}

	funcs := util.NewSingleLockedMap[string, [2]interface{}]()

	broadcast := func(worker string, opindex uint64, values []StateMergeValue) error {
		return funcs.GetOrCreate(
			worker,
			func(f [2]interface{}, _ bool) error {
				return f[0].(workerInitialInsertOpFunc)(opindex, values)
			},
			func() ([2]interface{}, error) {
				if slices.Index(workers, worker) < 0 {
					return [2]interface{}{}, util.ErrNotFound.Errorf("worker, %q", worker)
				}

				switch i, d, err := broadcastStateMergeValuesFuncs(ctx, worker); {
				case err != nil:
					return [2]interface{}{}, err
				default:
					return [2]interface{}{i, d}, nil
				}
			},
		)
	}

	return func(worker string, opindex uint64, values []StateMergeValue) error {
			return broadcastJw.NewJob(func(context.Context) error {
				return checkWorkerError(worker, broadcast(worker, opindex, values))
			})
		},
		func() error {
			defer funcs.Close()

			broadcastJw.JobDone()

			if err := broadcastJw.Wait(); err != nil {
				return err
			}

			if err := util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
				return true, doneJw.NewJob(func(context.Context) error {
					if f, found := funcs.Value(worker); found {
						return f[1].(func() error)()
					}

					switch _, d, err := broadcastStateMergeValuesFuncs(ctx, worker); {
					case err != nil:
						return err
					default:
						return d()
					}
				})
			}); err != nil {
				return err
			}

			doneJw.JobDone()

			return doneJw.Wait()
		},
		nil
}

func newStateMergeValueOwnersFunc(
	local string,
	owners func(stateKey string) []string,
) func([]StateMergeValue, func(worker string, _ []StateMergeValue) error) error {
	return func(values []StateMergeValue, f func(worker string, _ []StateMergeValue) error) error {
		if len(values) < 1 {
			return nil
		}

		m := map[string][]StateMergeValue{}

		defer func() {
			clear(m)

			m = nil
		}()

		_ = util.TraverseSliceV1(values, func(_ int, value StateMergeValue) (bool, error) {
			return true, util.TraverseSliceV1(owners(value.Key()), func(_ int, worker string) (bool, error) {
				if worker == local {
					return true, nil
				}

				m[worker] = append(m[worker], value)

				return true, nil
			})
		})

		return TraverseMap(m, func(worker string, values []StateMergeValue) (bool, error) {
			return true, f(worker, values)
		})
	}
}
