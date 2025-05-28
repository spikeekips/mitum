package dop6

import (
	"context"
	"slices"
	"sync/atomic"

	"github.com/pkg/errors"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type CheckFailedOwnersFunc = func(worker string, err error) (ok, subsEnough, ownersEnough bool, failed error)

func newManagerStateOwners(
	ctx context.Context,
	opsWorkers []string,
	opsShare, statesShare, workersSize uint64,
) (
	func(string, []string, []string) error,
	CheckFailedOwnersFunc,
	func() ([]string, []string, error),
	error,
) {
	var checkOpsWorker dop5.CheckFailedWorkersFunc

	switch w, err := dop5.NewFuncCheckFailedWorkers(uint64(len(opsWorkers)), opsShare, func(string) bool { return true }); {
	case err != nil:
		return nil, nil, nil, err
	default:
		checkOpsWorker = w
	}

	var addSubs, addOwners dutil.AppendSliceAddFunc[string]
	var doneSubs, doneOwners dutil.OutFunc[[]string]

	switch a, d, err := dutil.NewLockUniqueAppendSlice(workersSize, 0, func(w string) string { return w }); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addSubs, doneSubs = a, d
	}

	switch a, d, err := dutil.NewLockUniqueAppendSlice(workersSize, 0, func(w string) string { return w }); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addOwners, doneOwners = a, d
	}

	setStateSubs, checkFailedSubs := newFuncLazyCheckFailedWorkers(statesShare, func(string) bool { return true })
	setStateOwners, checkFailedOwners := newFuncLazyCheckFailedWorkers(statesShare, func(string) bool { return true })

	cancelOwners, waitOwners := dutil.NewContextDone(ctx)

	var subsEnough, ownersEnough atomic.Value

	return func(worker string, subs, affected []string) error {
			if slices.Index(opsWorkers, worker) < 0 {
				return errors.Errorf("not ops worker, %q", worker)
			}

			switch ok, enough, failed := checkOpsWorker(worker, nil); {
			case !ok:
			case failed != nil:
				return cancelOwners(failed)
			case ok:
				_, _ = addSubs(subs...)
				_, _ = addOwners(subs...)
				_, _ = addOwners(affected...)

				if enough {
					_, _ = setStateSubs(doneSubs())
					_, _ = setStateOwners(doneOwners())

					_ = cancelOwners(nil)
				}
			}

			return nil
		},
		func(worker string, err error) (bool, bool, bool, error) {
			var ok bool

			switch o, enough, failed := checkFailedSubs(worker, err); {
			case failed != nil:
				return o, false, false, failed
			default:
				ok = o

				if enough {
					subsEnough.CompareAndSwap(nil, true)
				}
			}

			switch o, enough, failed := checkFailedOwners(worker, err); {
			case failed != nil:
				return o, false, false, failed
			default:
				if !ok {
					ok = o
				}

				if enough {
					ownersEnough.CompareAndSwap(nil, true)
				}
			}

			if !ok {
				return false, false, false, nil
			}

			var s, o bool

			if i := subsEnough.Load(); i != nil {
				s = i.(bool)
			}

			if i := ownersEnough.Load(); i != nil {
				o = i.(bool)
			}

			return ok, s, s && o, nil
		},
		func() ([]string, []string, error) {
			if err := waitOwners(); err != nil {
				return nil, nil, err
			}

			return doneSubs(), doneOwners(), nil
		},
		nil
}
