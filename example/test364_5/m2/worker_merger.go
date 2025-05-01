package m2

import (
	"context"
	stderrors "errors"
	"slices"
	"sync"

	"github.com/pkg/errors"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type (
	addStateOwnersFunc              = func(owners []string)
	workerMergerReadyStateOwners    = func(subs, affected []string) error
	workerMergerWorkerMergeSMVsFunc = func(worker string, subs, affected []string, workerErrs error) error
	workerMergerUnmergedFunc        = func(worker string, workerErrs error) error
)

func newFuncWorkerMerger(
	ctx context.Context,
	local string,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	mergeReady, unmergeReady func(),
	whenReadyStateOwners workerMergerReadyStateOwners,
) (
	workerMergerWorkerMergeSMVsFunc,
	workerMergerUnmergedFunc,
	dutil.ErrorFunc,
	error,
) {
	_, wctxCancel, _, wait := dutil.NewContextWait(ctx)

	var checkWorker dop5.CheckFailedWorkersFunc
	var checkWorkerBlockUntil func(string) bool

	if slices.Index(opsWorkers, local) >= 0 {
		checkWorkerBlockUntil = func(worker string) bool {
			return worker == local
		}
	}

	switch w, err := dop5.NewFuncCheckFailedWorkers(uint64(len(opsWorkers)), opsShare, checkWorkerBlockUntil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		checkWorker = w
	}

	var addStateOwners dutil.In2ErrorFunc[[]string, []string]
	var readyStateOwners func() ([]string, []string, error)
	var checkUnmerged dutil.In2ErrorFunc[string, error]

	switch a, d, c, err := newFuncCollectStateOwners(local, uint64(len(workers)), statesShare, func() {
		unmergeReady()

		_ = wctxCancel(nil)
	}); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addStateOwners, readyStateOwners, checkUnmerged = a, d, c
	}

	checkMerged := func(worker string, err error) error {
		switch ok, enough, failed := checkWorker(worker, err); {
		case failed != nil:
			return failed
		case !ok, !enough:
			return nil
		}

		mergeReady()

		switch subs, affected, err := readyStateOwners(); {
		case err != nil:
			return err
		default:
			return whenReadyStateOwners(subs, affected)
		}
	}

	iferrCancel := dutil.NewIfErrErrorInFunc(wctxCancel)

	return func(worker string, subs, affected []string, workerErr error) error {
			switch {
			case worker == managerName:
				mergeReady()
				unmergeReady()

				// NOTE don't report owners(to manager)

				gerr := workerErr

				if workerErr != nil {
					gerr = stderrors.Join(dop5.ErrWorkersFailed, workerErr)
				}

				return wctxCancel(gerr)
			case slices.Index(opsWorkers, worker) < 0:
				return errors.Errorf("unknown ops worker, %q", worker)
			}

			if workerErr == nil {
				if err := addStateOwners(subs, affected); err != nil {
					return wctxCancel(err)
				}
			}

			return iferrCancel(checkMerged(worker, workerErr))
		},
		func(worker string, workerErr error) error {
			return iferrCancel(checkUnmerged(worker, workerErr))
		},
		wait,
		nil
}

func newFuncAddStateOwners(maxSize uint64) (
	addStateOwnersFunc,
	dutil.OutFunc[[]string],
	error,
) {
	switch add, done, err := dutil.NewLockComparableUniqueAppendSlice[string](maxSize, 0); {
	case err != nil:
		return nil, nil, err
	default:
		return func(owners []string) {
				if len(owners) < 1 {
					return
				}

				add(owners...)
			},
			done,
			nil
	}
}

func newFuncCollectStateOwners( //nolint:funlen
	local string,
	maxSize, statesShare uint64,
	unmregeReady func(),
) (
	dutil.In2ErrorFunc[[]string, []string],
	func() ([]string, []string, error),
	dutil.In2ErrorFunc[string /* worker */, error],
	error,
) {
	var addSub, addAffected addStateOwnersFunc
	var doneSub, doneAffected dutil.OutFunc[[]string]

	switch a, d, err := newFuncAddStateOwners(maxSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addSub, doneSub = a, d
	}

	switch a, d, err := newFuncAddStateOwners(maxSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addAffected, doneAffected = a, d
	}

	var checkWorker dop5.CheckFailedWorkersFunc

	var l sync.Mutex

	errs := map[string]error{}

	cw := func(worker string, err error) error {
		switch ok, enough, failed := checkWorker(worker, err); {
		case failed != nil:
			return failed
		case !ok:
		case enough:
			unmregeReady()
		}

		return nil
	}

	ready := func(subs []string) (dutil.ErrorFunc, error) {
		defer clear(errs)

		if len(subs) < 1 {
			return func() error { //nolint:unparam
					unmregeReady()

					return nil
				},
				nil
		}

		var checkWorkerBlockUntil func(string) bool

		if slices.Index(subs, local) >= 0 {
			checkWorkerBlockUntil = func(worker string) bool {
				return worker == local
			}
		}

		switch f, err := dop5.NewFuncCheckFailedWorkers(uint64(len(subs)), statesShare, checkWorkerBlockUntil); {
		case err != nil:
			return nil, err
		default:
			checkWorker = f
		}

		return func() error {
				for w, err := range errs {
					if err := cw(w, err); err != nil {
						return err
					}
				}

				return nil
			},
			nil
	}

	done := func() ([]string, dutil.ErrorFunc, error) {
		l.Lock()
		defer l.Unlock()

		if checkWorker != nil {
			return nil, nil, errors.New("already done")
		}

		subs := doneSub()

		f, err := ready(subs)

		return subs, f, err
	}

	check := func(worker string, err error) dutil.ErrorFunc {
		l.Lock()
		defer l.Unlock()

		if _, found := errs[worker]; found {
			return nil
		}

		if checkWorker == nil {
			errs[worker] = err

			return nil
		}

		return func() error {
			return cw(worker, err)
		}
	}

	return func(subs, affected []string) error {
			addSub(subs)
			addAffected(affected)

			return nil
		},
		func() ([]string, []string, error) {
			switch subs, f, err := done(); {
			case err != nil:
				return nil, nil, err
			default:
				return subs, doneAffected(), f()
			}
		},
		func(worker string, err error) error {
			if f := check(worker, err); f != nil {
				return f()
			}

			return nil
		},
		nil
}
