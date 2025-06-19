package dop6

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerMerger struct {
	baseMergerSuite
}

func (t *testWorkerMerger) newf(
	ctx context.Context,
	local string,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	mergeReadyf, unmergeReadyf func(),
	reportStateOwnersf workerMergerReadyStateOwners,
) (
	workerMergerWorkerMergeSMVsFunc,
	workerMergerUnmergedFunc,
	dutil.ErrorFunc,
	dutil.OutFunc[bool],
	dutil.OutFunc[bool],
	func() [2][]string,
	error,
) {
	owners := new(atomic.Value)

	var mergedDone, unmergedDone int64

	mergeReady := func() {
		if mergeReadyf != nil {
			mergeReadyf()
		}

		atomic.AddInt64(&mergedDone, 1)
	}
	unmergeReady := func() {
		if unmergeReadyf != nil {
			unmergeReadyf()
		}

		atomic.AddInt64(&unmergedDone, 1)
	}
	reportStateOwners := func(subs, affected []string) error {
		if reportStateOwnersf != nil {
			if err := reportStateOwnersf(subs, affected); err != nil {
				return err
			}
		}

		owners.Store([2][]string{subs, affected})

		return nil
	}

	merge, unmerge, wait, err := newFuncWorkerMerger(
		ctx,
		local,
		workers, workers,
		opsShare, statesShare,
		mergeReady,
		unmergeReady,
		reportStateOwners,
	)

	return merge, unmerge, wait,
		func() bool { return atomic.LoadInt64(&mergedDone) == 1 },
		func() bool { return atomic.LoadInt64(&unmergedDone) == 1 },
		func() [2][]string {
			i := owners.Load()
			if i == nil {
				return [2][]string{}
			}

			return i.([2][]string)
		},
		err
}

func (t *testWorkerMerger) TestCtxCancel() {
	local, workers := t.workers(1)

	t.Run("before new", func() {
		ctx, ctxCancel := t.Ctx()
		ctxCancel(nil)

		merge, unmerge, wait, mergedDone, unmergedDone, owners, err := t.newf(
			ctx,
			local,
			workers, workers,
			1, 1,
			nil, nil, nil,
		)
		t.NoError(err)

		t.NoError(merge(local, []string{local}, nil, nil))
		t.NoError(unmerge(local, nil))
		err = wait()
		t.ErrorIs(err, context.Canceled)

		t.True(mergedDone())
		t.True(unmergedDone())

		t.Equal([2][]string{{local}, nil}, owners())
	})
}

func (t *testWorkerMerger) TestZeroSubs() {
	local, workers := t.workers(1)

	ctx, ctxCancel := t.Ctx()
	defer ctxCancel(nil)

	merge, unmerge, wait, mergedDone, unmergedDone, owners, err := t.newf(
		ctx,
		local,
		workers, workers,
		1, 1,
		nil, nil, nil,
	)
	t.NoError(err)

	t.NoError(merge(local, nil, []string{local}, nil))
	t.NoError(unmerge(local, nil))
	t.NoError(wait())

	t.True(mergedDone())
	t.True(unmergedDone())

	t.Equal([2][]string{nil, {local}}, owners())
}

func (t *testWorkerMerger) Test1Worker() {
	local, workers := t.workers(1)

	t.Run("ok", func() {
		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		merge, unmerge, wait, mergedDone, unmergedDone, owners, err := t.newf(
			ctx,
			local,
			workers, workers,
			1, 1,
			nil, nil, nil,
		)
		t.NoError(err)

		t.NoError(merge(local, []string{local}, nil, nil))
		t.NoError(unmerge(local, nil))
		t.NoError(wait())

		t.True(mergedDone())
		t.True(unmergedDone())

		t.Equal([2][]string{{local}, nil}, owners())
	})

	t.Run("empty", func() {
		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		merge, unmerge, wait, mergedDone, unmergedDone, owners, err := t.newf(
			ctx,
			local,
			workers, workers,
			1, 1,
			nil, nil, nil,
		)
		t.NoError(err)

		t.NoError(merge(local, nil, nil, nil))
		t.NoError(unmerge(local, nil))
		t.NoError(wait())

		t.True(mergedDone())
		t.True(unmergedDone())

		t.Equal([2][]string{nil, nil}, owners())
	})

	t.Run("merge error", func() {
		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		merge, unmerge, wait, mergedDone, unmergedDone, owners, err := t.newf(
			ctx,
			local,
			workers, workers,
			1, 1,
			nil, nil, nil,
		)
		t.NoError(err)

		err = merge(local, []string{local}, nil, errors.New("showme"))
		t.ErrorIs(err, dop5.ErrWorkersFailed)

		t.NoError(unmerge(local, nil))

		err = wait()
		t.ErrorIs(err, dop5.ErrWorkersFailed)

		t.False(mergedDone())
		t.False(unmergedDone())

		t.Equal([2][]string{nil, nil}, owners())
	})

	t.Run("unmerge error", func() {
		local, workers := t.workers(2)

		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		merge, unmerge, wait, mergedDone, unmergedDone, owners, err := t.newf(
			ctx,
			local,
			workers, workers,
			1, 1,
			nil, nil, nil,
		)
		t.NoError(err)

		t.NoError(merge(local, workers, workers, nil))
		t.NoError(merge(workers[1], workers, workers, nil))
		err = unmerge(workers[1], errors.New("showme"))
		t.ErrorIs(err, dop5.ErrWorkersFailed)

		err = wait()
		t.ErrorIs(err, dop5.ErrWorkersFailed)

		t.True(mergedDone())
		t.False(unmergedDone())

		t.Equal([2][]string{workers, workers}, owners())
	})

	t.Run("manager done", func() {
		t.Run("ok", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			merge, _, wait, mergedDone, unmergedDone, owners, err := t.newf(
				ctx,
				local,
				workers, workers,
				1, 1,
				nil, nil, nil,
			)
			t.NoError(err)

			t.NoError(merge(managerName, []string{local}, nil, nil))
			t.NoError(wait())

			t.True(mergedDone())
			t.True(unmergedDone())

			t.Equal([2][]string{nil, nil}, owners())
		})

		t.Run("with error", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			merge, _, wait, mergedDone, unmergedDone, owners, err := t.newf(
				ctx,
				local,
				workers, workers,
				1, 1,
				nil, nil, nil,
			)
			t.NoError(err)

			err = merge(managerName, []string{local}, nil, errors.New("showme"))
			t.ErrorIs(err, dop5.ErrWorkersFailed)

			err = wait()
			t.ErrorIs(err, dop5.ErrWorkersFailed)

			t.True(mergedDone())
			t.True(unmergedDone())

			t.Equal([2][]string{nil, nil}, owners())
		})
	})
}

func (t *testWorkerMerger) Test3Worker() {
	local, workers := t.workers(3)

	t.Run("ok", func() {
		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		merge, unmerge, wait, mergedDone, unmergedDone, owners, err := t.newf(
			ctx,
			local,
			workers, workers,
			1, 1,
			nil, nil, nil,
		)
		t.NoError(err)

		for worker := range slices.Values(workers) {
			t.NoError(merge(worker, workers, nil, nil))
		}

		for worker := range slices.Values(workers) {
			t.NoError(unmerge(worker, nil))
		}

		t.NoError(wait())

		t.True(mergedDone())
		t.True(unmergedDone())

		t.Equal([2][]string{workers, nil}, owners())
	})

	t.Run("merger worker error", func() {
		t.Run("under failed", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			merge, unmerge, wait, mergedDone, unmergedDone, owners, err := t.newf(
				ctx,
				local,
				workers, workers,
				2, // 2 enough
				2,
				nil, nil, nil,
			)
			t.NoError(err)

			for worker := range slices.Values(workers[1:]) {
				t.NoError(merge(worker, workers, nil, nil))
			}

			t.NoError(merge(workers[0], workers, nil, errors.New("showme")))

			for worker := range slices.Values(workers) {
				t.NoError(unmerge(worker, nil))
			}

			t.NoError(wait())

			t.True(mergedDone())
			t.True(unmergedDone())

			t.Equal([2][]string{workers, nil}, owners())
		})

		t.Run("over failed", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			merge, unmerge, wait, mergedDone, unmergedDone, owners, err := t.newf(
				ctx,
				local,
				workers, workers,
				1, // 3 enough
				1,
				nil, nil, nil,
			)
			t.NoError(err)

			for worker := range slices.Values(workers[1:]) {
				t.NoError(merge(worker, workers, nil, nil))
			}

			err = merge(workers[0], workers, nil, errors.New("showme"))
			t.ErrorIs(err, dop5.ErrWorkersFailed)

			for worker := range slices.Values(workers) {
				t.NoError(unmerge(worker, nil))
			}

			err = wait()
			t.ErrorIs(err, dop5.ErrWorkersFailed)

			t.False(mergedDone())
			t.False(unmergedDone())

			t.Equal([2][]string{nil, nil}, owners())
		})
	})

	t.Run("unmerger worker error", func() {
		t.Run("under failed", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			merge, unmerge, wait, mergedDone, unmergedDone, owners, err := t.newf(
				ctx,
				local,
				workers, workers,
				1,
				2, // 2 enough
				nil, nil, nil,
			)
			t.NoError(err)

			for worker := range slices.Values(workers) {
				t.NoError(merge(worker, workers, nil, nil))
			}

			for worker := range slices.Values(workers[1:]) {
				t.NoError(unmerge(worker, nil))
			}

			t.NoError(unmerge(workers[0], errors.New("showme")))

			t.NoError(wait())

			t.True(mergedDone())
			t.True(unmergedDone())

			t.Equal([2][]string{workers, nil}, owners())
		})

		t.Run("over failed", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			merge, unmerge, wait, mergedDone, unmergedDone, owners, err := t.newf(
				ctx,
				local,
				workers, workers,
				1,
				1, // 3 enough
				nil, nil, nil,
			)
			t.NoError(err)

			for worker := range slices.Values(workers) {
				t.NoError(merge(worker, workers, nil, nil))
			}

			t.NoError(unmerge(workers[1], nil))

			err = unmerge(workers[2], errors.New("showme"))
			t.ErrorIs(err, dop5.ErrWorkersFailed)

			err = wait()
			t.ErrorIs(err, dop5.ErrWorkersFailed)

			t.True(mergedDone())
			t.False(unmergedDone())

			t.Equal([2][]string{workers, nil}, owners())
		})
	})

	t.Run("manager done", func() {
		t.Run("ok", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			merge, _, wait, mergedDone, unmergedDone, owners, err := t.newf(
				ctx,
				local,
				workers, workers,
				1, 1,
				nil, nil, nil,
			)
			t.NoError(err)

			t.NoError(merge(managerName, []string{local}, nil, nil))

			t.NoError(wait())

			t.True(mergedDone())
			t.True(unmergedDone())

			t.Equal([2][]string{nil, nil}, owners())
		})

		t.Run("with error", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			merge, _, wait, mergedDone, unmergedDone, owners, err := t.newf(
				ctx,
				local,
				workers, workers,
				1, 1,
				nil, nil, nil,
			)
			t.NoError(err)

			err = merge(managerName, []string{local}, nil, errors.New("showme"))
			t.ErrorIs(err, dop5.ErrWorkersFailed)

			err = wait()
			t.ErrorIs(err, dop5.ErrWorkersFailed)

			t.True(mergedDone())
			t.True(unmergedDone())

			t.Equal([2][]string{nil, nil}, owners())
		})
	})
}

func TestWorkerMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerMerger))
}
