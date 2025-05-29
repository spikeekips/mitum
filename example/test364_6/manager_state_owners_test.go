package dop6

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/pkg/errors"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerStateOwners struct {
	baseMergerSuite
}

func (t *testManagerStateOwners) TestNew() {
	t.Run("empty ops workers", func() {
		_, _, _, _, err := newManagerStateOwners(
			context.Background(),
			nil,
			1, 1, 1,
		)
		t.ErrorContains(err, "zero workers")
	})

	t.Run("zero workers", func() {
		_, workers := t.workers(1)

		_, _, _, _, err := newManagerStateOwners(
			context.Background(),
			workers,
			1, 1, 0,
		)
		t.ErrorContains(err, "under 1")
	})
}

func (t *testManagerStateOwners) TestCtxCanceled() {
	_, workers := t.workers(2)

	t.Run("before new", func() {
		ctx, cancel := t.Ctx()
		cancel(nil)

		workerOwners, _, _, wait, err := newManagerStateOwners(
			ctx,
			workers,
			1, 1, 1,
		)
		t.NoError(err)

		_, _, err = wait()
		t.ErrorIs(err, context.Canceled)

		t.NoError(workerOwners(workers[0], workers[:1], workers[:1]))
	})
}

func (t *testManagerStateOwners) TestWorkerOwners() {
	_, workers := t.workers(3)
	opsWorkers := workers[:2]

	t.Run("ok", func() {
		msubs := workers[:2]
		mowners := workers

		workerOwners, _, _, wait, err := newManagerStateOwners(
			context.Background(),
			opsWorkers,
			1, 1, uint64(len(workers)),
		)
		t.NoError(err)

		donech := make(chan error)
		go func() {
			_, _, err := wait()
			donech <- err
		}()

		for worker := range slices.Values(opsWorkers) {
			t.NoError(workerOwners(worker, []string{worker}, workers))
		}

		select {
		case <-time.After(time.Second):
			t.Fail("wait")
		case err := <-donech:
			t.NoError(err)
		}

		subs, owners, err := wait()
		t.NoError(err)

		slices.Sort(subs)
		slices.Sort(owners)
		t.Equal(msubs, subs)
		t.Equal(mowners, owners)
	})

	t.Run("not ops worker", func() {
		workerOwners, _, _, _, err := newManagerStateOwners(
			context.Background(),
			opsWorkers,
			1, 1, uint64(len(workers)),
		)
		t.NoError(err)

		err = workerOwners(workers[2], workers[:1], workers)
		t.ErrorContains(err, "not ops worker")
	})
}

func (t *testManagerStateOwners) TestWait() {
	_, workers := t.workers(3)
	opsWorkers := workers[:2]

	t.Run("ok", func() {
		msubs := workers[:2]
		mowners := workers

		workerOwners, _, _, wait, err := newManagerStateOwners(
			context.Background(),
			opsWorkers,
			1, 1, uint64(len(workers)),
		)
		t.NoError(err)

		donech := make(chan error)
		go func() {
			_, _, err := wait()
			donech <- err
		}()

		for worker := range slices.Values(opsWorkers) {
			t.NoError(workerOwners(worker, []string{worker}, workers))
		}

		select {
		case <-time.After(time.Second):
			t.Fail("wait")
		case err := <-donech:
			t.NoError(err)
		}

		subs, owners, err := wait()
		t.NoError(err)

		slices.Sort(subs)
		slices.Sort(owners)
		t.Equal(msubs, subs)
		t.Equal(mowners, owners)
	})
}

func (t *testManagerStateOwners) prepareCheckFailed(workers, opsWorkers, subs []string, statesShare uint64) (
	dop5.CheckFailedWorkersFunc,
	dop5.CheckFailedWorkersFunc,
) {
	workerOwners, cs, co, wait, err := newManagerStateOwners(
		context.Background(),
		opsWorkers,
		1, statesShare, uint64(len(workers)),
	)
	t.NoError(err)

	donech := make(chan error)
	go func() {
		_, _, err := wait()
		donech <- err
	}()

	for worker := range slices.Values(opsWorkers) {
		t.NoError(workerOwners(worker, subs, workers))
	}

	select {
	case <-time.After(time.Second):
		t.Fail("wait")
	case err := <-donech:
		t.NoError(err)
	}

	_, _, err = wait()
	t.NoError(err)

	return cs, co
}

func (t *testManagerStateOwners) TestCheckFailedSubs() {
	t.Run("no failed", func() {
		_, workers := t.workers(3)
		subs := workers[:2]

		checkSub, _ := t.prepareCheckFailed(workers, workers[:2], subs, 1)

		ok, enough, failed := checkSub(subs[0], nil)
		t.NoError(failed)
		t.True(ok)
		t.False(enough)

		ok, enough, failed = checkSub(subs[1], nil)
		t.NoError(failed)
		t.True(ok)
		t.True(enough)
	})

	t.Run("failed", func() {
		_, workers := t.workers(3)
		subs := workers[:2]

		checkSub, _ := t.prepareCheckFailed(workers, workers[:2], subs, 1)

		ok, enough, failed := checkSub(subs[0], nil)
		t.NoError(failed)
		t.True(ok)
		t.False(enough)

		ok, enough, failed = checkSub(subs[1], errors.New("showme"))
		t.ErrorContains(failed, "showme")
		t.ErrorIs(failed, dop5.ErrWorkersFailed)
		t.True(ok)
		t.False(enough)
	})

	t.Run("failed, but not sub", func() {
		_, workers := t.workers(4)
		subs := workers[:3]

		checkSub, checkOwners := t.prepareCheckFailed(workers, workers[:2], subs, 2)

		ok, enough, failed := checkSub(subs[0], nil)
		t.NoError(failed)
		t.True(ok)
		t.False(enough)

		ok, enough, failed = checkSub(subs[1], nil)
		t.NoError(failed)
		t.True(ok)
		t.True(enough)

		ok, enough, failed = checkSub(subs[2], nil)
		t.NoError(failed)
		t.False(ok)
		t.False(enough)

		ok, enough, failed = checkOwners(workers[0], nil)
		t.NoError(failed)
		t.True(ok)
		t.False(enough)

		ok, enough, failed = checkOwners(workers[1], errors.New("findme"))
		t.NoError(failed)
		t.True(ok)
		t.False(enough)

		ok, enough, failed = checkOwners(workers[3], errors.New("showme"))
		t.ErrorContains(failed, "showme")
		t.ErrorIs(failed, dop5.ErrWorkersFailed)
		t.True(ok)
		t.False(enough)
	})
}

func TestManagerStateOwners(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerStateOwners))
}
