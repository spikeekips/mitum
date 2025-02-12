package main

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testPhaseJobWorker struct {
	dutil.BaseSuite
}

func (t *testPhaseJobWorker) TestCancel() {
	t.Run("before new job", func() {
		ctx, cancel := t.Ctx()

		newJob, jobDone, wait, err := newFuncPhaseJobWorker(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, 0)
		t.NoError(err)

		cancel(nil)

		err = newJob(0, func(ctx context.Context, _ int, _ phaseJobWorkerNewJobFunc) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				return nil
			}
		})
		t.ErrorIs(err, context.Canceled)

		jobDone()
		err = wait()
		t.ErrorIs(err, context.Canceled, "%+v", err)
	})
}

func (t *testPhaseJobWorker) TestPhase() {
	t.Run("start from over zero", func() {
		ctx, _ := t.Ctx()

		var startPhase int = 3

		newJob, jobDone, wait, err := newFuncPhaseJobWorker(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, startPhase)
		t.NoError(err)

		njobs := 13
		endPhase := 3 + startPhase

		var called uint64
		var count func(context.Context, uint64, int, phaseJobWorkerNewJobFunc) error

		count = func(ctx context.Context, jobID uint64, phase int, c phaseJobWorkerNewJobFunc) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(rand.Intn(99)) * time.Millisecond):
				t.Print("job: phase=%d jobID=%d", phase, jobID)

				atomic.AddUint64(&called, 1)

				return c(
					jobID,
					func(ctx context.Context, phase int, c phaseJobWorkerNewJobFunc) error {
						if phase == endPhase {
							return nil
						}

						return count(ctx, jobID, phase, c)
					},
				)
			}
		}

		go func() {
			for i := range njobs {
				t.IfErrPanic(newJob(
					uint64(i),
					func(ctx context.Context, phase int, c phaseJobWorkerNewJobFunc) error {
						return count(ctx, uint64(i), phase, c)
					},
				))
			}

			jobDone()
		}()

		t.NoError(wait())

		ncalled := atomic.LoadUint64(&called)
		expected := uint64(njobs * (endPhase - startPhase))
		t.T().Logf("called=%d == expected=%d", ncalled, expected)
		t.Equal(expected, ncalled)
	})
}

func (t *testPhaseJobWorker) TestNewJob() {
	t.Run("ok", func() {
		ctx, _ := t.Ctx()

		newJob, jobDone, wait, err := newFuncPhaseJobWorker(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, 0)
		t.NoError(err)

		njobs := 13
		endPhase := 3

		var called uint64
		var count func(context.Context, uint64, int, phaseJobWorkerNewJobFunc) error

		count = func(ctx context.Context, jobID uint64, phase int, c phaseJobWorkerNewJobFunc) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(rand.Intn(99)) * time.Millisecond):
				t.Print("job: phase=%d jobID=%d", phase, jobID)

				atomic.AddUint64(&called, 1)

				return c(
					jobID,
					func(ctx context.Context, phase int, c phaseJobWorkerNewJobFunc) error {
						if phase == endPhase {
							return nil
						}

						return count(ctx, jobID, phase, c)
					},
				)
			}
		}

		go func() {
			for i := range njobs {
				t.IfErrPanic(newJob(
					uint64(i),
					func(ctx context.Context, phase int, c phaseJobWorkerNewJobFunc) error {
						return count(ctx, uint64(i), phase, c)
					},
				))
			}

			jobDone()
		}()

		t.NoError(wait())

		ncalled := atomic.LoadUint64(&called)
		expected := uint64(njobs * endPhase)
		t.T().Logf("called=%d == expected=%d", ncalled, expected)
		t.Equal(expected, ncalled)
	})

	t.Run("error: first phase", func() {
		ctx, _ := t.Ctx()

		newJob, jobDone, wait, err := newFuncPhaseJobWorker(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, 0)
		t.NoError(err)

		endPhase := 3
		njobs := 13

		var called uint64
		var count func(context.Context, uint64, int, phaseJobWorkerNewJobFunc) error

		count = func(ctx context.Context, jobID uint64, phase int, c phaseJobWorkerNewJobFunc) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(rand.Intn(99)) * time.Millisecond):
				var err error

				if phase == 0 && jobID == 9 {
					err = errors.New("showme")
				}

				t.Print("job: phase=%d jobID=%d err=%v", phase, jobID, err)

				if err != nil {
					return err
				}

				atomic.AddUint64(&called, 1)

				return c(
					jobID,
					func(ctx context.Context, phase int, c phaseJobWorkerNewJobFunc) error {
						if phase == endPhase {
							return nil
						}

						return count(ctx, jobID, phase, c)
					},
				)
			}
		}

		go func() {
			for i := range njobs {
				t.IfErrPanic(newJob(
					uint64(i),
					func(ctx context.Context, phase int, c phaseJobWorkerNewJobFunc) error {
						return count(ctx, uint64(i), phase, c)
					},
				))
			}

			jobDone()
		}()

		err = wait()
		t.ErrorContains(err, "showme")

		ncalled := atomic.LoadUint64(&called)
		expected := uint64(njobs * endPhase)
		t.T().Logf("called=%d < expected=%d", ncalled, expected)
		t.True(ncalled < expected, "called=%d < expected=%d", ncalled, expected)
	})

	t.Run("error: after first phase", func() {
		ctx, _ := t.Ctx()

		newJob, jobDone, wait, err := newFuncPhaseJobWorker(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, 0)
		t.NoError(err)

		njobs := 13
		endPhase := 3

		var called uint64
		var count func(context.Context, uint64, int, phaseJobWorkerNewJobFunc) error

		count = func(ctx context.Context, jobID uint64, phase int, c phaseJobWorkerNewJobFunc) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(rand.Intn(99)) * time.Millisecond):
				var err error

				if phase == endPhase && jobID == 9 {
					err = errors.New("showme")
				}

				t.Print("job: phase=%d jobID=%d err=%v", phase, jobID, err)

				if err != nil {
					return err
				}

				atomic.AddUint64(&called, 1)

				return c(
					jobID,
					func(ctx context.Context, phase int, c phaseJobWorkerNewJobFunc) error {
						if phase > endPhase {
							return nil
						}

						return count(ctx, jobID, phase, c)
					},
				)
			}
		}

		go func() {
			for i := range njobs {
				t.IfErrPanic(newJob(
					uint64(i),
					func(ctx context.Context, phase int, c phaseJobWorkerNewJobFunc) error {
						return count(ctx, uint64(i), phase, c)
					},
				))
			}

			jobDone()
		}()

		err = wait()
		t.ErrorIs(err, context.Canceled)

		ncalled := atomic.LoadUint64(&called)
		expected := uint64(njobs * (endPhase + 1))
		t.T().Logf("called=%d < expected=%d", ncalled, expected)
		t.True(ncalled < expected, "called=%d < expected=%d", ncalled, expected)
	})
}

func TestPhaseJobWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testPhaseJobWorker))
}
