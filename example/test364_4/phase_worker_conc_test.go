package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testConcPhaseJobWorker struct {
	suite.Suite
}

func (t *testConcPhaseJobWorker) newArgs() *concPhaseJobWorkerArgs {
	return newConcPhaseJobWorkerArgs("local")
}

func (t *testConcPhaseJobWorker) TestCancel() {
	t.Run("before new job", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		args := t.newArgs()

		newJob, jobDone, wait, err := NewConcPhaseJobWorker(ctx, args)
		t.NoError(err)

		cancel()

		err = newJob(0, func(ctx context.Context, _ int, _ PhaseJobWorkerNewJobFunc) error {
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

func TestConcPhaseJobWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testConcPhaseJobWorker))
}
