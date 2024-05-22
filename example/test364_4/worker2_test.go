package main

import (
	"context"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testPhaseJobWorker2 struct {
	suite.Suite
}

func (t *testPhaseJobWorker2) TestCancel() {
	t.Run("before new job", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newJob, jobDone, wait, err := NewPhaseJobWorker2(ctx, nil, 1<<9)
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

	t.Run("cancel next phase", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newJob, jobDone, wait, err := NewPhaseJobWorker2(ctx, nil, 1<<9)
		t.NoError(err)

		newjobch := make(chan struct{}, 1)
		nextphasecanceledch := make(chan int, 1)

		t.NoError(newJob(0, func(_ context.Context, _ int, j PhaseJobWorkerNewJobFunc) error {
			return j(0, func(_ context.Context, phase int, _ PhaseJobWorkerNewJobFunc) error {
				newjobch <- struct{}{}

				select {
				case <-ctx.Done():
					nextphasecanceledch <- phase
					return ctx.Err()
				case <-time.After(time.Second * 3):
					return nil
				}
			})
		}))

		jobDone()

		select {
		case <-newjobch:
			cancel()
		case <-time.After(time.Second):
			t.Fail("wait new job called")
		}

		select {
		case <-nextphasecanceledch:
			cancel()
		case <-time.After(time.Second):
			t.Fail("wait next phase canceled")
		}

		err = wait()
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testPhaseJobWorker2) TestNewJobError() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newJob, jobDone, wait, err := NewPhaseJobWorker2(ctx, nil, 1<<9)
		t.NoError(err)

		t.NoError(newJob(0, func(ctx context.Context, _ int, _ PhaseJobWorkerNewJobFunc) error {
			return errors.Errorf("showme")
		}))

		jobDone()
		err = wait()
		t.ErrorContains(err, "showme")
	})
}

func (t *testPhaseJobWorker2) TestJobID() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newJob, jobDone, wait, err := NewPhaseJobWorker2(ctx, nil, 1<<9)
		t.NoError(err)

		add, done, _ := util.CompactAppendSliceV1[[2]uint64](6)

		for i := uint64(0); i < 3; i++ {
			jobID := i

			t.NoError(newJob(jobID, func(ctx context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
				t.T().Logf("new job: jobID=%d phase=%d", jobID, phase)

				add([2]uint64{uint64(phase), jobID})

				return j(jobID, func(ctx context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
					t.T().Logf("new job: jobID=%d phase=%d", jobID, phase)

					add([2]uint64{uint64(phase), jobID})

					return nil
				})
			}))
		}

		jobDone()
		t.NoError(wait())

		done(func(s [][2]uint64) error {
			sort.Slice(s, func(i, j int) bool {
				switch {
				case s[i][0] < s[j][0]:
					return true
				case s[i][0] == s[j][0]:
					return s[i][1] < s[j][1]
				default:
					return false
				}
			})

			t.Equal([][2]uint64{{0, 0}, {0, 1}, {0, 2}, {1, 0}, {1, 1}, {1, 2}}, s)

			return nil
		})
	})

	t.Run("lower phase ignored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newJob, jobDone, wait, err := NewPhaseJobWorker2(ctx,
			func(ctx context.Context) (JobWorker, error) {
				return NewBaseJobWorker(ctx, 1)
			},
			1<<9)
		t.NoError(err)

		add, done, _ := util.CompactAppendSliceV1[[2]uint64](6)

		job10Called := make(chan struct{}, 1)

		t.NoError(newJob(0, func(ctx context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
			t.T().Logf("new job: phase=%d jobID=%d", phase, 0)

			add([2]uint64{uint64(phase), 0})

			return j(1, func(ctx context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
				t.T().Logf("new job: 1->0, phase=%d jobID=%d", phase, 1)

				add([2]uint64{uint64(phase), 1})

				job10Called <- struct{}{}

				return nil
			})
		}))

		<-job10Called // ignored

		for i := uint64(1); i < 3; i++ {
			jobID := i

			t.NoError(newJob(jobID, func(ctx context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
				t.T().Logf("new job: phase=%d jobID=%d", phase, jobID)

				add([2]uint64{uint64(phase), jobID})

				switch jobID {
				case 1:
					return j(jobID, func(ctx context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
						return errors.Errorf("showme")
					})
				default:
					return j(jobID, func(ctx context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
						t.T().Logf("new job: phase=%d jobID=%d", phase, jobID)

						add([2]uint64{uint64(phase), jobID})

						return nil
					})
				}
			}))
		}

		jobDone()
		t.NoError(wait())

		done(func(s [][2]uint64) error {
			sort.Slice(s, func(i, j int) bool {
				switch {
				case s[i][0] < s[j][0]:
					return true
				case s[i][0] == s[j][0]:
					return s[i][1] < s[j][1]
				default:
					return false
				}
			})

			t.Equal([][2]uint64{{0, 0}, {0, 2}, {1, 1}, {1, 2}}, s)

			return nil
		})
	})
}

func (t *testPhaseJobWorker2) runPhases(
	ctx context.Context,
	jobs uint64,
	c func() (jobID uint64, _ PhaseJobWorkerCallback2),
) error {
	newJob, jobDone, wait, err := NewPhaseJobWorker2(ctx, nil, 1<<9)
	t.NoError(err)

	for i := uint64(0); i < jobs; i++ {
		j, k := c()
		if err := newJob(j, k); err != nil {
			return err
		}
	}

	jobDone()

	return wait()
}

func (t *testPhaseJobWorker2) TestAdd() {
	t.Run("0 phase", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var jobID, sum uint64

		t.runPhases(ctx, 9, func() (uint64, PhaseJobWorkerCallback2) {
			jobID++

			return jobID, func(_ context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
				atomic.AddUint64(&sum, 1)

				return nil
			}
		})

		t.Equal(uint64(9), sum)
	})

	t.Run("n phase", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		jobID := util.EmptyLocked[uint64]()
		var sum uint64

		depth := 9

		newJobID := func() (j uint64) {
			jobID.Set(func(prev uint64, isempty bool) (uint64, error) {
				if !isempty {
					prev++
				}

				j = prev

				return prev, nil
			})

			return j
		}

		var f func() func(_ context.Context, phase int, _ PhaseJobWorkerNewJobFunc) error

		f = func() func(_ context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
			return func(_ context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
				id := newJobID()

				// t.T().Logf("phase=%d jobID=%d", phase, j)
				atomic.AddUint64(&sum, id)

				if phase == depth {
					return nil
				}

				return j(id, f())
			}
		}

		t.NoError(t.runPhases(ctx, 9, func() (uint64, PhaseJobWorkerCallback2) {
			id := newJobID()

			atomic.AddUint64(&sum, id)

			return id, func(_ context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
				id := newJobID()

				// t.T().Logf("phase=%d jobID=%d", phase, j)
				atomic.AddUint64(&sum, id)

				return j(id, f())
			}
		}))

		t.Equal(uint64(4851), sum, sum)
	})
}

func TestPhaseJobWorker2(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testPhaseJobWorker2))
}
