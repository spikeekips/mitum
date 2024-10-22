package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"golang.org/x/sync/semaphore"
)

type (
	serialWorkerNextJobFunc    func(_ context.Context, phase int, _ serialWorkerNewJobFunc) error
	serialWorkerNewNextJobFunc func(phase int) (_ serialWorkerNextJobFunc, ok bool, _ error)
	serialWorkerNewJobFunc     func(serialWorkerNewNextJobFunc) error
)

type SerialWorker struct {
	*logging.Logging
	newWorker func(context.Context) (JobWorker, error)
}

func NewSerialWorker(
	newWorker func(context.Context) (JobWorker, error),
) (*SerialWorker, error) {
	switch i, err := newWorker(context.Background()); {
	case err != nil:
		return nil, err
	default:
		i.Close()
	}

	return &SerialWorker{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "serial-workers")
		}),
		newWorker: newWorker,
	}, nil
}

func (sw *SerialWorker) New(ctx context.Context) (
	newJob func(serialWorkerNewNextJobFunc) error,
	done func() error,
	cancel func(),
) {
	return sw.enterPhase(ctx, 0)
}

func (sw *SerialWorker) enterPhase(ctx context.Context, phase int) (
	newJob func(serialWorkerNewNextJobFunc) error,
	done func() error,
	cancel func(),
) {
	l := sw.Log().With().Int("phase", phase).Logger()

	gctx, cancel := context.WithCancel(ctx)

	var ew JobWorker

	jobdonech := make(chan struct{}, 1)

	ewDone := func() {
		if ew != nil {
			ew.Done()
		}

		close(jobdonech)
		l.Debug().Msg("ew.Done()")
	}

	var nextLock sync.RWMutex
	var nextNewJob func(serialWorkerNewNextJobFunc) error
	var nextdone func() error
	var nextcancel func()

	newJob = sw.newJobFunc(gctx, phase,
		func(ctx context.Context) (JobWorker, error) {
			i, err := sw.newWorker(ctx)
			if err != nil {
				return nil, err
			}

			ew = i

			return ew, nil
		},
		func(f ContextWorkerCallback) error {
			if ew == nil {
				return errors.Errorf("failed ew")
			}

			return ew.NewJob(f)
		},
		ewDone,
		func(ctx context.Context) func(serialWorkerNewNextJobFunc) error {
			nextLock.Lock()
			defer nextLock.Unlock()

			nextNewJob, nextdone, nextcancel = sw.enterPhase(gctx, phase+1)

			return nextNewJob
		},
	)

	done = func() error {
		defer cancel()

		return sw.doneFunc(gctx, phase,
			func() JobWorker {
				return ew
			},
			jobdonech,
			func() func() {
				nextLock.RLock()
				defer nextLock.RUnlock()

				return nextcancel
			},
			func() func(serialWorkerNewNextJobFunc) error {
				nextLock.RLock()
				defer nextLock.RUnlock()

				return nextNewJob
			},
			func() func() error {
				nextLock.RLock()
				defer nextLock.RUnlock()

				return nextdone
			},
		)()
	}

	return newJob, done, cancel
}

func (sw *SerialWorker) newJobFunc(
	ctx context.Context,
	phase int,
	newWorker func(context.Context) (JobWorker, error),
	newJob func(ContextWorkerCallback) error,
	ewDone func(),
	nextWorker func(context.Context) func(serialWorkerNewNextJobFunc) error,
) func(serialWorkerNewNextJobFunc) error {
	l := sw.Log().With().Int("phase", phase).Logger()

	var ew JobWorker
	var nextNewJob func(serialWorkerNewNextJobFunc) error
	var ewonce, nextewonce sync.Once

	return func(jobf serialWorkerNewNextJobFunc) error {
		if jobf == nil {
			ewDone()

			return nil
		}

		var job serialWorkerNextJobFunc

		switch i, ok, err := jobf(phase); {
		case err != nil:
			return err
		case !ok:
			return nil
		case i == nil:
			ewDone()

			return nil
		default:
			job = i
		}

		ewonce.Do(func() {
			i, err := newWorker(ctx)
			if err != nil {
				return
			}

			ew = i
		})

		if ew == nil {
			return errors.Errorf("failed ew")
		}

		return newJob(func(ctx context.Context) error {
			return job(ctx, phase, func(nextjob serialWorkerNewNextJobFunc) error {
				nextewonce.Do(func() {
					l.Debug().Msg("next phase created")

					nextNewJob = nextWorker(ctx)
				})

				return nextNewJob(nextjob)
			})
		})
	}
}

func (sw *SerialWorker) doneFunc(
	ctx context.Context,
	phase int,
	ewf func() JobWorker,
	jobdonech <-chan struct{},
	nextcancelf func() func(),
	nextNewJobf func() func(serialWorkerNewNextJobFunc) error,
	nextdonef func() func() error,
) func() error {
	l := sw.Log().With().Int("phase", phase).Logger()

	return func() error {
		l.Debug().Msg("start")
		defer l.Debug().Msg("stop")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-jobdonech:
		}

		ew := ewf()

		if ew == nil {
			return nil
		}

		defer func() {
			ew.Close()

			l.Debug().Msg("ew.Close()")
		}()

		l.Debug().Msg("ew.Wait()")

		if err := ew.Wait(); err != nil {
			if c := nextcancelf(); c != nil {
				c()
			}

			return err
		}

		switch i := nextNewJobf(); {
		case i == nil:
			return nil
		default:
			if err := i(nil); err != nil {
				if c := nextcancelf(); c != nil {
					c()
				}

				return err
			}
		}

		switch i := nextdonef(); {
		case i == nil:
			return nil
		default:
			return i()
		}
	}
}

type ContextWorkerCallback func(context.Context) error

type JobWorker interface {
	NewJob(ContextWorkerCallback) error
	Done()
	Wait() error
	Close()
	Cancel()
}

type BaseJobWorker struct {
	ctx        func() context.Context
	ctxCancel  func(error)
	newJobCtx  func() context.Context
	NewJobFunc func(ContextWorkerCallback) error
	newJob     func(ContextWorkerCallback) error
	done       func()
	wait       func() error
}

func NewBaseJobWorker(ctx context.Context, semSize int64) (*BaseJobWorker, error) {
	if semSize < 1 {
		return nil, errors.Errorf("semSize under 1")
	}

	errNewJobDone := errors.New(fmt.Sprintf("no more new job; %s", util.UUID().String()))

	ectx, eCtxCancel := context.WithCancelCause(ctx)
	newJobCtx, newJobCtxCancel := context.WithCancelCause(ectx)

	wk := &BaseJobWorker{
		ctx:       func() context.Context { return ectx },
		ctxCancel: eCtxCancel,
		newJobCtx: func() context.Context { return newJobCtx },
		done: func() {
			newJobCtxCancel(errNewJobDone)
		},
		NewJobFunc: func(c ContextWorkerCallback) error {
			return c(ectx)
		},
	}

	sem := semaphore.NewWeighted(semSize)

	newJobf := func(c ContextWorkerCallback) error {
		if err := context.Cause(wk.newJobCtx()); err != nil {
			return errors.WithStack(err)
		}

		if err := sem.Acquire(wk.newJobCtx(), 1); err != nil {
			return errors.WithStack(err)
		}

		go func() {
			if err := wk.NewJobFunc(c); err != nil {
				wk.ctxCancel(err)
			}

			sem.Release(1)
		}()

		return nil
	}

	waitf := func() error {
		<-wk.newJobCtx().Done()

		if err := context.Cause(wk.newJobCtx()); err != nil && !errors.Is(err, errNewJobDone) {
			return errors.WithStack(err)
		}

		if err := sem.Acquire(wk.ctx(), semSize); err != nil {
			if cerr := context.Cause(wk.ctx()); cerr != nil {
				return errors.WithStack(cerr)
			}

			return errors.WithStack(err)
		}

		if err := context.Cause(wk.ctx()); err != nil {
			return errors.WithStack(err)
		}

		return nil
	}

	wk.newJob = func(c ContextWorkerCallback) error {
		switch err := newJobf(c); {
		case errors.Is(err, errNewJobDone):
			return util.ErrJobWorkerDone.Wrap(err)
		default:
			return err
		}
	}

	wk.wait = func() error {
		switch err := waitf(); {
		case errors.Is(err, errNewJobDone):
			return util.ErrJobWorkerDone.Wrap(err)
		default:
			return err
		}
	}

	return wk, nil
}

func (wk *BaseJobWorker) Cancel() {
	wk.ctxCancel(nil)
}

func (wk *BaseJobWorker) Close() {
	wk.Cancel()
}

func (wk *BaseJobWorker) Done() {
	wk.done()
}

func (wk *BaseJobWorker) NewJob(c ContextWorkerCallback) error {
	return wk.newJob(c)
}

// Wait waits until all job finished.
func (wk *BaseJobWorker) Wait() error {
	defer wk.ctxCancel(nil)

	return wk.wait()
}

// LazyWait don't wait until all job finished.
func (wk *BaseJobWorker) LazyWait() error {
	defer wk.ctxCancel(nil)

	waitch := make(chan error, 1)

	go func() {
		waitch <- wk.wait()
	}()

	select {
	case <-wk.ctx().Done():
		return errors.WithStack(context.Cause(wk.ctx()))
	case err := <-waitch:
		return err
	}
}

// NewErrCallbackJobWorker ignores job error.
func NewErrCallbackJobWorker(ctx context.Context, semSize int64, errf func(error)) (wk *BaseJobWorker, _ error) {
	if errf == nil {
		errf = func(error) {} //revive:disable-line:modifies-parameter
	}

	switch i, err := NewBaseJobWorker(ctx, semSize); {
	case err != nil:
		return nil, err
	default:
		wk = i
	}

	wk.NewJobFunc = func(c ContextWorkerCallback) error {
		if err := c(wk.ctx()); err != nil {
			errf(err)
		}

		return nil
	}

	return wk, nil
}
