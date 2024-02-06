package testopsworkers

import (
	"context"
	"crypto/rand"
	"math"
	"math/big"
	mathrand "math/rand"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	serialWorkerNextJobFunc    func(_ context.Context, jobid uint64, phase uint64, _ serialWorkerAddNextJobFunc) error
	serialWorkerNewNextJobFunc func(phase uint64) (_ serialWorkerNextJobFunc, ok bool)
	serialWorkerAddNextJobFunc func(serialWorkerNewNextJobFunc) error
)

type distributedWorker interface {
	NewJob(util.ContextWorkerCallback) error
	Done()
	Wait() error
	Close()
	Cancel()
}

type SerialWorkers struct {
	*logging.Logging
	newWorker func(context.Context) (distributedWorker, error)
}

func NewSerialWorkers(
	newWorker func(context.Context) (distributedWorker, error),
) (*SerialWorkers, error) {
	switch i, err := newWorker(context.Background()); {
	case err != nil:
		return nil, err
	default:
		i.Close()
	}

	return &SerialWorkers{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "serial-workers")
		}),
		newWorker: newWorker,
	}, nil
}

func (sw *SerialWorkers) New(ctx context.Context) (
	newJob func(serialWorkerNewNextJobFunc) error,
	done func() error,
	cancel func(),
) {
	return sw.enterPhase(ctx, 0)
}

func (sw *SerialWorkers) enterPhase(ctx context.Context, phase uint64) (
	newJob func(serialWorkerNewNextJobFunc) error,
	done func() error,
	cancel func(),
) {
	l := sw.Log().With().Uint64("phase", phase).Logger()

	gctx, cancel := context.WithCancel(ctx)

	var ew distributedWorker

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
		func(ctx context.Context) (distributedWorker, error) {
			i, err := sw.newWorker(ctx)
			if err != nil {
				return nil, err
			}

			ew = i

			return ew, nil
		},
		func(f util.ContextWorkerCallback) error {
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
			func() distributedWorker {
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

func (sw *SerialWorkers) newJobFunc(
	ctx context.Context,
	phase uint64,
	newWorker func(context.Context) (distributedWorker, error),
	newJob func(util.ContextWorkerCallback) error,
	ewDone func(),
	nextWorker func(context.Context) func(serialWorkerNewNextJobFunc) error,
) func(serialWorkerNewNextJobFunc) error {
	l := sw.Log().With().Uint64("phase", phase).Logger()

	var ew distributedWorker
	var nextNewJob func(serialWorkerNewNextJobFunc) error
	var ewonce, nextewonce sync.Once

	return func(jobf serialWorkerNewNextJobFunc) error {
		if jobf == nil {
			ewDone()

			return nil
		}

		var job serialWorkerNextJobFunc

		switch i, ok := jobf(phase); {
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

		return newJob(func(ctx context.Context, jobid uint64) error {
			return job(ctx, jobid, phase, func(nextjob serialWorkerNewNextJobFunc) error {
				nextewonce.Do(func() {
					l.Debug().Msg("next phase created")

					nextNewJob = nextWorker(ctx)
				})

				return nextNewJob(nextjob)
			})
		})
	}
}

func (sw *SerialWorkers) doneFunc(
	ctx context.Context,
	phase uint64,
	ewf func() distributedWorker,
	jobdonech <-chan struct{},
	nextcancelf func() func(),
	nextNewJobf func() func(serialWorkerNewNextJobFunc) error,
	nextdonef func() func() error,
) func() error {
	l := sw.Log().With().Uint64("phase", phase).Logger()

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

func newDjb2Seed() uint64 {
	max := big.NewInt(0).SetUint64(uint64(math.MaxUint64))

	switch rnd, err := rand.Int(rand.Reader, max); {
	case err != nil:
		return mathrand.Uint64()
	default:
		return rnd.Uint64()
	}
}

// stringdjb2 derived from
// https://github.com/patrickmn/go-cache/blob/master/sharded.go .
func stringdjb2(seed uint64, k string) uint64 {
	var (
		l = uint64(len(k))
		d = 5381 + seed + l
		i = uint64(0)
	)

	if l >= 4 { //nolint:gomnd //...
		for i < l-4 {
			d = (d * 33) ^ uint64(k[i])   //nolint:gomnd //...
			d = (d * 33) ^ uint64(k[i+1]) //nolint:gomnd //...
			d = (d * 33) ^ uint64(k[i+2]) //nolint:gomnd //...
			d = (d * 33) ^ uint64(k[i+3]) //nolint:gomnd //...
			i += 4
		}
	}

	switch l - i {
	case 1:
	case 2:
		d = (d * 33) ^ uint64(k[i]) //nolint:gomnd //...
	case 3: //nolint:gomnd //...
		d = (d * 33) ^ uint64(k[i])   //nolint:gomnd //...
		d = (d * 33) ^ uint64(k[i+1]) //nolint:gomnd //...
	case 4: //nolint:gomnd //...
		d = (d * 33) ^ uint64(k[i])   //nolint:gomnd //...
		d = (d * 33) ^ uint64(k[i+1]) //nolint:gomnd //...
		d = (d * 33) ^ uint64(k[i+2]) //nolint:gomnd //...
	}

	return d ^ (d >> 16) //nolint:gomnd //...
}
