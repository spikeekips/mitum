package main

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type (
	checkEnoughFailedWorkerFunc   func(worker string, _ func() error) (ok, enough, failed bool, _ error)
	checkEnoughFailedWorkersFuncs func(minEnough, maxFailed uint64) (checkEnoughFailedWorkerFunc, func(), error)
)

var (
	errCheckWorkersEnough = util.NewIDError("check workers; enough")
	errCheckWorkersFailed = util.NewIDError("check workers; failed")
)

func newCheckEnoughFailedWorkersFunc(
	minEnough, maxFailed uint64,
) (
	checkEnoughFailedWorkerFunc,
	func(),
	error,
) {
	if minEnough < 1 {
		return nil, nil, errors.Errorf("zero min enough")
	}

	var m util.LockedMap[string, struct{}]

	switch i, err := util.NewLockedMap[string, struct{}](minEnough+maxFailed, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	count := func() func(string, error) (_, _, _ bool) {
		var gIsClosed bool
		var enoughCount *uint64
		var failedCount *uint64
		var l sync.RWMutex

		f := func() (enough, failed bool) {
			if enoughCount != nil {
				enough = *enoughCount >= minEnough
			}

			if failedCount != nil {
				failed = *failedCount >= maxFailed
			}

			if failed && enough {
				enough = false
			}

			return enough, failed
		}

		c := func() (_, _, _ bool) {
			l.RLock()
			defer l.RUnlock()

			enough, failed := f()

			return gIsClosed, enough, failed
		}

		return func(worker string, err error) (isClosed, enough, failed bool) {
			if isClosed, enough, failed = c(); isClosed {
				return isClosed, enough, failed
			}

			l.Lock()
			defer l.Unlock()

			switch {
			case err != nil:
				if failedCount == nil {
					failedCount = new(uint64)
				}

				*failedCount++
			default:
				if enoughCount == nil {
					enoughCount = new(uint64)
				}

				*enoughCount++
			}

			enough, failed = f()

			if enough || failed {
				gIsClosed = true
			}

			return false, enough, failed
		}
	}()

	return func(worker string, f func() error) (ok, enough, failed bool, gerr error) {
			var isClosed bool

			switch _, _, err := m.Set(worker, func(_ struct{}, found bool) (struct{}, error) {
				if found {
					return struct{}{}, util.ErrLockedSetIgnore
				}

				if f != nil {
					gerr = f()
				}

				switch isClosed, enough, failed = count(worker, gerr); {
				case isClosed:
					return struct{}{}, util.ErrLockedSetIgnore
				default:
					ok = true

					return struct{}{}, nil
				}
			}); {
			case errors.Is(err, util.ErrLockedMapClosed):
				return false, enough, failed, nil
			default:
				if isClosed {
					m.Close()
				}

				return ok, enough, failed, gerr
			}
		},
		m.Close,
		nil
}
