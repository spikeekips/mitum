package util

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
)

var contextTimerPool = sync.Pool{
	New: func() interface{} {
		return new(ContextTimer)
	},
}

var (
	ContextTimerPoolGet = func() *ContextTimer {
		return contextTimerPool.Get().(*ContextTimer) //nolint:forcetypeassert //...
	}
	ContextTimerPoolPut = func(ct *ContextTimer) {
		ct.Lock()
		ct.ContextDaemon = nil
		ct.id = TimerID("")
		ct.interval = nil
		ct.callback = nil
		ct.c = 0
		ct.Unlock()

		contextTimerPool.Put(ct)
	}
)

type ContextTimer struct {
	callback func(int) (bool, error)
	*ContextDaemon
	interval        func(int, time.Duration) time.Duration
	id              TimerID
	defaultInterval time.Duration
	c               int
	sync.RWMutex
}

func NewContextTimer(id TimerID, interval time.Duration, callback func(int) (bool, error)) *ContextTimer {
	ct := ContextTimerPoolGet()
	ct.RWMutex = sync.RWMutex{}
	ct.id = id
	ct.interval = func(int, time.Duration) time.Duration {
		return interval
	}
	ct.defaultInterval = interval
	ct.callback = callback
	ct.ContextDaemon = NewContextDaemon("timer-"+string(id), ct.start)

	return ct
}

func (ct *ContextTimer) ID() TimerID {
	return ct.id
}

func (ct *ContextTimer) SetInterval(f func(int, time.Duration /* default */) time.Duration) Timer {
	ct.Lock()
	defer ct.Unlock()

	ct.interval = f

	return ct
}

func (ct *ContextTimer) Reset() error {
	ct.Lock()
	defer ct.Unlock()

	ct.c = 0

	return nil
}

func (ct *ContextTimer) Stop() error {
	errch := make(chan error)

	go func() {
		errch <- ct.ContextDaemon.Stop()
	}()

	select {
	case err := <-errch:
		return errors.Wrap(err, "failed to stop ContextTimer")
	case <-time.After(time.Millisecond * 300):
	}

	go func() {
		<-errch
		ContextTimerPoolPut(ct)
	}()

	return nil
}

func (ct *ContextTimer) count() int {
	ct.RLock()
	defer ct.RUnlock()

	return ct.c
}

func (ct *ContextTimer) start(ctx context.Context) error {
	if err := ct.Reset(); err != nil {
		return errors.Wrap(err, "failed to start ContextTimer")
	}

end:
	for {
		select {
		case <-ctx.Done():
			break end
		default:
			c := func() bool {
				ct.RLock()
				defer ct.RUnlock()

				return ct.interval == nil || ct.callback == nil
			}
			if c() {
				break end
			}

			if err := ct.prepareCallback(ctx); err != nil {
				break end
			}
		}
	}

	return nil
}

func (ct *ContextTimer) finishCallback(count int) {
	ct.Lock()
	defer ct.Unlock()

	if ct.c == count {
		ct.c++
	}
}

func (ct *ContextTimer) prepareCallback(ctx context.Context) error {
	ct.RLock()
	intervalfunc := ct.interval
	callback := ct.callback
	ct.RUnlock()

	count := ct.count()
	interval := intervalfunc(count, ct.defaultInterval)

	if interval < time.Nanosecond {
		return errors.Errorf("invalid interval; too narrow, %v", interval)
	}

	return ct.waitAndRun(ctx, interval, callback, count)
}

func (ct *ContextTimer) waitAndRun(
	ctx context.Context,
	interval time.Duration,
	callback func(int) (bool, error),
	count int,
) error {
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(interval):
	}

	if keep, err := callback(count); err != nil {
		return errors.Wrap(err, "failed to callback in ContextTimer")
	} else if !keep {
		return ErrStopTimer.Call()
	}

	ct.finishCallback(count)

	return nil
}
