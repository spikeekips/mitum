package util

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/slices"
)

var maxTimerSemsize int64 = 333

type TimerID string

func (ti TimerID) String() string {
	return string(ti)
}

type SimpleTimer struct {
	intervalFunc  func(uint64) time.Duration
	callback      func(context.Context, uint64) (bool, error)
	whenRemoved   func()
	expiredLocked *Locked[time.Time]
	id            TimerID
	called        uint64
	sync.RWMutex
}

func NewSimpleTimer(
	id TimerID,
	intervalFunc func(uint64) time.Duration,
	callback func(context.Context, uint64) (bool, error),
	whenRemoved func(),
) *SimpleTimer {
	if whenRemoved == nil {
		whenRemoved = func() {} //revive:disable-line:modifies-parameter
	}

	return &SimpleTimer{
		id:            id,
		intervalFunc:  intervalFunc,
		callback:      callback,
		whenRemoved:   whenRemoved,
		expiredLocked: NewLocked(time.Time{}),
	}
}

func (t *SimpleTimer) isExpired() bool {
	i, _ := t.expiredLocked.Value()

	return time.Now().After(i)
}

func (t *SimpleTimer) prepare() bool {
	t.RLock()
	defer t.RUnlock()

	i := t.intervalFunc(t.called)
	if i < 1 {
		return false
	}

	t.expiredLocked.SetValue(time.Now().Add(i + time.Hour))

	return true
}

func (t *SimpleTimer) run(ctx context.Context) (bool, error) {
	t.Lock()
	defer t.Unlock()

	next := t.intervalFunc(t.called + 1)

	defer func() {
		t.expiredLocked.SetValue(time.Now().Add(next))
		t.called++
	}()

	switch keep, err := t.callback(ctx, t.called); {
	case err != nil || !keep:
		return keep, err
	case next < 1:
		return false, nil
	default:
		return true, nil
	}
}

// SimpleTimers handles the multiple schdduled jobs without many goroutines.
type SimpleTimers struct {
	*ContextDaemon
	timers     LockedMap[TimerID, *SimpleTimer]
	ids        []TimerID
	resolution time.Duration
}

func NewSimpleTimers(size uint64, resolution time.Duration) (*SimpleTimers, error) {
	if resolution < 1 {
		return nil, errors.Errorf("too narrow resolution, %v", resolution)
	}

	timers, err := NewLockedMap[TimerID, *SimpleTimer](size, nil)
	if err != nil {
		return nil, err
	}

	ts := &SimpleTimers{
		timers:     timers,
		resolution: resolution,
	}

	ts.ContextDaemon = NewContextDaemon(ts.start)

	return ts, nil
}

func NewSimpleTimersFixedIDs(size uint64, interval time.Duration, ids []TimerID) (*SimpleTimers, error) {
	ts, err := NewSimpleTimers(size, interval)
	if err != nil {
		return nil, err
	}

	ts.ids = ids

	return ts, nil
}

func (ts *SimpleTimers) Stop() error {
	if err := ts.ContextDaemon.Stop(); err != nil {
		return err
	}

	_ = ts.removeAllTimers()

	ts.timers.Close()

	return nil
}

func (ts *SimpleTimers) New(
	id TimerID,
	intervalFunc func(uint64) time.Duration,
	callback func(context.Context, uint64) (bool, error),
) (bool, error) {
	return ts.NewTimer(NewSimpleTimer(id, intervalFunc, callback, nil))
}

func (ts *SimpleTimers) NewTimer(timer *SimpleTimer) (bool, error) {
	var keep bool

	if _, _, err := ts.timers.Set(timer.id, func(_ *SimpleTimer, found bool) (*SimpleTimer, error) {
		if len(ts.ids) > 0 {
			if slices.Index[TimerID](ts.ids, timer.id) < 0 {
				return nil, errors.Errorf("unknown timer id, %q", timer.id)
			}
		}

		interval := timer.intervalFunc(0)
		if interval < 1 {
			return nil, ErrLockedSetIgnore.WithStack()
		}

		keep = true

		_ = timer.expiredLocked.SetValue(time.Now().Add(interval))

		return timer, nil
	}); err != nil {
		return keep, err
	}

	return keep, nil
}

func (ts *SimpleTimers) StopAllTimers() error {
	_ = ts.removeAllTimers()

	return nil
}

// StopOthers stops timers except exclude timers.
func (ts *SimpleTimers) StopOthers(exclude []TimerID) error {
	var ids []TimerID

	ts.timers.Traverse(func(id TimerID, _ *SimpleTimer) bool {
		if slices.Index[TimerID](exclude, id) >= 0 {
			return true
		}

		ids = append(ids, id)

		return true
	})

	if len(ids) < 1 {
		return nil
	}

	return ts.StopTimers(ids)
}

func (ts *SimpleTimers) StopTimers(ids []TimerID) error {
	for i := range ids {
		_ = ts.removeTimer(ids[i])
	}

	return nil
}

func (ts *SimpleTimers) start(ctx context.Context) error {
	ticker := time.NewTicker(ts.resolution)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-ticker.C:
			if err := ts.iterate(ctx); err != nil {
				return err
			}
		}
	}
}

func (ts *SimpleTimers) iterate(ctx context.Context) error {
	var timers []*SimpleTimer

	ts.timers.Traverse(func(id TimerID, timer *SimpleTimer) bool {
		switch {
		case !timer.isExpired(),
			!timer.prepare():
		default:
			timers = append(timers, timer)
		}

		return true
	})

	if len(timers) < 1 {
		return nil
	}

	semsize := int64(len(timers))
	if semsize > maxTimerSemsize {
		semsize = maxTimerSemsize
	}

	wk, err := NewDistributeWorker(ctx, semsize, nil)
	if err != nil {
		return err
	}

	for i := range timers {
		tr := timers[i]

		_ = wk.NewJob(func(ctx context.Context, _ uint64) error {
			if keep, err := tr.run(ctx); err != nil || !keep {
				_ = ts.removeTimer(tr.id)
			}

			return nil
		})
	}

	wk.Done()

	go func() {
		defer wk.Close()

		if err := wk.Wait(); err != nil {
			return
		}
	}()

	return nil
}

func (ts *SimpleTimers) removeAllTimers() int64 {
	var removed int64

	for {
		var ids []TimerID

		ts.timers.Traverse(func(id TimerID, _ *SimpleTimer) bool {
			ids = append(ids, id)

			return true
		})

		if len(ids) < 1 {
			break
		}

		var c int64

		for i := range ids {
			if ts.removeTimer(ids[i]) {
				c++
			}
		}

		removed += c
	}

	return removed
}

func (ts *SimpleTimers) removeTimer(id TimerID) bool {
	removed, _ := ts.timers.Remove(id, func(timer *SimpleTimer, found bool) error {
		if found {
			timer.whenRemoved()
		}

		return nil
	})

	return removed
}
