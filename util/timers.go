package util

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
)

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

func NewSimpleTimers(size int64, resolution time.Duration) (*SimpleTimers, error) {
	if resolution < 1 {
		return nil, errors.Errorf("too narrow resolution, %v", resolution)
	}

	timers, err := NewLockedMap(TimerID(""), (*SimpleTimer)(nil), size)
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

func NewSimpleTimersFixedIDs(size int64, interval time.Duration, ids []TimerID) (*SimpleTimers, error) {
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
	if len(ts.ids) > 0 {
		if InSlice(ts.ids, timer.id) < 0 {
			return false, errors.Errorf("unknown timer id, %q", timer.id)
		}
	}

	interval := timer.intervalFunc(0)
	if interval < 1 {
		return false, nil
	}

	_ = timer.expiredLocked.SetValue(time.Now().Add(interval))

	_ = ts.timers.SetValue(timer.id, timer)

	return true, nil
}

func (ts *SimpleTimers) StopAllTimers() error {
	_ = ts.removeAllTimers()

	return nil
}

// StopOthers stops timers except exclude timers.
func (ts *SimpleTimers) StopOthers(exclude []TimerID) error {
	ids := make([]TimerID, ts.timers.Len())

	var i int
	ts.timers.Traverse(func(id TimerID, _ *SimpleTimer) bool {
		if InSlice(exclude, id) >= 0 {
			return true
		}

		ids[i] = id
		i++

		return true
	})

	ids = ids[:i]
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
			return ctx.Err()
		case <-ticker.C:
			if err := ts.iterate(ctx); err != nil {
				return err
			}
		}
	}
}

func (ts *SimpleTimers) iterate(ctx context.Context) error {
	semsize := int64(ts.timers.Len())
	if semsize > 333 { //nolint:gomnd //...
		semsize = 333
	}

	wk := NewDistributeWorker(ctx, semsize, nil)

	removech := make(chan TimerID, ts.timers.Len())

	ts.timers.Traverse(func(id TimerID, timer *SimpleTimer) bool {
		switch tr := timer; {
		case !tr.isExpired():
			return true
		case !tr.prepare():
			return true
		default:
			_ = wk.NewJob(func(ctx context.Context, _ uint64) error {
				if keep, err := tr.run(ctx); err != nil || !keep {
					removech <- tr.id
				}

				return nil
			})

			return true
		}
	})
	wk.Done()

	go func() {
		defer wk.Close()

		if err := wk.Wait(); err != nil {
			return
		}

		close(removech)

		for id := range removech {
			_ = ts.removeTimer(id)
		}
	}()

	return nil
}

func (ts *SimpleTimers) removeAllTimers() int64 {
	var removed int64

	for {
		ids := make([]TimerID, 333)

		var i int
		ts.timers.Traverse(func(id TimerID, _ *SimpleTimer) bool {
			ids[i] = id
			i++

			return i != len(ids)
		})

		ids = ids[:i]
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
