package util

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// Timers handles the multiple timers and controls them selectively.
type Timers struct {
	timers map[TimerID]Timer
	sync.RWMutex
	allowUnknown bool
}

func NewTimers(ids []TimerID, allowUnknown bool) *Timers {
	timers := map[TimerID]Timer{}

	for _, id := range ids {
		timers[id] = nil
	}

	return &Timers{
		timers:       timers,
		allowUnknown: allowUnknown,
	}
}

// Start of Timers does nothing
func (ts *Timers) Start(context.Context) error {
	ts.Lock()
	defer ts.Unlock()

	if ts.timers == nil {
		ts.timers = map[TimerID]Timer{}
	}

	return nil
}

// Stop of Timers will stop all the timers
func (ts *Timers) Stop() error {
	ts.Lock()
	defer ts.Unlock()

	if ts.timers == nil { // NOTE already stopped
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(len(ts.timers))

	for id := range ts.timers {
		timer := ts.timers[id]
		if timer == nil {
			wg.Done()
			continue
		}

		go func(t Timer) {
			defer wg.Done()

			_ = t.Stop()
		}(timer)
	}

	wg.Wait()

	ts.timers = nil

	return nil
}

func (ts *Timers) ResetTimer(id TimerID) error {
	ts.RLock()
	defer ts.RUnlock()

	if ts.timers == nil {
		return nil
	}

	switch t, found := ts.timers[id]; {
	case !found:
		return errors.Errorf("timer, %q not found", id)
	case t == nil:
		return errors.Errorf("timer, %q not running", id)
	default:
		return t.Reset() //nolint:wrapcheck //...
	}
}

// SetTimer sets the timer with id
func (ts *Timers) SetTimer(timer Timer) error {
	ts.Lock()
	defer ts.Unlock()

	if ts.timers == nil {
		return nil
	}

	if _, found := ts.timers[timer.ID()]; !found {
		if !ts.allowUnknown {
			return errors.Errorf("not allowed to add new timer: %s", timer.ID())
		}
	}

	if existing := ts.timers[timer.ID()]; existing != nil && existing.IsStarted() {
		if err := existing.Stop(); err != nil {
			return errors.WithMessagef(err, "failed to stop timer, %q", timer.ID())
		}
	}

	ts.timers[timer.ID()] = timer

	return nil
}

// StartTimers starts timers with the given ids, before starting timers, stops
// the other timers if stopOthers is true.
func (ts *Timers) StartTimers(ids []TimerID, stopOthers bool) error { // revive:disable-line:flag-parameter
	ts.Lock()
	defer ts.Unlock()

	if ts.timers == nil {
		return nil
	}

	sids := make([]string, len(ids))

	for i := range ids {
		sids[i] = ids[i].String()
	}

	if stopOthers {
		stopIDs := make([]TimerID, len(ts.timers))

		var n int

		for id := range ts.timers {
			if InSlice(sids, id.String()) >= 0 {
				continue
			}

			stopIDs[n] = id
			n++
		}

		if n > 0 {
			stopIDs = stopIDs[:n]
			if err := ts.stopTimers(stopIDs); err != nil {
				return errors.WithMessage(err, "failed to start timers")
			}
		}
	}

	callback := func(t Timer) {
		if t.IsStarted() {
			return
		}

		_ = t.Start(context.Background())
	}

	return ts.traverse(callback, ids)
}

func (ts *Timers) StopTimers(ids []TimerID) error {
	ts.Lock()
	defer ts.Unlock()

	if ts.timers == nil {
		return nil
	}

	return ts.stopTimers(ids)
}

func (ts *Timers) StopTimersAll() error {
	ts.Lock()
	defer ts.Unlock()

	if ts.timers == nil {
		return nil
	}

	ids := make([]TimerID, len(ts.timers))

	var i int

	for id := range ts.timers {
		ids[i] = id
		i++
	}

	if len(ids) < 1 {
		return nil
	}

	return ts.stopTimers(ids)
}

func (ts *Timers) Started() []TimerID {
	ts.RLock()
	defer ts.RUnlock()

	if ts.timers == nil {
		return nil
	}

	started := make([]TimerID, len(ts.timers))

	var n int

	for id := range ts.timers {
		timer := ts.timers[id]
		if timer != nil && ts.timers[id].IsStarted() {
			started[n] = id
			n++
		}
	}

	return started[:n]
}

func (ts *Timers) IsTimerStarted(id TimerID) bool {
	ts.RLock()
	defer ts.RUnlock()

	if ts.timers == nil {
		return false
	}

	switch timer, found := ts.timers[id]; {
	case !found:
		return false
	case timer == nil:
		return false
	default:
		return timer.IsStarted()
	}
}

func (ts *Timers) stopTimers(ids []TimerID) error {
	callback := func(t Timer) {
		if !t.IsStarted() {
			return
		}

		_ = t.Stop()
	}

	if err := ts.traverse(callback, ids); err != nil {
		return errors.WithMessage(err, "failed to stop timers")
	}

	for _, id := range ids {
		ts.timers[id] = nil
	}

	return nil
}

func (ts *Timers) checkExists(ids []TimerID) error {
	for _, id := range ids {
		if _, found := ts.timers[id]; !found {
			return errors.Errorf("timer not found: %s", id)
		}
	}

	return nil
}

func (ts *Timers) traverse(callback func(Timer), ids []TimerID) error {
	if !ts.allowUnknown {
		if err := ts.checkExists(ids); err != nil {
			return err
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(ids))

	for _, id := range ids {
		go func(id TimerID) {
			defer wg.Done()

			timer := ts.timers[id]
			if timer == nil {
				return
			}

			callback(timer)
		}(id)
	}

	wg.Wait()

	return nil
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

func (t *SimpleTimer) run(ctx context.Context) (bool, error) {
	t.Lock()
	defer t.Unlock()

	if i := t.intervalFunc(t.called); i < 1 {
		return false, nil
	}

	defer func() {
		t.expiredLocked.SetValue(time.Now().Add(t.intervalFunc(t.called + 1)))
		t.called++
	}()

	switch keep, err := t.callback(ctx, t.called); {
	case err != nil || !keep:
		return keep, err
	default:
		return true, nil
	}
}

type SimpleTimers struct {
	*ContextDaemon
	timers   LockedMap[TimerID, *SimpleTimer]
	ids      []TimerID
	interval time.Duration
}

func NewSimpleTimers(size int64, interval time.Duration) (*SimpleTimers, error) {
	if interval < 1 {
		return nil, errors.Errorf("too narrow interval, %v", interval)
	}

	timers, err := NewLockedMap(TimerID(""), (*SimpleTimer)(nil), size)
	if err != nil {
		return nil, err
	}

	ts := &SimpleTimers{
		timers:   timers,
		interval: interval,
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
	ticker := time.NewTicker(ts.interval)
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
		tr := timer

		if !timer.isExpired() {
			return true
		}

		timer.expiredLocked.SetValue(time.Now().Add(time.Hour * 3333)) //nolint:gomnd // big enough

		_ = wk.NewJob(func(ctx context.Context, _ uint64) error {
			if keep, err := tr.run(ctx); err != nil || !keep { // NOTE remove
				removech <- tr.id
			}

			return nil
		})

		return true
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
