package util

import (
	"sync"

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
func (ts *Timers) Start() error {
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
			return errors.Wrapf(err, "failed to stop timer, %q", timer.ID())
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
			if InStringSlice(id.String(), sids) {
				continue
			}

			stopIDs[n] = id
			n++
		}

		if n > 0 {
			stopIDs = stopIDs[:n]
			if err := ts.stopTimers(stopIDs); err != nil {
				return errors.Wrap(err, "failed to start timers")
			}
		}
	}

	callback := func(t Timer) {
		if t.IsStarted() {
			return
		}

		_ = t.Start()
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
		return errors.Wrap(err, "failed to stop timers")
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
