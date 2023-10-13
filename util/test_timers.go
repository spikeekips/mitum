//go:build test
// +build test

package util

func (ts *SimpleTimers) TimerIDs() []TimerID {
	var ids []TimerID

	ts.timers.Traverse(func(id TimerID, _ *SimpleTimer) bool {
		ids = append(ids, id)

		return true
	})

	return ids
}
