//go:build test
// +build test

package util

import (
	"sort"
)

func (ts *SimpleTimers) TimerIDs() []TimerID {
	var ids []TimerID

	ts.timers.Traverse(func(id TimerID, _ *SimpleTimer) bool {
		ids = append(ids, id)

		return true
	})

	sort.Slice(ids, func(i, j int) bool {
		return ids[i].String() < ids[j].String()
	})

	return ids
}
