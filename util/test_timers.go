//go:build test
// +build test

package util

import (
	"sort"
	"strings"
)

func (ts *SimpleTimers) TimerIDs() []TimerID {
	var ids []TimerID

	ts.timers.Traverse(func(id TimerID, _ *SimpleTimer) bool {
		ids = append(ids, id)

		return true
	})

	sort.Slice(ids, func(i, j int) bool {
		return strings.Compare(ids[i].String(), ids[j].String()) < 0
	})

	return ids
}
