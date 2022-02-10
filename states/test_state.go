//go:build test
// +build test

package states

import "github.com/spikeekips/mitum/util"

func (st *baseStateHandler) setTimers(t *util.Timers) *baseStateHandler {
	st.ts = t

	return nil
}
