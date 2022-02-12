//go:build test
// +build test

package isaac

import "github.com/spikeekips/mitum/util"

func (st *baseStateHandler) setTimers(t *util.Timers) *baseStateHandler {
	st.ts = t

	return nil
}
