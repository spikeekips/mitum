//go:build test
// +build test

package isaacstates

import "github.com/spikeekips/mitum/util"

func (st *baseHandler) setTimers(t *util.Timers) *baseHandler {
	st.timers = t

	return nil
}
