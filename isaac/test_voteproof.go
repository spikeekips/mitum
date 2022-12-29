//go:build test
// +build test

package isaac

import (
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/stretchr/testify/assert"
)

func (vp *baseVoteproof) SetID(id string) {
	vp.id = id
}

func (vp *baseVoteproof) SetResult(r base.VoteResult) *baseVoteproof {
	switch r {
	case base.VoteResultDraw:
		vp.majority = nil
	case base.VoteResultNotYet:
		vp.finishedAt = time.Time{}
	case base.VoteResultMajority:
		if vp.majority == nil {
			panic("empty majority")
		}

		vp.finishedAt = localtime.Now().UTC()
	}

	return vp
}

func EqualVoteproof(t *assert.Assertions, a, b base.Voteproof) {
	if a == nil {
		t.Equal(a, b)

		return
	}

	base.EqualVoteproof(t, a, b)

	wa, oka := a.(WithdrawVoteproof)
	wb, okb := b.(WithdrawVoteproof)

	t.Equal(oka, okb)

	if oka {
		wopa := wa.Withdraws()
		wopb := wb.Withdraws()

		t.Equal(len(wopa), len(wopb))

		for i := range wopa {
			base.EqualOperation(t, wopa[i], wopb[i])
		}
	}
}
