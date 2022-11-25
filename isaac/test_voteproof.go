//go:build test
// +build test

package isaac

import (
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/localtime"
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
