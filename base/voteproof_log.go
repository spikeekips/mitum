package base

import (
	"fmt"

	"github.com/spikeekips/mitum/util"
)

func VoteproofLog(vp Voteproof) fmt.Stringer {
	return util.Stringer(func() string {
		if vp == nil {
			return ""
		}

		return vp.ID()
	})
}
