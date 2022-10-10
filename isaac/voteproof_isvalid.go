package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

func IsValidVoteproofWithSuffrage(vp base.Voteproof, suf base.Suffrage) error {
	e := util.ErrInvalid.Errorf("invalid voteproof with suffrage")

	if vp == nil {
		return e.Errorf("nil voteproof")
	}

	var withdraws []base.SuffrageWithdrawOperation

	switch t := vp.(type) {
	case INITVoteproof:
		withdraws = t.Withdraws()
	case ACCEPTVoteproof:
		withdraws = t.Withdraws()
	default:
		return e.Errorf("unknown voteproof, %T", vp)
	}

	rsuf := suf

	if len(withdraws) > 0 {
		th := base.DefaultThreshold.Threshold(uint(suf.Len()))
		if n := uint(len(withdraws)); n > uint(suf.Len())-th {
			th = uint(suf.Len()) - n
		}

		for i := range withdraws {
			if n := uint(len(withdraws[i].NodeSigns())); n < th {
				return e.Errorf("insufficient withdraw node signs; node signs=%d threshold=%d", n, th)
			}

			if err := IsValidWithdrawWithSuffrage(vp.Point().Height(), withdraws[i], suf); err != nil {
				return e.Wrap(err)
			}
		}

		nodes := suf.Nodes()

		filtered := util.Filter2Slices(nodes, withdraws, func(_, _ interface{}, i, j int) bool {
			return nodes[i].Address().Equal(withdraws[j].WithdrawFact().Node())
		})

		switch i, err := NewSuffrage(filtered); {
		case err != nil:
			return e.Wrap(err)
		default:
			rsuf = i
		}
	}

	if err := base.IsValidVoteproofWithSuffrage(vp, rsuf); err != nil {
		return e.Wrap(err)
	}

	return nil
}
