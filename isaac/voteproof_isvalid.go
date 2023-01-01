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
	case INITWithdrawVoteproof:
		withdraws = t.Withdraws()
	case ACCEPTWithdrawVoteproof:
		withdraws = t.Withdraws()
	case INITStuckVoteproof:
		withdraws = t.Withdraws()
	case ACCEPTStuckVoteproof:
		withdraws = t.Withdraws()
	case INITVoteproof:
	case ACCEPTVoteproof:
	default:
		return e.Errorf("unknown voteproof, %T", vp)
	}

	th := vp.Threshold()
	rsuf := suf

	if len(withdraws) > 0 {
		for i := range withdraws {
			if err := IsValidWithdrawWithSuffrage(vp.Point().Height(), withdraws[i], suf); err != nil {
				return e.Wrap(err)
			}
		}

		switch i, err := NewSuffrageWithWithdraws(suf, vp.Threshold(), withdraws); {
		case err != nil:
			return e.Wrap(err)
		default:
			rsuf = i
			th = base.MaxThreshold
		}
	}

	if err := base.IsValidVoteproofWithSuffrage(vp, rsuf, th); err != nil {
		return e.Wrap(err)
	}

	return nil
}
