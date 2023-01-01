package isaacstates

import (
	"sync"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type LastVoteproofsHandler struct {
	ivp base.INITVoteproof
	avp base.ACCEPTVoteproof
	mvp base.Voteproof
	sync.RWMutex
}

func NewLastVoteproofsHandler() *LastVoteproofsHandler {
	return &LastVoteproofsHandler{}
}

func (l *LastVoteproofsHandler) Last() LastVoteproofs {
	l.RLock()
	defer l.RUnlock()

	return LastVoteproofs{
		ivp: l.ivp,
		avp: l.avp,
		mvp: l.mvp,
	}
}

func (l *LastVoteproofsHandler) IsNew(vp base.Voteproof) bool {
	l.RLock()
	defer l.RUnlock()

	lvp := findLastVoteproofs(l.ivp, l.avp)

	if lvp == nil {
		return true
	}

	return isNewBallotVoteproof(vp, lvp.Point(), lvp.Result() == base.VoteResultMajority)
}

func (l *LastVoteproofsHandler) Set(vp base.Voteproof) bool {
	l.Lock()
	defer l.Unlock()

	lvp := findLastVoteproofs(l.ivp, l.avp)

	if lvp != nil && !isNewBallotVoteproof(vp, lvp.Point(), lvp.Result() == base.VoteResultMajority) {
		return false
	}

	switch vp.Point().Stage() { //nolint:exhaustive //...
	case base.StageINIT:
		l.ivp = vp.(base.INITVoteproof) //nolint:forcetypeassert //...
	case base.StageACCEPT:
		l.avp = vp.(base.ACCEPTVoteproof) //nolint:forcetypeassert //...
	}

	if vp.Result() == base.VoteResultMajority {
		l.mvp = vp
	}

	return true
}

func (l *LastVoteproofsHandler) ForceSet(vp base.Voteproof) bool {
	l.Lock()
	defer l.Unlock()

	switch vp.Point().Stage() { //nolint:exhaustive //...
	case base.StageINIT:
		l.ivp = vp.(base.INITVoteproof) //nolint:forcetypeassert //...
	case base.StageACCEPT:
		l.avp = vp.(base.ACCEPTVoteproof) //nolint:forcetypeassert //...

		if l.ivp != nil && l.ivp.Point().Compare(l.avp.Point()) > 0 {
			l.ivp = nil

			l.mvp = nil
			if l.avp.Result() == base.VoteResultMajority {
				l.mvp = l.avp
			}
		}
	}

	if vp.Result() == base.VoteResultMajority {
		l.mvp = vp
	}

	return true
}

type LastVoteproofs struct {
	ivp base.INITVoteproof
	avp base.ACCEPTVoteproof
	mvp base.Voteproof
}

func (l LastVoteproofs) Cap() base.Voteproof {
	return findLastVoteproofs(l.ivp, l.avp)
}

func (l LastVoteproofs) INIT() base.INITVoteproof {
	return l.ivp
}

// PreviousBlockForNextRound finds the previous block hash from last majority
// voteproof.
//
// --------------------------------------
// | m        | v      |   | heights    |
// --------------------------------------
// | init     | init   | X |            |
// | accept   | init   | O | m == v - 1 |
// | init     | accept | O | m == v     |
// | accept   | accept | O | m == v - 1 |
// --------------------------------------
//
// * 'm' is last majority voteproof
// * 'v' is draw voteproof, new incoming voteproof for next round
func (l LastVoteproofs) PreviousBlockForNextRound(vp base.Voteproof) util.Hash {
	if l.mvp == nil {
		return nil
	}

	if _, ok := vp.(isaac.WithdrawVoteproof); !ok {
		if _, ok := vp.(isaac.StuckVoteproof); !ok {
			if vp.Result() != base.VoteResultDraw {
				return nil
			}
		}
	}

	switch l.mvp.Point().Stage() { //nolint:exhaustive //...
	case base.StageINIT:
		if l.mvp.Point().Height() != vp.Point().Height() {
			return nil
		}

		return l.mvp.Majority().(base.INITBallotFact).PreviousBlock() //nolint:forcetypeassert //...
	case base.StageACCEPT:
		if l.mvp.Point().Height() != vp.Point().Height()-1 {
			return nil
		}

		return l.mvp.Majority().(base.ACCEPTBallotFact).NewBlock() //nolint:forcetypeassert //...
	}

	return nil
}

func (l LastVoteproofs) ACCEPT() base.ACCEPTVoteproof {
	return l.avp
}

func (l LastVoteproofs) IsNew(vp base.Voteproof) bool {
	lvp := l.Cap()
	if lvp == nil {
		return true
	}

	return isNewBallotVoteproof(vp, lvp.Point(), lvp.Result() == base.VoteResultMajority)
}

func findLastVoteproofs(ivp, avp base.Voteproof) base.Voteproof {
	switch {
	case ivp == nil:
		return avp
	case avp == nil:
		return ivp
	}

	switch c := avp.Point().Point.Compare(ivp.Point().Point); {
	case c < 0:
		return ivp
	default:
		return avp
	}
}
