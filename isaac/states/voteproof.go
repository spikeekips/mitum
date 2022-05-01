package isaacstates

import (
	"sync"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type LastVoteproofsHandler struct {
	sync.RWMutex
	ivp base.INITVoteproof
	avp base.ACCEPTVoteproof
	mvp base.Voteproof
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

	if lvp := findLastVoteproofs(l.ivp, l.avp); lvp != nil && vp.Point().Compare(lvp.Point()) < 1 {
		return false
	}

	return true
}

func (l *LastVoteproofsHandler) Set(vp base.Voteproof) bool {
	l.Lock()
	defer l.Unlock()

	if lvp := findLastVoteproofs(l.ivp, l.avp); lvp != nil && vp.Point().Compare(lvp.Point()) < 1 {
		return false
	}

	switch vp.Point().Stage() {
	case base.StageINIT:
		l.ivp = vp.(base.INITVoteproof)
	case base.StageACCEPT:
		l.avp = vp.(base.ACCEPTVoteproof)
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
	switch {
	case l.mvp == nil:
		return nil
	case vp.Result() != base.VoteResultDraw:
		return nil
	}

	switch l.mvp.Point().Stage() {
	case base.StageINIT:
		if l.mvp.Point().Height() != vp.Point().Height() {
			return nil
		}

		return l.mvp.Majority().(base.INITBallotFact).PreviousBlock()
	case base.StageACCEPT:
		if l.mvp.Point().Height() != vp.Point().Height()-1 {
			return nil
		}

		return l.mvp.Majority().(base.ACCEPTBallotFact).NewBlock()
	}

	return nil
}

func (l LastVoteproofs) ACCEPT() base.ACCEPTVoteproof {
	return l.avp
}

func (l LastVoteproofs) IsNew(vp base.Voteproof) bool {
	if lvp := l.Cap(); lvp != nil && vp.Point().Compare(lvp.Point()) < 1 {
		return false
	}

	return true
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
