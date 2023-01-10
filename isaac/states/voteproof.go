package isaacstates

import (
	"sync"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type LastVoteproofsHandler struct {
	cache *util.GCache[string, LastVoteproofs]
	ivp   base.INITVoteproof
	avp   base.ACCEPTVoteproof
	mvp   base.Voteproof
	sync.RWMutex
}

func NewLastVoteproofsHandler() *LastVoteproofsHandler {
	return &LastVoteproofsHandler{
		cache: util.NewLRUGCache("", LastVoteproofs{}, 1<<3), //nolint:gomnd //...
	}
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

func (l *LastVoteproofsHandler) Voteproofs(point base.StagePoint) (LastVoteproofs, bool) {
	return l.cache.Get(point.String())
}

func (l *LastVoteproofsHandler) IsNew(vp base.Voteproof) bool {
	l.RLock()
	defer l.RUnlock()

	lvp := findLastVoteproofs(l.ivp, l.avp)

	if lvp == nil {
		return true
	}

	return newLastPointFromVoteproof(lvp).isNewVoteproof(vp)
}

func (l *LastVoteproofsHandler) Set(vp base.Voteproof) bool {
	l.Lock()
	defer l.Unlock()

	lvps := LastVoteproofs{
		ivp: l.ivp,
		avp: l.avp,
		mvp: l.mvp,
	}

	if lvp := lvps.Cap(); lvp != nil {
		if !newLastPointFromVoteproof(lvp).isNewVoteproof(vp) {
			return false
		}
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

	if lvps.Cap() != nil {
		l.cache.Set(vp.Point().String(), lvps, 0)
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

	return newLastPointFromVoteproof(lvp).isNewVoteproof(vp)
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
