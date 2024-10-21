package isaac

import (
	"sync"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

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

func (l LastVoteproofs) Majority() base.Voteproof {
	return l.mvp
}

func (l LastVoteproofs) IsNew(vp base.Voteproof) bool {
	lvp := l.Cap()
	if lvp == nil {
		return true
	}

	last, err := NewLastPointFromVoteproof(lvp)
	if err != nil {
		return false
	}

	return IsNewVoteproof(last, vp)
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

type LastVoteproofsHandler struct {
	cache util.GCache[string, LastVoteproofs]
	last  LastVoteproofs
	l     sync.RWMutex
}

func NewLastVoteproofsHandler() *LastVoteproofsHandler {
	return &LastVoteproofsHandler{
		cache: util.NewLRUGCache[string, LastVoteproofs](1 << 3), //nolint:mnd // keep last 8 voteproofs
	}
}

func (l *LastVoteproofsHandler) Last() LastVoteproofs {
	l.l.RLock()
	defer l.l.RUnlock()

	return l.last
}

func (l *LastVoteproofsHandler) Voteproofs(point base.StagePoint) (LastVoteproofs, bool) {
	return l.cache.Get(point.String())
}

func (l *LastVoteproofsHandler) IsNew(vp base.Voteproof) bool {
	l.l.RLock()
	defer l.l.RUnlock()

	lvp := l.last.Cap()
	if lvp == nil {
		return true
	}

	lp, err := NewLastPointFromVoteproof(lvp)
	if err != nil {
		return false
	}

	return IsNewVoteproof(lp, vp)
}

func (l *LastVoteproofsHandler) Set(vp base.Voteproof) bool {
	l.l.Lock()
	defer l.l.Unlock()

	lvps := l.last

	if lvp := lvps.Cap(); lvp != nil {
		lp, err := NewLastPointFromVoteproof(lvp)
		if err != nil {
			return false
		}

		if !IsNewVoteproof(lp, vp) {
			return l.fillMissing(lvp, vp)
		}
	}

	switch vp.Point().Stage() { //nolint:exhaustive //...
	case base.StageINIT:
		l.last.ivp = vp.(base.INITVoteproof) //nolint:forcetypeassert //...
	case base.StageACCEPT:
		l.last.avp = vp.(base.ACCEPTVoteproof) //nolint:forcetypeassert //...
	default:
		return false
	}

	if vp.Result() == base.VoteResultMajority {
		l.last.mvp = vp
	}

	if lvps.Cap() != nil {
		l.cache.Set(vp.Point().String(), lvps, 0)
	}

	return true
}

func (l *LastVoteproofsHandler) ForceSetLast(vp base.Voteproof) bool {
	l.l.Lock()
	defer l.l.Unlock()

	lvps := l.last

	switch vp.Point().Stage() { //nolint:exhaustive //...
	case base.StageINIT:
		l.last.ivp = vp.(base.INITVoteproof) //nolint:forcetypeassert //...

		if l.last.avp != nil && l.last.avp.Point().Point.Compare(vp.Point().Point) >= 0 {
			l.last.avp = nil
			l.last.mvp = nil
		}
	case base.StageACCEPT:
		l.last.avp = vp.(base.ACCEPTVoteproof) //nolint:forcetypeassert //...

		if l.last.ivp != nil && l.last.ivp.Point().Point.Compare(vp.Point().Point) > 0 {
			l.last.ivp = nil
			l.last.mvp = nil
		}
	}

	if vp.Result() == base.VoteResultMajority {
		l.last.mvp = vp
	}

	if lvps.Cap() != nil {
		l.cache.Set(vp.Point().String(), lvps, 0)
	}

	return true
}

func (l *LastVoteproofsHandler) fillMissing(lvp, vp base.Voteproof) bool {
	var cached LastVoteproofs

	switch i, found := l.cache.Get(lvp.Point().String()); {
	case !found:
	default:
		cached = i
	}

	var filled bool

	lp := lvp.Point()

	switch {
	case cached.ivp != nil:
	case lp.Stage() != base.StageACCEPT:
	case vp.Point().Stage() != base.StageINIT:
	case !lp.Point.Equal(vp.Point().Point):
	default:
		filled = true
		cached.ivp = vp.(base.INITVoteproof) //nolint:forcetypeassert //...

		if l.last.ivp == nil {
			l.last.ivp = cached.ivp
		}
	}

	switch {
	case cached.avp != nil:
	case lp.Stage() != base.StageINIT:
	case vp.Point().Stage() != base.StageACCEPT:
	case lp.Height() != vp.Point().Height()+1:
	default:
		filled = true
		cached.avp = vp.(base.ACCEPTVoteproof) //nolint:forcetypeassert //...

		if l.last.avp == nil {
			l.last.avp = cached.avp
		}
	}

	if !filled {
		return false
	}

	if cached.mvp == nil && vp.Result() == base.VoteResultMajority {
		cached.mvp = vp
	}

	l.cache.Set(lvp.Point().String(), cached, 0)

	return true
}
