package isaacstates

import (
	"math"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type Ballotbox struct {
	getSuffrage                  isaac.GetSuffrageByBlockHeight
	isValidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error
	vrs                          map[string]*voterecords
	vpch                         chan base.Voteproof
	lsp                          *util.Locked
	removed                      []*voterecords
	threshold                    base.Threshold
	vrsLock                      sync.RWMutex
	countLock                    sync.Mutex
}

func NewBallotbox(
	getSuffrage isaac.GetSuffrageByBlockHeight,
	threshold base.Threshold,
) *Ballotbox {
	return &Ballotbox{
		getSuffrage:                  getSuffrage,
		threshold:                    threshold,
		vrs:                          map[string]*voterecords{},
		vpch:                         make(chan base.Voteproof, math.MaxUint16),
		lsp:                          util.NewLocked(base.ZeroStagePoint),
		isValidVoteproofWithSuffrage: base.IsValidVoteproofWithSuffrage,
	}
}

func (box *Ballotbox) Vote(bl base.Ballot) (bool, error) {
	box.vrsLock.Lock()
	defer box.vrsLock.Unlock()

	voted, callback, err := box.vote(bl)
	if err != nil {
		return false, err
	}

	if callback != nil {
		go func() {
			_ = callback()
		}()
	}

	return voted, nil
}

func (box *Ballotbox) Voteproof() <-chan base.Voteproof {
	if box == nil {
		return nil
	}

	return box.vpch
}

// Count should be called for checking next voteproofs after new block saved.
func (box *Ballotbox) Count() {
	box.countLock.Lock()
	defer box.countLock.Unlock()

	rvrs, nvrs := box.notFinishedVoterecords()

	for i := range rvrs {
		vr := rvrs[len(rvrs)-i-1]
		if vp := box.count(vr, vr.stagepoint); vp != nil {
			break
		}
	}

	for i := range nvrs {
		vr := nvrs[i]
		if _, found, err := vr.getSuffrage(); !found || err != nil {
			break
		}

		_ = box.count(vr, vr.stagepoint)
	}

	box.clean()
}

func (box *Ballotbox) countWithVoterecords(vr *voterecords, stagepoint base.StagePoint) base.Voteproof {
	box.countLock.Lock()
	defer box.countLock.Unlock()

	vp := box.count(vr, stagepoint)

	box.clean()

	return vp
}

func (box *Ballotbox) filterNewBallot(bl base.Ballot) bool {
	lsp := box.lastStagePoint()

	return lsp.IsZero() || bl.Point().Compare(lsp) > 0
}

func (box *Ballotbox) vote(bl base.Ballot) (bool, func() base.Voteproof, error) {
	e := util.StringErrorFunc("failed to vote")

	if !box.filterNewBallot(bl) {
		return false, nil, nil
	}

	vr := box.newVoterecords(bl)

	var vp base.Voteproof
	voted, validated, err := vr.vote(bl)

	switch {
	case err != nil:
		return false, nil, e(err, "")
	case voted && validated:
		if ok, found := isNewVoteproof(bl.Voteproof(), box.lastStagePoint(), box.threshold); ok && found {
			vp = bl.Voteproof()
		}
	}

	return voted,
		func() base.Voteproof {
			if vp != nil {
				box.vpch <- vp
			}

			return box.countWithVoterecords(vr, bl.Point())
		},
		nil
}

func (box *Ballotbox) voterecords(stagepoint base.StagePoint) *voterecords {
	box.vrsLock.RLock()
	defer box.vrsLock.RUnlock()

	vr, found := box.vrs[stagepoint.String()]
	if !found {
		return nil
	}

	return vr
}

func (box *Ballotbox) newVoterecords(bl base.Ballot) *voterecords {
	stagepoint := bl.Point()
	if vr, found := box.vrs[stagepoint.String()]; found {
		return vr
	}

	vr := newVoterecords(stagepoint, box.isValidVoteproofWithSuffrage, box.getSuffrage, box.threshold)
	box.vrs[stagepoint.String()] = vr

	return vr
}

func (box *Ballotbox) lastStagePoint() base.StagePoint {
	i, _ := box.lsp.Value()

	return i.(base.StagePoint) //nolint:forcetypeassert //...
}

func (box *Ballotbox) setLastStagePoint(p base.StagePoint) bool {
	_, err := box.lsp.Set(func(i interface{}) (interface{}, error) {
		lsp := i.(base.StagePoint) //nolint:forcetypeassert //...

		if lsp.Compare(p) >= 0 {
			return nil, errors.Errorf("not higher")
		}

		return p, nil
	})

	return err == nil
}

func (box *Ballotbox) count(vr *voterecords, stagepoint base.StagePoint) base.Voteproof {
	if stagepoint.IsZero() {
		return nil
	}

	if i := box.voterecords(stagepoint); i == nil {
		return nil
	}

	if vr.stagepoint.Compare(stagepoint) != 0 {
		return nil
	}

	lsp := box.lastStagePoint()
	if !lsp.IsZero() && stagepoint.Compare(lsp) < 1 {
		return nil
	}

	vps := vr.count(box.lastStagePoint())
	if len(vps) < 1 {
		return nil
	}

	last := vps[len(vps)-1]

	for i := range vps {
		if box.setLastStagePoint(vps[len(vps)-1-i].Point()) {
			break
		}
	}

	for i := range vps {
		box.vpch <- vps[i]
	}

	return last
}

func (box *Ballotbox) clean() {
	box.vrsLock.Lock()
	defer box.vrsLock.Unlock()

	for i := range box.removed {
		voterecordsPoolPut(box.removed[i])
	}

	lsp := box.lastStagePoint()
	stagepoint := lsp.Decrease()

	if lsp.IsZero() || stagepoint.IsZero() {
		return
	}

	vrs := box.filterVoterecords(func(vr *voterecords) (bool, bool) {
		return true, stagepoint.Compare(vr.stagepoint) >= 0
	})

	box.removed = make([]*voterecords, len(vrs))

	for i := range vrs {
		vr := vrs[i]

		delete(box.vrs, vr.stagepoint.String())
		box.removed[i] = vr
	}
}

func (box *Ballotbox) filterVoterecords(filter func(*voterecords) (bool, bool)) []*voterecords {
	vrs := make([]*voterecords, len(box.vrs))

	var n int

	for stagepoint := range box.vrs {
		vr := box.vrs[stagepoint]

		keep, ok := filter(vr)
		if ok {
			vrs[n] = vr
			n++
		}

		if !keep {
			break
		}
	}

	return vrs[:n]
}

// notFinishedVoterecords sorts higher point will be counted first
func (box *Ballotbox) notFinishedVoterecords() ([]*voterecords, []*voterecords) {
	box.vrsLock.RLock()
	defer box.vrsLock.RUnlock()

	lsp := box.lastStagePoint()

	vrs := box.filterVoterecords(func(vr *voterecords) (bool, bool) {
		switch {
		case vr.finished():
			return true, false
		case !lsp.IsZero() && vr.stagepoint.Compare(lsp) < 1:
			return true, false
		default:
			return true, true
		}
	})

	if len(vrs) < 2 { //nolint:gomnd //...
		return nil, vrs
	}

	sort.Slice(vrs, func(i, j int) bool {
		return vrs[i].stagepoint.Compare(vrs[j].stagepoint) < 0
	})

	last := vrs[len(vrs)-1]
	p := last.stagepoint

	switch {
	case p.Stage() == base.StageACCEPT:
		p = p.SetStage(base.StageINIT)
	case p.Stage() == base.StageINIT:
		p = base.NewStagePoint(base.NewPoint(p.Height()-1, base.Round(0)), base.StageACCEPT)
	}

	var rvrs, nvrs []*voterecords

end:
	for i := range vrs {
		vr := vrs[i]
		stagepoint := vr.stagepoint

		switch {
		case stagepoint.Height() == p.Height() && stagepoint.Compare(p.SetStage(base.StageINIT)) != 0:
			rvrs = append(rvrs, vr)

			continue end
		case stagepoint.Compare(p) < 0:
			continue end
		}

		nvrs = append(nvrs, vr)
	}

	return rvrs, nvrs
}

type voterecords struct {
	m                            map[string]base.BallotFact
	ballots                      map[string]base.Ballot
	isValidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error
	getSuffrageFunc              isaac.GetSuffrageByBlockHeight
	nodes                        map[string]struct{} // revive:disable-line:nested-structs
	voted                        map[string]base.Ballot
	suf                          *util.Locked
	set                          []string
	sfs                          []base.BallotSignedFact
	stagepoint                   base.StagePoint
	threshold                    base.Threshold
	sync.RWMutex
	f bool
}

func newVoterecords(
	stagepoint base.StagePoint,
	isValidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error,
	getSuffrageFunc isaac.GetSuffrageByBlockHeight,
	threshold base.Threshold,
) *voterecords {
	vr := voterecordsPool.Get().(*voterecords) //nolint:forcetypeassert //...

	vr.stagepoint = stagepoint
	vr.isValidVoteproofWithSuffrage = isValidVoteproofWithSuffrage
	vr.getSuffrageFunc = getSuffrageFunc
	vr.threshold = threshold
	vr.voted = map[string]base.Ballot{}
	vr.m = map[string]base.BallotFact{}
	vr.set = nil
	vr.sfs = nil
	vr.nodes = map[string]struct{}{}
	vr.f = false
	vr.ballots = map[string]base.Ballot{}
	vr.suf = util.EmptyLocked()

	return vr
}

func (vr *voterecords) vote(bl base.Ballot) (voted bool, validated bool, err error) {
	vr.Lock()
	defer vr.Unlock()

	node := bl.SignedFact().Node().String()
	if _, found := vr.nodes[node]; found {
		return false, false, nil
	}

	if _, found := vr.ballots[node]; found {
		return false, false, nil
	}

	e := util.StringErrorFunc("failed to vote")

	switch _, found, err := vr.getSuffrage(); {
	case err != nil:
		return false, false, e(err, "")
	case !found:
		vr.ballots[node] = bl

		return true, false, nil
	}

	if err := isValidBallotWithSuffrage(bl, vr.getSuffrageFunc, vr.isValidVoteproofWithSuffrage); err != nil {
		return false, false, e(err, "")
	}

	vr.voted[node] = bl
	vr.nodes[node] = struct{}{}

	return true, true, nil
}

func (vr *voterecords) finished() bool {
	vr.RLock()
	defer vr.RUnlock()

	return vr.f
}

func (vr *voterecords) count(lastStagePoint base.StagePoint) []base.Voteproof {
	vr.Lock()
	defer vr.Unlock()

	if len(vr.voted) < 1 {
		if _, found, err := vr.getSuffrage(); err != nil || !found {
			return nil
		}
	}

	var digged base.Voteproof
	var vpchecked bool

	if len(vr.ballots) > 0 {
		for i := range vr.ballots {
			bl := vr.ballots[i]
			if err := isValidBallotWithSuffrage(bl, vr.getSuffrageFunc, vr.isValidVoteproofWithSuffrage); err != nil {
				delete(vr.ballots, i)

				continue
			}

			vr.voted[bl.SignedFact().Node().String()] = bl

			if !vpchecked {
				ok, found := isNewVoteproof(bl.Voteproof(), lastStagePoint, vr.threshold)
				if ok && found {
					digged = bl.Voteproof()
				}

				vpchecked = !ok || found
			}
		}

		vr.ballots = map[string]base.Ballot{}
	}

	var vps []base.Voteproof
	if digged != nil {
		vps = append(vps, digged)
	}

	if vp := vr.countFromBallots(); vp != nil {
		vps = append(vps, vp)
	}

	return vps
}

func (vr *voterecords) countFromBallots() base.Voteproof {
	// NOTE if finished, return nil
	switch {
	case vr.f:
		return nil
	case len(vr.voted) < 1:
		return nil
	}

	var suf base.Suffrage

	switch i, found, err := vr.getSuffrage(); {
	case err != nil, !found:
		return nil
	default:
		suf = i
	}

	collectedsfs := make([]base.BallotSignedFact, len(vr.voted))
	var i int

	for k := range vr.voted {
		collectedsfs[i] = vr.voted[k].SignedFact()
		i++
	}

	set, sfs, m, err := base.CountBallotSignedFacts(collectedsfs)
	if err != nil {
		return nil
	}

	vr.voted = map[string]base.Ballot{}

	if len(set) < 1 {
		return nil
	}

	for i := range m {
		vr.m[i] = m[i]
	}

	vr.set = append(vr.set, set...)
	vr.sfs = append(vr.sfs, sfs...)

	if uint(len(vr.set)) < vr.threshold.Threshold(uint(suf.Len())) {
		return nil
	}

	var majority base.BallotFact
	result, majoritykey := vr.threshold.VoteResult(uint(suf.Len()), vr.set)

	switch result {
	case base.VoteResultDraw:
	case base.VoteResultMajority:
		majority = vr.m[majoritykey]
	default:
		return nil
	}

	vr.f = true

	return vr.newVoteproof(result, vr.sfs, majority)
}

func (vr *voterecords) newVoteproof(
	result base.VoteResult,
	sfs []base.BallotSignedFact,
	majority base.BallotFact,
) base.Voteproof {
	switch vr.stagepoint.Stage() {
	case base.StageINIT:
		vp := isaac.NewINITVoteproof(vr.stagepoint.Point)
		_ = vp.SetResult(result).
			SetSignedFacts(sfs).
			SetMajority(majority).
			SetThreshold(vr.threshold).
			Finish()

		return vp
	case base.StageACCEPT:
		vp := isaac.NewACCEPTVoteproof(vr.stagepoint.Point)
		_ = vp.SetResult(result).
			SetSignedFacts(sfs).
			SetMajority(majority).
			SetThreshold(vr.threshold).
			Finish()

		return vp
	default:
		panic("unknown stage found to create voteproof")
	}
}

func (vr *voterecords) getSuffrage() (base.Suffrage, bool, error) {
	i, err := vr.suf.Set(func(i interface{}) (interface{}, error) {
		if i != nil {
			return i, util.ErrFound.Call()
		}

		switch j, found, err := vr.getSuffrageFunc(vr.stagepoint.Height()); {
		case err != nil:
			return nil, err
		case !found:
			return nil, util.ErrNotFound.Call()
		default:
			return j, util.ErrFound.Call()
		}
	})

	switch {
	case err == nil:
		return nil, false, nil
	case errors.Is(err, util.ErrFound):
		return i.(base.Suffrage), true, nil //nolint:forcetypeassert //...
	case errors.Is(err, util.ErrNotFound):
		return nil, false, nil
	default:
		return nil, false, err
	}
}

var voterecordsPool = sync.Pool{
	New: func() interface{} {
		return new(voterecords)
	},
}

var voterecordsPoolPut = func(vr *voterecords) {
	vr.Lock()
	defer vr.Unlock()

	vr.stagepoint = base.ZeroStagePoint
	vr.isValidVoteproofWithSuffrage = nil
	vr.getSuffrageFunc = nil
	vr.threshold = base.Threshold(-1)
	vr.voted = nil
	vr.set = nil
	vr.m = nil
	vr.sfs = nil
	vr.nodes = nil
	vr.f = false
	vr.ballots = nil
	vr.suf = nil

	voterecordsPool.Put(vr)
}

func isNewVoteproof(
	vp base.Voteproof,
	lastStagePoint base.StagePoint,
	threshold base.Threshold,
) (ok bool, found bool) {
	return vp.Point().Compare(lastStagePoint) > 0, vp.Threshold() >= threshold
}

func isValidBallotWithSuffrage(
	bl base.Ballot,
	getSuffrage isaac.GetSuffrageByBlockHeight,
	checkValid func(base.Voteproof, base.Suffrage) error,
) error {
	e := util.StringErrorFunc("invalid signed facts in ballot with suffrage")

	switch suf, found, err := getSuffrage(bl.Point().Height()); {
	case err != nil:
		return e(util.ErrInvalid.Wrap(err), "")
	case !found:
		return e(util.ErrInvalid.Errorf("suffrage not found"), "")
	case !suf.Exists(bl.SignedFact().Node()):
		return e(util.ErrInvalid.Errorf("ballot not in suffrage"), "")
	case !suf.ExistsPublickey(bl.SignedFact().Node(), bl.SignedFact().Signer()):
		return e(util.ErrInvalid.Errorf("wrong publickey"), "")
	}

	if err := isValidVoteproofWithSuffrage(bl.Voteproof(), getSuffrage, checkValid); err != nil {
		return e(err, "")
	}

	return nil
}

func isValidVoteproofWithSuffrage(
	vp base.Voteproof,
	getSuffrage isaac.GetSuffrageByBlockHeight,
	checkValid func(base.Voteproof, base.Suffrage) error,
) error {
	e := util.StringErrorFunc("invalid signed facts in voteproof with suffrage")

	switch suf, found, err := getSuffrage(vp.Point().Height()); {
	case err != nil:
		return e(util.ErrInvalid.Wrap(err), "")
	case !found:
		return e(util.ErrInvalid.Errorf("empty suffrage"), "")
	default:
		cf := checkValid
		if cf == nil {
			cf = func(base.Voteproof, base.Suffrage) error { return nil }
		}

		if err := cf(vp, suf); err != nil {
			return e(err, "invalid voteproof")
		}

		return nil
	}
}
