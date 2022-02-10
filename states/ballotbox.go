package states

import (
	"math"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type Ballotbox struct {
	suf                          func(base.Height) base.Suffrage
	threshold                    base.Threshold
	vrsLock                      sync.RWMutex
	vrs                          map[base.StagePoint]*voterecords
	vpch                         chan base.Voteproof
	lsp                          *util.Locked // stagepoint of last voteproof
	countLock                    sync.Mutex
	removed                      []*voterecords
	isValidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error
}

func NewBallotbox(
	suf func(base.Height) base.Suffrage,
	threshold base.Threshold,
) *Ballotbox {
	return &Ballotbox{
		suf:                          suf,
		threshold:                    threshold,
		vrs:                          map[base.StagePoint]*voterecords{},
		vpch:                         make(chan base.Voteproof, math.MaxUint16),
		lsp:                          util.NewLocked(base.ZeroStagePoint),
		isValidVoteproofWithSuffrage: base.IsValidVoteproofWithSuffrage,
	}
}

func (box *Ballotbox) Vote(bl base.Ballot) error {
	box.vrsLock.Lock()
	defer box.vrsLock.Unlock()

	callback, err := box.vote(bl)
	if err != nil {
		return err
	}

	if callback != nil {
		go func() {
			_ = callback()
		}()
	}

	return nil
}

func (box *Ballotbox) Voteproof() <-chan base.Voteproof {
	return box.vpch
}

func (box *Ballotbox) Count() {
	box.countLock.Lock()
	defer box.countLock.Unlock()

	rvrs, nvrs := box.notFinishedVoterecords()

	for i := range rvrs {
		vr := rvrs[len(rvrs)-i-1]
		if vp := box.count(vr, vr.stagepoint()); vp != nil {
			break
		}
	}

	for i := range nvrs {
		vr := nvrs[i]
		stagepoint := vr.stagepoint()
		if suf := box.suf(stagepoint.Height()); suf == nil {
			break
		}
		_ = box.count(vr, stagepoint)
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

func (box *Ballotbox) filterNewBallot(bl base.Ballot) error {
	e := util.StringErrorFunc("failed to vote")

	if !bl.Point().Stage().CanVote() {
		return e(nil, "unvotable ballot, %q", bl.Point().Stage())
	}

	if lsp := box.lastStagePoint(); !lsp.IsZero() && bl.Point().Compare(lsp) < 1 {
		return e(nil, "old ballot; ignore")
	}

	return nil
}

func (box *Ballotbox) vote(bl base.Ballot) (func() base.Voteproof, error) {
	e := util.StringErrorFunc("failed to vote")

	if err := box.filterNewBallot(bl); err != nil {
		return nil, e(err, "")
	}

	vr := box.newVoterecords(bl)

	var vps []base.Voteproof
	av, iv, err := isValidBallotWithSuffrage(bl, box.suf, box.isValidVoteproofWithSuffrage)
	switch {
	case err != nil:
		return nil, e(err, "")
	case !vr.vote(bl, av, iv):
		return nil, nil
	default:
		vps = digVoteproofsFromBallot(bl, av, iv, box.lastStagePoint(), box.threshold)
	}

	return func() base.Voteproof {
		for i := range vps {
			box.vpch <- vps[i]
		}

		return box.countWithVoterecords(vr, bl.Point())
	}, nil
}

func (box *Ballotbox) voterecords(stagepoint base.StagePoint) *voterecords {
	box.vrsLock.RLock()
	defer box.vrsLock.RUnlock()

	vr, found := box.vrs[stagepoint]
	if !found {
		return nil
	}

	return vr
}

func (box *Ballotbox) newVoterecords(bl base.Ballot) *voterecords {
	stagepoint := bl.Point()
	if vr, found := box.vrs[stagepoint]; found {
		return vr
	}

	vr := newVoterecords(stagepoint, box.isValidVoteproofWithSuffrage)
	box.vrs[stagepoint] = vr

	return vr
}

func (box *Ballotbox) lastStagePoint() base.StagePoint {
	return box.lsp.Value().(base.StagePoint)
}

func (box *Ballotbox) setLastStagePoint(p base.StagePoint) bool {
	err := box.lsp.Set(func(i interface{}) (interface{}, error) {
		lsp := i.(base.StagePoint)

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

	if vr.stagepoint().Compare(stagepoint) != 0 {
		return nil
	}

	lsp := box.lastStagePoint()
	if !lsp.IsZero() && stagepoint.Compare(lsp) < 1 {
		return nil
	}

	vps := vr.count(box.suf, box.threshold, box.lastStagePoint())
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
		return true, stagepoint.Compare(vr.stagepoint()) >= 0
	})

	box.removed = make([]*voterecords, len(vrs))
	for i := range vrs {
		vr := vrs[i]

		delete(box.vrs, vr.stagepoint())
		box.removed[i] = vr
	}
}

func (box *Ballotbox) filterVoterecords(filter func(*voterecords) (bool, bool)) []*voterecords {
	var vrs []*voterecords
	for stagepoint := range box.vrs {
		vr := box.vrs[stagepoint]
		keep, ok := filter(vr)
		if ok {
			vrs = append(vrs, vr)
		}

		if !keep {
			break
		}
	}

	return vrs
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
		case !lsp.IsZero() && vr.stagepoint().Compare(lsp) < 1:
			return true, false
		default:
			return true, true
		}
	})

	if len(vrs) < 2 {
		return nil, vrs
	}

	sort.Slice(vrs, func(i, j int) bool {
		return vrs[i].stagepoint().Compare(vrs[j].stagepoint()) < 0
	})

	last := vrs[len(vrs)-1]
	p := last.stagepoint()

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
		stagepoint := vr.stagepoint()
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
	sync.RWMutex
	sp                           base.StagePoint
	voted                        map[string]base.Ballot
	set                          []string
	m                            map[string]base.BallotFact
	sfs                          []base.BallotSignedFact
	nodes                        map[string]struct{}
	f                            bool
	validated                    map[string][2]bool
	isValidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error
	ballots                      []base.Ballot
	voteproofsPassed             [2]bool
}

func newVoterecords(
	stagepoint base.StagePoint,
	isValidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error,
) *voterecords {
	vr := voterecordsPool.Get().(*voterecords)
	vr.sp = stagepoint
	vr.voted = map[string]base.Ballot{}
	vr.m = map[string]base.BallotFact{}
	vr.set = nil
	vr.sfs = nil
	vr.nodes = map[string]struct{}{}
	vr.f = false
	vr.validated = map[string][2]bool{}
	vr.isValidVoteproofWithSuffrage = isValidVoteproofWithSuffrage
	vr.ballots = nil
	vr.voteproofsPassed = [2]bool{}

	return vr
}

func (vr *voterecords) vote(bl base.Ballot, av, iv bool) bool {
	vr.Lock()
	defer vr.Unlock()

	node := bl.SignedFact().Node().String()
	_, found := vr.nodes[node]
	if found {
		return false
	}

	vr.voted[node] = bl
	vr.nodes[node] = struct{}{}
	vr.validated[node] = [2]bool{av, iv}
	switch {
	case av && iv:
		vr.voteproofsPassed = [2]bool{av, iv}
	case av:
		vr.voteproofsPassed[1] = av
		vr.ballots = append(vr.ballots, bl)
	default:
		vr.ballots = append(vr.ballots, bl)
	}

	return true
}

func (vr *voterecords) stagepoint() base.StagePoint {
	vr.RLock()
	defer vr.RUnlock()

	return vr.sp
}

func (vr *voterecords) finished() bool {
	vr.RLock()
	defer vr.RUnlock()

	return vr.f
}

func (vr *voterecords) count(
	getSuffrage func(base.Height) base.Suffrage,
	threshold base.Threshold,
	lastStagePoint base.StagePoint,
) []base.Voteproof {
	vr.Lock()
	defer vr.Unlock()

	vps := vr.digVoteproofsFromBallots(getSuffrage, threshold, lastStagePoint)

	vp := vr.countFromBallots(getSuffrage, threshold, lastStagePoint)
	if vp != nil {
		vps = append(vps, vp)
	}

	return vps
}

func (vr *voterecords) newVoteproof(
	threshold base.Threshold,
	result base.VoteResult,
	sfs []base.BallotSignedFact,
	majority base.BallotFact,
) base.Voteproof {
	switch vr.sp.Stage() {
	case base.StageINIT:
		vp := NewINITVoteproof(vr.sp.Point)
		vp.SetResult(result)
		vp.SetSignedFacts(sfs)
		vp.SetMajority(majority)
		vp.SetThreshold(threshold)
		vp.finish()

		return vp
	case base.StageACCEPT:
		vp := NewACCEPTVoteproof(vr.sp.Point)
		vp.SetResult(result)
		vp.SetSignedFacts(sfs)
		vp.SetMajority(majority)
		vp.SetThreshold(threshold)
		vp.finish()

		return vp
	default:
		panic("unknown stage found to create voteproof")
	}
}

// digVoteproofsFromBallots voteproofs from collected ballots
func (vr *voterecords) digVoteproofsFromBallots(
	getSuffrage func(base.Height) base.Suffrage,
	threshold base.Threshold,
	lastStagePoint base.StagePoint,
) []base.Voteproof {
	if vr.voteproofsPassed[1] {
		return nil
	}

	if suf := getSuffrage(vr.sp.Height() - 1); suf == nil {
		return nil
	}

	var vps []base.Voteproof
	var filtered []base.Ballot

end:
	for i := range vr.ballots {
		bl := vr.ballots[i]
		node := bl.SignedFact().Node().String()

		av, iv := vr.voteproofsPassed[0], vr.voteproofsPassed[1]

		var err error
		switch {
		case !av:
			av, iv, err = isValidBallotWithSuffrage(bl, getSuffrage, vr.isValidVoteproofWithSuffrage)
			switch {
			case err != nil:
				delete(vr.voted, node)

				continue end
			case av && !iv:
				filtered = append(filtered, bl)
			case av && iv:
			default:
				return nil
			}
		case !iv:
			iv, err = isValidVoteproofWithSuffrage(bl.INITVoteproof(), getSuffrage, vr.isValidVoteproofWithSuffrage)
			if err != nil {
				delete(vr.voted, node)

				continue end
			}
		}

		vr.validated[node] = [2]bool{av, iv}
		vr.voteproofsPassed = [2]bool{av, iv}
		vps = digVoteproofsFromBallot(bl, av, iv, lastStagePoint, threshold)

		break end
	}

	vr.ballots = filtered

	return vps
}

func (vr *voterecords) countFromBallots(
	getSuffrage func(base.Height) base.Suffrage,
	threshold base.Threshold,
	lastStagePoint base.StagePoint,
) base.Voteproof {
	// NOTE if finished, return nil
	switch {
	case vr.f:
		return nil
	case len(vr.voted) < 1:
		return nil
	}

	suf := getSuffrage(vr.sp.Height())
	if suf == nil {
		return nil
	}

	var allsfs []base.BallotSignedFact

end:
	for i := range vr.voted {
		bl := vr.voted[i]
		sf := bl.SignedFact() // nolint:typecheck

		if !suf.Exists(sf.Node()) {
			continue
		}

		if v, found := vr.validated[sf.Node().String()]; !found || !v[1] {
			switch av, iv, err := isValidBallotWithSuffrage(bl, getSuffrage, vr.isValidVoteproofWithSuffrage); {
			case err != nil:
				continue end
			default:
				vr.validated[sf.Node().String()] = [2]bool{av, iv}
			}
		}

		allsfs = append(allsfs, sf)
	}

	set, sfs, m := base.CountBallotSignedFacts(allsfs)

	vr.voted = map[string]base.Ballot{}

	if len(set) < 1 {
		return nil
	}

	for i := range m {
		vr.m[i] = m[i]
	}

	vr.set = append(vr.set, set...)
	vr.sfs = append(vr.sfs, sfs...)

	if uint(len(vr.set)) < threshold.Threshold(uint(suf.Len())) {
		return nil
	}

	var majority base.BallotFact
	result, majoritykey := threshold.VoteResult(uint(suf.Len()), vr.set)
	switch result {
	case base.VoteResultDraw:
	case base.VoteResultMajority:
		majority = vr.m[majoritykey]
	default:
		return nil
	}

	vr.f = true

	return vr.newVoteproof(threshold, result, vr.sfs, majority)
}

var voterecordsPool = sync.Pool{
	New: func() interface{} {
		return new(voterecords)
	},
}

var voterecordsPoolPut = func(vr *voterecords) {
	vr.Lock()
	defer vr.Unlock()

	vr.sp = base.ZeroStagePoint
	vr.voted = nil
	vr.set = nil
	vr.m = nil
	vr.sfs = nil
	vr.nodes = nil
	vr.f = false
	vr.validated = nil
	vr.isValidVoteproofWithSuffrage = nil
	vr.ballots = nil
	vr.voteproofsPassed = [2]bool{}

	voterecordsPool.Put(vr)
}

func digVoteproofFromBallot( // BLOCK rename to digVoteproofFromBallot
	vp base.Voteproof,
	lastStagePoint base.StagePoint,
	threshold base.Threshold,
) bool {
	switch {
	case vp == nil:
		return false
	case vp.Point().Compare(lastStagePoint) <= 0:
		return false
	case vp.Threshold() < threshold:
		return false
	default:
		return true
	}
}

func digVoteproofsFromBallot(
	bl base.Ballot,
	av, iv bool,
	lastStagePoint base.StagePoint,
	threshold base.Threshold,
) []base.Voteproof {
	var vps []base.Voteproof
	if av {
		vp := bl.ACCEPTVoteproof()
		if digVoteproofFromBallot(vp, lastStagePoint, threshold) {
			vps = append(vps, vp)
		}
	}

	if iv {
		vp := bl.INITVoteproof()
		if digVoteproofFromBallot(vp, lastStagePoint, threshold) {
			vps = append(vps, vp)
		}
	}

	return vps
}

func isValidBallotWithSuffrage(
	bl base.Ballot,
	getSuffrage func(base.Height) base.Suffrage,
	checkValid func(base.Voteproof, base.Suffrage) error,
) (
	bool, /* accept voteproof passed */
	bool, /* init voteproof passed */
	error,
) {
	e := util.StringErrorFunc("invalid signed facts in ballot with suffrage")

	switch suf := getSuffrage(bl.Point().Height()); {
	case suf == nil:
		return false, false, nil
	case !suf.Exists(bl.SignedFact().Node()):
		return false, false, e(util.InvalidError.Errorf("ballot not in suffrage"), "")
	}

	switch v, err := isValidVoteproofWithSuffrage(bl.ACCEPTVoteproof(), getSuffrage, checkValid); {
	case err != nil:
		return false, false, e(err, "")
	case !v:
		return false, false, nil
	}

	switch v, err := isValidVoteproofWithSuffrage(bl.INITVoteproof(), getSuffrage, checkValid); {
	case err != nil:
		return true, false, e(err, "")
	case !v:
		return true, false, nil
	}

	return true, true, nil
}

func isValidVoteproofWithSuffrage(
	vp base.Voteproof,
	getSuffrage func(base.Height) base.Suffrage,
	checkValid func(base.Voteproof, base.Suffrage) error,
) (bool, error) {
	if vp == nil {
		return false, nil
	}

	e := util.StringErrorFunc("invalid signed facts in voteproof with suffrage")

	suf := getSuffrage(vp.Point().Height())
	if suf == nil {
		return false, nil
	}

	if checkValid == nil {
		checkValid = func(base.Voteproof, base.Suffrage) error { return nil }
	}

	if err := checkValid(vp, suf); err != nil {
		return false, e(err, "invalid voteproof")
	}

	return true, nil
}
