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
	isvalidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error
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
		isvalidVoteproofWithSuffrage: base.IsValidVoteproofWithSuffrage,
	}
}

func (box *Ballotbox) Vote(bl base.Ballot) error {
	box.vrsLock.Lock()
	defer box.vrsLock.Unlock()

	return box.vote(bl, nil)
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

func (box *Ballotbox) filterNewBallot(bl base.Ballot) (bool, error) {
	e := util.StringErrorFunc("failed to vote")

	if !bl.Point().Stage().CanVote() {
		return false, e(nil, "unvotable ballot, %q", bl.Point().Stage())
	}

	if lsp := box.lastStagePoint(); !lsp.IsZero() && bl.Point().Compare(lsp) < 1 {
		return false, e(nil, "old ballot; ignore")
	}

	isvalidBallotWithSuffrage := base.IsValidBallotWithSuffrage
	if box.isvalidVoteproofWithSuffrage == nil {
		isvalidBallotWithSuffrage = func(base.Ballot, func(base.Height) base.Suffrage) (bool, error) {
			return true, nil
		}
	}

	validated, err := isvalidBallotWithSuffrage(bl, box.suf)
	if err != nil {
		return false, e(err, "")
	}

	return validated, nil
}

func (box *Ballotbox) vote(bl base.Ballot, wait chan base.Voteproof) error {
	e := util.StringErrorFunc("failed to vote")

	validated, err := box.filterNewBallot(bl)
	if err != nil {
		return e(err, "")
	}

	vr := box.newVoterecords(bl)
	if !vr.vote(bl, validated) {
		return nil
	}

	switch vps, err := digVoteproofsFromBallot(
		bl,
		box.suf,
		box.threshold,
		box.lastStagePoint(),
		box.isvalidVoteproofWithSuffrage,
		validated,
	); {
	case err != nil:
		return e(err, "")
	default:
		for i := range vps {
			box.vpch <- vps[i]
		}
	}

	go func() {
		vp := box.countWithVoterecords(vr, bl.Point())
		if wait != nil {
			wait <- vp
		}
	}()

	return nil
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

	vr := newVoterecords(stagepoint, box.isvalidVoteproofWithSuffrage)
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

	vp := vr.count(box.suf, box.threshold)
	switch {
	case vp == nil:
		return nil
	case !box.setLastStagePoint(vp.Point()):
		return nil
	default:
		box.vpch <- vp

		return vp
	}
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
	validated                    map[string]struct{}
	isvalidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error
}

func newVoterecords(
	stagepoint base.StagePoint,
	isvalidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error,
) *voterecords {
	vr := voterecordsPool.Get().(*voterecords)
	vr.sp = stagepoint
	vr.voted = map[string]base.Ballot{}
	vr.m = map[string]base.BallotFact{}
	vr.set = nil
	vr.sfs = nil
	vr.nodes = map[string]struct{}{}
	vr.f = false
	vr.validated = map[string]struct{}{}
	vr.isvalidVoteproofWithSuffrage = isvalidVoteproofWithSuffrage

	return vr
}

func (vr *voterecords) vote(bl base.Ballot, validated bool) bool {
	vr.Lock()
	defer vr.Unlock()

	node := bl.SignedFact().Node().String()
	_, found := vr.nodes[node]
	if found {
		return false
	}

	vr.voted[node] = bl
	vr.nodes[node] = struct{}{}
	if validated {
		vr.validated[node] = struct{}{}
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

func (vr *voterecords) count(suff func(base.Height) base.Suffrage, threshold base.Threshold) base.Voteproof {
	vr.Lock()
	defer vr.Unlock()

	// BLOCK produce voteproofs from ballot

	// NOTE if finished, return nil
	switch {
	case vr.f:
		return nil
	case len(vr.voted) < 1:
		return nil
	}

	suf := suff(vr.sp.Height())
	if suf == nil {
		return nil
	}

	isvalidBallotWithSuffrage := base.IsValidBallotWithSuffrage
	if vr.isvalidVoteproofWithSuffrage == nil {
		isvalidBallotWithSuffrage = func(base.Ballot, func(base.Height) base.Suffrage) (bool, error) {
			return true, nil
		}
	}

	var allsfs []base.BallotSignedFact
	for i := range vr.voted {
		bl := vr.voted[i]
		sf := bl.SignedFact() // nolint:typecheck

		if !suf.Exists(sf.Node()) {
			continue
		}

		if _, found := vr.validated[sf.Node().String()]; !found {
			if _, err := isvalidBallotWithSuffrage(bl, suff); err != nil {
				continue
			}

			vr.validated[sf.Node().String()] = struct{}{}
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
	vr.isvalidVoteproofWithSuffrage = nil

	voterecordsPool.Put(vr)
}

func digVoteproofsFromBallot(
	bl base.Ballot,
	suff func(base.Height) base.Suffrage,
	threshold base.Threshold,
	lastStagePoint base.StagePoint,
	isvalidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error,
	alreadyValidated bool,
) ([]base.Voteproof, error) {
	e := util.StringErrorFunc("invalid voteproofs in ballot")

	var vps []base.Voteproof

	avp := bl.ACCEPTVoteproof()
	suf := suff(avp.Point().Height())
	if suf == nil {
		return nil, nil
	}

	switch validated, err := digVoteproofFromBallot(
		avp,
		suff,
		threshold,
		lastStagePoint,
		isvalidVoteproofWithSuffrage,
		alreadyValidated,
	); {
	case err != nil:
		return nil, e(err, "")
	case validated:
		vps = append(vps, bl.ACCEPTVoteproof())
	}

	switch validated, err := digVoteproofFromBallot(
		bl.INITVoteproof(),
		suff,
		threshold,
		lastStagePoint,
		isvalidVoteproofWithSuffrage,
		alreadyValidated,
	); {
	case err != nil:
		return nil, e(err, "")
	case validated:
		vps = append(vps, bl.INITVoteproof())
	}

	return vps, nil
}

func digVoteproofFromBallot(
	vp base.Voteproof,
	suff func(base.Height) base.Suffrage,
	threshold base.Threshold,
	lastStagePoint base.StagePoint,
	isvalidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error,
	alreadyValidated bool,
) (bool, error) {
	if vp == nil {
		return false, nil
	}

	e := util.StringErrorFunc("invalid voteproof in ballot; %q", vp.Point().Stage())

	switch {
	case vp.Point().Compare(lastStagePoint) <= 0:
		return false, nil
	case vp.Threshold() < threshold:
		return false, nil
	}

	if alreadyValidated {
		return true, nil
	}

	suf := suff(vp.Point().Height())
	if suf == nil {
		return false, nil
	}

	if isvalidVoteproofWithSuffrage != nil {
		if err := isvalidVoteproofWithSuffrage(vp, suf); err != nil {
			return false, e(err, "")
		}
	}

	return true, nil
}
