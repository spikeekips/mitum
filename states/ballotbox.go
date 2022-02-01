package states

import (
	"context"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/semaphore"
)

type Ballotbox struct {
	suf       func(base.Height) base.Suffrage
	threshold base.Threshold
	vrsLock   sync.RWMutex
	vrs       map[base.StagePoint]*voterecords
	vpch      chan base.Voteproof
	lsp       *util.Locked
}

func NewBallotbox(
	suf func(base.Height) base.Suffrage,
	threshold base.Threshold,
) *Ballotbox {
	return &Ballotbox{
		suf:       suf,
		threshold: threshold,
		vrs:       map[base.StagePoint]*voterecords{},
		vpch:      make(chan base.Voteproof),
		lsp:       util.NewLocked(base.ZeroStagePoint()),
	}
}

func (box *Ballotbox) Vote(sf base.BallotSignedFact) error {
	e := util.StringErrorFunc("failed to vote")

	fact := sf.Fact().(base.BallotFact)
	if !fact.Stage().CanVote() {
		return e(nil, "unvotable ballot, %q", fact.Stage())
	}

	if !box.filterNewBallot(sf) {
		return e(nil, "old ballot")
	}

	point := fact.Point()
	if suf := box.suf(point.Height()); suf != nil && !suf.Exists(sf.Node()) {
		return e(util.NotFoundError.Errorf("ballot not in suffrage from %q", sf.Node()), "")
	}

	box.vote(sf)

	return nil
}

func (box *Ballotbox) vote(sf base.BallotSignedFact) {
	box.vrsLock.Lock()
	defer box.vrsLock.Unlock()

	fact := sf.Fact().(base.BallotFact)

	stagepoint := base.NewStagePoint(fact.Point(), fact.Stage())
	vr, found := box.vrs[stagepoint]
	if !found {
		vr = newVoterecords(stagepoint)
		box.vrs[stagepoint] = vr
	}

	if vr.vote(sf) {
		go func() {
			box.vrsLock.Lock()
			defer box.vrsLock.Unlock()

			box.count(stagepoint, vr)
		}()
	}
}

func (box *Ballotbox) filterNewBallot(sf base.BallotSignedFact) bool {
	lsp := box.lastStagePoint()
	if lsp.IsZero() {
		return true
	}

	fact := sf.Fact().(base.BallotFact)
	return base.NewStagePoint(fact.Point(), fact.Stage()).Compare(lsp) > 0
}

func (box *Ballotbox) Voteproof() <-chan base.Voteproof {
	return box.vpch
}

// Count triggers to count voterecords without incoming ballots.
func (box *Ballotbox) Count() {
	box.vrsLock.Lock()
	defer box.vrsLock.Unlock()

	vrs := box.notFinishedVoterecords()
	if len(vrs) < 1 {
		return
	}

	ctx := context.Background()
	sem := semaphore.NewWeighted(100)

	for i := range vrs {
		if err := sem.Acquire(ctx, 1); err != nil {
			break
		}

		vr := vrs[i]
		stagepoint := vr.stagepoint()
		go func() {
			defer sem.Release(1)

			box.count(stagepoint, vr)
		}()
	}

	if err := sem.Acquire(ctx, 100); err != nil {
		return
	}
}

func (box *Ballotbox) CountByStagePoint(stagepoint base.StagePoint) {
	box.vrsLock.Lock()
	defer box.vrsLock.Unlock()

	vr := box.voterecords(stagepoint)
	if vr == nil {
		return
	}

	box.count(stagepoint, vr)
}

func (box *Ballotbox) CountByPoint(point base.Point) {
	box.CountByStagePoint(base.NewStagePoint(point, base.StageACCEPT))
	box.CountByStagePoint(base.NewStagePoint(point, base.StageINIT))
}

func (box *Ballotbox) count(stagepoint base.StagePoint, vr *voterecords) {
	if stagepoint.IsZero() {
		return
	}

	if _, found := box.vrs[stagepoint]; !found {
		return
	}

	lsp := box.lastStagePoint()
	if !lsp.IsZero() && stagepoint.Compare(lsp) < 1 {
		return
	}

	suf := box.suf(stagepoint.Height())
	if suf == nil {
		return
	}

	vp := vr.count(suf, box.threshold)
	if vp == nil {
		return
	}

	if box.setLastStagePoint(vp) && !lsp.IsZero() {
		go func() {
			box.clean(lsp.Decrease())
		}()
	}

	go func() {
		box.vpch <- vp
	}()
}

func (box *Ballotbox) clean(stagepoint base.StagePoint) {
	box.vrsLock.Lock()
	defer box.vrsLock.Unlock()

	vrs := box.filterVoterecords(func(vr *voterecords) (bool, bool) {
		return true, vr.stagepoint().Compare(stagepoint) > 0
	})

	for i := range vrs {
		vr := vrs[i]

		delete(box.vrs, vr.stagepoint())
		voterecordsPoolPut(vr)
	}
}

// notFinishedVoterecords sorts higher point will be counted first
func (box *Ballotbox) notFinishedVoterecords() []*voterecords {
	lsp := box.lastStagePoint()

	vrs := box.filterVoterecords(func(vr *voterecords) (bool, bool) {
		switch {
		case vr.voteproof() != nil: // NOTE already finished
			return true, false
		case !lsp.IsZero() && vr.stagepoint().Compare(lsp) < 1:
			return true, false
		default:
			return true, true
		}
	})

	if len(vrs) < 2 {
		return vrs
	}

	sort.Slice(vrs, func(i, j int) bool {
		return vrs[i].stagepoint().Compare(vrs[j].stagepoint()) > 0
	})

	return vrs
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

func (box *Ballotbox) voterecords(stagepoint base.StagePoint) *voterecords {
	vr, found := box.vrs[stagepoint]
	if !found {
		return nil
	}

	return vr
}

func (box *Ballotbox) lastStagePoint() base.StagePoint {
	return box.lsp.Value().(base.StagePoint)
}

func (box *Ballotbox) setLastStagePoint(vp base.Voteproof) bool {
	err := box.lsp.Set(func(i interface{}) (interface{}, error) {
		lsp := i.(base.StagePoint)

		b := base.NewStagePoint(vp.Point(), vp.Stage())
		if lsp.Compare(b) >= 0 {
			return nil, errors.Errorf("not higher")
		}

		return b, nil
	})

	return err == nil
}

type voterecords struct {
	sync.RWMutex
	sp    base.StagePoint
	voted map[string]base.BallotSignedFact
	set   []string
	m     map[string]base.BallotFact
	sfs   []base.BallotSignedFact
	vp    base.Voteproof
}

func newVoterecords(stagepoint base.StagePoint) *voterecords {
	vr := voterecordsPool.Get().(*voterecords)
	vr.sp = stagepoint
	vr.voted = map[string]base.BallotSignedFact{}
	vr.m = map[string]base.BallotFact{}
	vr.set = nil
	vr.sfs = nil
	vr.vp = nil

	return vr
}

func (vr *voterecords) stagepoint() base.StagePoint {
	vr.RLock()
	defer vr.RUnlock()

	return vr.sp
}

func (vr *voterecords) voteproof() base.Voteproof {
	vr.RLock()
	defer vr.RUnlock()

	return vr.vp
}

func (vr *voterecords) vote(sf base.BallotSignedFact) bool {
	vr.Lock()
	defer vr.Unlock()

	_, found := vr.voted[sf.Node().String()]
	if found {
		return false
	}

	vr.voted[sf.Node().String()] = sf

	return true
}

func (vr *voterecords) count(suf base.Suffrage, threshold base.Threshold) base.Voteproof {
	vr.Lock()
	defer vr.Unlock()

	// NOTE if finished, return nil
	if vr.vp != nil {
		return nil
	}

	if len(vr.voted) < 1 {
		return nil
	}

	var allsfs []base.BallotSignedFact
	for i := range vr.voted {
		v := vr.voted[i]
		if !suf.Exists(v.Node()) {
			continue
		}

		allsfs = append(allsfs, v)
	}

	set, sfs, m := base.CountBallotSignedFacts(allsfs)

	vr.voted = map[string]base.BallotSignedFact{}

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

	vp := vr.newVoteproof(threshold, result, vr.sfs, majority)
	vr.vp = vp

	return vp
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
		vp.Finish()

		return vp
	case base.StageACCEPT:
		vp := NewACCEPTVoteproof(vr.sp.Point)
		vp.SetResult(result)
		vp.SetSignedFacts(sfs)
		vp.SetMajority(majority)
		vp.SetThreshold(threshold)
		vp.Finish()

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

	vr.sp = base.ZeroStagePoint()
	vr.voted = nil
	vr.set = nil
	vr.m = nil
	vr.sfs = nil
	vr.vp = nil

	voterecordsPool.Put(vr)
}
