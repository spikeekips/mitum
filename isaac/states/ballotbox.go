package isaacstates

import (
	"math"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type Ballotbox struct {
	*logging.Logging
	local            base.Address
	getSuffrage      isaac.GetSuffrageByBlockHeight
	isValidVoteproof func(base.Voteproof, base.Suffrage) error
	suffrageVote     func(base.SuffrageWithdrawOperation) error
	newBallot        func(base.Ballot)
	vrs              map[string]*voterecords
	vpch             chan base.Voteproof
	lsp              *util.Locked
	removed          []*voterecords
	vrsLock          sync.RWMutex
	countLock        sync.Mutex
}

func NewBallotbox(
	local base.Address,
	getSuffrage isaac.GetSuffrageByBlockHeight,
	isValidVoteproof func(base.Voteproof, base.Suffrage) error,
) *Ballotbox {
	return &Ballotbox{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "ballotbox")
		}),
		local:            local,
		getSuffrage:      getSuffrage,
		vrs:              map[string]*voterecords{},
		vpch:             make(chan base.Voteproof, math.MaxUint16),
		lsp:              util.NewLocked(base.ZeroStagePoint),
		isValidVoteproof: isValidVoteproof,
		newBallot:        func(base.Ballot) {},
	}
}

func (box *Ballotbox) Vote(bl base.Ballot, threshold base.Threshold) (bool, error) {
	box.vrsLock.Lock()
	defer box.vrsLock.Unlock()

	voted, callback, err := box.vote(bl, threshold)
	if err != nil {
		return false, err
	}

	if callback != nil {
		go func() {
			_ = callback()
		}()
	}

	box.newBallot(bl)

	return voted, nil
}

func (box *Ballotbox) Voteproof() <-chan base.Voteproof {
	if box == nil {
		return nil
	}

	return box.vpch
}

// Count should be called for checking next voteproofs after new block saved.
func (box *Ballotbox) Count(threshold base.Threshold) {
	box.countLock.Lock()
	defer box.countLock.Unlock()

	rvrs, nvrs := box.notFinishedVoterecords()

	for i := range rvrs {
		vr := rvrs[len(rvrs)-i-1]
		if vp := box.count(vr, vr.stagepoint, threshold); vp != nil {
			break
		}
	}

	for i := range nvrs {
		vr := nvrs[i]
		if _, found, err := vr.getSuffrage(); !found || err != nil {
			break
		}

		_ = box.count(vr, vr.stagepoint, threshold)
	}

	box.clean()
}

func (box *Ballotbox) SetSuffrageVote(f func(base.SuffrageWithdrawOperation) error) {
	box.suffrageVote = f
}

func (box *Ballotbox) SetNewBallot(f func(base.Ballot)) {
	box.newBallot = f
}

func (box *Ballotbox) countWithVoterecords(
	vr *voterecords, stagepoint base.StagePoint, threshold base.Threshold,
) base.Voteproof {
	box.countLock.Lock()
	defer box.countLock.Unlock()

	vp := box.count(vr, stagepoint, threshold)

	box.clean()

	return vp
}

func (box *Ballotbox) filterNewBallot(bl base.Ballot) bool {
	lsp := box.lastStagePoint()

	return lsp.IsZero() || bl.Point().Compare(lsp) > 0
}

func (box *Ballotbox) vote(bl base.Ballot, threshold base.Threshold) (bool, func() base.Voteproof, error) {
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
		if isnew, overthreshold := isNewVoteproof(
			box.local,
			bl.Voteproof(),
			box.lastStagePoint(),
			threshold,
		); isnew && overthreshold {
			vp = bl.Voteproof()
		}
	}

	return voted,
		func() base.Voteproof {
			if vp != nil {
				box.vpch <- vp
			}

			return box.countWithVoterecords(vr, bl.Point(), threshold)
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

	vr := newVoterecords(stagepoint, box.isValidVoteproof, box.getSuffrage, box.suffrageVote, box.Logging.Log())
	box.vrs[stagepoint.String()] = vr

	return vr
}

func (box *Ballotbox) lastStagePoint() base.StagePoint {
	i, _ := box.lsp.Value()

	return i.(base.StagePoint) //nolint:forcetypeassert //...
}

func (box *Ballotbox) setLastStagePoint(p base.StagePoint) bool {
	_, err := box.lsp.Set(func(_ bool, i interface{}) (interface{}, error) {
		lsp := i.(base.StagePoint) //nolint:forcetypeassert //...

		if lsp.Compare(p) >= 0 {
			return nil, errors.Errorf("not higher")
		}

		return p, nil
	})

	return err == nil
}

func (box *Ballotbox) count(vr *voterecords, stagepoint base.StagePoint, threshold base.Threshold) base.Voteproof {
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

	vps := vr.count(box.local, box.lastStagePoint(), threshold)
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

	for i := range vrs {
		vr := vrs[i]
		stagepoint := vr.stagepoint

		switch {
		case stagepoint.Height() == p.Height() && stagepoint.Compare(p.SetStage(base.StageINIT)) != 0:
			rvrs = append(rvrs, vr)

			continue
		case stagepoint.Compare(p) < 0:
			continue
		}

		nvrs = append(nvrs, vr)
	}

	return rvrs, nvrs
}

type voterecords struct {
	ballots          map[string]base.Ballot
	isValidVoteproof func(base.Voteproof, base.Suffrage) error
	suffrageVote     func(base.SuffrageWithdrawOperation) error
	getSuffrageFunc  isaac.GetSuffrageByBlockHeight
	nodes            map[string]struct{} // revive:disable-line:nested-structs
	voted            map[string]base.Ballot
	suf              *util.Locked
	vsuf             *util.Locked // NOTE suffrage for voteproof
	log              zerolog.Logger
	stagepoint       base.StagePoint
	sync.RWMutex
	f bool
}

func newVoterecords(
	stagepoint base.StagePoint,
	isValidVoteproof func(base.Voteproof, base.Suffrage) error,
	getSuffrageFunc isaac.GetSuffrageByBlockHeight,
	suffrageVote func(base.SuffrageWithdrawOperation) error,
	log *zerolog.Logger,
) *voterecords {
	vr := voterecordsPool.Get().(*voterecords) //nolint:forcetypeassert //...

	vr.stagepoint = stagepoint
	vr.isValidVoteproof = isValidVoteproof
	vr.getSuffrageFunc = getSuffrageFunc
	vr.voted = map[string]base.Ballot{}
	vr.nodes = map[string]struct{}{}
	vr.f = false
	vr.ballots = map[string]base.Ballot{}
	vr.suf = util.EmptyLocked()
	vr.vsuf = util.EmptyLocked()
	vr.suffrageVote = suffrageVote

	switch {
	case log == nil:
		vr.log = zerolog.Nop()
	default:
		vr.log = log.With().Interface("point", stagepoint).Logger()
	}

	return vr
}

func (vr *voterecords) vote(bl base.Ballot) (voted bool, validated bool, err error) {
	vr.Lock()
	defer vr.Unlock()

	node := bl.SignFact().Node().String()
	if _, found := vr.nodes[node]; found {
		return false, false, nil
	}

	if _, found := vr.ballots[node]; found {
		return false, false, nil
	}

	e := util.StringErrorFunc("failed to vote")

	var suf base.Suffrage

	switch i, found, err := vr.getSuffrage(); {
	case err != nil:
		return false, false, e(err, "")
	case !found:
		vr.ballots[node] = bl

		return true, false, nil
	default:
		suf = i
	}

	if err := vr.isValidBallotWithSuffrage(bl, suf); err != nil {
		vr.log.Error().Err(err).Interface("ballot", bl).Interface("suffrage", suf).Msg("invalid ballot")

		return false, false, nil
	}

	if err := vr.learnWithdraws(bl); err != nil {
		vr.log.Error().Err(err).Interface("ballot", bl).Msg("failed suffrage voting")

		return true, true, err
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

func (vr *voterecords) count(
	local base.Address,
	lastStagePoint base.StagePoint,
	threshold base.Threshold,
) []base.Voteproof {
	vr.Lock()
	defer vr.Unlock()

	suf, _, _ := vr.getSuffrage()

	if len(vr.voted) < 1 {
		if suf == nil {
			return nil
		}
	}

	var digged base.Voteproof
	var vpchecked bool

	if len(vr.ballots) > 0 {
		for i := range vr.ballots {
			bl := vr.ballots[i]

			if err := vr.isValidBallotWithSuffrage(bl, suf); err != nil {
				delete(vr.ballots, i)

				continue
			}

			if err := vr.learnWithdraws(bl); err != nil {
				continue
			}

			vr.voted[bl.SignFact().Node().String()] = bl

			if !vpchecked {
				isnew, overthreshold := isNewVoteproof(local, bl.Voteproof(), lastStagePoint, threshold)
				if isnew && overthreshold {
					digged = bl.Voteproof()
				}

				vpchecked = !isnew || overthreshold
			}
		}

		vr.ballots = map[string]base.Ballot{}
	}

	var vps []base.Voteproof
	if digged != nil {
		vps = append(vps, digged)
	}

	if vp := vr.countFromBallots(local, threshold); vp != nil {
		vps = append(vps, vp)
	}

	return vps
}

func (vr *voterecords) countFromBallots(local base.Address, threshold base.Threshold) base.Voteproof {
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

	sfs := make([]base.BallotSignFact, len(vr.voted))
	var i int

	for k := range vr.voted {
		sfs[i] = vr.voted[k].SignFact()
		i++
	}

	var set []string
	var m map[string]base.BallotFact

	switch i, j := base.CountBallotSignFacts(sfs); {
	case len(i) < 1:
		return nil
	default:
		set = i
		m = j
	}

	switch wsfs, majority, withdraws := vr.countWithWithdraws(local, suf, threshold); {
	case len(wsfs) < 1:
	default:
		vr.f = true

		return vr.newVoteproof(wsfs, majority, threshold, withdraws)
	}

	if uint(len(set)) < threshold.Threshold(uint(suf.Len())) {
		return nil
	}

	var majority base.BallotFact

	switch result, majoritykey := threshold.VoteResult(uint(suf.Len()), set); result {
	case base.VoteResultDraw:
	case base.VoteResultMajority:
		majority = m[majoritykey]
	default:
		return nil
	}

	vr.f = true

	return vr.newVoteproof(sfs, majority, threshold, nil)
}

func (vr *voterecords) countWithWithdraws(
	local base.Address,
	suf base.Suffrage,
	threshold base.Threshold,
) ([]base.BallotSignFact, base.BallotFact, []base.SuffrageWithdrawOperation) {
	sorted := sortBallotSignFactsByWithdraws(local, vr.voted)
	if len(sorted) < 1 {
		return nil, nil, nil
	}

	for i := range sorted {
		wfacts := sorted[i][0].([]base.SuffrageWithdrawFact)         //nolint:forcetypeassert //...
		wsfs := sorted[i][1].([]base.BallotSignFact)                 //nolint:forcetypeassert //...
		withdraws := sorted[i][2].([]base.SuffrageWithdrawOperation) //nolint:forcetypeassert //...

		set, m := base.CountBallotSignFacts(wsfs)

		newthreshold := threshold
		quorum := uint(suf.Len())

		if uint(len(wfacts)) > quorum-base.DefaultThreshold.Threshold(quorum) {
			newthreshold = base.MaxThreshold
			quorum = uint(suf.Len() - len(wfacts))
		}

		if uint(len(set)) < newthreshold.Threshold(quorum) {
			continue
		}

		var majority base.BallotFact

		switch result, majoritykey := newthreshold.VoteResult(quorum, set); result {
		case base.VoteResultDraw:
		case base.VoteResultMajority:
			majority = m[majoritykey]
		default:
			continue
		}

		return wsfs, majority, withdraws
	}

	return nil, nil, nil
}

func (vr *voterecords) newVoteproof(
	sfs []base.BallotSignFact,
	majority base.BallotFact,
	threshold base.Threshold,
	withdraws []base.SuffrageWithdrawOperation,
) base.Voteproof {
	switch vr.stagepoint.Stage() {
	case base.StageINIT:
		vp := isaac.NewINITVoteproof(vr.stagepoint.Point)
		_ = vp.
			SetSignFacts(sfs).
			SetMajority(majority).
			SetThreshold(threshold).
			SetWithdraws(withdraws).
			Finish()

		return vp
	case base.StageACCEPT:
		vp := isaac.NewACCEPTVoteproof(vr.stagepoint.Point)
		_ = vp.
			SetSignFacts(sfs).
			SetMajority(majority).
			SetThreshold(threshold).
			SetWithdraws(withdraws).
			Finish()

		return vp
	default:
		panic("unknown stage found to create voteproof")
	}
}

func (vr *voterecords) getSuffrage() (base.Suffrage, bool, error) {
	i, err := vr.suf.Set(func(_ bool, i interface{}) (interface{}, error) {
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

func (vr *voterecords) getVoteproofSuffrage() (base.Suffrage, bool, error) {
	i, err := vr.vsuf.Set(func(_ bool, i interface{}) (interface{}, error) {
		if i != nil {
			return i, util.ErrFound.Call()
		}

		var height base.Height

		switch {
		case vr.stagepoint.Stage() == base.StageACCEPT,
			vr.stagepoint.Round() > 0:
			height = vr.stagepoint.Height()
		default:
			height = vr.stagepoint.Height().Prev()
		}

		switch j, found, err := vr.getSuffrageFunc(height); {
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

func (vr *voterecords) learnWithdraws(bl base.Ballot) error {
	if vr.suffrageVote == nil {
		return nil
	}

	bw, ok := bl.(isaac.BallotWithdraws)
	if !ok {
		return nil
	}

	w := bw.Withdraws()

	for i := range w {
		if err := vr.suffrageVote(w[i]); err != nil {
			return err
		}
	}

	return nil
}

func (vr *voterecords) isValidBallotWithSuffrage(bl base.Ballot, suf base.Suffrage) error {
	e := util.ErrInvalid.Errorf("invalid ballot with suffrage")

	var vsuf base.Suffrage

	switch i, found, err := vr.getVoteproofSuffrage(); {
	case err != nil:
		return e.Wrap(err)
	case !found:
		return e.Errorf("voteproof suffrage not found")
	default:
		vsuf = i
	}

	if !suf.ExistsPublickey(bl.SignFact().Node(), bl.SignFact().Signer()) {
		return e.Errorf("node not in suffrage")
	}

	if wbl, ok := bl.(isaac.BallotWithdraws); ok {
		withdraws := wbl.Withdraws()

		for i := range withdraws {
			if err := isaac.IsValidWithdrawWithSuffrage(bl.Point().Height(), withdraws[i], suf); err != nil {
				return e.Wrap(err)
			}
		}
	}

	if err := isaac.IsValidVoteproofWithSuffrage(bl.Voteproof(), vsuf); err != nil {
		return e.Wrap(err)
	}

	if vr.isValidVoteproof != nil {
		if err := vr.isValidVoteproof(bl.Voteproof(), vsuf); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
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
	vr.isValidVoteproof = nil
	vr.getSuffrageFunc = nil
	vr.voted = nil
	vr.nodes = nil
	vr.f = false
	vr.ballots = nil
	vr.suf = nil
	vr.vsuf = nil
	vr.log = zerolog.Nop()

	voterecordsPool.Put(vr)
}

func isNewVoteproof(
	local base.Address,
	vp base.Voteproof,
	lastStagePoint base.StagePoint,
	threshold base.Threshold,
) (isnew bool, overthreshold bool) {
	if w, ok := vp.(isaac.WithdrawVoteproof); ok {
		withdraws := w.Withdraws()

		for i := range withdraws {
			// NOTE if local is in withdraws, ignore voteproof.
			if withdraws[i].WithdrawFact().Node().Equal(local) {
				return false, false
			}
		}
	}

	return vp.Point().Compare(lastStagePoint) > 0, vp.Threshold() >= threshold
}

func sortBallotSignFactsByWithdraws(local base.Address, ballots map[string]base.Ballot) [][3]interface{} {
	sfs := make([]base.BallotSignFact, len(ballots))

	{
		var n int

		for i := range ballots {
			sf := ballots[i].SignFact()
			sfs[n] = sf

			n++
		}
	}

	mw := map[string][3]interface{}{}

	for i := range ballots {
		bl := ballots[i]

		if _, found := mw[bl.SignFact().Fact().Hash().String()]; found {
			continue
		}

		m, found := extractWithdrawsFromBallot(local, bl, sfs)
		if !found {
			continue
		}

		mw[bl.SignFact().Fact().Hash().String()] = m
	}

	if len(mw) < 1 {
		return nil
	}

	ms := make([][3]interface{}, len(mw))

	var n int

	for i := range mw {
		ms[n] = mw[i]

		n++
	}

	sort.Slice(ms, func(i, j int) bool {
		return len(ms[i][0].([]base.SuffrageWithdrawFact)) > //nolint:forcetypeassert //...
			len(ms[j][0].([]base.SuffrageWithdrawFact)) //nolint:forcetypeassert //...
	})

	return ms
}

func extractWithdrawsFromBallot(
	local base.Address,
	bl base.Ballot,
	sfs []base.BallotSignFact,
) (m [3]interface{}, found bool) {
	var withdraws []base.SuffrageWithdrawOperation

	switch w, ok := bl.(isaac.BallotWithdraws); {
	case !ok:
		return m, false
	default:
		withdraws = w.Withdraws()
	}

	sf := bl.SignFact()

	wfacts := sf.Fact().(isaac.BallotWithdrawFacts).WithdrawFacts() //nolint:forcetypeassert //...

	if util.InSliceFunc(wfacts, func(_ interface{}, j int) bool {
		return wfacts[j].Node().Equal(local) // NOTE if local is in withdraws, ignore
	}) >= 0 {
		return m, false
	}

	m[0] = wfacts
	m[1] = util.FilterSlice(sfs, func(_ interface{}, j int) bool {
		return util.InSliceFunc(wfacts, func(_ interface{}, k int) bool {
			return wfacts[k].Node().Equal(sfs[j].Node())
		}) < 0
	})
	m[2] = withdraws

	return m, true
}
