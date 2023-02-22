package isaacstates

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/semaphore"
)

func mustNewLastPoint(
	point base.StagePoint,
	isMajority, isSuffrageConfirm bool,
) isaac.LastPoint {
	p, err := isaac.NewLastPoint(point, isMajority, isSuffrageConfirm)
	if err != nil {
		panic(err)
	}

	return p
}

func (box *Ballotbox) voteAndWait(bl base.Ballot, threshold base.Threshold) (bool, []base.Voteproof, error) {
	var withdraws []base.SuffrageWithdrawOperation
	if w, ok := bl.(base.HasWithdraws); ok {
		withdraws = w.Withdraws()
	}

	voted, callback, err := box.vote(bl.SignFact(), bl.Voteproof(), withdraws, threshold)
	if err != nil {
		return voted, nil, err
	}

	if callback == nil {
		return voted, nil, nil
	}

	return voted, callback(), nil
}

type baseTestBallotbox struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *baseTestBallotbox) SetupSuite() {
	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *baseTestBallotbox) initBallot(node isaac.LocalNode, nodes []isaac.LocalNode, point base.Point, prev, proposal util.Hash, withdraws []base.SuffrageWithdrawOperation, vp base.Voteproof) isaac.INITBallot {
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	if vp == nil {
		if point.Round() == 0 {
			afact := isaac.NewACCEPTBallotFact(point.PrevHeight(), valuehash.RandomSHA256(), prev, nil)

			asfs := make([]base.BallotSignFact, len(nodes))
			for i := range nodes {
				n := nodes[i]
				sf := isaac.NewACCEPTBallotSignFact(afact)
				t.NoError(sf.NodeSign(n.Privatekey(), t.networkID, n.Address()))
				asfs[i] = sf
			}

			avp := isaac.NewACCEPTVoteproof(afact.Point().Point)
			avp.
				SetMajority(afact).
				SetSignFacts(asfs).
				SetThreshold(base.Threshold(100)).
				Finish()

			vp = avp
		} else {
			isfs := make([]base.BallotSignFact, len(nodes))
			for i := range nodes {
				n := nodes[i]

				ifact := isaac.NewINITBallotFact(point.PrevRound(), prev, proposal, withdrawfacts)

				switch {
				case n.Address().Equal(node.Address()):
				case i%2 == 0:
				default:
					ifact = isaac.NewINITBallotFact(point.PrevRound(), prev, valuehash.RandomSHA256(), withdrawfacts)
				}

				sf := isaac.NewINITBallotSignFact(ifact)
				t.NoError(sf.NodeSign(n.Privatekey(), t.networkID, n.Address()))
				isfs[i] = sf
			}

			if len(withdraws) > 0 {
				ivp := isaac.NewINITWithdrawVoteproof(point.PrevRound())
				ivp.
					SetSignFacts(isfs).
					SetThreshold(base.Threshold(100))
				ivp.SetWithdraws(withdraws)
				ivp.Finish()

				vp = ivp
			} else {
				ivp := isaac.NewINITVoteproof(point.PrevRound())
				ivp.
					SetSignFacts(isfs).
					SetThreshold(base.Threshold(100)).
					Finish()

				vp = ivp
			}
		}
	}

	fact := isaac.NewINITBallotFact(point, prev, proposal, withdrawfacts)

	signfact := isaac.NewINITBallotSignFact(fact)
	t.NoError(signfact.NodeSign(node.Privatekey(), t.networkID, node.Address()))

	return isaac.NewINITBallot(vp, signfact, withdraws)
}

func (t *baseTestBallotbox) acceptBallot(node isaac.LocalNode, nodes []isaac.LocalNode, point base.Point, pr, block util.Hash, withdraws []base.SuffrageWithdrawOperation) isaac.ACCEPTBallot {
	prev := valuehash.RandomSHA256()

	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	ifact := isaac.NewINITBallotFact(point, prev, pr, withdrawfacts)

	isfs := make([]base.BallotSignFact, len(nodes))
	for i := range nodes {
		n := nodes[i]
		sf := isaac.NewINITBallotSignFact(ifact)
		t.NoError(sf.NodeSign(n.Privatekey(), t.networkID, n.Address()))
		isfs[i] = sf
	}

	var ivp base.INITVoteproof

	if len(withdraws) > 0 {
		i := isaac.NewINITWithdrawVoteproof(ifact.Point().Point)
		i.
			SetMajority(ifact).
			SetSignFacts(isfs).
			SetThreshold(base.Threshold(100))
		i.SetWithdraws(withdraws)
		i.Finish()

		ivp = i
	} else {
		i := isaac.NewINITVoteproof(ifact.Point().Point)
		i.
			SetMajority(ifact).
			SetSignFacts(isfs).
			SetThreshold(base.Threshold(100)).
			Finish()

		ivp = i
	}

	fact := isaac.NewACCEPTBallotFact(point, pr, block, withdrawfacts)

	signfact := isaac.NewACCEPTBallotSignFact(fact)

	t.NoError(signfact.NodeSign(node.Privatekey(), t.networkID, node.Address()))

	return isaac.NewACCEPTBallot(ivp, signfact, withdraws)
}

func (t *baseTestBallotbox) compareStagePoint(a base.StagePoint, i interface{}) {
	var b base.StagePoint
	switch t := i.(type) {
	case base.Voteproof:
		b = t.Point()
	case base.StagePoint:
		b = t
	}

	t.Equal(0, a.Compare(b))
}

type testBallotbox struct {
	baseTestBallotbox
}

func (t *testBallotbox) TestVoteSignFactINITBallotSignFact() {
	suf, nodes := isaac.NewTestSuffrage(1)
	th := base.Threshold(100)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()

	bl := t.initBallot(nodes[0], suf.Locals(), point, prev, valuehash.RandomSHA256(), nil, nil)
	box.SetLastPoint(mustNewLastPoint(bl.Voteproof().Point(), true, false))

	voted, err := box.VoteSignFact(bl.SignFact(), th)
	t.NoError(err)
	t.True(voted)

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(point, vp.Point().Point)
		t.Equal(th, vp.Threshold())

		base.EqualBallotFact(t.Assert(), bl.SignFact().Fact().(base.BallotFact), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Since(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignFacts()))
		base.EqualBallotSignFact(t.Assert(), bl.SignFact(), vp.SignFacts()[0])

		last := box.LastPoint()
		t.compareStagePoint(last.StagePoint, vp)
	}
}

func (t *testBallotbox) TestVoteINITBallotSignFact() {
	suf, nodes := isaac.NewTestSuffrage(1)
	th := base.Threshold(100)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()

	bl := t.initBallot(nodes[0], suf.Locals(), point, prev, valuehash.RandomSHA256(), nil, nil)
	box.SetLastPoint(mustNewLastPoint(bl.Voteproof().Point(), true, false))

	voted, err := box.Vote(bl, th)
	t.NoError(err)
	t.True(voted)

	sfs := box.Voted(base.NewStagePoint(point, base.StageINIT), []base.Address{nodes[0].Address()})
	t.Equal(1, len(sfs))
	base.EqualBallotSignFact(t.Assert(), bl.BallotSignFact(), sfs[0])

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(point, vp.Point().Point)
		t.Equal(th, vp.Threshold())

		base.EqualBallotFact(t.Assert(), bl.SignFact().Fact().(base.BallotFact), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Since(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignFacts()))
		base.EqualBallotSignFact(t.Assert(), bl.SignFact(), vp.SignFacts()[0])

		last := box.LastPoint()
		t.compareStagePoint(last.StagePoint, vp)
	}
}

func (t *testBallotbox) TestVotePreviousRoundAlreadyMajority() {
	suf, nodes := isaac.NewTestSuffrage(3)
	th := base.Threshold(100)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 1)

	t.T().Log("last point:", point.PrevRound())
	box.SetLastPoint(mustNewLastPoint(base.NewStagePoint(point.PrevRound(), base.StageINIT), true, false))

	t.T().Log("new ballot:", point)

	prev := valuehash.RandomSHA256()

	bl := t.initBallot(nodes[0], suf.Locals(), point, prev, valuehash.RandomSHA256(), nil, nil)

	voted, err := box.Vote(bl, th)
	t.NoError(err)
	t.True(voted)
}

func (t *testBallotbox) TestVoteACCEPTBallotSignFact() {
	suf, nodes := isaac.NewTestSuffrage(1)
	th := base.Threshold(100)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 1)
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl := t.acceptBallot(nodes[0], suf.Locals(), point, pr, block, nil)
	box.SetLastPoint(mustNewLastPoint(bl.Voteproof().Point(), true, false))

	voted, err := box.Vote(bl, th)
	t.NoError(err)
	t.True(voted)

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(point, vp.Point().Point)
		t.Equal(base.StageACCEPT, vp.Point().Stage())
		t.Equal(th, vp.Threshold())

		base.EqualBallotFact(t.Assert(), bl.SignFact().Fact().(base.BallotFact), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Since(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignFacts()))
		base.EqualBallotSignFact(t.Assert(), bl.SignFact(), vp.SignFacts()[0])

		last := box.LastPoint()
		t.compareStagePoint(last.StagePoint, vp)
	}
}

func (t *testBallotbox) TestVoteSamePointAndStageWithLastVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(3)
	th := base.Threshold(60)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 1)
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl0 := t.acceptBallot(nodes[0], suf.Locals(), point, pr, block, nil)
	box.SetLastPoint(mustNewLastPoint(bl0.Voteproof().Point(), true, false))

	voted, err := box.Vote(bl0, th)
	t.NoError(err)
	t.True(voted)

	bl1 := t.acceptBallot(nodes[1], suf.Locals(), point, pr, block, nil)
	voted, err = box.Vote(bl1, th)
	t.NoError(err)
	t.True(voted)

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))

		return
	case vp := <-box.Voteproof():
		last := box.LastPoint()
		t.compareStagePoint(last.StagePoint, vp.Point())
	}

	// vote again
	bl2 := t.acceptBallot(nodes[2], suf.Locals(), point, pr, block, nil)
	voted, err = box.Vote(bl2, th)
	t.NoError(err)
	t.False(voted)
}

func (t *testBallotbox) TestOldBallotSignFact() {
	suf, nodes := isaac.NewTestSuffrage(2)
	th := base.Threshold(100)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 1)

	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl0 := t.acceptBallot(nodes[0], suf.Locals(), point, pr, block, nil)
	box.SetLastPoint(mustNewLastPoint(bl0.Voteproof().Point(), true, false))

	voted, err := box.Vote(bl0, th)
	t.NoError(err)
	t.True(voted)

	bl1 := t.acceptBallot(nodes[1], suf.Locals(), point, pr, block, nil)
	voted, err = box.Vote(bl1, th)
	t.NoError(err)
	t.True(voted)

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))

		return
	case vp := <-box.Voteproof():
		last := box.LastPoint()
		t.compareStagePoint(last.StagePoint, vp)
	}

	bl01 := t.initBallot(nodes[0], suf.Locals(), point, valuehash.RandomSHA256(), pr, nil, nil)
	voted, err = box.Vote(bl01, th)
	t.NoError(err)
	t.False(voted)
}

func (t *testBallotbox) TestUnknownSuffrageNode() {
	suf, _ := isaac.NewTestSuffrage(1)
	th := base.Threshold(100)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 1)
	prev := valuehash.RandomSHA256()

	unknown := isaac.RandomLocalNode()

	bl := t.initBallot(unknown, suf.Locals(), point, prev, valuehash.RandomSHA256(), nil, nil)
	voted, err := box.Vote(bl, th)
	t.NoError(err)
	t.False(voted)
}

func (t *testBallotbox) TestNilSuffrage() {
	n0 := isaac.RandomLocalNode()

	th := base.Threshold(100)
	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return nil, false, nil
		},
	)

	point := base.RawPoint(33, 1)
	prev := valuehash.RandomSHA256()

	bl := t.initBallot(n0, []isaac.LocalNode{n0}, point, prev, valuehash.RandomSHA256(), nil, nil)
	voted, err := box.Vote(bl, th)
	t.NoError(err)
	t.True(voted)

	stagepoint := base.NewStagePoint(point, base.StageINIT)
	vr, found := box.voterecords(stagepoint, false)
	t.True(found)
	t.Equal(stagepoint, vr.stagepoint())

	vr.Lock()
	defer vr.Unlock()

	vbl := vr.ballots[n0.Address().String()]
	base.EqualBallotSignFact(t.Assert(), bl.SignFact(), vbl)
}

func (t *testBallotbox) TestNilSuffrageCount() {
	suf, nodes := isaac.NewTestSuffrage(1)
	th := base.Threshold(100)

	var i int64
	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			if atomic.LoadInt64(&i) < 1 {
				atomic.StoreInt64(&i, 1)
				return nil, false, nil
			}

			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()

	bl := t.initBallot(nodes[0], suf.Locals(), point, prev, valuehash.RandomSHA256(), nil, nil)
	box.SetLastPoint(mustNewLastPoint(bl.Voteproof().Point(), true, false))

	_, _, err := box.voteAndWait(bl, th)
	t.NoError(err)

	go box.Count(th)

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(point, vp.Point().Point)
		t.Equal(base.StageINIT, vp.Point().Stage())
		t.Equal(th, vp.Threshold())

		base.EqualBallotFact(t.Assert(), bl.SignFact().Fact().(base.BallotFact), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Since(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignFacts()))
		base.EqualBallotSignFact(t.Assert(), bl.SignFact(), vp.SignFacts()[0])

		last := box.LastPoint()
		t.compareStagePoint(last.StagePoint, vp)
	}
}

func (t *testBallotbox) TestVoteproofOrder() {
	suf, nodes := isaac.NewTestSuffrage(2)
	th := base.Threshold(100)

	var enablesuf int64
	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			if atomic.LoadInt64(&enablesuf) < 1 {
				return nil, false, nil
			}

			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 22)

	// prev prev ACCEPT vote
	ppblock := valuehash.RandomSHA256()
	pppr := valuehash.RandomSHA256()

	for i := range nodes {
		bl := t.acceptBallot(nodes[i], suf.Locals(), base.NewPoint(point.Height()-1, base.Round(0)), pppr, ppblock, nil)
		_, vp, err := box.voteAndWait(bl, th)
		t.NoError(err)
		t.Nil(vp)
	}

	// prev INIT vote
	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	for i := range nodes {
		bl := t.initBallot(nodes[i], suf.Locals(), point, prev, pr, nil, nil)
		_, vp, err := box.voteAndWait(bl, th)
		t.NoError(err)
		t.Nil(vp)
	}

	// prev ACCEPT vote
	block := valuehash.RandomSHA256()

	for i := range nodes {
		bl := t.acceptBallot(nodes[i], suf.Locals(), point, pr, block, nil)
		_, vp, err := box.voteAndWait(bl, th)
		t.NoError(err)
		t.Nil(vp)
	}

	// next INIT vote
	nextpr := valuehash.RandomSHA256()
	nextpoint := base.NewPoint(point.Height()+1, base.Round(0))
	for i := range nodes {
		bl := t.initBallot(nodes[i], suf.Locals(), nextpoint, block, nextpr, nil, nil)
		_, vp, err := box.voteAndWait(bl, th)
		t.NoError(err)
		t.Nil(vp)
	}

	atomic.StoreInt64(&enablesuf, 1)
	box.SetLastPoint(mustNewLastPoint(base.NewStagePoint(point, base.StageINIT), true, false))

	go box.Count(th)

	var rvps []base.Voteproof

	after := time.After(time.Second * 2)
end:
	for {
		select {
		case <-after:
			break end
		case vp := <-box.Voteproof():
			rvps = append(rvps, vp)
		}
	}

	for i := range rvps {
		t.T().Logf("%d voteproof: %q", i, rvps[i].Point())
	}

	t.Equal(2, len(rvps))
	for i := range rvps {
		vp := rvps[i]
		t.NoError(vp.IsValid(t.networkID))
	}

	t.Equal(point, rvps[0].Point().Point)
	t.Equal(nextpoint, rvps[1].Point().Point)
	t.Equal(base.StageACCEPT, rvps[0].Point().Stage())
	t.Equal(base.StageINIT, rvps[1].Point().Stage())
}

func (t *testBallotbox) TestVoteproofFromBallotACCEPTVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(2)
	th := base.Threshold(100)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 0)
	prevpoint := point.PrevHeight()
	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	bl := t.initBallot(nodes[0], suf.Locals(), point, prev, pr, nil, nil)
	box.SetLastPoint(mustNewLastPoint(bl.Voteproof().Point().Decrease(), true, false))

	voted, err := box.Vote(bl, th)
	t.NoError(err)
	t.True(voted)

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(prevpoint, vp.Point().Point)

		base.EqualVoteproof(t.Assert(), bl.Voteproof(), vp)
	}
}

func (t *testBallotbox) TestVoteproofFromBallotINITVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(2)
	th := base.Threshold(100)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 0)
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl := t.acceptBallot(nodes[0], suf.Locals(), point, pr, block, nil)
	box.SetLastPoint(mustNewLastPoint(bl.Voteproof().Point().Decrease(), true, false))

	voted, err := box.Vote(bl, th)
	t.NoError(err)
	t.True(voted)

	var rvps []base.Voteproof
end:
	for {
		select {
		case <-time.After(time.Second * 2):
			break end
		case vp := <-box.Voteproof():
			t.NoError(vp.IsValid(t.networkID))

			rvps = append(rvps, vp)
		}
	}

	t.T().Logf("ballot: %q", bl.Point())
	for i := range rvps {
		t.T().Logf("%d voteproof: %q", i, rvps[i].Point())
	}

	t.Equal(1, len(rvps))
	t.Equal(point, rvps[0].Point().Point)

	base.EqualVoteproof(t.Assert(), bl.Voteproof(), rvps[0])
}

func (t *testBallotbox) TestVoteproofFromBallotWhenCount() {
	suf, nodes := isaac.NewTestSuffrage(2)
	th := base.Threshold(100)

	var i int64
	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			if atomic.LoadInt64(&i) < 1 {
				return nil, false, nil
			}

			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 0)
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl := t.acceptBallot(nodes[0], suf.Locals(), point, pr, block, nil)

	_, vp, err := box.voteAndWait(bl, th)
	t.NoError(err)
	t.Nil(vp)

	var rvps []base.Voteproof
end0:
	for {
		select {
		case <-time.After(time.Second * 2):
			break end0
		case vp := <-box.Voteproof():
			t.NoError(vp.IsValid(t.networkID))

			rvps = append(rvps, vp)
		}
	}

	t.Empty(rvps)

	atomic.StoreInt64(&i, 1)
	go box.Count(th)

end1:
	for {
		select {
		case <-time.After(time.Second * 2):
			break end1
		case vp := <-box.Voteproof():
			t.NoError(vp.IsValid(t.networkID))

			rvps = append(rvps, vp)
		}
	}

	t.T().Logf("ballot: %q", bl.Point())
	for i := range rvps {
		t.T().Logf("%d voteproof: %q", i, rvps[i].Point())
	}

	t.Equal(1, len(rvps))

	t.Equal(bl.Voteproof().Point(), rvps[0].Point())

	base.EqualVoteproof(t.Assert(), bl.Voteproof(), rvps[0])
}

func (t *testBallotbox) TestAsyncVoterecords() {
	max := 500

	suf, nodes := isaac.NewTestSuffrage(max + 2)
	th := base.Threshold(100)
	stagepoint := base.NewStagePoint(base.RawPoint(33, 44), base.StageINIT)
	vr := newVoterecords(
		stagepoint,
		func(base.Voteproof, base.Suffrage) error { return nil },
		func(base.Height) (base.Suffrage, bool, error) { return suf, true, nil },
		nil, false, nil,
	)

	ctx := context.TODO()
	sem := semaphore.NewWeighted(300)

	for i := range make([]int, max) {
		if err := sem.Acquire(ctx, 1); err != nil {
			panic(err)
		}

		go func(i int) {
			defer sem.Release(1)

			bl := t.initBallot(nodes[i], nil, stagepoint.Point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil, nil)
			_, _, _ = vr.vote(bl.BallotSignFact(), bl.Voteproof(), nil, isaac.LastPoint{})

			if i%3 == 0 {
				_ = vr.count(base.RandomAddress(""), mustNewLastPoint(base.ZeroStagePoint, true, false), th, 0)
			}
		}(i)
	}

	if err := sem.Acquire(ctx, 300); err != nil {
		panic(err)
	}
}

func (t *testBallotbox) TestAsyncVoteAndClean() {
	max := 500
	th := base.Threshold(10)

	suf, nodes := isaac.NewTestSuffrage(max)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	ctx := context.TODO()
	sem := semaphore.NewWeighted(300)

	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	for i := range make([]int, max) {
		if err := sem.Acquire(ctx, 1); err != nil {
			panic(err)
		}

		go func(i int) {
			defer sem.Release(1)

			point := base.NewPoint(base.Height(int64(i)%4), base.Round(0))

			bl := t.initBallot(nodes[i], nil, point, prev, pr, nil, nil)
			box.Vote(bl, th)

			if i%2 == 0 {
				go box.Count(th)
			}
		}(i)
	}

	if err := sem.Acquire(ctx, 300); err != nil {
		panic(err)
	}
}

func (t *testBallotbox) TestDifferentSuffrage() {
	vsuf, vnodes := isaac.NewTestSuffrage(1)

	suf, nodes := isaac.NewTestSuffrage(3)
	th := base.Threshold(100)

	point := base.RawPoint(33, 0)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(height base.Height) (base.Suffrage, bool, error) {
			// return suf, true, nil
			switch {
			case height == point.Height().Prev():
				return vsuf, true, nil
			case height == point.Height():
				return suf, true, nil
			default:
				return nil, false, nil
			}
		},
	)

	prev := valuehash.RandomSHA256()

	afact := isaac.NewACCEPTBallotFact(point.PrevHeight(), valuehash.RandomSHA256(), prev, nil)
	asfs := make([]base.BallotSignFact, len(vnodes))
	for i := range vnodes {
		n := vnodes[i]
		sf := isaac.NewACCEPTBallotSignFact(afact)
		t.NoError(sf.NodeSign(n.Privatekey(), t.networkID, n.Address()))
		asfs[i] = sf
	}

	avp := isaac.NewACCEPTVoteproof(point.PrevHeight())
	avp.
		SetMajority(afact).
		SetSignFacts(asfs).
		SetThreshold(base.Threshold(100)).
		Finish()

	proposal := valuehash.RandomSHA256()
	fact := isaac.NewINITBallotFact(point, prev, proposal, nil)

	for i := range nodes {
		node := nodes[i]

		bl := t.initBallot(node, suf.Locals(), point, prev, proposal, nil, avp)
		box.SetLastPoint(mustNewLastPoint(bl.Voteproof().Point(), true, false))

		voted, err := box.Vote(bl, th)
		t.NoError(err)
		t.True(voted)
	}

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(point, vp.Point().Point)
		t.Equal(th, vp.Threshold())

		base.EqualBallotFact(t.Assert(), fact, vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Since(vp.FinishedAt()) < time.Millisecond*800)

		last := box.LastPoint()
		t.compareStagePoint(last.StagePoint, vp)
	}
}

func (t *testBallotbox) TestSetLastStagePointReversalByVoteproof() {
	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return nil, false, nil
		},
	)

	point := base.RawPoint(33, 0)

	t.Run("next round + not majority -> previous round + majority", func() {
		t.True(box.SetLastPoint(mustNewLastPoint(base.NewStagePoint(point.NextRound(), base.StageINIT), false, false)))
		t.False(box.SetLastPoint(mustNewLastPoint(base.NewStagePoint(point, base.StageINIT), true, false)))
	})

	t.Run("next round + majority -> previous round + majority", func() {
		point = point.NextHeight()

		t.True(box.SetLastPoint(mustNewLastPoint(base.NewStagePoint(point.NextRound(), base.StageINIT), true, false)))
		t.False(box.SetLastPoint(mustNewLastPoint(base.NewStagePoint(point, base.StageINIT), true, false)))
	})
}

func (t *testBallotbox) TestDiggVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(3)
	th := base.Threshold(100)
	local := nodes[0]

	point := base.RawPoint(33, 0)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(height base.Height) (base.Suffrage, bool, error) {
			if height == point.Height() {
				return suf, true, nil
			}

			return nil, false, nil
		},
	)

	t.Run("accept ballot of same height", func() {
		box.SetLastPoint(mustNewLastPoint(base.NewStagePoint(point, base.StageINIT), true, false))

		abl := t.acceptBallot(local, nodes, point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

		voted, err := box.Vote(abl, th)
		t.NoError(err)
		t.True(voted)

		select {
		case <-time.After(time.Second):
		case <-box.Voteproof():
			t.NoError(errors.Errorf("no voteproof expected"))
		}
	})

	t.Run("init ballot of next height", func() {
		box.SetLastPoint(mustNewLastPoint(base.NewStagePoint(point, base.StageINIT), true, false))

		ibl := t.initBallot(local, nodes, point.NextHeight(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil, nil)

		voted, err := box.Vote(ibl, th)
		t.NoError(err)
		t.True(voted)

		select {
		case <-time.After(time.Second):
			t.NoError(errors.Errorf("failed to wait voteproof"))
		case vp := <-box.Voteproof():
			t.NoError(vp.IsValid(t.networkID))
		}
	})

	t.Run("init ballot of next next height", func() {
		box.SetLastPoint(mustNewLastPoint(base.NewStagePoint(point, base.StageINIT), true, false))

		ibl := t.initBallot(local, nodes, point.NextHeight().NextHeight(), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil, nil)

		voted, err := box.Vote(ibl, th)
		t.NoError(err)
		t.True(voted)

		select {
		case <-time.After(time.Second):
		case <-box.Voteproof():
			t.NoError(errors.Errorf("no voteproof expected"))
		}
	})
}

func (t *testBallotbox) TestMissingNodes() {
	t.Run("not yet finished", func() {
		point := base.RawPoint(33, 0)
		suf, nodes := isaac.NewTestSuffrage(3)

		t.T().Log("suf:", suf.Len())

		th := base.Threshold(100)

		box := NewBallotbox(
			base.RandomAddress(""),
			func(base.Height) (base.Suffrage, bool, error) {
				return suf, true, nil
			},
		)

		prev := valuehash.RandomSHA256()
		bl := t.initBallot(nodes[0], suf.Locals(), point, prev, valuehash.RandomSHA256(), nil, nil)

		voted, err := box.Vote(bl, th)
		t.NoError(err)
		t.True(voted)

		t.Run("unknown nodes", func() {
			founds, ok, err := box.MissingNodes(base.NewStagePoint(point.PrevRound(), base.StageINIT), th)
			t.NoError(err)
			t.False(ok)
			t.Equal(0, len(founds))
		})

		t.Run("known nodes", func() {
			founds, ok, err := box.MissingNodes(base.NewStagePoint(point, base.StageINIT), th)
			t.NoError(err)
			t.True(ok)
			t.Equal(suf.Len()-1, len(founds))

			for i := range nodes {
				node := nodes[i]
				if node.Address().Equal(nodes[0].Address()) {
					continue
				}

				t.True(util.InSliceFunc(founds, func(j base.Address) bool {
					return node.Address().Equal(j)
				}) >= 0)
			}
		})
	})

	t.Run("finished", func() {
		point := base.RawPoint(33, 0)
		suf, nodes := isaac.NewTestSuffrage(3)

		t.T().Log("suf:", suf.Len())

		th := base.Threshold(100)

		box := NewBallotbox(
			base.RandomAddress(""),
			func(base.Height) (base.Suffrage, bool, error) {
				return suf, true, nil
			},
		)

		prev := valuehash.RandomSHA256()
		pr := valuehash.RandomSHA256()

		for i := range nodes {
			bl := t.initBallot(nodes[i], suf.Locals(), point, prev, pr, nil, nil)

			voted, err := box.Vote(bl, th)
			t.NoError(err)
			t.True(voted)
		}

		founds, ok, err := box.MissingNodes(base.NewStagePoint(point, base.StageINIT), th)
		t.NoError(err)
		t.True(ok)
		t.Equal(0, len(founds))
	})

	t.Run("suffrage not found", func() {
		point := base.RawPoint(33, 0)
		suf, nodes := isaac.NewTestSuffrage(3)

		th := base.Threshold(100)

		box := NewBallotbox(
			base.RandomAddress(""),
			func(base.Height) (base.Suffrage, bool, error) {
				return nil, false, nil
			},
		)

		prev := valuehash.RandomSHA256()
		pr := valuehash.RandomSHA256()

		for i := range nodes {
			bl := t.initBallot(nodes[i], suf.Locals(), point, prev, pr, nil, nil)

			voted, err := box.Vote(bl, th)
			t.NoError(err)
			t.True(voted)
		}

		founds, ok, err := box.MissingNodes(base.NewStagePoint(point, base.StageINIT), th)
		t.NoError(err)
		t.False(ok)
		t.Equal(0, len(founds))
	})
}

func (t *testBallotbox) TestVoteproofFinished() {
	t.Run("finished", func() {
		point := base.RawPoint(33, 0)
		suf, nodes := isaac.NewTestSuffrage(3)

		th := base.Threshold(100)

		box := NewBallotbox(
			base.RandomAddress(""),
			func(base.Height) (base.Suffrage, bool, error) {
				return suf, true, nil
			},
		)

		prev := valuehash.RandomSHA256()
		pr := valuehash.RandomSHA256()

		for i := range nodes {
			bl := t.initBallot(nodes[i], suf.Locals(), point, prev, pr, nil, nil)

			voted, err := box.Vote(bl, th)
			t.NoError(err)
			t.True(voted)
		}

		box.Count(th)

		vr, found := box.voterecords(base.NewStagePoint(point, base.StageINIT), false)

		t.True(found)
		t.NotNil(vr)
		t.True(vr.finished())
	})

	t.Run("not yet finished", func() {
		point := base.RawPoint(33, 0)
		suf, nodes := isaac.NewTestSuffrage(3)

		th := base.Threshold(100)

		box := NewBallotbox(
			base.RandomAddress(""),
			func(base.Height) (base.Suffrage, bool, error) {
				return suf, true, nil
			},
		)

		prev := valuehash.RandomSHA256()
		pr := valuehash.RandomSHA256()

		for i := range nodes[:2] {
			bl := t.initBallot(nodes[i], suf.Locals(), point, prev, pr, nil, nil)

			voted, err := box.Vote(bl, th)
			t.NoError(err)
			t.True(voted)
		}

		box.Count(th)

		vr, found := box.voterecords(base.NewStagePoint(point, base.StageINIT), false)
		t.True(found)
		t.NotNil(vr)
		t.False(vr.finished())
	})
}

func (t *testBallotbox) TestCopyVotedDATARACE() {
	point := base.RawPoint(33, 0)

	suf, nodes := isaac.NewTestSuffrage(33 * 3)

	th := base.Threshold(100)

	box := NewBallotbox(
		base.RandomAddress(""),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wkb := util.NewDistributeWorker(ctx, int64(len(nodes))*2, nil)
	defer wkb.Close()

	bls := make([]base.Ballot, len(nodes))

	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	vpl := util.EmptyLocked((base.Voteproof)(nil))

	for range bls {
		t.NoError(wkb.NewJob(func(_ context.Context, i uint64) error {
			vp, _ := vpl.Value()

			bl := t.initBallot(nodes[i], nodes, point, prev, pr, nil, vp)

			vpl.Set(func(vp base.Voteproof, isempty bool) (base.Voteproof, error) {
				if isempty {
					return bl.Voteproof(), nil
				}

				return nil, util.ErrLockedSetIgnore.Call()
			})

			bls[i] = bl

			return nil
		}))
	}

	wkb.Done()
	t.NoError(wkb.Wait())

	wk := util.NewDistributeWorker(ctx, int64(len(nodes))*2, nil)
	defer wk.Close()

	for i := range bls {
		bl := bls[i]

		t.NoError(wk.NewJob(func(ctx context.Context, _ uint64) error {
			ticker := time.NewTicker(time.Millisecond * 3)
			defer ticker.Stop()

			var n int
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
					if n > 333 {
						return nil
					}
					n++

					box.MissingNodes(base.NewStagePoint(point, base.StageINIT), th)
				}
			}
		}))

		t.NoError(wk.NewJob(func(ctx context.Context, _ uint64) error {
			donech := make(chan error, 1)
			go func() {
				switch voted, err := box.Vote(bl, th); {
				case err != nil:
					donech <- err
				case !voted:
					donech <- errors.Errorf("not voted")
				default:
					donech <- nil
				}
			}()

			ticker := time.NewTicker(time.Millisecond * 3)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return nil
				case err := <-donech:
					return err
				case <-ticker.C:
					box.MissingNodes(base.NewStagePoint(point, base.StageINIT), th)
				}
			}
		}))
	}

	wk.Done()
	t.NoError(wk.Wait())
}

func TestBallotbox(t *testing.T) {
	suite.Run(t, new(testBallotbox))
}

type testBallotboxWithWithdraw struct {
	baseTestBallotbox
	encoder.BaseTest
}

func (t *testBallotboxWithWithdraw) SetupSuite() {
	t.baseTestBallotbox.SetupSuite()

	t.BaseTest.MarshalFunc = util.MarshalJSONIndent
}

func (t *testBallotboxWithWithdraw) withdraws(height base.Height, withdrawnodes []base.Address, signs []isaac.LocalNode) []base.SuffrageWithdrawOperation {
	ops := make([]base.SuffrageWithdrawOperation, len(withdrawnodes))

	for i := range withdrawnodes {
		withdrawnode := withdrawnodes[i]

		fact := isaac.NewSuffrageWithdrawFact(withdrawnode, height, height+1, util.UUID().String())
		op := isaac.NewSuffrageWithdrawOperation(fact)

		for j := range signs {
			node := signs[j]

			if node.Address().Equal(withdrawnode) {
				continue
			}

			t.NoError(op.NodeSign(node.Privatekey(), t.networkID, node.Address()))
		}

		ops[i] = op
	}

	return ops
}

func (t *testBallotboxWithWithdraw) TestINITBallot() {
	suf, nodes := isaac.NewTestSuffrage(3)
	th := base.DefaultThreshold

	local := nodes[0]
	withdrawnode := nodes[1]
	other := nodes[2]

	box := NewBallotbox(
		local.Address(),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	withdraws := t.withdraws(point.Height()-1, []base.Address{withdrawnode.Address()}, nodes)

	t.T().Log("withdraw:", t.StringMarshal(withdraws))

	bl0 := t.initBallot(local, suf.Locals(), point, prev, pr, withdraws, nil)
	t.NoError(bl0.IsValid(t.networkID))

	t.T().Log("ballot local:", t.StringMarshal(bl0))

	bl1 := t.initBallot(other, suf.Locals(), point, prev, pr, withdraws, nil)
	t.NoError(bl1.IsValid(t.networkID))

	t.T().Log("ballot other:", t.StringMarshal(bl1))

	box.SetLastPoint(mustNewLastPoint(bl0.Voteproof().Point(), true, false))

	voted, vp, err := box.voteAndWait(bl0, th)
	t.NoError(err)
	t.True(voted)
	t.Nil(vp)

	voted, vps, err := box.voteAndWait(bl1, th)
	t.NoError(err)
	t.True(voted)
	t.NotEmpty(vps)

	t.T().Log("voteproof:", t.StringMarshal(vps))

	for i := range vps {
		t.NoError(isaac.IsValidVoteproofWithSuffrage(vps[i], suf))
	}
}

func (t *testBallotboxWithWithdraw) TestINITBallotWithdrawOthers() {
	suf, nodes := isaac.NewTestSuffrage(3)
	th := base.DefaultThreshold

	local := nodes[0]
	withdrawnodes := []base.Address{nodes[1].Address(), nodes[2].Address()}

	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	withdraws := t.withdraws(point.Height()-1, withdrawnodes, nodes)

	t.T().Log("withdraw:", t.StringMarshal(withdraws))

	bl := t.initBallot(local, suf.Locals(), point, prev, pr, withdraws, nil)
	t.NoError(bl.IsValid(t.networkID))

	t.T().Log("ballot local:", t.StringMarshal(bl))

	t.Run("local box", func() {
		box := NewBallotbox(
			local.Address(),
			func(base.Height) (base.Suffrage, bool, error) {
				return suf, true, nil
			},
		)
		box.SetLastPoint(mustNewLastPoint(bl.Voteproof().Point(), true, false))

		voted, vps, err := box.voteAndWait(bl, th)
		t.NoError(err)
		t.True(voted)
		t.NotEmpty(vps)

		t.T().Log("voteproof:", t.StringMarshal(vps))

		for i := range vps {
			t.NoError(isaac.IsValidVoteproofWithSuffrage(vps[i], suf))
		}
	})

	t.Run("other box", func() {
		box := NewBallotbox(
			withdrawnodes[0],
			func(base.Height) (base.Suffrage, bool, error) {
				return suf, true, nil
			},
		)
		box.SetLastPoint(mustNewLastPoint(bl.Voteproof().Point(), true, false))

		voted, vp, err := box.voteAndWait(bl, th)
		t.NoError(err)
		t.True(voted)
		t.Nil(vp)
	})
}

func (t *testBallotboxWithWithdraw) TestACCEPTBallot() {
	suf, nodes := isaac.NewTestSuffrage(3)
	th := base.DefaultThreshold

	local := nodes[0]
	withdrawnode := nodes[1]
	other := nodes[2]
	newsuf, err := isaac.NewSuffrage([]base.Node{local, other})
	t.NoError(err)

	box := NewBallotbox(
		local.Address(),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 0)
	pr := valuehash.RandomSHA256()
	newblock := valuehash.RandomSHA256()

	withdraws := t.withdraws(point.Height()-1, []base.Address{withdrawnode.Address()}, nodes)

	t.T().Log("withdraw:", t.StringMarshal(withdraws))

	bl0 := t.acceptBallot(local, newsuf.Locals(), point, pr, newblock, withdraws)
	t.NoError(bl0.IsValid(t.networkID))

	t.T().Log("ballot local:", t.StringMarshal(bl0))

	bl1 := t.acceptBallot(other, newsuf.Locals(), point, pr, newblock, withdraws)
	t.NoError(bl1.IsValid(t.networkID))

	t.T().Log("ballot other:", t.StringMarshal(bl1))

	box.SetLastPoint(mustNewLastPoint(bl0.Voteproof().Point(), true, false))

	voted, vp, err := box.voteAndWait(bl0, th)
	t.NoError(err)
	t.True(voted)
	t.Nil(vp)

	voted, vps, err := box.voteAndWait(bl1, th)
	t.NoError(err)
	t.True(voted)
	t.NotEmpty(vps)

	t.T().Log("voteproof:", t.StringMarshal(vps))

	for i := range vps {
		t.NoError(isaac.IsValidVoteproofWithSuffrage(vps[i], suf))
	}
}

func (t *testBallotboxWithWithdraw) TestACCEPTBallotWithdrawOthers() {
	suf, nodes := isaac.NewTestSuffrage(3)
	th := base.DefaultThreshold

	local := nodes[0]
	withdrawnodes := []base.Address{nodes[1].Address(), nodes[2].Address()}
	newsuf, err := isaac.NewSuffrage([]base.Node{local})
	t.NoError(err)

	point := base.RawPoint(33, 0)
	pr := valuehash.RandomSHA256()
	newblock := valuehash.RandomSHA256()

	withdraws := t.withdraws(point.Height()-1, withdrawnodes, nodes)

	t.T().Log("withdraw:", t.StringMarshal(withdraws))

	bl := t.acceptBallot(local, newsuf.Locals(), point, pr, newblock, withdraws)
	t.NoError(bl.IsValid(t.networkID))

	t.T().Log("ballot local:", t.StringMarshal(bl))

	t.Run("local box:", func() {
		box := NewBallotbox(
			local.Address(),
			func(base.Height) (base.Suffrage, bool, error) {
				return suf, true, nil
			},
		)

		box.SetLastPoint(mustNewLastPoint(bl.Voteproof().Point(), true, false))

		voted, vps, err := box.voteAndWait(bl, th)
		t.NoError(err)
		t.True(voted)
		t.NotEmpty(vps)

		t.T().Log("voteproof:", t.StringMarshal(vps))

		for i := range vps {
			t.NoError(isaac.IsValidVoteproofWithSuffrage(vps[i], suf))
		}
	})

	t.Run("other box:", func() {
		box := NewBallotbox(
			withdrawnodes[0],
			func(base.Height) (base.Suffrage, bool, error) {
				return suf, true, nil
			},
		)

		box.SetLastPoint(mustNewLastPoint(bl.Voteproof().Point(), true, false))

		voted, vp, err := box.voteAndWait(bl, th)
		t.NoError(err)
		t.True(voted)
		t.Nil(vp)
	})
}

func (t *testBallotboxWithWithdraw) TestINITBallotJointWithdrawsOverThreshold() {
	suf, nodes := isaac.NewTestSuffrage(20)
	th := base.DefaultThreshold

	local := nodes[0]
	withdrawnodes := []base.Address{
		nodes[10].Address(),
		nodes[11].Address(),
		nodes[12].Address(),
		nodes[13].Address(),
		nodes[14].Address(),
		nodes[15].Address(),
		nodes[16].Address(),
		nodes[17].Address(),
		nodes[18].Address(),
		nodes[19].Address(),
	}

	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	fullsigned := t.withdraws(point.Height()-1, withdrawnodes[:1], nodes[:13])
	notfullsigned := t.withdraws(point.Height()-1, withdrawnodes[1:], nodes[:10])

	var withdraws []base.SuffrageWithdrawOperation
	withdraws = append(withdraws, fullsigned...)
	withdraws = append(withdraws, notfullsigned...)

	t.T().Log("full signed withdraw:", t.StringMarshal(fullsigned))
	t.T().Log("not full signed withdraw:", t.StringMarshal(notfullsigned))

	box := NewBallotbox(
		local.Address(),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	box.SetLastPoint(mustNewLastPoint(base.NewStagePoint(point.PrevHeight(), base.StageACCEPT), true, false))

	expected := 9

	var vp base.Voteproof

	for i := range nodes {
		node := nodes[i]

		if util.InSliceFunc(withdrawnodes, func(addr base.Address) bool {
			return addr.Equal(node.Address())
		}) >= 0 {
			break
		}

		bl := t.initBallot(node, suf.Locals(), point, prev, pr, withdraws, nil)
		t.NoError(bl.IsValid(t.networkID))
		voted, vps, err := box.voteAndWait(bl, th)
		t.NoError(err)

		var ivp base.Voteproof
		if len(vps) > 0 {
			ivp = vps[0]
		}

		t.T().Logf("voted: %-2d, voted=%-5v vp=%-5v err=%v\n", i, voted, ivp == nil, err)

		switch {
		case i < expected:
			t.True(voted)
			t.Nil(ivp)
		case i == expected:
			t.True(voted)
			t.NotNil(ivp)

			vp = ivp
		default:
			t.False(voted)
			t.Nil(ivp)
		}
	}

	t.T().Log("voteproof:", t.StringMarshal(vp))

	t.NoError(isaac.IsValidVoteproofWithSuffrage(vp, suf))
}

func (t *testBallotboxWithWithdraw) TestINITBallotJointWithdrawsSafeThreshold() {
	suf, nodes := isaac.NewTestSuffrage(20)
	th := base.DefaultThreshold

	local := nodes[0]
	withdrawnodes := []base.Address{
		nodes[14].Address(),
		nodes[15].Address(),
		nodes[16].Address(),
		nodes[17].Address(),
		nodes[18].Address(),
		nodes[19].Address(),
	}

	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	fullsigned := t.withdraws(point.Height()-1, withdrawnodes, nodes[:15])

	var withdraws []base.SuffrageWithdrawOperation
	withdraws = append(withdraws, fullsigned...)

	t.T().Log("full signed withdraw:", t.StringMarshal(fullsigned))

	box := NewBallotbox(
		local.Address(),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	box.SetLastPoint(mustNewLastPoint(base.NewStagePoint(point.PrevHeight(), base.StageACCEPT), true, false))

	expected := 13

	var vp base.Voteproof

	for i := range nodes {
		node := nodes[i]

		if util.InSliceFunc(withdrawnodes, func(addr base.Address) bool {
			return addr.Equal(node.Address())
		}) >= 0 {
			break
		}

		bl := t.initBallot(node, suf.Locals(), point, prev, pr, withdraws, nil)
		t.NoError(bl.IsValid(t.networkID))
		voted, vps, err := box.voteAndWait(bl, th)
		t.NoError(err)

		var ivp base.Voteproof
		if len(vps) > 0 {
			ivp = vps[0]
		}

		t.T().Logf("voted: %-2d, voted=%-5v vp=%-5v err=%v\n", i, voted, ivp == nil, err)

		switch {
		case i < expected:
			t.True(voted)
			t.Nil(ivp)
		case i == expected:
			t.True(voted)
			t.NotNil(ivp)

			vp = ivp
		default:
			t.False(voted)
			t.Nil(ivp)
		}
	}

	t.T().Log("voteproof:", t.StringMarshal(vp))

	t.NoError(isaac.IsValidVoteproofWithSuffrage(vp, suf))
}

func (t *testBallotboxWithWithdraw) TestINITBallotButDraw() {
	suf, nodes := isaac.NewTestSuffrage(3)
	th := base.DefaultThreshold

	local := nodes[0]
	withdrawnode := nodes[1]
	other := nodes[2]

	box := NewBallotbox(
		local.Address(),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)
	box.SetCountAfter(1)

	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	withdraws := t.withdraws(point.Height()-1, []base.Address{withdrawnode.Address()}, nodes)

	t.T().Log("withdraw:", t.StringMarshal(withdraws))

	bl0 := t.initBallot(local, suf.Locals(), point, prev, pr, withdraws, nil)
	t.NoError(bl0.IsValid(t.networkID))

	t.T().Log("ballot local:", t.StringMarshal(bl0))

	bl1 := t.initBallot(other, suf.Locals(), point, prev, pr, nil, nil)
	t.NoError(bl1.IsValid(t.networkID))

	t.T().Log("ballot other:", t.StringMarshal(bl1))

	box.SetLastPoint(mustNewLastPoint(bl0.Voteproof().Point(), true, false))

	voted, vp, err := box.voteAndWait(bl0, th)
	t.NoError(err)
	t.True(voted)
	t.Nil(vp)

	voted, vp, err = box.voteAndWait(bl1, th)
	t.NoError(err)
	t.True(voted)
	t.Nil(vp)

	box.countHoldeds()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.Equal(base.VoteResultDraw, vp.Result())
	}
}

func (t *testBallotboxWithWithdraw) TestSuffrageConfirmBallots() {
	point := base.RawPoint(33, 0)

	suf, nodes := isaac.NewTestSuffrage(3)
	th := base.DefaultThreshold

	local := nodes[0]
	withdrawnode := nodes[2]

	box := NewBallotbox(
		local.Address(),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	withdraws := t.withdraws(point.Height()-1, []base.Address{withdrawnode.Address()}, nodes)
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdrawfacts {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()
	ifact := isaac.NewINITBallotFact(point, prev, pr, withdrawfacts)
	isfs := make([]base.BallotSignFact, len(nodes)-1)

	var n int
	for i := range nodes {
		node := nodes[i]
		if node.Address().Equal(withdrawnode.Address()) {
			continue
		}
		sf := isaac.NewINITBallotSignFact(ifact)
		t.NoError(sf.NodeSign(node.Privatekey(), t.networkID, node.Address()))

		isfs[n] = sf
		n++
	}

	// expected majority init voteproof with withdraws
	ivp := isaac.NewINITWithdrawVoteproof(point)
	_ = ivp.
		SetSignFacts(isfs).
		SetMajority(ifact).
		SetThreshold(th)
	ivp.SetWithdraws(withdraws)
	ivp.Finish()

	box.SetLastPoint(mustNewLastPoint(ivp.Point(), true, false))

	sfact := isaac.NewSuffrageConfirmBallotFact(ifact.Point().Point, ifact.PreviousBlock(), ifact.Proposal(), ifact.WithdrawFacts())
	t.NoError(sfact.IsValid(nil))

	go func() {
		for i := range nodes {
			node := nodes[i]
			if node.Address().Equal(withdrawnode.Address()) {
				continue
			}

			sf := isaac.NewINITBallotSignFact(sfact)
			t.NoError(sf.NodeSign(node.Privatekey(), t.networkID, node.Address()))

			bl := isaac.NewINITBallot(ivp, sf, nil)
			// t.T().Log("ballot:", t.StringMarshal(bl))

			voted, err := box.Vote(bl, th)
			if err != nil {
				panic(err)
			}
			if !voted {
				panic(errors.Errorf("not voted: %d", i))
			}
		}
	}()

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.NotNil(vp.Majority())
		t.IsType(isaac.SuffrageConfirmBallotFact{}, vp.Majority())
		t.True(vp.Point().Equal(ivp.Point()))

		// t.T().Log("#1 voteproof:", t.StringMarshal(vp))
	}
}

func (t *testBallotboxWithWithdraw) TestVoteproofFromSuffrageConfirmBallots() {
	point := base.RawPoint(33, 0)

	suf, nodes := isaac.NewTestSuffrage(3)
	th := base.DefaultThreshold

	local := nodes[0]
	withdrawnode := nodes[2]

	box := NewBallotbox(
		local.Address(),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	withdraws := t.withdraws(point.Height()-1, []base.Address{withdrawnode.Address()}, nodes)
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdrawfacts {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()
	ifact := isaac.NewINITBallotFact(point, prev, pr, withdrawfacts)
	isfs := make([]base.BallotSignFact, len(nodes)-1)

	var n int
	for i := range nodes {
		node := nodes[i]
		if node.Address().Equal(withdrawnode.Address()) {
			continue
		}
		sf := isaac.NewINITBallotSignFact(ifact)
		t.NoError(sf.NodeSign(node.Privatekey(), t.networkID, node.Address()))

		isfs[n] = sf
		n++
	}

	// expected majority init voteproof with withdraws
	ivp := isaac.NewINITWithdrawVoteproof(point)
	_ = ivp.
		SetSignFacts(isfs).
		SetMajority(ifact).
		SetThreshold(th)
	ivp.SetWithdraws(withdraws)
	ivp.Finish()

	// NOTE set last stage point is not init marjoity
	box.SetLastPoint(mustNewLastPoint(base.NewStagePoint(point.NextRound(), base.StageINIT), false, false))

	sfact := isaac.NewSuffrageConfirmBallotFact(point, prev, valuehash.RandomSHA256(), withdrawfacts)
	t.NoError(sfact.IsValid(nil))

	go func() {
		for i := range nodes {
			node := nodes[i]
			if node.Address().Equal(withdrawnode.Address()) {
				continue
			}

			sf := isaac.NewINITBallotSignFact(sfact)
			t.NoError(sf.NodeSign(node.Privatekey(), t.networkID, node.Address()))

			bl := isaac.NewINITBallot(ivp, sf, nil)
			// t.T().Log("ballot:", t.StringMarshal(bl))

			voted, err := box.Vote(bl, th)
			if err != nil {
				panic(err)
			}
			if !voted {
				panic(errors.Errorf("not voted: %d", i))
			}
		}
	}()

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		// NOTE init voteproof from suffrage confirm ballot
		t.NoError(vp.IsValid(t.networkID))

		t.NotNil(vp.Majority())
		t.IsType(isaac.INITBallotFact{}, vp.Majority())
		t.True(vp.Point().Equal(ivp.Point()))
		t.Equal(ivp.ID(), vp.ID())

		// t.T().Log("#1 voteproof:", t.StringMarshal(vp))
	}
}

func (t *testBallotboxWithWithdraw) TestVoteproofWithWithdraws() {
	t.Run("not yet finished", func() {
		point := base.RawPoint(33, 0)
		suf, nodes := isaac.NewTestSuffrage(3)
		withdrawnode := nodes[2]

		th := base.Threshold(67)

		withdraws := t.withdraws(point.Height(), []base.Address{withdrawnode.Address()}, nodes)
		withdrawfacts := make([]util.Hash, len(withdraws))
		for i := range withdrawfacts {
			withdrawfacts[i] = withdraws[i].Fact().Hash()
		}

		box := NewBallotbox(
			base.RandomAddress(""),
			func(base.Height) (base.Suffrage, bool, error) {
				return suf, true, nil
			},
		)

		prev := valuehash.RandomSHA256()
		pr := valuehash.RandomSHA256()

		for i := range nodes {
			node := nodes[i]
			if node.Address().Equal(withdrawnode.Address()) {
				continue
			}

			bl := t.initBallot(nodes[i], suf.Locals(), point, prev, pr, nil, nil)

			voted, err := box.Vote(bl, th)
			t.NoError(err)
			t.True(voted)
		}

		vp, err := box.StuckVoteproof(base.NewStagePoint(point, base.StageINIT), th, withdraws)
		t.NoError(err)
		t.NotNil(vp)

		t.NoError(vp.IsValid(t.networkID))

		svp, ok := vp.(base.StuckVoteproof)
		t.True(ok)

		t.True(svp.IsStuckVoteproof())

		t.T().Log("voteproof:", t.StringMarshal(vp))
	})

	t.Run("finished", func() {
		point := base.RawPoint(33, 0)
		suf, nodes := isaac.NewTestSuffrage(3)
		withdrawnode := nodes[2]

		th := base.Threshold(100)

		withdraws := t.withdraws(point.Height(), []base.Address{withdrawnode.Address()}, nodes)
		withdrawfacts := make([]util.Hash, len(withdraws))
		for i := range withdrawfacts {
			withdrawfacts[i] = withdraws[i].Fact().Hash()
		}

		box := NewBallotbox(
			base.RandomAddress(""),
			func(base.Height) (base.Suffrage, bool, error) {
				return suf, true, nil
			},
		)

		prev := valuehash.RandomSHA256()
		pr := valuehash.RandomSHA256()

		for i := range nodes {
			bl := t.initBallot(nodes[i], suf.Locals(), point, prev, pr, nil, nil)

			voted, err := box.Vote(bl, th)
			t.NoError(err)
			t.True(voted)
		}

		vp, err := box.StuckVoteproof(base.NewStagePoint(point, base.StageINIT), th, withdraws)
		t.NoError(err)
		t.Nil(vp)
	})
}

func TestBallotboxWithWithdraw(t *testing.T) {
	suite.Run(t, new(testBallotboxWithWithdraw))
}
