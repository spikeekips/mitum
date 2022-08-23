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
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/semaphore"
)

func (box *Ballotbox) voteAndWait(bl base.Ballot, threshold base.Threshold) (bool, base.Voteproof, error) {
	voted, callback, err := box.vote(bl, threshold)
	if err != nil {
		return voted, nil, err
	}

	if callback == nil {
		return voted, nil, nil
	}

	return voted, callback(), nil
}

type testBallotbox struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testBallotbox) SetupSuite() {
	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testBallotbox) initBallot(node isaac.LocalNode, nodes []isaac.LocalNode, point base.Point, prev, proposal util.Hash) isaac.INITBallot {
	afact := isaac.NewACCEPTBallotFact(point.PrevHeight(), valuehash.RandomSHA256(), prev)

	asfs := make([]base.BallotSignedFact, len(nodes))
	for i := range nodes {
		n := nodes[i]
		sf := isaac.NewACCEPTBallotSignedFact(n.Address(), afact)
		t.NoError(sf.Sign(n.Privatekey(), t.networkID))
		asfs[i] = sf
	}

	avp := isaac.NewACCEPTVoteproof(afact.Point().Point)
	avp.SetResult(base.VoteResultMajority).
		SetMajority(afact).
		SetSignedFacts(asfs).
		SetThreshold(base.Threshold(100)).
		Finish()

	fact := isaac.NewINITBallotFact(point, prev, proposal)

	signedFact := isaac.NewINITBallotSignedFact(node.Address(), fact)
	t.NoError(signedFact.Sign(node.Privatekey(), t.networkID))

	return isaac.NewINITBallot(avp, signedFact)
}

func (t *testBallotbox) acceptBallot(node isaac.LocalNode, nodes []isaac.LocalNode, point base.Point, pr, block util.Hash) isaac.ACCEPTBallot {
	prev := valuehash.RandomSHA256()

	ifact := isaac.NewINITBallotFact(point, prev, pr)

	isfs := make([]base.BallotSignedFact, len(nodes))
	for i := range nodes {
		n := nodes[i]
		sf := isaac.NewINITBallotSignedFact(n.Address(), ifact)
		t.NoError(sf.Sign(n.Privatekey(), t.networkID))
		isfs[i] = sf
	}

	ivp := isaac.NewINITVoteproof(ifact.Point().Point)
	ivp.SetResult(base.VoteResultMajority).
		SetMajority(ifact).
		SetSignedFacts(isfs).
		SetThreshold(base.Threshold(100)).
		Finish()

	fact := isaac.NewACCEPTBallotFact(point, pr, block)

	signedFact := isaac.NewACCEPTBallotSignedFact(node.Address(), fact)

	t.NoError(signedFact.Sign(node.Privatekey(), t.networkID))

	return isaac.NewACCEPTBallot(ivp, signedFact)
}

func (t *testBallotbox) compareStagePoint(a base.StagePoint, i interface{}) {
	var b base.StagePoint
	switch t := i.(type) {
	case base.Voteproof:
		b = t.Point()
	case base.StagePoint:
		b = t
	}

	t.Equal(0, a.Compare(b))
}

func (t *testBallotbox) TestVoteINITBallotSignedFact() {
	suf, nodes := isaac.NewTestSuffrage(1)
	th := base.Threshold(100)

	box := NewBallotbox(
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()

	bl := t.initBallot(nodes[0], suf.Locals(), point, prev, valuehash.RandomSHA256())
	box.setLastStagePoint(bl.Voteproof().Point())

	voted, err := box.Vote(bl, th)
	t.NoError(err)
	t.True(voted)

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(point, vp.Point().Point)
		t.Equal(th, vp.Threshold())

		base.EqualBallotFact(t.Assert(), bl.SignedFact().Fact().(base.BallotFact), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Since(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignedFacts()))
		base.EqualBallotSignedFact(t.Assert(), bl.SignedFact(), vp.SignedFacts()[0])

		t.compareStagePoint(box.lastStagePoint(), vp)
	}
}

func (t *testBallotbox) TestVoteACCEPTBallotSignedFact() {
	suf, nodes := isaac.NewTestSuffrage(1)
	th := base.Threshold(100)

	box := NewBallotbox(
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 1)
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl := t.acceptBallot(nodes[0], suf.Locals(), point, pr, block)
	box.setLastStagePoint(bl.Voteproof().Point())

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

		base.EqualBallotFact(t.Assert(), bl.SignedFact().Fact().(base.BallotFact), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Since(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignedFacts()))
		base.EqualBallotSignedFact(t.Assert(), bl.SignedFact(), vp.SignedFacts()[0])

		t.compareStagePoint(box.lastStagePoint(), vp)
	}
}

func (t *testBallotbox) TestVoteSamePointAndStageWithLastVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(3)
	th := base.Threshold(60)

	box := NewBallotbox(
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 1)
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl0 := t.acceptBallot(nodes[0], suf.Locals(), point, pr, block)
	box.setLastStagePoint(bl0.Voteproof().Point())

	voted, err := box.Vote(bl0, th)
	t.NoError(err)
	t.True(voted)

	bl1 := t.acceptBallot(nodes[1], suf.Locals(), point, pr, block)
	voted, err = box.Vote(bl1, th)
	t.NoError(err)
	t.True(voted)

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))

		return
	case vp := <-box.Voteproof():
		t.compareStagePoint(box.lastStagePoint(), vp.Point())
	}

	// vote again
	bl2 := t.acceptBallot(nodes[2], suf.Locals(), point, pr, block)
	voted, err = box.Vote(bl2, th)
	t.NoError(err)
	t.False(voted)
}

func (t *testBallotbox) TestOldBallotSignedFact() {
	suf, nodes := isaac.NewTestSuffrage(2)
	th := base.Threshold(100)

	box := NewBallotbox(
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 1)

	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl0 := t.acceptBallot(nodes[0], suf.Locals(), point, pr, block)
	box.setLastStagePoint(bl0.Voteproof().Point())

	voted, err := box.Vote(bl0, th)
	t.NoError(err)
	t.True(voted)

	bl1 := t.acceptBallot(nodes[1], suf.Locals(), point, pr, block)
	voted, err = box.Vote(bl1, th)
	t.NoError(err)
	t.True(voted)

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))

		return
	case vp := <-box.Voteproof():
		t.compareStagePoint(box.lastStagePoint(), vp)
	}

	bl01 := t.initBallot(nodes[0], suf.Locals(), point, valuehash.RandomSHA256(), pr)
	voted, err = box.Vote(bl01, th)
	t.NoError(err)
	t.False(voted)
}

func (t *testBallotbox) TestUnknownSuffrageNode() {
	suf, _ := isaac.NewTestSuffrage(1)
	th := base.Threshold(100)

	box := NewBallotbox(
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 1)
	prev := valuehash.RandomSHA256()

	unknown := isaac.RandomLocalNode()

	bl := t.initBallot(unknown, suf.Locals(), point, prev, valuehash.RandomSHA256())
	voted, err := box.Vote(bl, th)
	t.NoError(err)
	t.False(voted)
}

func (t *testBallotbox) TestNilSuffrage() {
	n0 := isaac.RandomLocalNode()

	th := base.Threshold(100)
	box := NewBallotbox(
		func(base.Height) (base.Suffrage, bool, error) {
			return nil, false, nil
		},
	)

	point := base.RawPoint(33, 1)
	prev := valuehash.RandomSHA256()

	bl := t.initBallot(n0, []isaac.LocalNode{n0}, point, prev, valuehash.RandomSHA256())
	voted, err := box.Vote(bl, th)
	t.NoError(err)
	t.True(voted)

	stagepoint := base.NewStagePoint(point, base.StageINIT)
	vr := box.voterecords(stagepoint)
	t.NotNil(vr)
	t.Equal(stagepoint, vr.stagepoint)

	vr.Lock()
	defer vr.Unlock()

	vbl := vr.ballots[n0.Address().String()]
	base.EqualBallot(t.Assert(), bl, vbl)
}

func (t *testBallotbox) TestNilSuffrageCount() {
	suf, nodes := isaac.NewTestSuffrage(1)
	th := base.Threshold(100)

	var i int64
	box := NewBallotbox(
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

	bl := t.initBallot(nodes[0], suf.Locals(), point, prev, valuehash.RandomSHA256())
	box.setLastStagePoint(bl.Voteproof().Point())

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

		base.EqualBallotFact(t.Assert(), bl.SignedFact().Fact().(base.BallotFact), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Since(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignedFacts()))
		base.EqualBallotSignedFact(t.Assert(), bl.SignedFact(), vp.SignedFacts()[0])

		t.compareStagePoint(box.lastStagePoint(), vp)
	}
}

func (t *testBallotbox) TestVoteproofOrder() {
	suf, nodes := isaac.NewTestSuffrage(2)
	th := base.Threshold(100)

	var enablesuf int64
	box := NewBallotbox(
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
		bl := t.acceptBallot(nodes[i], suf.Locals(), base.NewPoint(point.Height()-1, base.Round(0)), pppr, ppblock)
		_, vp, err := box.voteAndWait(bl, th)
		t.NoError(err)
		t.Nil(vp)
	}

	// prev INIT vote
	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	for i := range nodes {
		bl := t.initBallot(nodes[i], suf.Locals(), point, prev, pr)
		_, vp, err := box.voteAndWait(bl, th)
		t.NoError(err)
		t.Nil(vp)
	}

	// prev ACCEPT vote
	block := valuehash.RandomSHA256()

	for i := range nodes {
		bl := t.acceptBallot(nodes[i], suf.Locals(), point, pr, block)
		_, vp, err := box.voteAndWait(bl, th)
		t.NoError(err)
		t.Nil(vp)
	}

	// next INIT vote
	nextpr := valuehash.RandomSHA256()
	nextpoint := base.NewPoint(point.Height()+1, base.Round(0))
	for i := range nodes {
		bl := t.initBallot(nodes[i], suf.Locals(), nextpoint, block, nextpr)
		_, vp, err := box.voteAndWait(bl, th)
		t.NoError(err)
		t.Nil(vp)
	}

	atomic.StoreInt64(&enablesuf, 1)
	box.setLastStagePoint(base.NewStagePoint(point, base.StageINIT))

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
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 0)
	prevpoint := point.PrevHeight()
	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	bl := t.initBallot(nodes[0], suf.Locals(), point, prev, pr)
	box.setLastStagePoint(bl.Voteproof().Point().Decrease())

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
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 0)
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl := t.acceptBallot(nodes[0], suf.Locals(), point, pr, block)
	box.setLastStagePoint(bl.Voteproof().Point().Decrease())

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

	bl := t.acceptBallot(nodes[0], suf.Locals(), point, pr, block)

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
	vr := newVoterecords(stagepoint, nil, func(base.Height) (base.Suffrage, bool, error) { return suf, true, nil })

	ctx := context.TODO()
	sem := semaphore.NewWeighted(300)

	for i := range make([]int, max) {
		if err := sem.Acquire(ctx, 1); err != nil {
			panic(err)
		}

		go func(i int) {
			defer sem.Release(1)

			bl := t.initBallot(nodes[i], nil, stagepoint.Point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
			_, _, _ = vr.vote(bl)

			if i%3 == 0 {
				_ = vr.count(base.ZeroStagePoint, th)
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

	box := NewBallotbox(func(base.Height) (base.Suffrage, bool, error) {
		return suf, true, nil
	})
	box.isValidVoteproofWithSuffrage = func(base.Voteproof, base.Suffrage) error {
		return nil
	}

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

			bl := t.initBallot(nodes[i], nil, point, prev, pr)
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

func TestBallotbox(t *testing.T) {
	suite.Run(t, new(testBallotbox))
}
