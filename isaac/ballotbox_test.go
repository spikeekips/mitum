package isaac

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/semaphore"
)

func (box *Ballotbox) voteAndWait(bl base.Ballot) (base.Voteproof, error) {
	callback, err := box.vote(bl)
	if err != nil {
		return nil, err
	}

	if callback == nil {
		return nil, nil
	}

	return callback(), nil
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

func (t *testBallotbox) initBallot(node base.Address, nodes []base.Address, point base.Point, prev, proposal util.Hash) INITBallot {
	afact := NewACCEPTBallotFact(point.Decrease(), valuehash.RandomSHA256(), prev)

	asfs := make([]base.BallotSignedFact, len(nodes))
	for i := range nodes {
		sf := NewACCEPTBallotSignedFact(nodes[i], afact)
		t.NoError(sf.Sign(t.priv, t.networkID))
		asfs[i] = sf
	}

	avp := NewACCEPTVoteproof(afact.Point().Point)
	avp.SetResult(base.VoteResultMajority).
		SetMajority(afact).
		SetSignedFacts(asfs).
		SetThreshold(base.Threshold(100)).
		finish()

	fact := NewINITBallotFact(point, prev, proposal)

	signedFact := NewINITBallotSignedFact(node, fact)
	t.NoError(signedFact.Sign(t.priv, t.networkID))

	return NewINITBallot(avp, signedFact)
}

func (t *testBallotbox) acceptBallot(node base.Address, nodes []base.Address, point base.Point, pr, block util.Hash) ACCEPTBallot {
	prev := valuehash.RandomSHA256()

	ifact := NewINITBallotFact(point, prev, pr)

	isfs := make([]base.BallotSignedFact, len(nodes))
	for i := range nodes {
		sf := NewINITBallotSignedFact(nodes[i], ifact)
		t.NoError(sf.Sign(t.priv, t.networkID))
		isfs[i] = sf
	}

	ivp := NewINITVoteproof(ifact.Point().Point)
	ivp.SetResult(base.VoteResultMajority).
		SetMajority(ifact).
		SetSignedFacts(isfs).
		SetThreshold(base.Threshold(100)).
		finish()

	fact := NewACCEPTBallotFact(point, pr, block)

	signedFact := NewACCEPTBallotSignedFact(node, fact)

	t.NoError(signedFact.Sign(t.priv, t.networkID))

	return NewACCEPTBallot(ivp, signedFact)
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
	n0 := base.RandomAddress("")

	suf, err := newSuffrage([]base.Address{n0})
	t.NoError(err)
	th := base.Threshold(100)

	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			return suf
		},
		th,
	)

	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()

	bl := t.initBallot(n0, suf.Nodes(), point, prev, valuehash.RandomSHA256())
	box.setLastStagePoint(bl.Voteproof().Point())

	t.NoError(box.Vote(bl))

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(point, vp.Point().Point)
		t.Equal(th, vp.Threshold())

		base.CompareBallotFact(t.Assert(), bl.SignedFact().Fact().(base.BallotFact), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Since(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignedFacts()))
		base.CompareBallotSignedFact(t.Assert(), bl.SignedFact(), vp.SignedFacts()[0])

		t.compareStagePoint(box.lastStagePoint(), vp)
	}
}

func (t *testBallotbox) TestVoteACCEPTBallotSignedFact() {
	n0 := base.RandomAddress("")

	suf, err := newSuffrage([]base.Address{n0})
	t.NoError(err)
	th := base.Threshold(100)

	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			return suf
		},
		th,
	)

	point := base.RawPoint(33, 1)
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl := t.acceptBallot(n0, suf.Nodes(), point, pr, block)
	box.setLastStagePoint(bl.Voteproof().Point())

	t.NoError(box.Vote(bl))

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(point, vp.Point().Point)
		t.Equal(base.StageACCEPT, vp.Point().Stage())
		t.Equal(th, vp.Threshold())

		base.CompareBallotFact(t.Assert(), bl.SignedFact().Fact().(base.BallotFact), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Since(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignedFacts()))
		base.CompareBallotSignedFact(t.Assert(), bl.SignedFact(), vp.SignedFacts()[0])

		t.compareStagePoint(box.lastStagePoint(), vp)
	}
}

func (t *testBallotbox) TestVoteSamePointAndStageWithLastVoteproof() {
	n0 := base.RandomAddress("")
	n1 := base.RandomAddress("")
	n2 := base.RandomAddress("")

	suf, err := newSuffrage([]base.Address{n0, n1, n2})
	t.NoError(err)
	th := base.Threshold(60)

	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			return suf
		},
		th,
	)

	point := base.RawPoint(33, 1)
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl0 := t.acceptBallot(n0, suf.Nodes(), point, pr, block)
	box.setLastStagePoint(bl0.Voteproof().Point())

	t.NoError(box.Vote(bl0))

	bl1 := t.acceptBallot(n1, suf.Nodes(), point, pr, block)
	t.NoError(box.Vote(bl1))

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))

		return
	case vp := <-box.Voteproof():
		t.compareStagePoint(box.lastStagePoint(), vp.Point())
	}

	// vote again
	bl2 := t.acceptBallot(n2, suf.Nodes(), point, pr, block)
	err = box.Vote(bl2)
	t.Error(err)
	t.Contains(err.Error(), "old ballot")
}

func (t *testBallotbox) TestOldBallotSignedFact() {
	n0 := base.RandomAddress("")
	n1 := base.RandomAddress("")

	suf, err := newSuffrage([]base.Address{n0, n1})
	t.NoError(err)
	th := base.Threshold(100)

	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			return suf
		},
		th,
	)

	point := base.RawPoint(33, 1)

	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl0 := t.acceptBallot(n0, suf.Nodes(), point, pr, block)
	box.setLastStagePoint(bl0.Voteproof().Point())

	t.NoError(box.Vote(bl0))
	bl1 := t.acceptBallot(n1, suf.Nodes(), point, pr, block)
	t.NoError(box.Vote(bl1))

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))

		return
	case vp := <-box.Voteproof():
		t.compareStagePoint(box.lastStagePoint(), vp)
	}

	bl01 := t.initBallot(n0, suf.Nodes(), point, valuehash.RandomSHA256(), pr)
	err = box.Vote(bl01)
	t.Error(err)
	t.Contains(err.Error(), "old ballot")
}

func (t *testBallotbox) TestUnknownSuffrageNode() {
	n0 := base.RandomAddress("")
	n1 := base.RandomAddress("")

	suf, err := newSuffrage([]base.Address{n0})
	t.NoError(err)
	th := base.Threshold(100)

	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			return suf
		},
		th,
	)

	point := base.RawPoint(33, 1)
	prev := valuehash.RandomSHA256()

	bl := t.initBallot(n1, suf.Nodes(), point, prev, valuehash.RandomSHA256())
	err = box.Vote(bl)
	t.Error(err)
	t.Contains(err.Error(), "ballot not in suffrage")
}

func (t *testBallotbox) TestNilSuffrage() {
	n0 := base.RandomAddress("")

	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			return nil
		},
		base.Threshold(100),
	)

	point := base.RawPoint(33, 1)
	prev := valuehash.RandomSHA256()

	bl := t.initBallot(n0, []base.Address{n0}, point, prev, valuehash.RandomSHA256())
	t.NoError(box.Vote(bl))

	stagepoint := base.NewStagePoint(point, base.StageINIT)
	vr := box.voterecords(stagepoint)
	t.NotNil(vr)
	t.Equal(stagepoint, vr.stagepoint)

	vbl := vr.ballots[n0.String()]
	base.CompareBallot(t.Assert(), bl, vbl)
}

func (t *testBallotbox) TestNilSuffrageCount() {
	n0 := base.RandomAddress("")

	suf, _ := newSuffrage([]base.Address{n0})
	th := base.Threshold(100)

	var i int64
	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			if atomic.LoadInt64(&i) < 1 {
				atomic.StoreInt64(&i, 1)
				return nil
			}

			return suf
		},
		th,
	)

	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()

	bl := t.initBallot(n0, suf.Nodes(), point, prev, valuehash.RandomSHA256())
	box.setLastStagePoint(bl.Voteproof().Point())

	_, err := box.voteAndWait(bl)
	t.NoError(err)

	go box.Count()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(point, vp.Point().Point)
		t.Equal(base.StageINIT, vp.Point().Stage())
		t.Equal(th, vp.Threshold())

		base.CompareBallotFact(t.Assert(), bl.SignedFact().Fact().(base.BallotFact), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Since(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignedFacts()))
		base.CompareBallotSignedFact(t.Assert(), bl.SignedFact(), vp.SignedFacts()[0])

		t.compareStagePoint(box.lastStagePoint(), vp)
	}
}

func (t *testBallotbox) TestVoteproofOrder() {
	n0 := base.RandomAddress("")
	n1 := base.RandomAddress("")

	suf, _ := newSuffrage([]base.Address{n0, n1})
	th := base.Threshold(100)

	var enablesuf int64
	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			if atomic.LoadInt64(&enablesuf) < 1 {
				return nil
			}

			return suf
		},
		th,
	)

	point := base.RawPoint(33, 22)
	nodes := suf.Nodes()

	// prev prev ACCEPT vote
	ppblock := valuehash.RandomSHA256()
	pppr := valuehash.RandomSHA256()

	for i := range nodes {
		bl := t.acceptBallot(nodes[i], suf.Nodes(), base.NewPoint(point.Height()-1, base.Round(0)), pppr, ppblock)
		vp, err := box.voteAndWait(bl)
		t.NoError(err)
		t.Nil(vp)
	}

	// prev INIT vote
	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	for i := range nodes {
		bl := t.initBallot(nodes[i], suf.Nodes(), point, prev, pr)
		vp, err := box.voteAndWait(bl)
		t.NoError(err)
		t.Nil(vp)
	}

	// prev ACCEPT vote
	block := valuehash.RandomSHA256()

	for i := range nodes {
		bl := t.acceptBallot(nodes[i], suf.Nodes(), point, pr, block)
		vp, err := box.voteAndWait(bl)
		t.NoError(err)
		t.Nil(vp)
	}

	// next INIT vote
	nextpr := valuehash.RandomSHA256()
	nextpoint := base.NewPoint(point.Height()+1, base.Round(0))
	for i := range nodes {
		bl := t.initBallot(nodes[i], suf.Nodes(), nextpoint, block, nextpr)
		vp, err := box.voteAndWait(bl)
		t.NoError(err)
		t.Nil(vp)
	}

	atomic.StoreInt64(&enablesuf, 1)
	box.setLastStagePoint(base.NewStagePoint(point, base.StageINIT))

	go box.Count()

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
	n0 := base.RandomAddress("")
	n1 := base.RandomAddress("")

	suf, err := newSuffrage([]base.Address{n0, n1})
	t.NoError(err)
	th := base.Threshold(100)

	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			return suf
		},
		th,
	)

	point := base.RawPoint(33, 0)
	prevpoint := point.Decrease()
	prev := valuehash.RandomSHA256()
	pr := valuehash.RandomSHA256()

	bl := t.initBallot(n0, suf.Nodes(), point, prev, pr)
	box.setLastStagePoint(bl.Voteproof().Point().Decrease())

	t.NoError(box.Vote(bl))

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(prevpoint, vp.Point().Point)

		base.CompareVoteproof(t.Assert(), bl.Voteproof(), vp)
	}
}

func (t *testBallotbox) TestVoteproofFromBallotINITVoteproof() {
	n0 := base.RandomAddress("")
	n1 := base.RandomAddress("")

	suf, err := newSuffrage([]base.Address{n0, n1})
	t.NoError(err)
	th := base.Threshold(100)

	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			return suf
		},
		th,
	)

	point := base.RawPoint(33, 0)
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl := t.acceptBallot(n0, suf.Nodes(), point, pr, block)
	box.setLastStagePoint(bl.Voteproof().Point().Decrease())

	t.NoError(box.Vote(bl))

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

	base.CompareVoteproof(t.Assert(), bl.Voteproof(), rvps[0])
}

func (t *testBallotbox) TestVoteproofFromBallotWhenCount() {
	n0 := base.RandomAddress("")
	n1 := base.RandomAddress("")

	suf, err := newSuffrage([]base.Address{n0, n1})
	t.NoError(err)
	th := base.Threshold(100)

	var i int64
	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			if atomic.LoadInt64(&i) < 1 {
				return nil
			}

			return suf
		},
		th,
	)

	point := base.RawPoint(33, 0)
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	bl := t.acceptBallot(n0, suf.Nodes(), point, pr, block)

	vp, err := box.voteAndWait(bl)
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
	go box.Count()

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

	base.CompareVoteproof(t.Assert(), bl.Voteproof(), rvps[0])
}

func (t *testBallotbox) TestAsyncVoterecords() {
	var max int64 = 500
	nodes := make([]base.Address, max+2)
	for i := range nodes {
		nodes[i] = base.RandomAddress("")
	}

	suf, _ := newSuffrage(nodes)
	th := base.Threshold(100)
	stagepoint := base.NewStagePoint(base.RawPoint(33, 44), base.StageINIT)
	vr := newVoterecords(stagepoint, nil, func(base.Height) base.Suffrage { return suf }, th)

	ctx := context.TODO()
	sem := semaphore.NewWeighted(300)

	for i := range make([]int, max) {
		if err := sem.Acquire(ctx, 1); err != nil {
			panic(err)
		}

		go func(i int) {
			defer sem.Release(1)

			bl := t.initBallot(nodes[i], nil, stagepoint.Point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
			_, _ = vr.vote(bl)

			if i%3 == 0 {
				_ = vr.count(base.ZeroStagePoint)
			}
		}(i)
	}

	if err := sem.Acquire(ctx, 300); err != nil {
		panic(err)
	}
}

func (t *testBallotbox) TestAsyncVoteAndClean() {
	var max int64 = 500
	th := base.Threshold(10)

	nodes := make([]base.Address, max)
	for i := range nodes {
		nodes[i] = base.RandomAddress("")
	}

	suf, _ := newSuffrage(nodes)

	box := NewBallotbox(func(base.Height) base.Suffrage {
		return suf
	}, th)
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
			box.Vote(bl)

			if i%2 == 0 {
				go box.Count()
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
