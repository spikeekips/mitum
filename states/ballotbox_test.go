package states

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

type testBallotbox struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testBallotbox) SetupSuite() {
	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testBallotbox) initBallot(node base.Address, point base.Point, prev util.Hash) INITBallotSignedFact {
	fact := NewINITBallotFact(point, prev)

	signedFact := NewINITBallotSignedFact(node, fact)
	t.NoError(signedFact.Sign(t.priv, t.networkID))

	return signedFact
}

func (t *testBallotbox) acceptBallot(node base.Address, point base.Point, pr, block util.Hash) ACCEPTBallotSignedFact {
	fact := NewACCEPTBallotFact(point, pr, block)
	signedFact := NewACCEPTBallotSignedFact(node, fact)
	t.NoError(signedFact.Sign(t.priv, t.networkID))

	return signedFact
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

	point := base.NewPoint(base.Height(33), base.Round(1))
	prev := valuehash.RandomSHA256()

	sf := t.initBallot(n0, point, prev)
	t.NoError(box.Vote(sf))

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(point, vp.Point().Point)
		t.Equal(base.StageINIT, vp.Stage())
		t.Equal(th, vp.Threshold())

		base.CompareBallotFact(t.Assert(), sf.BallotFact(), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Now().Sub(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignedFacts()))
		base.CompareBallotSignedFact(t.Assert(), sf, vp.SignedFacts()[0])

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

	point := base.NewPoint(base.Height(33), base.Round(1))
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	sf := t.acceptBallot(n0, point, pr, block)
	t.NoError(box.Vote(sf))

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(point, vp.Point().Point)
		t.Equal(base.StageACCEPT, vp.Stage())
		t.Equal(th, vp.Threshold())

		base.CompareBallotFact(t.Assert(), sf.BallotFact(), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Now().Sub(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignedFacts()))
		base.CompareBallotSignedFact(t.Assert(), sf, vp.SignedFacts()[0])

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

	point := base.NewPoint(base.Height(33), base.Round(1))
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	sf0 := t.acceptBallot(n0, point, pr, block)
	t.NoError(box.Vote(sf0))
	sf1 := t.acceptBallot(n1, point, pr, block)
	t.NoError(box.Vote(sf1))

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))

		return
	case vp := <-box.Voteproof():
		t.compareStagePoint(box.lastStagePoint(), vp)
	}

	// vote again
	sf2 := t.acceptBallot(n2, point, pr, block)
	err = box.Vote(sf2)
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

	point := base.NewPoint(base.Height(33), base.Round(1))
	pr := valuehash.RandomSHA256()
	block := valuehash.RandomSHA256()

	sf0 := t.acceptBallot(n0, point, pr, block)
	t.NoError(box.Vote(sf0))
	sf1 := t.acceptBallot(n1, point, pr, block)
	t.NoError(box.Vote(sf1))

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))

		return
	case vp := <-box.Voteproof():
		t.compareStagePoint(box.lastStagePoint(), vp)
	}

	sf01 := t.initBallot(n0, point, valuehash.RandomSHA256())
	err = box.Vote(sf01)
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

	point := base.NewPoint(base.Height(33), base.Round(1))
	prev := valuehash.RandomSHA256()

	sf := t.initBallot(n1, point, prev)
	err = box.Vote(sf)
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

	point := base.NewPoint(base.Height(33), base.Round(1))
	prev := valuehash.RandomSHA256()

	sf := t.initBallot(n0, point, prev)
	t.NoError(box.Vote(sf))

	stagepoint := base.NewStagePoint(point, base.StageINIT)
	vr := box.voterecords(stagepoint)
	t.NotNil(vr)
	t.Equal(stagepoint, vr.stagepoint())

	vsf := vr.voted[n0.String()]
	base.CompareBallotSignedFact(t.Assert(), sf, vsf)
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

	point := base.NewPoint(base.Height(33), base.Round(1))
	prev := valuehash.RandomSHA256()

	sf := t.initBallot(n0, point, prev)
	t.NoError(box.Vote(sf))

	box.Count()

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to wait voteproof"))
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))

		t.Equal(point, vp.Point().Point)
		t.Equal(base.StageINIT, vp.Stage())
		t.Equal(th, vp.Threshold())

		base.CompareBallotFact(t.Assert(), sf.BallotFact(), vp.Majority())
		t.Equal(base.VoteResultMajority, vp.Result())

		t.True(time.Now().Sub(vp.FinishedAt()) < time.Millisecond*800)
		t.Equal(1, len(vp.SignedFacts()))
		base.CompareBallotSignedFact(t.Assert(), sf, vp.SignedFacts()[0])

		t.compareStagePoint(box.lastStagePoint(), vp)
	}
}

func (t *testBallotbox) TestCleanUnknownNodes() {
	n0 := base.RandomAddress("")
	n1 := base.RandomAddress("")

	suf, _ := newSuffrage([]base.Address{n0})

	var i int64
	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			if atomic.LoadInt64(&i) < 1 {
				atomic.StoreInt64(&i, 1)
				return nil
			}

			return suf
		},
		base.Threshold(100),
	)

	point := base.NewPoint(base.Height(33), base.Round(1))
	prev := valuehash.RandomSHA256()

	sf := t.initBallot(n1, point, prev)
	t.NoError(box.Vote(sf))

	t.Equal(1, len(box.vrs))

	stagepoint := base.NewStagePoint(point, base.StageINIT)
	vr := box.voterecords(stagepoint)

	box.Count()

	// NOTE unknown suffrage node removed from voterecords
	vr.RLock()
	t.Equal(0, len(vr.voted))
	vr.RUnlock()
}

// BLOCK async voting

func (t *testBallotbox) TestAsyncVoterecords() {
	stagepoint := base.NewStagePoint(base.NewPoint(base.Height(33), base.Round(44)), base.StageINIT)
	vr := newVoterecords(stagepoint)

	var max int64 = 500
	th := base.Threshold(100)

	ctx := context.TODO()
	sem := semaphore.NewWeighted(300)

	nodes := make([]base.Address, max+2)
	for i := range nodes {
		nodes[i] = base.RandomAddress("")
	}

	suf, _ := newSuffrage(nodes)

	for i := range make([]int, max) {
		if err := sem.Acquire(ctx, 1); err != nil {
			panic(err)
		}

		go func(i int) {
			defer sem.Release(1)

			sf := t.initBallot(nodes[i], stagepoint.Point, valuehash.RandomSHA256())
			vr.vote(sf)

			if i%3 == 0 {
				vp := vr.count(suf, th)
				t.Nil(vp)
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

	ctx := context.TODO()
	sem := semaphore.NewWeighted(300)

	prev := valuehash.RandomSHA256()

	for i := range make([]int, max) {
		if err := sem.Acquire(ctx, 1); err != nil {
			panic(err)
		}

		go func(i int) {
			defer sem.Release(1)

			point := base.NewPoint(base.Height(int64(i)%11), base.Round(0))

			sf := t.initBallot(nodes[i], point, prev)
			box.Vote(sf)

			if i%2 == 0 {
				box.Count()
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
