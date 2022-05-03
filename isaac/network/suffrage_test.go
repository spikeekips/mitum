package isaacnetwork

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testSuffrageChecker struct {
	suite.Suite
	newci func() quictransport.BaseConnInfo
}

func (t *testSuffrageChecker) SetupSuite() {
	t.newci = quictransport.RandomConnInfoGenerator()
}

func (t *testSuffrageChecker) TestNew() {
	info := newSuffrageInfo(base.Height(33), 3, 3)

	lastSuffrageFunc := func(context.Context, quictransport.ConnInfo) (base.SuffrageInfo, bool, error) {
		return info, true, nil
	}

	dis := []quictransport.ConnInfo{t.newci(), t.newci()}

	checker, err := NewSuffrageChecker(dis, nil, lastSuffrageFunc)
	t.NoError(err)
	t.NotNil(checker)

	rinfo, updated, err := checker.Check(context.Background())
	t.NoError(err)
	t.True(updated)
	t.NotNil(rinfo)

	base.EqualSuffrageInfo(t.Assert(), info, rinfo)
	base.EqualSuffrageInfo(t.Assert(), info, checker.SuffrageInfo())
}

func (t *testSuffrageChecker) TestInitialInfo() {
	initialinfo := newSuffrageInfo(base.Height(33), 3, 3)
	info := newSuffrageInfo(initialinfo.Height()-1, 3, 3)

	lastSuffrageFunc := func(context.Context, quictransport.ConnInfo) (base.SuffrageInfo, bool, error) {
		return info, true, nil
	}

	dis := []quictransport.ConnInfo{t.newci(), t.newci()}

	checker, err := NewSuffrageChecker(dis, initialinfo, lastSuffrageFunc)
	t.NoError(err)
	t.NotNil(checker)

	rinfo, updated, err := checker.Check(context.Background())
	t.NoError(err)
	t.False(updated)
	t.NotNil(rinfo)

	base.EqualSuffrageInfo(t.Assert(), initialinfo, rinfo)
	base.EqualSuffrageInfo(t.Assert(), initialinfo, checker.SuffrageInfo())
}

func (t *testSuffrageChecker) TestTopHeight() {
	infos := map[string]base.SuffrageInfo{}
	dis := make([]quictransport.ConnInfo, 11)

	var top base.SuffrageInfo
	for i := range make([]int64, len(dis)) {
		ci := t.newci()
		dis[i] = ci
		info := newSuffrageInfo(base.Height(i), 3, 3)
		infos[ci.String()] = info

		top = info
	}

	lastSuffrageFunc := func(_ context.Context, ci quictransport.ConnInfo) (base.SuffrageInfo, bool, error) {
		s := ci.(quictransport.BaseConnInfo).String()

		info, found := infos[s]
		if !found {
			return nil, false, errors.Errorf("unknown conninfo, %q", s)
		}

		return info, true, nil
	}

	checker, err := NewSuffrageChecker(dis, nil, lastSuffrageFunc)
	t.NoError(err)
	t.NotNil(checker)

	rinfo, updated, err := checker.Check(context.Background())
	t.NoError(err)
	t.True(updated)
	t.NotNil(rinfo)

	base.EqualSuffrageInfo(t.Assert(), top, rinfo)
	base.EqualSuffrageInfo(t.Assert(), top, checker.SuffrageInfo())
}

func (t *testSuffrageChecker) TestCallbacks() {
	infos := map[string]base.SuffrageInfo{}
	dis := make([]quictransport.ConnInfo, 11)

	var top base.SuffrageInfo
	for i := range make([]int64, len(dis)) {
		ci := t.newci()
		dis[i] = ci
		info := newSuffrageInfo(base.Height(i), 3, 3)
		infos[ci.String()] = info

		top = info
	}

	lastSuffrageFunc := func(_ context.Context, ci quictransport.ConnInfo) (base.SuffrageInfo, bool, error) {
		s := ci.(quictransport.BaseConnInfo).String()

		info, found := infos[s]
		if !found {
			return nil, false, errors.Errorf("unknown conninfo, %q", s)
		}

		return info, true, nil
	}

	checker, err := NewSuffrageChecker(dis, nil, lastSuffrageFunc)
	t.NoError(err)
	t.NotNil(checker)
	checker.minIntervalSuffrageChecker = time.Millisecond * 100
	checker.interval = time.Millisecond * 100

	calledch0 := make(chan base.SuffrageInfo, 1)
	calledch1 := make(chan base.SuffrageInfo, 1)
	checker.AddCallback(func(_ context.Context, info base.SuffrageInfo) {
		calledch0 <- info
	})
	checker.AddCallback(func(_ context.Context, info base.SuffrageInfo) {
		calledch1 <- info
	})

	t.NoError(checker.Start())
	defer checker.Stop()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait to notify new SuffrageInfo, but failed"))

		return
	case rinfo := <-calledch0:
		t.NotNil(rinfo)

		base.EqualSuffrageInfo(t.Assert(), top, rinfo)
	}

	rinfo := <-calledch1
	t.NotNil(rinfo)

	base.EqualSuffrageInfo(t.Assert(), top, rinfo)
	base.EqualSuffrageInfo(t.Assert(), top, checker.SuffrageInfo())
}

func TestSuffrageChecker(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testSuffrageChecker))
}

func newSuffrageInfo(height base.Height, nsufs, ncans int) isaac.SuffrageInfo {
	sufs := make([]base.Node, nsufs)
	for i := range sufs {
		sufs[i] = isaac.RandomLocalNode()
	}

	cans := make([]base.SuffrageCandidate, 4)
	for i := range cans {
		cans[i] = isaac.NewSuffrageCandidate(isaac.RandomLocalNode(), base.Height(33), base.Height(55))
	}

	return isaac.NewSuffrageNodesNetworkInfo(valuehash.RandomSHA256(), height, sufs, cans)
}
