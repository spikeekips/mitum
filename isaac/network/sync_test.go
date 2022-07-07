package isaacnetwork

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testSyncSourceChecker struct {
	isaacdatabase.BaseTestDatabase
	isaac.BaseTestBallots
}

func (t *testSyncSourceChecker) SetupTest() {
	t.BaseTestDatabase.SetupTest()
	t.BaseTestBallots.SetupTest()
}

func (t *testSyncSourceChecker) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: NodeChallengeRequestHeaderHint, Instance: NodeChallengeRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: ResponseHeaderHint, Instance: ResponseHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SuffrageNodeConnInfoRequestHeaderHint, Instance: SuffrageNodeConnInfoRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SyncSourceConnInfoRequestHeaderHint, Instance: SyncSourceConnInfoRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: NodeConnInfoHint, Instance: NodeConnInfo{}}))
}

func (t *testSyncSourceChecker) clientWritef(handlers map[string]*QuicstreamHandlers) func(ctx context.Context, ci quicstream.UDPConnInfo, f quicstream.ClientWriteFunc) (io.ReadCloser, func() error, error) {
	return func(ctx context.Context, ci quicstream.UDPConnInfo, f quicstream.ClientWriteFunc) (io.ReadCloser, func() error, error) {
		r := bytes.NewBuffer(nil)
		if err := f(r); err != nil {
			return nil, nil, errors.WithStack(err)
		}

		uprefix, err := quicstream.ReadPrefix(r)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		var handler quicstream.Handler

		if _, found := handlers[ci.String()]; !found {
			return nil, nil, errors.Errorf("unknown conninfo, %q", ci)
		}

		switch {
		case bytes.Equal(uprefix, quicstream.HashPrefix(HandlerPrefixSuffrageNodeConnInfo)):
			handler = handlers[ci.String()].SuffrageNodeConnInfo
		case bytes.Equal(uprefix, quicstream.HashPrefix(HandlerPrefixSyncSourceConnInfo)):
			handler = handlers[ci.String()].SyncSourceConnInfo
		case bytes.Equal(uprefix, quicstream.HashPrefix(HandlerPrefixNodeChallenge)):
			handler = handlers[ci.String()].NodeChallenge
		default:
			return nil, nil, errors.Errorf("unknown prefix, %q", uprefix)
		}

		w := bytes.NewBuffer(nil)
		if err := handler(nil, r, w); err != nil {
			return nil, nil, errors.Wrap(err, "failed to handle request")
		}

		return io.NopCloser(w), func() error { return nil }, nil
	}
}

func (t *testSyncSourceChecker) ncis(n int) ([]isaac.LocalNode, []isaac.NodeConnInfo) {
	locals := make([]isaac.LocalNode, n)
	ncis := make([]isaac.NodeConnInfo, n)
	for i := range ncis {
		local := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		locals[i] = local
		ncis[i] = NewNodeConnInfo(local.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure())
	}

	return locals, ncis
}

func (t *testSyncSourceChecker) handlers(n int) ([]isaac.NodeConnInfo, map[string]*QuicstreamHandlers) {
	handlers := map[string]*QuicstreamHandlers{}

	locals, ncis := t.ncis(n)

	for i := range ncis {
		ci, err := ncis[i].UDPConnInfo()
		t.NoError(err)

		handlers[ci.String()] = NewQuicstreamHandlers(locals[i], t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	}

	return ncis, handlers
}

func (t *testSyncSourceChecker) TestFetchFromSuffrageNodes() {
	ncis, handlers := t.handlers(3)

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncis = append(ncis, NewNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))
	}

	local := isaac.RandomLocalNode()
	localci := quicstream.RandomConnInfo()

	handlers[localci.String()] = NewQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil, nil,
		func() ([]isaac.NodeConnInfo, error) {
			return ncis, nil
		},
		nil)

	client := newBaseNetworkClient(t.Encs, t.Enc, time.Second, t.clientWritef(handlers))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker := NewSyncSourceChecker(local, t.NodePolicy.NetworkID(), client, time.Second, t.Enc, nil, nil)

	ucis, err := checker.fetch(ctx, SyncSource{Type: SyncSourceTypeSuffrageNodes, Source: localci})
	t.NoError(err)
	t.Equal(len(ncis), len(ucis))

	for i := range ncis {
		ac := ncis[i].(NodeConnInfo)
		bc := ucis[i].(NodeConnInfo)

		t.True(base.IsEqualNode(ac, bc))
		t.Equal(ac.String(), bc.String())
	}
}

func (t *testSyncSourceChecker) TestFetchFromSyncSources() {
	ncis, handlers := t.handlers(3)

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncis = append(ncis, NewNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))
	}

	local := isaac.RandomLocalNode()
	localci := quicstream.RandomConnInfo()

	handlers[localci.String()] = NewQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		func() ([]isaac.NodeConnInfo, error) {
			return ncis, nil
		},
	)

	client := newBaseNetworkClient(t.Encs, t.Enc, time.Second, t.clientWritef(handlers))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker := NewSyncSourceChecker(local, t.NodePolicy.NetworkID(), client, time.Second, t.Enc, nil, nil)

	ucis, err := checker.fetch(ctx, SyncSource{Type: SyncSourceTypeSyncSources, Source: localci})
	t.NoError(err)
	t.Equal(len(ncis), len(ucis))

	for i := range ncis {
		ac := ncis[i].(NodeConnInfo)
		bc := ucis[i].(NodeConnInfo)

		t.True(base.IsEqualNode(ac, bc))
		t.Equal(ac.String(), bc.String())
	}
}

func (t *testSyncSourceChecker) TestFetchFromNodeButFailedToSignature() {
	ncis, handlers := t.handlers(2)

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()
		ncis = append(ncis, NewNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))

		handlers[ci.String()] = NewQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
		// NOTE with t.Local insteadd of node
	}

	local := isaac.RandomLocalNode()
	localci := quicstream.RandomConnInfo()

	handlers[localci.String()] = NewQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil, nil,
		func() ([]isaac.NodeConnInfo, error) {
			return ncis, nil
		},
		nil)

	client := newBaseNetworkClient(t.Encs, t.Enc, time.Second, t.clientWritef(handlers))

	checker := NewSyncSourceChecker(local, t.NodePolicy.NetworkID(), client, time.Second, t.Enc, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ucis, err := checker.fetch(ctx, SyncSource{Type: SyncSourceTypeSuffrageNodes, Source: localci})
	t.Error(err)
	t.Nil(ucis)

	t.True(errors.Is(err, base.SignatureVerificationError))
}

func (t *testSyncSourceChecker) httpserver(handler http.HandlerFunc) *httptest.Server {
	ts := httptest.NewUnstartedServer(handler)
	ts.EnableHTTP2 = true
	ts.StartTLS()

	return ts
}

func (t *testSyncSourceChecker) TestFetchFromURL() {
	ncis, handlers := t.handlers(2)

	client := newBaseNetworkClient(t.Encs, t.Enc, time.Second, t.clientWritef(handlers))

	ts := t.httpserver(func(w http.ResponseWriter, r *http.Request) {
		b, err := t.Enc.Marshal(ncis)
		if err != nil {
			w.WriteHeader(500)

			return
		}

		w.Write(b)
	})
	defer ts.Close()

	local := isaac.RandomLocalNode()
	checker := NewSyncSourceChecker(local, t.NodePolicy.NetworkID(), client, time.Second, t.Enc, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	u, _ := url.Parse(ts.URL)
	u.Fragment = "tls_insecure"

	ucis, err := checker.fetch(ctx, SyncSource{Type: SyncSourceTypeURL, Source: u})
	t.NoError(err)
	t.NotNil(ucis)
	t.Equal(len(ncis), len(ucis))

	for i := range ncis {
		ac := ncis[i].(NodeConnInfo)
		bc := ucis[i].(NodeConnInfo)

		t.True(base.IsEqualNode(ac, bc))
		t.Equal(ac.String(), bc.String())
	}
}

func (t *testSyncSourceChecker) TestCheckSameResult() {
	ncislocal, handlers := t.handlers(3)
	_, ncisurl := t.ncis(3)
	ncisurl = append(ncisurl, ncislocal[0]) // NOTE duplicated

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncislocal = append(ncislocal, NewNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))
	}
	ncis := make([]isaac.NodeConnInfo, len(ncislocal)+len(ncisurl)-1)
	copy(ncis, ncislocal)
	copy(ncis[len(ncislocal):], ncisurl[:len(ncisurl)-1])

	localci := quicstream.RandomConnInfo()

	handlers[localci.String()] = NewQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil, nil,
		func() ([]isaac.NodeConnInfo, error) {
			return ncislocal, nil
		},
		nil)

	ts := t.httpserver(func(w http.ResponseWriter, r *http.Request) {
		b, err := t.Enc.Marshal(ncisurl)
		if err != nil {
			w.WriteHeader(500)

			return
		}

		w.Write(b)
	})
	defer ts.Close()

	client := newBaseNetworkClient(t.Encs, t.Enc, time.Second, t.clientWritef(handlers))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	u, _ := url.Parse(ts.URL)
	u.Fragment = "tls_insecure"

	cis := []SyncSource{
		{Type: SyncSourceTypeSuffrageNodes, Source: localci},
		{Type: SyncSourceTypeURL, Source: u},
	}

	local := isaac.RandomLocalNode()
	checker := NewSyncSourceChecker(local, t.NodePolicy.NetworkID(), client, time.Second, t.Enc, cis, nil)

	ucis, err := checker.check(ctx)
	t.NoError(err)
	t.NotNil(ucis)
	t.Equal(len(ncis), len(ucis))

	for i := range ncis {
		ac := ncis[i].(NodeConnInfo)
		bc := ucis[i].(NodeConnInfo)

		t.True(base.IsEqualNode(ac, bc))
		t.Equal(ac.String(), bc.String())
	}
}

func (t *testSyncSourceChecker) TestCheckFilterLocal() {
	ncis, handlers := t.handlers(3)

	localci, err := ncis[1].UDPConnInfo()
	t.NoError(err)
	local := handlers[localci.String()].local

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncis = append(ncis, NewNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))
	}

	handlers[localci.String()] = NewQuicstreamHandlers(local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil, nil,
		func() ([]isaac.NodeConnInfo, error) {
			return ncis, nil
		},
		nil)

	client := newBaseNetworkClient(t.Encs, t.Enc, time.Second, t.clientWritef(handlers))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cis := []SyncSource{{Type: SyncSourceTypeSuffrageNodes, Source: localci}}

	checker := NewSyncSourceChecker(local, t.NodePolicy.NetworkID(), client, time.Second, t.Enc, cis, nil)

	ucis, err := checker.check(ctx)
	t.NoError(err)
	t.NotNil(ucis)

	li := util.FilterSlices(ncis, func(_ interface{}, i int) bool {
		return !ncis[i].Address().Equal(local.Address())
	})

	nciswithoutlocal := make([]isaac.NodeConnInfo, len(ncis)-1)
	for i := range li {
		nciswithoutlocal[i] = li[i].(isaac.NodeConnInfo)
	}

	t.Equal(len(nciswithoutlocal), len(ucis))

	for i := range nciswithoutlocal {
		ac := nciswithoutlocal[i].(NodeConnInfo)
		bc := ucis[i].(NodeConnInfo)

		t.True(base.IsEqualNode(ac, bc))
		t.Equal(ac.String(), bc.String())
	}
}

func (t *testSyncSourceChecker) TestCalled() {
	calledch := make(chan int64)

	local := isaac.RandomLocalNode()
	checker := NewSyncSourceChecker(local, t.NodePolicy.NetworkID(), nil, time.Millisecond*100, t.Enc, nil,
		func(called int64, nics []isaac.NodeConnInfo, err error) {
			calledch <- called
		},
	)
	defer checker.Stop()

	t.NoError(checker.Start())

	donech := make(chan struct{})
	go func() {
		for called := range calledch {
			if called > 3 {
				break
			}
		}

		donech <- struct{}{}
	}()

	select {
	case <-time.After(time.Second * 2):
		t.Error(errors.Errorf("failed to get enough called count"))
	case <-donech:
	}
}

func TestSyncSourceChecker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testSyncSourceChecker))
}
