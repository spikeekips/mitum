package isaacnetwork

import (
	"context"
	"io"
	"net"
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
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testSyncSourceChecker) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: NodeChallengeRequestHeaderHint, Instance: NodeChallengeRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SuffrageNodeConnInfoRequestHeaderHint, Instance: SuffrageNodeConnInfoRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SyncSourceConnInfoRequestHeaderHint, Instance: SyncSourceConnInfoRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: NodeConnInfoHint, Instance: NodeConnInfo{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: quicstream.DefaultResponseHeaderHint, Instance: quicstream.DefaultResponseHeader{}}))
}

type handlers struct {
	local                 base.LocalNode
	localParams           base.LocalParams
	encs                  *encoder.Encoders
	idletimeout           time.Duration
	suffrageNodeConnInfof func() ([]isaac.NodeConnInfo, error)
	syncSourceConnInfof   func() ([]isaac.NodeConnInfo, error)
}

func (h *handlers) SuffrageNodeConnInfo(ctx context.Context, addr net.Addr, r io.Reader, w io.Writer, detail quicstream.RequestHeadDetail) error {
	return QuicstreamHandlerSuffrageNodeConnInfo(h.suffrageNodeConnInfof)(ctx, addr, r, w, detail)
}

func (h *handlers) SyncSourceConnInfo(ctx context.Context, addr net.Addr, r io.Reader, w io.Writer, detail quicstream.RequestHeadDetail) error {
	return QuicstreamHandlerSyncSourceConnInfo(h.syncSourceConnInfof)(ctx, addr, r, w, detail)
}

func (h *handlers) NodeChallenge(ctx context.Context, addr net.Addr, r io.Reader, w io.Writer, detail quicstream.RequestHeadDetail) error {
	return QuicstreamHandlerNodeChallenge(h.local, h.localParams)(ctx, addr, r, w, detail)
}

func (t *testSyncSourceChecker) openstreamf(h *handlers) (quicstream.OpenStreamFunc, func()) {
	hr, cw := io.Pipe()
	cr, hw := io.Pipe()

	ph := quicstream.NewPrefixHandler(nil).
		Add(HandlerPrefixSuffrageNodeConnInfo, quicstream.NewHeaderHandler(t.Encs, 0, h.SuffrageNodeConnInfo)).
		Add(HandlerPrefixSyncSourceConnInfo, quicstream.NewHeaderHandler(t.Encs, 0, h.SyncSourceConnInfo)).
		Add(HandlerPrefixNodeChallenge, quicstream.NewHeaderHandler(t.Encs, 0, h.NodeChallenge))

	ci := quicstream.RandomConnInfo()

	handlerf := func() error {
		defer hw.Close()

		if err := ph.Handler(ci.Addr(), hr, hw); err != nil {
			if errors.Is(err, quicstream.ErrHandlerNotFound) {
				go io.ReadAll(cr)
				go io.ReadAll(hr)
			}

			defer func() {
				hw.Close()
			}()

			return quicstream.HeaderWriteHead(context.Background(), hw, t.Enc,
				quicstream.NewDefaultResponseHeader(false, err))
		}

		return nil
	}

	donech := make(chan error, 1)
	go func() {
		donech <- handlerf()
	}()

	return func(context.Context, quicstream.UDPConnInfo) (io.Reader, io.WriteCloser, error) {
			return cr, cw, nil
		}, func() {
			hr.Close()
			hw.Close()
			cr.Close()
			cw.Close()

			<-donech
		}
}

func (t *testSyncSourceChecker) openstreamfs(handlersmap map[string]*handlers) (quicstream.OpenStreamFunc, func()) {
	ops := map[string]quicstream.OpenStreamFunc{}
	cancels := make([]func(), len(handlersmap))

	var n int

	for i := range handlersmap {
		ops[i], cancels[n] = t.openstreamf(handlersmap[i])

		n++
	}

	return func(ctx context.Context, ci quicstream.UDPConnInfo) (io.Reader, io.WriteCloser, error) {
			op, found := ops[ci.String()]
			if !found {
				return nil, nil, errors.Errorf("unknown conn, %q", ci.String())
			}

			return op(ctx, ci)
		}, func() {
			for i := range cancels {
				cancels[i]()
			}
		}
}

func (t *testSyncSourceChecker) ncis(n int) ([]base.LocalNode, []isaac.NodeConnInfo) {
	locals := make([]base.LocalNode, n)
	ncis := make([]isaac.NodeConnInfo, n)
	for i := range ncis {
		local := base.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		locals[i] = local
		ncis[i] = NewNodeConnInfo(local.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure())
	}

	return locals, ncis
}

func (t *testSyncSourceChecker) handlers(n int) ([]isaac.NodeConnInfo, map[string]*handlers) {
	handlersmap := map[string]*handlers{}

	locals, ncis := t.ncis(n)

	for i := range ncis {
		ci, err := ncis[i].UDPConnInfo()
		t.NoError(err)

		handlersmap[ci.String()] = &handlers{localParams: t.LocalParams, local: locals[i], encs: t.Encs, idletimeout: time.Second}
	}

	return ncis, handlersmap
}

func (t *testSyncSourceChecker) TestFetchFromSuffrageNodes() {
	ncis, handlersmap := t.handlers(3)

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := base.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncis = append(ncis, NewNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))
	}

	local := base.RandomLocalNode()
	localci := quicstream.RandomConnInfo()

	handlersmap[localci.String()] = &handlers{local: t.Local, localParams: t.LocalParams, encs: t.Encs, idletimeout: time.Second}
	handlersmap[localci.String()].suffrageNodeConnInfof = func() ([]isaac.NodeConnInfo, error) {
		return ncis, nil
	}

	openstreamf, handlercancel := t.openstreamfs(handlersmap)
	defer handlercancel()

	client := NewBaseClient(t.Encs, t.Enc, openstreamf)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker := NewSyncSourceChecker(local, t.LocalParams.NetworkID(), client, time.Second, t.Enc, nil, nil, nil)

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
	ncis, handlersmap := t.handlers(3)

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := base.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncis = append(ncis, NewNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))
	}

	local := base.RandomLocalNode()
	localci := quicstream.RandomConnInfo()

	handlersmap[localci.String()] = &handlers{local: t.Local, localParams: t.LocalParams, encs: t.Encs, idletimeout: time.Second}
	handlersmap[localci.String()].syncSourceConnInfof = func() ([]isaac.NodeConnInfo, error) {
		return ncis, nil
	}

	openstreamf, handlercancel := t.openstreamfs(handlersmap)
	defer handlercancel()

	client := NewBaseClient(t.Encs, t.Enc, openstreamf)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker := NewSyncSourceChecker(local, t.LocalParams.NetworkID(), client, time.Second, t.Enc, nil, nil, nil)

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
	ncis, handlersmap := t.handlers(2)

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := base.RandomLocalNode()
		ci := quicstream.RandomConnInfo()
		ncis = append(ncis, NewNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))

		handlersmap[ci.String()] = &handlers{local: t.Local, localParams: t.LocalParams, encs: t.Encs, idletimeout: time.Second}
		// NOTE with t.Local instead of node
	}

	local := base.RandomLocalNode()
	localci := quicstream.RandomConnInfo()

	handlersmap[localci.String()] = &handlers{local: t.Local, localParams: t.LocalParams, encs: t.Encs, idletimeout: time.Second}
	handlersmap[localci.String()].suffrageNodeConnInfof = func() ([]isaac.NodeConnInfo, error) {
		return ncis, nil
	}

	openstreamf, handlercancel := t.openstreamfs(handlersmap)
	defer func() {
		handlercancel()
	}()

	client := NewBaseClient(t.Encs, t.Enc, openstreamf)

	checker := NewSyncSourceChecker(local, t.LocalParams.NetworkID(), client, time.Second, t.Enc, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ucis, err := checker.fetch(ctx, SyncSource{Type: SyncSourceTypeSuffrageNodes, Source: localci})
	t.Error(err)
	t.Nil(ucis)

	t.True(errors.Is(err, base.ErrSignatureVerification))
}

func (t *testSyncSourceChecker) httpserver(handler http.HandlerFunc) *httptest.Server {
	ts := httptest.NewUnstartedServer(handler)
	ts.EnableHTTP2 = true
	ts.StartTLS()

	return ts
}

func (t *testSyncSourceChecker) TestFetchFromURL() {
	ncis, handlersmap := t.handlers(2)

	openstreamf, handlercancel := t.openstreamfs(handlersmap)
	defer handlercancel()

	client := NewBaseClient(t.Encs, t.Enc, openstreamf)

	ts := t.httpserver(func(w http.ResponseWriter, r *http.Request) {
		b, err := t.Enc.Marshal(ncis)
		if err != nil {
			w.WriteHeader(500)

			return
		}

		w.Write(b)
	})
	defer ts.Close()

	local := base.RandomLocalNode()
	checker := NewSyncSourceChecker(local, t.LocalParams.NetworkID(), client, time.Second, t.Enc, nil, nil, nil)

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
	ncislocal, handlersmap := t.handlers(3)
	_, ncisurl := t.ncis(3)
	ncisurl = append(ncisurl, ncislocal[0]) // NOTE duplicated

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := base.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncislocal = append(ncislocal, NewNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))
	}
	ncis := make([]isaac.NodeConnInfo, len(ncislocal)+len(ncisurl)-1)
	copy(ncis, ncislocal)
	copy(ncis[len(ncislocal):], ncisurl[:len(ncisurl)-1])

	localci := quicstream.RandomConnInfo()

	handlersmap[localci.String()] = &handlers{local: t.Local, localParams: t.LocalParams, encs: t.Encs, idletimeout: time.Second}
	handlersmap[localci.String()].suffrageNodeConnInfof = func() ([]isaac.NodeConnInfo, error) {
		return ncislocal, nil
	}

	ts := t.httpserver(func(w http.ResponseWriter, r *http.Request) {
		b, err := t.Enc.Marshal(ncisurl)
		if err != nil {
			w.WriteHeader(500)

			return
		}

		w.Write(b)
	})
	defer ts.Close()

	openstreamf, handlercancel := t.openstreamfs(handlersmap)
	defer handlercancel()

	client := NewBaseClient(t.Encs, t.Enc, openstreamf)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	u, _ := url.Parse(ts.URL)
	u.Fragment = "tls_insecure"

	cis := []SyncSource{
		{Type: SyncSourceTypeSuffrageNodes, Source: localci},
		{Type: SyncSourceTypeURL, Source: u},
	}

	local := base.RandomLocalNode()
	checker := NewSyncSourceChecker(local, t.LocalParams.NetworkID(), client, time.Second, t.Enc, cis, nil, nil)

	ucis, err := checker.check(ctx, cis)
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
	ncis, handlersmap := t.handlers(3)

	localci, err := ncis[1].UDPConnInfo()
	t.NoError(err)
	local := handlersmap[localci.String()].local

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := base.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncis = append(ncis, NewNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))
	}

	handlersmap[localci.String()] = &handlers{local: local, localParams: t.LocalParams, encs: t.Encs, idletimeout: time.Second}
	handlersmap[localci.String()].suffrageNodeConnInfof = func() ([]isaac.NodeConnInfo, error) {
		return ncis, nil
	}

	openstreamf, handlercancel := t.openstreamfs(handlersmap)
	defer handlercancel()

	client := NewBaseClient(t.Encs, t.Enc, openstreamf)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cis := []SyncSource{{Type: SyncSourceTypeSuffrageNodes, Source: localci}}

	checker := NewSyncSourceChecker(local, t.LocalParams.NetworkID(), client, time.Second, t.Enc, cis, nil, nil)

	ucis, err := checker.check(ctx, cis)
	t.NoError(err)
	t.NotNil(ucis)

	nciswithoutlocal := util.FilterSlice(ncis, func(i isaac.NodeConnInfo) bool {
		return !i.Address().Equal(local.Address())
	})

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

	local := base.RandomLocalNode()
	checker := NewSyncSourceChecker(local, t.LocalParams.NetworkID(), nil, time.Millisecond*100, t.Enc, nil,
		func(called int64, nics []isaac.NodeConnInfo, err error) {
			calledch <- called
		},
		nil,
	)
	defer checker.Stop()

	t.NoError(checker.Start(context.Background()))

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
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testSyncSourceChecker))
}
