package isaacnetwork

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

func TestBaseNodeConnInfoEncode(t *testing.T) {
	tt := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	tt.Encode = func() (interface{}, []byte) {
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: BaseNodeConnInfoHint, Instance: BaseNodeConnInfo{}}))

		node := base.RandomNode()
		ci, err := quicstream.NewBaseConnInfoFromString("1.2.3.4:4321#tls_insecure")
		tt.NoError(err)

		nc := NewBaseNodeConnInfo(node, ci)
		_ = (interface{})(nc).(isaac.NodeConnInfo)

		b, err := enc.Marshal(nc)
		tt.NoError(err)

		tt.T().Log("marshaled:", string(b))

		return nc, b
	}
	tt.Decode = func(b []byte) interface{} {
		var u BaseNodeConnInfo

		tt.NoError(encoder.Decode(enc, b, &u))

		return u
	}
	tt.Compare = func(a interface{}, b interface{}) {
		ap := a.(BaseNodeConnInfo)
		bp := b.(BaseNodeConnInfo)

		tt.True(base.IsEqualNode(ap, bp))
		tt.Equal(ap.String(), bp.String())
	}

	suite.Run(t, tt)
}

type testNodeConnInfoChecker struct {
	isaacdatabase.BaseTestDatabase
	isaac.BaseTestBallots
}

func (t *testNodeConnInfoChecker) SetupTest() {
	t.BaseTestDatabase.SetupTest()
	t.BaseTestBallots.SetupTest()
}

func (t *testNodeConnInfoChecker) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: NodeChallengeRequestHeaderHint, Instance: NodeChallengeRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: ResponseHeaderHint, Instance: ResponseHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SuffrageNodeConnInfoRequestHeaderHint, Instance: SuffrageNodeConnInfoRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: BaseNodeConnInfoHint, Instance: BaseNodeConnInfo{}}))
}

func (t *testNodeConnInfoChecker) clientWritef(handlers map[string]*QuicstreamHandlers) func(ctx context.Context, ci quicstream.ConnInfo, f quicstream.ClientWriteFunc) (io.ReadCloser, func() error, error) {
	return func(ctx context.Context, ci quicstream.ConnInfo, f quicstream.ClientWriteFunc) (io.ReadCloser, func() error, error) {
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

func (t *testNodeConnInfoChecker) ncis(n int) ([]isaac.LocalNode, []isaac.NodeConnInfo) {
	locals := make([]isaac.LocalNode, n)
	ncis := make([]isaac.NodeConnInfo, n)
	for i := range ncis {
		local := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		locals[i] = local
		ncis[i] = NewBaseNodeConnInfo(local.BaseNode, ci)
	}

	return locals, ncis
}

func (t *testNodeConnInfoChecker) handlers(n int) ([]isaac.NodeConnInfo, map[string]*QuicstreamHandlers) {
	handlers := map[string]*QuicstreamHandlers{}

	locals, ncis := t.ncis(n)

	for i := range ncis {
		handlers[ncis[i].String()] = NewQuicstreamHandlers(locals[i], t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil, nil)
	}

	return ncis, handlers
}

func (t *testNodeConnInfoChecker) TestFetchFromNode() {
	ncis, handlers := t.handlers(3)

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncis = append(ncis, NewBaseNodeConnInfo(node.BaseNode, ci))
	}

	localci := quicstream.RandomConnInfo()

	handlers[localci.String()] = NewQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil,
		func() ([]isaac.NodeConnInfo, error) {
			return ncis, nil
		},
	)

	client := newBaseNetworkClient(t.Encs, t.Enc, time.Second, t.clientWritef(handlers))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker := NewNodeConnInfoChecker(t.NodePolicy.NetworkID(), client, time.Second, t.Enc, nil, nil)

	ucis, err := checker.fetch(ctx, localci)
	t.NoError(err)
	t.Equal(len(ncis), len(ucis))

	for i := range ncis {
		ac := ncis[i]
		bc := ucis[i]

		t.True(base.IsEqualNode(ac, bc))
		t.Equal(ac.Addr().String(), bc.Addr().String())
	}
}

func (t *testNodeConnInfoChecker) TestFetchFromNodeButFailedToSignature() {
	ncis, handlers := t.handlers(2)

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()
		ncis = append(ncis, NewBaseNodeConnInfo(node.BaseNode, ci))

		handlers[ci.String()] = NewQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil, nil)
		// NOTE with t.Local insteadd of node
	}

	localci := quicstream.RandomConnInfo()

	handlers[localci.String()] = NewQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil,
		func() ([]isaac.NodeConnInfo, error) {
			return ncis, nil
		},
	)

	client := newBaseNetworkClient(t.Encs, t.Enc, time.Second, t.clientWritef(handlers))

	checker := NewNodeConnInfoChecker(t.NodePolicy.NetworkID(), client, time.Second, t.Enc, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ucis, err := checker.fetch(ctx, localci)
	t.Error(err)
	t.Nil(ucis)

	t.True(errors.Is(err, base.SignatureVerificationError))
}

func (t *testNodeConnInfoChecker) httpserver(handler http.HandlerFunc) *httptest.Server {
	ts := httptest.NewUnstartedServer(handler)
	ts.EnableHTTP2 = true
	ts.StartTLS()

	return ts
}

func (t *testNodeConnInfoChecker) TestFetchFromURL() {
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

	checker := NewNodeConnInfoChecker(t.NodePolicy.NetworkID(), client, time.Second, t.Enc, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ucis, err := checker.fetch(ctx, ts.URL+"#tls_insecure")
	t.NoError(err)
	t.NotNil(ucis)
	t.Equal(len(ncis), len(ucis))

	for i := range ncis {
		ac := ncis[i]
		bc := ucis[i]

		t.True(base.IsEqualNode(ac, bc))
		t.Equal(ac.Addr().String(), bc.Addr().String())
	}
}

func (t *testNodeConnInfoChecker) TestCheckSameResult() {
	ncislocal, handlers := t.handlers(3)
	_, ncisurl := t.ncis(3)
	ncisurl = append(ncisurl, ncislocal[0]) // NOTE duplicated

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncislocal = append(ncislocal, NewBaseNodeConnInfo(node.BaseNode, ci))
	}
	ncis := make([]isaac.NodeConnInfo, len(ncislocal)+len(ncisurl)-1)
	copy(ncis, ncislocal)
	copy(ncis[len(ncislocal):], ncisurl[:len(ncisurl)-1])

	localci := quicstream.RandomConnInfo()

	handlers[localci.String()] = NewQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil,
		func() ([]isaac.NodeConnInfo, error) {
			return ncislocal, nil
		},
	)

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

	cis := []interface{}{localci, ts.URL + "#tls_insecure"}
	checker := NewNodeConnInfoChecker(t.NodePolicy.NetworkID(), client, time.Second, t.Enc, cis, nil)

	ucis, err := checker.check(ctx)
	t.NoError(err)
	t.NotNil(ucis)
	t.Equal(len(ncis), len(ucis))

	for i := range ncis {
		ac := ncis[i]
		bc := ucis[i]

		t.True(base.IsEqualNode(ac, bc))
		t.Equal(ac.Addr().String(), bc.Addr().String())
	}
}

func (t *testNodeConnInfoChecker) TestCalled() {
	calledch := make(chan int64)
	checker := NewNodeConnInfoChecker(t.NodePolicy.NetworkID(), nil, time.Millisecond*100, t.Enc, nil,
		func(called int64, nics []isaac.NodeConnInfo, err error) {
			calledch <- called
		},
	)
	defer checker.Stop()

	t.NoError(checker.Start())

	donech := make(chan struct{})
	go func() {
	end:
		for {
			select {
			case called := <-calledch:
				if called > 3 {
					break end
				}
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

func TestNodeConnInfoChecker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testNodeConnInfoChecker))
}
