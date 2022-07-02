package isaacnetwork

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testNodeConnInfo struct {
	isaac.BaseTestBallots
}

func (t *testNodeConnInfo) TestNew() {
	node := base.NewBaseNode(isaac.NodeHint, base.NewMPrivatekey().Publickey(), base.RandomAddress("local-"))

	nci := NewBaseNodeConnInfo(node, "127.0.0.1:1234", true)

	t.NoError(nci.IsValid(nil))
}

func (t *testNodeConnInfo) TestInvalid() {
	t.Run("wrong ip", func() {
		node := base.NewBaseNode(isaac.NodeHint, base.NewMPrivatekey().Publickey(), base.RandomAddress("local-"))

		nci := NewBaseNodeConnInfo(node, "1.2.3.500:1234", true)

		t.NoError(nci.IsValid(nil))

		_, err := nci.ConnInfo()
		t.Error(err)

		var dnserr *net.DNSError
		t.True(errors.As(err, &dnserr))
	})

	t.Run("dns error", func() {
		node := base.NewBaseNode(isaac.NodeHint, base.NewMPrivatekey().Publickey(), base.RandomAddress("local-"))

		nci := NewBaseNodeConnInfo(node, "a.b.c.d:1234", true)

		t.NoError(nci.IsValid(nil))

		_, err := nci.ConnInfo()
		t.Error(err)

		var dnserr *net.DNSError
		t.True(errors.As(err, &dnserr))
	})

	t.Run("empty host", func() {
		node := base.NewBaseNode(isaac.NodeHint, base.NewMPrivatekey().Publickey(), base.RandomAddress("local-"))

		nci := NewBaseNodeConnInfo(node, ":1234", true)

		err := nci.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
	})

	t.Run("empty port", func() {
		node := base.NewBaseNode(isaac.NodeHint, base.NewMPrivatekey().Publickey(), base.RandomAddress("local-"))

		nci := NewBaseNodeConnInfo(node, "a.b.c.d", true)

		err := nci.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
	})
}

func TestNodeConnInfo(t *testing.T) {
	suite.Run(t, new(testNodeConnInfo))
}

func TestBaseNodeConnInfoEncode(t *testing.T) {
	tt := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	tt.Encode = func() (interface{}, []byte) {
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: BaseNodeConnInfoHint, Instance: BaseNodeConnInfo{}}))

		node := base.RandomNode()

		nc := NewBaseNodeConnInfo(node, "1.2.3.4:4321", true)
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

		aci, err := ap.ConnInfo()
		tt.NoError(err)
		bci, err := bp.ConnInfo()
		tt.NoError(err)

		tt.Equal(aci.String(), bci.String())
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

func (t *testNodeConnInfoChecker) clientWritef(handlers map[string]*QuicstreamHandlers) func(ctx context.Context, ci quicstream.UDPConnInfo, f quicstream.ClientWriteFunc) (io.ReadCloser, func() error, error) {
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
		ncis[i] = NewBaseNodeConnInfo(local.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure())
	}

	return locals, ncis
}

func (t *testNodeConnInfoChecker) handlers(n int) ([]isaac.NodeConnInfo, map[string]*QuicstreamHandlers) {
	handlers := map[string]*QuicstreamHandlers{}

	locals, ncis := t.ncis(n)

	for i := range ncis {
		ci, err := ncis[i].ConnInfo()
		t.NoError(err)

		handlers[ci.String()] = NewQuicstreamHandlers(locals[i], t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil, nil)
	}

	return ncis, handlers
}

func (t *testNodeConnInfoChecker) TestFetchFromNode() {
	ncis, handlers := t.handlers(3)

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncis = append(ncis, NewBaseNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))
	}

	local := isaac.RandomLocalNode()
	localci := quicstream.RandomConnInfo()

	handlers[localci.String()] = NewQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil,
		func() ([]isaac.NodeConnInfo, error) {
			return ncis, nil
		},
	)

	client := newBaseNetworkClient(t.Encs, t.Enc, time.Second, t.clientWritef(handlers))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker := NewNodeConnInfoChecker(local, t.NodePolicy.NetworkID(), client, time.Second, t.Enc, nil, nil)

	ucis, err := checker.fetch(ctx, localci)
	t.NoError(err)
	t.Equal(len(ncis), len(ucis))

	for i := range ncis {
		ac := ncis[i].(BaseNodeConnInfo)
		bc := ucis[i].(BaseNodeConnInfo)

		t.True(base.IsEqualNode(ac, bc))
		t.Equal(ac.addr, bc.addr)
		t.Equal(ac.tlsinsecure, bc.tlsinsecure)
	}
}

func (t *testNodeConnInfoChecker) TestFetchFromNodeButFailedToSignature() {
	ncis, handlers := t.handlers(2)

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()
		ncis = append(ncis, NewBaseNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))

		handlers[ci.String()] = NewQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil, nil)
		// NOTE with t.Local insteadd of node
	}

	local := isaac.RandomLocalNode()
	localci := quicstream.RandomConnInfo()

	handlers[localci.String()] = NewQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil,
		func() ([]isaac.NodeConnInfo, error) {
			return ncis, nil
		},
	)

	client := newBaseNetworkClient(t.Encs, t.Enc, time.Second, t.clientWritef(handlers))

	checker := NewNodeConnInfoChecker(local, t.NodePolicy.NetworkID(), client, time.Second, t.Enc, nil, nil)

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

	local := isaac.RandomLocalNode()
	checker := NewNodeConnInfoChecker(local, t.NodePolicy.NetworkID(), client, time.Second, t.Enc, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ucis, err := checker.fetch(ctx, ts.URL+"#tls_insecure")
	t.NoError(err)
	t.NotNil(ucis)
	t.Equal(len(ncis), len(ucis))

	for i := range ncis {
		ac := ncis[i].(BaseNodeConnInfo)
		bc := ucis[i].(BaseNodeConnInfo)

		t.True(base.IsEqualNode(ac, bc))
		t.Equal(ac.addr, bc.addr)
		t.Equal(ac.tlsinsecure, bc.tlsinsecure)
	}
}

func (t *testNodeConnInfoChecker) TestCheckSameResult() {
	ncislocal, handlers := t.handlers(3)
	_, ncisurl := t.ncis(3)
	ncisurl = append(ncisurl, ncislocal[0]) // NOTE duplicated

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncislocal = append(ncislocal, NewBaseNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))
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

	local := isaac.RandomLocalNode()
	checker := NewNodeConnInfoChecker(local, t.NodePolicy.NetworkID(), client, time.Second, t.Enc, cis, nil)

	ucis, err := checker.check(ctx)
	t.NoError(err)
	t.NotNil(ucis)
	t.Equal(len(ncis), len(ucis))

	for i := range ncis {
		ac := ncis[i].(BaseNodeConnInfo)
		bc := ucis[i].(BaseNodeConnInfo)

		t.True(base.IsEqualNode(ac, bc))
		t.Equal(ac.addr, bc.addr)
		t.Equal(ac.tlsinsecure, bc.tlsinsecure)
	}
}

func (t *testNodeConnInfoChecker) TestCheckFilterLocal() {
	ncis, handlers := t.handlers(3)

	localci, err := ncis[1].ConnInfo()
	t.NoError(err)
	local := handlers[localci.String()].local

	{ // NOTE add unknown node to []isaac.NodeConnInfo
		node := isaac.RandomLocalNode()
		ci := quicstream.RandomConnInfo()

		ncis = append(ncis, NewBaseNodeConnInfo(node.BaseNode, ci.UDPAddr().String(), ci.TLSInsecure()))
	}

	handlers[localci.String()] = NewQuicstreamHandlers(local, t.NodePolicy, t.Encs, t.Enc, time.Second, nil, nil, nil, nil, nil, nil, nil,
		func() ([]isaac.NodeConnInfo, error) {
			return ncis, nil
		},
	)

	client := newBaseNetworkClient(t.Encs, t.Enc, time.Second, t.clientWritef(handlers))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cis := []interface{}{localci}

	checker := NewNodeConnInfoChecker(local, t.NodePolicy.NetworkID(), client, time.Second, t.Enc, cis, nil)

	ucis, err := checker.check(ctx)
	t.NoError(err)
	t.NotNil(ucis)

	li := util.FilterSlices(ncis, func(a interface{}) bool {
		aci := a.(isaac.NodeConnInfo) //nolint:forcetypeassert //...

		return !aci.Address().Equal(local.Address())
	})

	nciswithoutlocal := make([]isaac.NodeConnInfo, len(ncis)-1)
	for i := range li {
		nciswithoutlocal[i] = li[i].(isaac.NodeConnInfo)
	}

	t.Equal(len(nciswithoutlocal), len(ucis))

	for i := range nciswithoutlocal {
		ac := nciswithoutlocal[i].(BaseNodeConnInfo)
		bc := ucis[i].(BaseNodeConnInfo)

		t.True(base.IsEqualNode(ac, bc))
		t.Equal(ac.addr, bc.addr)
		t.Equal(ac.tlsinsecure, bc.tlsinsecure)
	}
}

func (t *testNodeConnInfoChecker) TestCalled() {
	calledch := make(chan int64)

	local := isaac.RandomLocalNode()
	checker := NewNodeConnInfoChecker(local, t.NodePolicy.NetworkID(), nil, time.Millisecond*100, t.Enc, nil,
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

func TestNodeConnInfoChecker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testNodeConnInfoChecker))
}
