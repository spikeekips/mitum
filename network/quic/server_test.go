package quicnetwork

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/block"
	"github.com/spikeekips/mitum/base/key"
	"github.com/spikeekips/mitum/base/seal"
	"github.com/spikeekips/mitum/base/state"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

type testQuicSever struct {
	suite.Suite
	encs  *encoder.Encoders
	enc   encoder.Encoder
	bind  string
	certs []tls.Certificate
	url   *url.URL
}

func (t *testQuicSever) SetupTest() {
	t.encs = encoder.NewEncoders()
	t.enc = jsonenc.NewEncoder()
	_ = t.encs.AddEncoder(t.enc)
	_ = t.encs.AddHinter(key.BTCPrivatekeyHinter)
	_ = t.encs.AddHinter(key.BTCPublickeyHinter)
	_ = t.encs.AddHinter(valuehash.SHA256{})
	_ = t.encs.AddHinter(seal.DummySeal{})
	_ = t.encs.AddHinter(base.BaseNodeV0{})
	_ = t.encs.AddHinter(base.StringAddress(""))
	_ = t.encs.AddHinter(block.ManifestV0{})
	_ = t.encs.AddHinter(network.NodeInfoV0{})
	_ = t.encs.AddHinter(state.StateV0{})
	_ = t.encs.AddHinter(state.BytesValue{})

	port, err := util.FreePort("udp")
	t.NoError(err)

	t.bind = fmt.Sprintf("localhost:%d", port)

	priv, err := util.GenerateED25519Privatekey()
	t.NoError(err)

	certs, err := util.GenerateTLSCerts(t.bind, priv)
	t.NoError(err)
	t.certs = certs

	t.url = &url.URL{Scheme: "quic", Host: t.bind}
}

func (t *testQuicSever) readyServer() *Server {
	qs, err := NewPrimitiveQuicServer(t.bind, t.certs)
	t.NoError(err)

	qn, err := NewServer(qs, t.encs, t.enc)
	t.NoError(err)

	t.NoError(qn.Start())

	_, port, err := net.SplitHostPort(t.bind)
	t.NoError(err)

	maxRetries := 3
	var retries int
	for {
		if retries == maxRetries {
			t.NoError(xerrors.Errorf("quic server did not respond"))
			break
		}

		if err := util.CheckPort("udp", fmt.Sprintf("127.0.0.1:%s", port), time.Millisecond*50); err == nil {
			break
		}
		<-time.After(time.Millisecond * 10)
		retries++
	}

	return qn
}

func (t *testQuicSever) TestNew() {
	qs, err := NewPrimitiveQuicServer(t.bind, t.certs)
	t.NoError(err)

	qn, err := NewServer(qs, t.encs, t.enc)
	t.NoError(err)

	t.Implements((*network.Server)(nil), qn)
}

func (t *testQuicSever) TestSendSeal() {
	qn := t.readyServer()
	defer qn.Stop()

	received := make(chan seal.Seal, 10)
	qn.SetNewSealHandler(func(sl seal.Seal) error {
		received <- sl
		return nil
	})

	qc, err := NewChannel(t.url.String(), 2, true, time.Millisecond*500, 3, nil, t.encs, t.enc)
	t.NoError(err)

	sl := seal.NewDummySeal(key.MustNewBTCPrivatekey())

	t.NoError(qc.SendSeal(sl))

	select {
	case <-time.After(time.Second):
		t.NoError(xerrors.Errorf("failed to receive respond"))
	case r := <-received:
		t.Equal(sl.Hint(), r.Hint())
		t.True(sl.Hash().Equal(r.Hash()))
		t.True(sl.BodyHash().Equal(r.BodyHash()))
		t.True(sl.Signer().Equal(r.Signer()))
		t.Equal(sl.Signature(), r.Signature())
		t.True(localtime.Equal(sl.SignedAt(), r.SignedAt()))
	}

	// NOTE if already known seal received, server returns 200
	qn.SetHasSealHandler(func(h valuehash.Hash) (bool, error) {
		return true, nil
	})

	t.NoError(qc.SendSeal(sl))
}

func (t *testQuicSever) TestGetSeals() {
	qn := t.readyServer()
	defer qn.Stop()

	var hs []valuehash.Hash
	seals := map[string]seal.Seal{}
	for i := 0; i < 3; i++ {
		sl := seal.NewDummySeal(key.MustNewBTCPrivatekey())

		seals[sl.Hash().String()] = sl
		hs = append(hs, sl.Hash())
	}

	qn.SetGetSealsHandler(func(hs []valuehash.Hash) ([]seal.Seal, error) {
		var sls []seal.Seal

		for _, ih := range hs {
			h := ih.(valuehash.Bytes)
			if sl, found := seals[h.String()]; found {
				sls = append(sls, sl)
			}
		}

		return sls, nil
	})

	qc, err := NewChannel(t.url.String(), 2, true, time.Millisecond*500, 3, nil, t.encs, t.enc)
	t.NoError(err)

	{ // get all
		l, err := qc.Seals(hs)
		t.NoError(err)
		t.Equal(len(hs), len(l))

		sm := map[string]seal.Seal{}
		for _, s := range l {
			sm[s.Hash().String()] = s
		}

		for h, sl := range seals {
			t.True(sl.Hash().Equal(sm[h].Hash()))
		}
	}

	{ // some of them
		l, err := qc.Seals(hs[:2])
		t.NoError(err)
		t.Equal(len(hs[:2]), len(l))

		sm := map[string]seal.Seal{}
		for _, s := range l {
			sm[s.Hash().String()] = s
		}

		for _, h := range hs[:2] {
			t.True(seals[h.String()].Hash().Equal(sm[h.String()].Hash()))
		}
	}

	{ // with unknown
		bad := hs[:2]
		bad = append(bad, valuehash.RandomSHA256())

		l, err := qc.Seals(bad)
		t.NoError(err)
		t.Equal(len(hs[:2]), len(l))

		sm := map[string]seal.Seal{}
		for _, s := range l {
			sm[s.Hash().String()] = s
		}

		for _, h := range hs[:2] {
			t.True(seals[h.String()].Hash().Equal(sm[h.String()].Hash()))
		}
	}
}

func (t *testQuicSever) TestNodeInfo() {
	qn := t.readyServer()
	defer qn.Stop()

	nid := []byte("test-network-id")

	var ni network.NodeInfo
	{
		blk, err := block.NewTestBlockV0(base.Height(33), base.Round(0), valuehash.RandomSHA256(), valuehash.RandomSHA256())
		t.NoError(err)

		suffrage := base.NewFixedSuffrage(base.RandomStringAddress(), nil)

		ni = network.NewNodeInfoV0(
			base.RandomNode("n0"),
			nid,
			base.StateBooting,
			blk.Manifest(),
			util.Version("0.1.1"),
			"quic://local",
			map[string]interface{}{"showme": 1.1},
			nil,
			suffrage,
		)
	}

	qn.SetNodeInfoHandler(func() (network.NodeInfo, error) {
		return ni, nil
	})

	qc, err := NewChannel(t.url.String(), 2, true, time.Millisecond*500, 3, nil, t.encs, t.enc)
	t.NoError(err)

	nni, err := qc.NodeInfo()
	t.NoError(err)

	network.CompareNodeInfo(t.T(), ni, nni)
}

func TestQuicSever(t *testing.T) {
	suite.Run(t, new(testQuicSever))
}
