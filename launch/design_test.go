package launch

import (
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"
)

type testNodeStorageDesign struct {
	suite.Suite
	enc *jsonenc.Encoder
}

func (t *testNodeStorageDesign) SetupSuite() {
	t.enc = jsonenc.NewEncoder()
}

func (t *testNodeStorageDesign) TestDecode() {
	t.Run("ok", func() {
		b := []byte(`
base: /tmp/a/b/c
database: redis://
`)

		var a NodeStorageDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("/tmp/a/b/c", a.Base)
		t.Equal("redis:", a.Database.String())
	})

	t.Run("empty base", func() {
		b := []byte(`
database: redis://
`)

		var a NodeStorageDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("", a.Base)
		t.Equal("redis:", a.Database.String())
	})

	t.Run("empty database", func() {
		b := []byte(`
base: /tmp/a/b/c
`)

		var a NodeStorageDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("/tmp/a/b/c", a.Base)
		t.Nil(a.Database)
	})
}

func (t *testNodeStorageDesign) TestIsValid() {
	t.Run("ok", func() {
		a := NodeStorageDesign{
			Base:     "/tmp/a/b/c",
			Database: &url.URL{Scheme: LeveldbURIScheme, Path: "/a/b/c"},
		}

		t.NoError(a.IsValid(nil))

		t.Equal("/tmp/a/b/c", a.Base)
		t.Equal(LeveldbURIScheme+"://"+"/a/b/c", a.Database.String())
	})

	t.Run("empty base", func() {
		a := NodeStorageDesign{
			Database: &url.URL{Scheme: LeveldbURIScheme, Path: "/a/b/c"},
		}

		t.NoError(a.IsValid(nil))
		t.Empty(a.Base)

		node := base.RandomAddress("")
		t.NoError(a.Patch(node))

		t.Equal(LeveldbURIScheme+"://"+"/a/b/c", a.Database.String())
	})

	t.Run("patched", func() {
		a := NodeStorageDesign{}

		t.NoError(a.IsValid(nil))
		t.Empty(a.Base)
		t.Nil(a.Database)

		node := base.RandomAddress("")
		t.NoError(a.Patch(node))

		t.Equal(filepath.Join(DefaultStorageBase, node.String()), a.Base)
		t.Equal(defaultDatabaseURL(a.Base), a.Database)
	})

	t.Run("empty database", func() {
		a := NodeStorageDesign{
			Base: "/tmp/a/b/c",
		}

		t.NoError(a.IsValid(nil))

		t.Equal("/tmp/a/b/c", a.Base)
		t.Nil(a.Database)
	})

	t.Run("invalid database", func() {
		a := NodeStorageDesign{
			Base:     "/tmp/a/b/c",
			Database: &url.URL{Path: "/a/b/c"},
		}

		err := a.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "wrong database")
	})
}

func TestNodeStorageDesign(t *testing.T) {
	suite.Run(t, new(testNodeStorageDesign))
}

type testNodeNetworkDesign struct {
	suite.Suite
	enc *jsonenc.Encoder
}

func (t *testNodeNetworkDesign) SetupSuite() {
	t.enc = jsonenc.NewEncoder()
}

func (t *testNodeNetworkDesign) TestIsValid() {
	addrport := mustResolveUDPAddr("1.2.3.4:4321")
	publish := mustResolveUDPAddr("4.3.2.1:1234")

	t.Run("ok", func() {
		a := NodeNetworkDesign{
			Bind:          addrport,
			PublishString: publish.String(),
			TLSInsecure:   true,
		}

		t.NoError(a.IsValid(nil))
	})

	t.Run("wrong bind", func() {
		addrport := mustResolveUDPAddr("1.2.3.4:4321")
		addrport.Port = 0

		a := NodeNetworkDesign{
			Bind:          addrport,
			PublishString: publish.String(),
			TLSInsecure:   true,
		}

		err := a.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "invalid bind")
	})

	t.Run("wrong publish; wrong port", func() {
		publish := mustResolveUDPAddr("4.3.2.1:1234")
		publish.Port = 0
		a := NodeNetworkDesign{
			Bind:          addrport,
			PublishString: publish.String(),
			TLSInsecure:   true,
		}

		err := a.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "invalid publish port")
	})

	t.Run("empty publish", func() {
		a := NodeNetworkDesign{
			Bind:          addrport,
			PublishString: "",
			TLSInsecure:   true,
		}

		t.NoError(a.IsValid(nil))
		t.NotNil(a.publish)
		t.Equal(DefaultNetworkBind.String(), a.publish.String())
	})
}

func (t *testNodeNetworkDesign) TestDecode() {
	t.Run("ok", func() {
		b := []byte(`
bind: 0.0.0.0:1234
publish: 1.2.3.4:4321
tls_insecure: true
`)

		var a NodeNetworkDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("0.0.0.0:1234", a.Bind.String())
		t.Equal("1.2.3.4:4321", a.PublishString)
		t.Equal(true, a.TLSInsecure)
	})

	t.Run("empty bind", func() {
		b := []byte(`
publish: 1.2.3.4:4321
tls_insecure: true
`)

		var a NodeNetworkDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Nil(a.Bind)
		t.Equal("1.2.3.4:4321", a.PublishString)
		t.Equal(true, a.TLSInsecure)
	})

	t.Run("empty publish", func() {
		b := []byte(`
bind: 0.0.0.0:1234
tls_insecure: true
`)

		var a NodeNetworkDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("0.0.0.0:1234", a.Bind.String())
		t.Nil(a.publish)
		t.Equal(true, a.TLSInsecure)
	})

	t.Run("empty TLSInsecure", func() {
		b := []byte(`
bind: 0.0.0.0:1234
publish: 1.2.3.4:4321
`)

		var a NodeNetworkDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("0.0.0.0:1234", a.Bind.String())
		t.Equal("1.2.3.4:4321", a.PublishString)
		t.Equal(false, a.TLSInsecure)
	})
}

func TestNodeNetworkDesign(t *testing.T) {
	suite.Run(t, new(testNodeNetworkDesign))
}

type testSyncSourcesDesign struct {
	suite.Suite
	enc *jsonenc.Encoder
}

func (t *testSyncSourcesDesign) SetupSuite() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
}

func (t *testSyncSourcesDesign) TestDecode() {
	t.Run("ok: url", func() {
		b := []byte(`
- https://a.b.c.d:1234#tls_insecure
`)

		var s SyncSourcesDesign
		t.NoError(s.DecodeYAML(b, t.enc))

		t.NoError(s.IsValid(nil))
		t.Equal(1, len(s))
		t.NoError(s[0].IsValid(nil))

		t.Equal(isaacnetwork.SyncSourceTypeURL, s[0].Type)
		t.Equal("https://a.b.c.d:1234#tls_insecure", s[0].Source.(*url.URL).String())
	})

	t.Run("invalid: url", func() {
		b := []byte(`
- https://
`)

		var s SyncSourcesDesign
		err := s.DecodeYAML(b, t.enc)
		t.Error(err)
		t.ErrorContains(err, "missing host")
	})

	t.Run("invalid type", func() {
		b := []byte(`
- type: sync-source-node
  findme: https://a.b.c.d:1234#tls_insecure
`)

		var s SyncSourcesDesign
		err := s.DecodeYAML(b, t.enc)
		t.Error(err)
		t.ErrorContains(err, "decode node")
	})

	t.Run("ok: NodeConnInfo", func() {
		b := []byte(`
- type: sync-source-node
  address: showme-nodesas
  publickey: oxkQTcfKzrC67GE8ChZmZw8SBBBYefMp5859R2AZ8bB9mpu
  publish: 1.2.3.4:1234
  tls_insecure: true
`)

		var s SyncSourcesDesign
		t.NoError(s.DecodeYAML(b, t.enc))

		i := s[0]
		t.NoError(i.IsValid(nil))
		t.NoError(i.IsValid(nil))

		t.Equal(isaacnetwork.SyncSourceTypeNode, i.Type)

		a, ok := (i.Source).(isaac.NodeConnInfo)
		t.True(ok)
		t.Equal("showme-nodesas", a.Address().String())
		t.Equal("oxkQTcfKzrC67GE8ChZmZw8SBBBYefMp5859R2AZ8bB9mpu", a.Publickey().String())
		t.Equal("1.2.3.4:1234", a.Addr().String())
		t.True(a.TLSInsecure())
	})

	t.Run("ok: SuffrageNode", func() {
		b := []byte(`
- type: sync-source-suffrage-nodes
  publish: 1.2.3.4:1234
  tls_insecure: true
`)

		var s SyncSourcesDesign
		t.NoError(s.DecodeYAML(b, t.enc))

		i := s[0]
		t.NoError(i.IsValid(nil))
		t.NoError(i.IsValid(nil))

		t.Equal(isaacnetwork.SyncSourceTypeSuffrageNodes, i.Type)

		a, ok := (i.Source).(network.ConnInfo)
		t.True(ok)
		t.Equal("1.2.3.4:1234", a.Addr().String())
		t.True(a.TLSInsecure())
	})

	t.Run("ok: SyncSource", func() {
		b := []byte(`
- type: sync-source-sync-sources
  publish: 1.2.3.4:1234
  tls_insecure: true
`)

		var s SyncSourcesDesign
		t.NoError(s.DecodeYAML(b, t.enc))

		i := s[0]
		t.NoError(i.IsValid(nil))
		t.NoError(i.IsValid(nil))

		t.Equal(isaacnetwork.SyncSourceTypeSyncSources, i.Type)

		a, ok := (i.Source).(network.ConnInfo)
		t.True(ok)
		t.Equal("1.2.3.4:1234", a.Addr().String())
		t.True(a.TLSInsecure())
	})
}

func TestSyncSourcesDesign(t *testing.T) {
	suite.Run(t, new(testSyncSourcesDesign))
}

type testNodeDesign struct {
	suite.Suite
	enc *jsonenc.Encoder
}

func (t *testNodeDesign) SetupSuite() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.MPrivatekeyHint, Instance: base.MPrivatekey{}}))
}

func (t *testNodeDesign) TestIsValid() {
	addrport := mustResolveUDPAddr("1.2.3.4:4321")
	publish := mustResolveUDPAddr("4.3.2.1:1234")

	networkID := base.NetworkID(util.UUID().String())

	t.Run("ok", func() {
		a := NodeDesign{
			Address:    base.RandomAddress(""),
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  networkID,
			Network: NodeNetworkDesign{
				Bind:          addrport,
				PublishString: publish.String(),
				TLSInsecure:   true,
			},
			Storage: NodeStorageDesign{
				Base:     "/tmp/a/b/c",
				Database: &url.URL{Scheme: LeveldbURIScheme, Path: "/a/b/c"},
			},
			LocalParams: defaultLocalParams(networkID),
		}

		t.NoError(a.IsValid(nil))
	})

	t.Run("empty network", func() {
		a := NodeDesign{
			Address:    base.RandomAddress(""),
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  base.NetworkID(util.UUID().String()),
		}

		t.NoError(a.IsValid(nil))

		t.Equal(DefaultNetworkBind, a.Network.Bind)
		t.Equal(DefaultNetworkBind.String(), a.Network.PublishString)
	})

	t.Run("empty storage", func() {
		a := NodeDesign{
			Address:    base.RandomAddress(""),
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  base.NetworkID(util.UUID().String()),
			Network: NodeNetworkDesign{
				Bind:          addrport,
				PublishString: publish.String(),
				TLSInsecure:   true,
			},
		}

		t.NoError(a.IsValid(nil))

		t.Equal(DefaultStorageBase+"/"+a.Address.String(), a.Storage.Base)
		t.Equal((&url.URL{
			Scheme: LeveldbURIScheme,
			Path:   filepath.Join(DefaultStorageBase, a.Address.String(), DefaultStorageDatabaseDirectoryName),
		}).String(), a.Storage.Database.String())
	})

	t.Run("same sync_sources with address", func() {
		address := base.RandomAddress("")

		a := NodeDesign{
			Address:    address,
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  base.NetworkID(util.UUID().String()),
			Network: NodeNetworkDesign{
				Bind:          addrport,
				PublishString: publish.String(),
				TLSInsecure:   true,
			},
			Storage: NodeStorageDesign{
				Base:     "/tmp/a/b/c",
				Database: &url.URL{Scheme: LeveldbURIScheme, Path: "/a/b/c"},
			},
			SyncSources: []isaacnetwork.SyncSource{
				{Type: isaacnetwork.SyncSourceTypeURL, Source: &url.URL{Scheme: "https", Host: "a:3333"}},
				{Type: isaacnetwork.SyncSourceTypeNode, Source: isaacnetwork.MustNodeConnInfo(
					isaac.NewNode(base.NewMPrivatekey().Publickey(), address), // same address
					mustResolveUDPAddr("4.3.2.1:4444").String(), true,
				)},
			},
		}

		t.NoError(a.IsValid(nil))
	})

	t.Run("same sync_sources with publish", func() {
		nci, err := isaacnetwork.NewNodeConnInfo(
			isaac.NewNode(base.NewMPrivatekey().Publickey(), base.RandomAddress("")),
			publish.String(), true,
		)
		t.NoError(err)

		a := NodeDesign{
			Address:    base.RandomAddress(""),
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  base.NetworkID(util.UUID().String()),
			Network: NodeNetworkDesign{
				Bind:          addrport,
				PublishString: publish.String(),
				TLSInsecure:   true,
			},
			Storage: NodeStorageDesign{
				Base:     "/tmp/a/b/c",
				Database: &url.URL{Scheme: LeveldbURIScheme, Path: "/a/b/c"},
			},
			SyncSources: []isaacnetwork.SyncSource{
				{Type: isaacnetwork.SyncSourceTypeNode, Source: nci},
			},
		}

		err = a.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "sync source has same with publish address")
	})

	t.Run("same sync_sources with resolved publish", func() {
		publishstring := "localhost:1234"
		publish := mustResolveUDPAddr(publishstring)

		nci, err := isaacnetwork.NewNodeConnInfo(
			isaac.NewNode(base.NewMPrivatekey().Publickey(), base.RandomAddress("")),
			publish.String(), true,
		)
		t.NoError(err)

		a := NodeDesign{
			Address:    base.RandomAddress(""),
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  base.NetworkID(util.UUID().String()),
			Network: NodeNetworkDesign{
				Bind:          addrport,
				PublishString: publishstring,
				TLSInsecure:   true,
			},
			Storage: NodeStorageDesign{
				Base:     "/tmp/a/b/c",
				Database: &url.URL{Scheme: LeveldbURIScheme, Path: "/a/b/c"},
			},
			SyncSources: []isaacnetwork.SyncSource{
				{Type: isaacnetwork.SyncSourceTypeNode, Source: nci},
			},
		}

		err = a.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "sync source has same with publish resolved address")
	})

	t.Run("empty time server", func() {
		a := NodeDesign{
			Address:    base.RandomAddress(""),
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  networkID,
		}

		t.NoError(a.IsValid(nil))
	})

	t.Run("name time server", func() {
		a := NodeDesign{
			Address:    base.RandomAddress(""),
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  networkID,
			TimeServer: "un.org",
		}

		t.NoError(a.IsValid(nil))
		t.Equal("un.org", a.TimeServer)
		t.Empty(a.TimeServerPort)
	})

	t.Run("unresolve time server", func() {
		a := NodeDesign{
			Address:    base.RandomAddress(""),
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  networkID,
			TimeServer: "xxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		}

		err := a.IsValid(nil)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid time server")

		switch s := err.Error(); {
		case strings.Contains(s, "no such host"):
		case strings.Contains(s, "failure in name resolution"):
		case strings.Contains(s, "server misbehaving"):
		default:
			t.NoError(errors.Errorf("unknown error: %+v", err))
		}
	})

	t.Run("invalid time server", func() {
		a := NodeDesign{
			Address:    base.RandomAddress(""),
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  networkID,
			TimeServer: "a/10",
		}

		err := a.IsValid(nil)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid time server")
	})

	t.Run("time server with port", func() {
		a := NodeDesign{
			Address:    base.RandomAddress(""),
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  networkID,
			TimeServer: "1.1.1.1:10",
		}

		t.NoError(a.IsValid(nil))
		t.Equal("1.1.1.1", a.TimeServer)
		t.Equal(10, a.TimeServerPort)
	})
}

func (t *testNodeDesign) TestDecode() {
	t.Run("ok", func() {
		b := []byte(`
address: no0sas
privatekey: 9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr
network_id: hehe 1 2 3 4
network:
  bind: 0.0.0.0:1234
  publish: 1.2.3.4:4321
  tls_insecure: true
storage:
  base: /tmp/a/b/c
  database: redis://
parameters:
  isaac:
    threshold: 77.7
    wait_preparing_init_ballot: 10s
  misc:
    valid_proposal_operation_expire: 11s
    object_cache_size: 33
  memberlist:
    tcp_timeout: 6s
    udp_buffer_size: 333
  network:
    timeout_request: 33s
    default_handler_timeout: 33s
    handler_timeout:
      request_proposal: 9s
      ask_handover: 12s
    rate_limit:
      default:
        default: 33/s
        proposal: 44/m
      suffrage:
        default: 333/s
        state: 444/h
      net:
        - 192.168.0.0/24:
            default: 3333/s
            blockmap: 4444/h
      node:
        no0sas:
          default: 33333/s
          operation: 44444/ns
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Equal("0.0.0.0:1234", a.Network.Bind.String())
		t.Equal("1.2.3.4:4321", a.Network.PublishString)
		t.Equal(true, a.Network.TLSInsecure)

		t.Equal("/tmp/a/b/c", a.Storage.Base)
		t.Equal("redis:", a.Storage.Database.String())

		isaacparams := isaac.DefaultParams(a.NetworkID)
		isaacparams.SetThreshold(77.7)
		isaacparams.SetWaitPreparingINITBallot(time.Second * 10)

		isaac.EqualLocalParams(t.Assert(), isaacparams, a.LocalParams.ISAAC)

		misc := defaultMISCParams()
		misc.SetValidProposalOperationExpire(time.Second * 11)
		misc.SetObjectCacheSize(33)

		equalMISCParams(t.Assert(), misc, a.LocalParams.MISC)

		memberlistparams := defaultMemberlistParams()
		memberlistparams.SetTCPTimeout(time.Second * 6)
		memberlistparams.SetUDPBufferSize(333)
		equalMemberlistParams(t.Assert(), memberlistparams, a.LocalParams.Memberlist)

		nw := defaultNetworkParams()
		nw.SetTimeoutRequest(time.Second * 33)
		nw.SetDefaultHandlerTimeout(time.Second * 33)
		t.NoError(nw.SetHandlerTimeout("request_proposal", time.Second*9))
		t.NoError(nw.SetHandlerTimeout("ask_handover", time.Second*12))
		equalNetworkParams(t.Assert(), nw, a.LocalParams.Network)

		t.Run("rate limit", func() {
			rm := a.LocalParams.Network.RateLimit().DefaultRuleMap()
			t.Equal(rate.Every(time.Second), rm.d.Limit)
			t.Equal(33, rm.d.Burst)
			t.Equal(rate.Every(time.Minute), rm.m["proposal"].Limit)
			t.Equal(44, rm.m["proposal"].Burst)

			{
				rs, ok := a.LocalParams.Network.RateLimit().SuffrageRuleSet().(*SuffrageRateLimiterRuleSet)
				t.True(ok)
				t.Equal(rate.Every(time.Second), rs.rules.d.Limit)
				t.Equal(333, rs.rules.d.Burst)
				t.Equal(rate.Every(time.Hour), rs.rules.m["state"].Limit)
				t.Equal(444, rs.rules.m["state"].Burst)
			}

			{
				rs, ok := a.LocalParams.Network.RateLimit().NetRuleSet().(NetRateLimiterRuleSet)
				t.True(ok)
				rs0, found := rs.rules["192.168.0.0/24"]
				t.True(found)

				t.Equal(rate.Every(time.Second), rs0.d.Limit)
				t.Equal(3333, rs0.d.Burst)
				t.Equal(rate.Every(time.Hour), rs0.m["blockmap"].Limit)
				t.Equal(4444, rs0.m["blockmap"].Burst)
			}

			{
				rs, ok := a.LocalParams.Network.RateLimit().NodeRuleSet().(NodeRateLimiterRuleSet)
				t.True(ok)
				rs0, found := rs.rules["no0sas"]
				t.True(found)

				t.Equal(rate.Every(time.Second), rs0.d.Limit)
				t.Equal(33333, rs0.d.Burst)
				t.Equal(rate.Every(time.Nanosecond), rs0.m["operation"].Limit)
				t.Equal(44444, rs0.m["operation"].Burst)
			}
		})
	})

	t.Run("empty network", func() {
		b := []byte(`
address: no0sas
privatekey: 9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr
network_id: hehe 1 2 3 4
storage:
  base: /tmp/a/b/c
  database: redis://
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Nil(a.Network.Bind)
		t.Nil(a.Network.publish)
		t.Equal(false, a.Network.TLSInsecure)

		t.Equal("/tmp/a/b/c", a.Storage.Base)
		t.Equal("redis:", a.Storage.Database.String())
	})

	t.Run("empty storage", func() {
		b := []byte(`
address: no0sas
privatekey: 9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr
network_id: hehe 1 2 3 4
network:
  bind: 0.0.0.0:1234
  publish: 1.2.3.4:4321
  tls_insecure: true
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Equal("0.0.0.0:1234", a.Network.Bind.String())
		t.Equal("1.2.3.4:4321", a.Network.PublishString)
		t.Equal(true, a.Network.TLSInsecure)

		t.Equal("", a.Storage.Base)
		t.Nil(a.Storage.Database)
	})

	t.Run("missing isaac params", func() {
		b := []byte(`
address: no0sas
privatekey: 9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr
network_id: hehe 1 2 3 4
network:
  bind: 0.0.0.0:1234
  publish: 1.2.3.4:4321
  tls_insecure: true
storage:
  base: /tmp/a/b/c
  database: redis://
parameters:
  memberlist:
    tcp_timeout: 6s
    udp_buffer_size: 333
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Equal("0.0.0.0:1234", a.Network.Bind.String())
		t.Equal("1.2.3.4:4321", a.Network.PublishString)
		t.Equal(true, a.Network.TLSInsecure)

		t.Equal("/tmp/a/b/c", a.Storage.Base)
		t.Equal("redis:", a.Storage.Database.String())

		isaac.EqualLocalParams(t.Assert(), isaac.DefaultParams(a.NetworkID), a.LocalParams.ISAAC)

		memberlistparams := defaultMemberlistParams()
		memberlistparams.SetTCPTimeout(time.Second * 6)
		memberlistparams.SetUDPBufferSize(333)
		memberlistparams.SetRetransmitMult(3)

		equalMemberlistParams(t.Assert(), memberlistparams, a.LocalParams.Memberlist)
	})

	t.Run("missing params", func() {
		b := []byte(`
address: no0sas
privatekey: 9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr
network_id: hehe 1 2 3 4
network:
  bind: 0.0.0.0:1234
  publish: 1.2.3.4:4321
  tls_insecure: true
storage:
  base: /tmp/a/b/c
  database: redis://
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Equal("0.0.0.0:1234", a.Network.Bind.String())
		t.Equal("1.2.3.4:4321", a.Network.PublishString)
		t.Equal(true, a.Network.TLSInsecure)

		t.Equal("/tmp/a/b/c", a.Storage.Base)
		t.Equal("redis:", a.Storage.Database.String())

		isaac.EqualLocalParams(t.Assert(), isaac.DefaultParams(a.NetworkID), a.LocalParams.ISAAC)

		memberlistparams := defaultMemberlistParams()
		equalMemberlistParams(t.Assert(), memberlistparams, a.LocalParams.Memberlist)
	})

	t.Run("missing memberlist params", func() {
		b := []byte(`
address: no0sas
privatekey: 9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr
network_id: hehe 1 2 3 4
network:
  bind: 0.0.0.0:1234
  publish: 1.2.3.4:4321
  tls_insecure: true
storage:
  base: /tmp/a/b/c
  database: redis://
parameters:
  isaac:
    threshold: 77.7
    wait_preparing_init_ballot: 10s
  misc:
    valid_proposal_operation_expire: 11s
    object_cache_size: 33
  network:
    timeout_request: 33s
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Equal("0.0.0.0:1234", a.Network.Bind.String())
		t.Equal("1.2.3.4:4321", a.Network.PublishString)
		t.Equal(true, a.Network.TLSInsecure)

		t.Equal("/tmp/a/b/c", a.Storage.Base)
		t.Equal("redis:", a.Storage.Database.String())

		isaacparams := isaac.DefaultParams(a.NetworkID)
		isaacparams.SetThreshold(77.7)
		isaacparams.SetWaitPreparingINITBallot(time.Second * 10)

		isaac.EqualLocalParams(t.Assert(), isaacparams, a.LocalParams.ISAAC)

		misc := defaultMISCParams()
		misc.SetValidProposalOperationExpire(time.Second * 11)
		misc.SetObjectCacheSize(33)

		equalMISCParams(t.Assert(), misc, a.LocalParams.MISC)

		nw := defaultNetworkParams()
		nw.SetTimeoutRequest(time.Second * 33)
		equalNetworkParams(t.Assert(), nw, a.LocalParams.Network)

		memberlistparams := defaultMemberlistParams()
		equalMemberlistParams(t.Assert(), memberlistparams, a.LocalParams.Memberlist)
	})

	t.Run("missing misc params", func() {
		b := []byte(`
address: no0sas
privatekey: 9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr
network_id: hehe 1 2 3 4
network:
  bind: 0.0.0.0:1234
  publish: 1.2.3.4:4321
  tls_insecure: true
storage:
  base: /tmp/a/b/c
  database: redis://
parameters:
  isaac:
    threshold: 77.7
    wait_preparing_init_ballot: 10s
  memberlist:
    tcp_timeout: 6s
    udp_buffer_size: 333
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Equal("0.0.0.0:1234", a.Network.Bind.String())
		t.Equal("1.2.3.4:4321", a.Network.PublishString)
		t.Equal(true, a.Network.TLSInsecure)

		t.Equal("/tmp/a/b/c", a.Storage.Base)
		t.Equal("redis:", a.Storage.Database.String())

		isaacparams := isaac.DefaultParams(a.NetworkID)
		isaacparams.SetThreshold(77.7)
		isaacparams.SetWaitPreparingINITBallot(time.Second * 10)

		isaac.EqualLocalParams(t.Assert(), isaacparams, a.LocalParams.ISAAC)

		misc := defaultMISCParams()

		equalMISCParams(t.Assert(), misc, a.LocalParams.MISC)

		nw := defaultNetworkParams()
		equalNetworkParams(t.Assert(), nw, a.LocalParams.Network)

		memberlistparams := defaultMemberlistParams()
		memberlistparams.SetTCPTimeout(time.Second * 6)
		memberlistparams.SetUDPBufferSize(333)

		equalMemberlistParams(t.Assert(), memberlistparams, a.LocalParams.Memberlist)
	})

	t.Run("missing network params", func() {
		b := []byte(`
address: no0sas
privatekey: 9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr
network_id: hehe 1 2 3 4
network:
  bind: 0.0.0.0:1234
  publish: 1.2.3.4:4321
  tls_insecure: true
storage:
  base: /tmp/a/b/c
  database: redis://
parameters:
  isaac:
    threshold: 77.7
    wait_preparing_init_ballot: 10s
  misc:
    valid_proposal_operation_expire: 11s
    object_cache_size: 33
  memberlist:
    tcp_timeout: 6s
    udp_buffer_size: 333
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Equal("0.0.0.0:1234", a.Network.Bind.String())
		t.Equal("1.2.3.4:4321", a.Network.PublishString)
		t.Equal(true, a.Network.TLSInsecure)

		t.Equal("/tmp/a/b/c", a.Storage.Base)
		t.Equal("redis:", a.Storage.Database.String())

		isaacparams := isaac.DefaultParams(a.NetworkID)
		isaacparams.SetThreshold(77.7)
		isaacparams.SetWaitPreparingINITBallot(time.Second * 10)

		isaac.EqualLocalParams(t.Assert(), isaacparams, a.LocalParams.ISAAC)

		misc := defaultMISCParams()
		misc.SetValidProposalOperationExpire(time.Second * 11)
		misc.SetObjectCacheSize(33)

		equalMISCParams(t.Assert(), misc, a.LocalParams.MISC)

		nw := defaultNetworkParams()
		equalNetworkParams(t.Assert(), nw, a.LocalParams.Network)

		memberlistparams := defaultMemberlistParams()
		memberlistparams.SetTCPTimeout(time.Second * 6)
		memberlistparams.SetUDPBufferSize(333)
		equalMemberlistParams(t.Assert(), memberlistparams, a.LocalParams.Memberlist)

		{
			d := nw.RateLimit().DefaultRuleMap().d
			t.Equal(defaultRateLimiter.Limit, d.Limit)
			t.Equal(defaultRateLimiter.Burst, d.Burst)
		}
	})
}

func (t *testNodeDesign) TestEncode() {
	t.Run("ok", func() {
		b := []byte(`
address: no0sas
privatekey: 9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr
network_id: hehe 1 2 3 4
network:
  bind: 0.0.0.0:1234
  publish: 1.2.3.4:4321
  tls_insecure: true
storage:
  base: /tmp/a/b/c
  database: redis://
parameters:
  isaac:
    threshold: 77.7
    wait_preparing_init_ballot: 10s
  misc:
    valid_proposal_operation_expire: 11s
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Equal("0.0.0.0:1234", a.Network.Bind.String())
		t.Equal("1.2.3.4:4321", a.Network.PublishString)
		t.Equal(true, a.Network.TLSInsecure)

		t.Equal("/tmp/a/b/c", a.Storage.Base)
		t.Equal("redis:", a.Storage.Database.String())

		ub, err := yaml.Marshal(a)
		t.NoError(err)

		t.T().Logf("marshaled:\n%s", string(ub))

		var ua NodeDesign
		t.NoError(ua.DecodeYAML(ub, t.enc))

		t.Equal("no0sas", ua.Address.String())
		t.Equal("9gKYPx4FSXbL65d2efDUMjKtaagMsNSinF9u5FMBKD7bmpr", ua.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(ua.NetworkID))

		t.Equal("0.0.0.0:1234", ua.Network.Bind.String())
		t.Equal("1.2.3.4:4321", ua.Network.PublishString)
		t.Equal(true, ua.Network.TLSInsecure)

		t.Equal("/tmp/a/b/c", ua.Storage.Base)
		t.Equal("redis:", ua.Storage.Database.String())

		params := isaac.DefaultParams(ua.LocalParams.ISAAC.NetworkID())
		params.SetThreshold(77.7)
		params.SetWaitPreparingINITBallot(time.Second * 10)

		isaac.EqualLocalParams(t.Assert(), params, ua.LocalParams.ISAAC)

		misc := defaultMISCParams()
		misc.SetValidProposalOperationExpire(time.Second * 11)

		equalMISCParams(t.Assert(), misc, a.LocalParams.MISC)

		nw := defaultNetworkParams()
		equalNetworkParams(t.Assert(), nw, a.LocalParams.Network)
	})
}

func TestNodeDesign(t *testing.T) {
	suite.Run(t, new(testNodeDesign))
}

type testGenesisOpertionsDesign struct {
	suite.Suite
	enc       *jsonenc.Encoder
	networkID base.NetworkID
}

func (t *testGenesisOpertionsDesign) SetupSuite() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: isaac.NodeHint, Instance: base.BaseNode{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: isaac.NetworkPolicyHint, Instance: isaac.NetworkPolicy{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: isaac.FixedSuffrageCandidateLimiterRuleHint, Instance: isaac.FixedSuffrageCandidateLimiterRule{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: isaacoperation.SuffrageGenesisJoinFactHint, Instance: isaacoperation.SuffrageGenesisJoinFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: isaacoperation.GenesisNetworkPolicyFactHint, Instance: isaacoperation.GenesisNetworkPolicyFact{}}))

	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testGenesisOpertionsDesign) newSuffrageGenesisJoinFact() isaacoperation.SuffrageGenesisJoinFact {
	return isaacoperation.NewSuffrageGenesisJoinFact(
		[]base.Node{
			isaac.NewNode(base.NewMPrivatekey().Publickey(), base.RandomAddress("")),
		},
		t.networkID,
	)
}

func (t *testGenesisOpertionsDesign) newGenesisNetworkPolicyFact() isaacoperation.GenesisNetworkPolicyFact {
	policy := isaac.DefaultNetworkPolicy()
	policy.SetMaxOperationsInProposal(33)

	return isaacoperation.NewGenesisNetworkPolicyFact(policy)
}

func (t *testGenesisOpertionsDesign) TestDecode() {
	suffact := t.newSuffrageGenesisJoinFact()
	policyfact := t.newGenesisNetworkPolicyFact()

	g := GenesisDesign{Facts: []base.Fact{suffact, policyfact}}

	var b []byte
	{
		rb, err := t.enc.Marshal(g)
		t.NoError(err)
		t.T().Logf("json marshaled:\n%s", string(rb))

		var m map[string][]map[string]interface{}
		t.NoError(yaml.Unmarshal(rb, &m))

		for i := range m["facts"] {
			delete(m["facts"][i], "hash")
			delete(m["facts"][i], "token")
		}

		b, err = yaml.Marshal(m)
		t.NoError(err)

		t.T().Logf("yaml marshaled:\n%s", string(b))
	}

	var u GenesisDesign
	t.NoError(u.DecodeYAML(b, t.enc))

	usuffact := u.Facts[0].(isaacoperation.SuffrageGenesisJoinFact)
	t.Nil(usuffact.Hash())
	t.Empty(usuffact.Token())

	t.True(suffact.Hint().Equal(usuffact.Hint()))
	t.Equal(len(suffact.Nodes()), len(usuffact.Nodes()))

	ans := suffact.Nodes()
	bns := usuffact.Nodes()

	for i := range ans {
		a := ans[i]
		b := bns[i]

		t.True(a.Address().Equal(b.Address()))
		t.True(a.Publickey().Equal(b.Publickey()))
	}

	upolicyfact := u.Facts[1].(isaacoperation.GenesisNetworkPolicyFact)
	t.Nil(upolicyfact.Hash())
	t.Empty(upolicyfact.Token())

	t.True(policyfact.Hint().Equal(upolicyfact.Hint()))
	base.EqualNetworkPolicy(t.Assert(), policyfact.Policy(), upolicyfact.Policy())
}

func TestGenesisOpertionsDesign(t *testing.T) {
	suite.Run(t, new(testGenesisOpertionsDesign))
}

func TestLocalParamsYAML(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	networkID := base.NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		p := defaultLocalParams(networkID)
		p.ISAAC.SetThreshold(77.7)
		p.Memberlist.SetTCPTimeout(time.Second * 6)
		p.Memberlist.SetUDPBufferSize(333)
		p.Memberlist.SetRetransmitMult(3)
		p.Memberlist.SetExtraSameMemberLimit(9)
		p.MISC.SetValidProposalOperationExpire(time.Second * 11)

		b, err := yaml.Marshal(&p)
		t.NoError(err)

		t.T().Log("marshaled:\n", string(b))

		return p, b
	}
	t.Decode = func(b []byte) interface{} {
		p := defaultLocalParams(networkID)

		enc := jsonenc.NewEncoder()

		t.NoError(p.DecodeYAML(b, enc))

		t.NotNil(p)
		t.NoError(p.IsValid(networkID))

		return p
	}
	t.Compare = func(a, b interface{}) {
		ap, ok := a.(*LocalParams)
		t.True(ok)
		bp, ok := b.(*LocalParams)
		t.True(ok)

		isaac.EqualLocalParams(t.Assert(), ap.ISAAC, bp.ISAAC)
		equalMemberlistParams(t.Assert(), ap.Memberlist, bp.Memberlist)
		equalMISCParams(t.Assert(), ap.MISC, bp.MISC)

		nw := defaultNetworkParams()
		equalNetworkParams(t.Assert(), nw, ap.Network)
	}

	suite.Run(tt, t)
}

func TestLocalParamsJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	networkID := base.NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		p := defaultLocalParams(networkID)
		p.ISAAC.SetThreshold(77.7)
		p.Memberlist.SetTCPTimeout(time.Second * 6)
		p.Memberlist.SetUDPBufferSize(333)
		p.Memberlist.SetRetransmitMult(3)
		p.Memberlist.SetExtraSameMemberLimit(9)
		p.MISC.SetValidProposalOperationExpire(time.Second * 11)

		b, err := util.MarshalJSONIndent(&p)
		t.NoError(err)

		t.T().Log("marshaled:\n", string(b))

		return p, b
	}
	t.Decode = func(b []byte) interface{} {
		p := defaultLocalParams(networkID)

		t.NoError(util.UnmarshalJSON(b, p))

		t.NotNil(p)
		t.NoError(p.IsValid(networkID))

		return p
	}
	t.Compare = func(a, b interface{}) {
		ap, ok := a.(*LocalParams)
		t.True(ok)
		bp, ok := b.(*LocalParams)
		t.True(ok)

		isaac.EqualLocalParams(t.Assert(), ap.ISAAC, bp.ISAAC)
		equalMemberlistParams(t.Assert(), ap.Memberlist, bp.Memberlist)
		equalMISCParams(t.Assert(), ap.MISC, bp.MISC)

		nw := defaultNetworkParams()
		equalNetworkParams(t.Assert(), nw, ap.Network)
	}

	suite.Run(tt, t)
}

type testNetworkParams struct {
	suite.Suite
}

func (t *testNetworkParams) TestGetHandlerTimeout() {
	params := defaultNetworkParams()

	t.Run("get; unknown", func() {
		_, err := params.HandlerTimeout(util.UUID().String())
		t.Error(err)
		t.True(errors.Is(err, util.ErrNotFound))
	})

	t.Run("set; unknown", func() {
		err := params.SetHandlerTimeout(util.UUID().String(), time.Second)
		t.Error(err)
		t.True(errors.Is(err, util.ErrNotFound))
	})

	t.Run("not set", func() {
		d, err := params.HandlerTimeout("set_allow_consensus")
		t.NoError(err)
		t.Equal(params.DefaultHandlerTimeout(), d)
	})

	t.Run("set", func() {
		n := params.DefaultHandlerTimeout() + 33
		t.NoError(params.SetHandlerTimeout("set_allow_consensus", n))

		d, err := params.HandlerTimeout("set_allow_consensus")
		t.NoError(err)
		t.Equal(n, d)
	})

	t.Run("same with default timeout", func() {
		n := params.DefaultHandlerTimeout() + 33
		t.NoError(params.SetHandlerTimeout("set_allow_consensus", n))

		_, found := params.handlerTimeouts["set_allow_consensus"]
		t.True(found)

		t.NoError(params.SetHandlerTimeout("set_allow_consensus", params.DefaultHandlerTimeout()))

		d, err := params.HandlerTimeout("set_allow_consensus")
		t.NoError(err)
		t.Equal(params.DefaultHandlerTimeout(), d)

		_, found = params.handlerTimeouts["set_allow_consensus"]
		t.False(found)
	})
}

func (t *testNetworkParams) TestDecode() {
	params := defaultNetworkParams()

	t.NoError(params.SetHandlerTimeout("set_allow_consensus", params.DefaultHandlerTimeout()+33))
	t.NoError(params.SetHandlerTimeout("ask_handover", params.DefaultHandlerTimeout()+44))

	t.Run("marshal json", func() {
		b, err := util.MarshalJSONIndent(params)
		t.NoError(err)
		t.T().Log("marshaled:\n", string(b))

		uparams := defaultNetworkParams()
		t.NoError(util.UnmarshalJSON(b, uparams))

		equalNetworkParams(t.Assert(), params, uparams)
	})

	t.Run("marshal yaml", func() {
		b, err := yaml.Marshal(params)
		t.NoError(err)
		t.T().Log("marshaled:\n", string(b))

		uparams := defaultNetworkParams()
		t.NoError(yaml.Unmarshal(b, uparams))

		equalNetworkParams(t.Assert(), params, uparams)
	})
}

func (t *testNetworkParams) TestInvalidRateLimitHandler() {
	y := `
default:
  default: 33/s
  proposal: 44/m
suffrage:
  default: 333/s
  state: 444/h
net:
  - 192.168.0.0/24:
      default: 3333/s
      blockmap: 4444/h
node:
  no0sas:
    default: 33333/s
    operatio: 44444/ns
`

	var a *NetworkRateLimitParams
	t.NoError(yaml.Unmarshal([]byte(y), &a))

	err := a.IsValid(nil)
	t.Error(err)
	t.ErrorContains(err, "unknown network handler")
}

func TestNetworkParams(t *testing.T) {
	suite.Run(t, new(testNetworkParams))
}

func mustResolveUDPAddr(s string) *net.UDPAddr {
	a, _ := net.ResolveUDPAddr("udp", s)

	return a
}

func equalMemberlistParams(t *assert.Assertions, a, b *MemberlistParams) {
	t.Equal(a.TCPTimeout(), b.TCPTimeout())
	t.Equal(a.RetransmitMult(), b.RetransmitMult())
	t.Equal(a.ProbeTimeout(), b.ProbeTimeout())
	t.Equal(a.ProbeInterval(), b.ProbeInterval())
	t.Equal(a.SuspicionMult(), b.SuspicionMult())
	t.Equal(a.SuspicionMaxTimeoutMult(), b.SuspicionMaxTimeoutMult())
	t.Equal(a.UDPBufferSize(), b.UDPBufferSize())
	t.Equal(a.ExtraSameMemberLimit(), b.ExtraSameMemberLimit())
}

func equalMISCParams(t *assert.Assertions, a, b *MISCParams) {
	t.Equal(a.SyncSourceCheckerInterval(), b.SyncSourceCheckerInterval())
	t.Equal(a.ValidProposalOperationExpire(), b.ValidProposalOperationExpire())
	t.Equal(a.ValidProposalSuffrageOperationsExpire(), b.ValidProposalSuffrageOperationsExpire())
	t.Equal(a.MaxMessageSize(), b.MaxMessageSize())
	t.Equal(a.ObjectCacheSize(), b.ObjectCacheSize())
}

func equalNetworkParams(t *assert.Assertions, a, b *NetworkParams) {
	t.Equal(a.TimeoutRequest(), b.TimeoutRequest())
	t.Equal(a.HandshakeIdleTimeout(), b.HandshakeIdleTimeout())
	t.Equal(a.MaxIdleTimeout(), b.MaxIdleTimeout())
	t.Equal(a.KeepAlivePeriod(), b.KeepAlivePeriod())
	t.Equal(a.DefaultHandlerTimeout(), b.DefaultHandlerTimeout())

	t.Equal(len(a.handlerTimeouts), len(b.handlerTimeouts))

	for i, da := range a.handlerTimeouts {
		db, found := b.handlerTimeouts[i]
		t.True(found)

		t.Equal(da, db)
	}
}
