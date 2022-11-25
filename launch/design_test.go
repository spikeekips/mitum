package launch

import (
	"net"
	"net/url"
	"path/filepath"
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
	"github.com/stretchr/testify/suite"
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
		t.ErrorContains(err, "failed to decode node")
	})

	t.Run("ok: NodeConnInfo", func() {
		b := []byte(`
- type: sync-source-node
  address: showme-nodesas
  publickey: oxkQTcfKzrC67GE8ChZmZw8SBBBYefMp5859R2AZ8bB9mpu
  publish: a.b.c.d:1234
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
		t.Equal("a.b.c.d:1234", a.Addr().String())
		t.True(a.TLSInsecure())
	})

	t.Run("ok: SuffrageNode", func() {
		b := []byte(`
- type: sync-source-suffrage-nodes
  publish: a.b.c.d:1234
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
		t.Equal("a.b.c.d:1234", a.Addr().String())
		t.True(a.TLSInsecure())
	})

	t.Run("ok: SyncSource", func() {
		b := []byte(`
- type: sync-source-sync-sources
  publish: a.b.c.d:1234
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
		t.Equal("a.b.c.d:1234", a.Addr().String())
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
			LocalParams: isaac.DefaultLocalParams(networkID),
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

		nci := isaacnetwork.NewNodeConnInfo(
			isaac.NewNode(base.NewMPrivatekey().Publickey(), address),
			publish.String(), true,
		)

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
				{Type: isaacnetwork.SyncSourceTypeURL, Source: &url.URL{Scheme: "https", Host: "a:1234"}},
				{Type: isaacnetwork.SyncSourceTypeNode, Source: nci},
			},
		}

		err := a.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "same node address with local")
	})

	t.Run("same sync_sources with publish", func() {
		nci := isaacnetwork.NewNodeConnInfo(
			isaac.NewNode(base.NewMPrivatekey().Publickey(), base.RandomAddress("")),
			publish.String(), true,
		)

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

		err := a.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "sync source has same with publish address")
	})

	t.Run("same sync_sources with resolved publish", func() {
		publishstring := "localhost:1234"
		publish := mustResolveUDPAddr(publishstring)

		nci := isaacnetwork.NewNodeConnInfo(
			isaac.NewNode(base.NewMPrivatekey().Publickey(), base.RandomAddress("")),
			publish.String(), true,
		)

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

		err := a.IsValid(nil)
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
		t.ErrorContains(err, "no such host")
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

	t.Run("missing local params", func() {
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
  threshold: 77.7
  wait_preparing_init_ballot: 10s
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

		params := isaac.DefaultLocalParams(a.NetworkID)
		params.SetThreshold(77.7)
		params.SetWaitPreparingINITBallot(time.Second * 10)
		params.SetValidProposalOperationExpire(time.Second * 11)

		isaac.EqualLocalParams(t.Assert(), params, a.LocalParams)
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

func mustResolveUDPAddr(s string) *net.UDPAddr {
	a, _ := net.ResolveUDPAddr("udp", s)

	return a
}
