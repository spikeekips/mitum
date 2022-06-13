package launch

import (
	"net/netip"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
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

		t.Equal(DefaultStorageBase, a.Base)
		t.Equal(LeveldbURIScheme+"://"+"/a/b/c", a.Database.String())
	})

	t.Run("empty database", func() {
		a := NodeStorageDesign{
			Base: "/tmp/a/b/c",
		}

		t.NoError(a.IsValid(nil))

		t.Equal("/tmp/a/b/c", a.Base)
		t.Equal(DefaultStorageDatabase.String(), a.Database.String())
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
	addrport := netip.MustParseAddrPort("1.2.3.4:4321")
	t.Run("ok", func() {
		a := NodeNetworkDesign{
			Bind:        &addrport,
			Publish:     "4.3.2.1:1234",
			TLSInsecure: true,
		}

		t.NoError(a.IsValid(nil))
	})

	t.Run("wrong bind", func() {
		addrport := netip.AddrPortFrom(netip.Addr{}, 1234)
		a := NodeNetworkDesign{
			Bind:        &addrport,
			Publish:     "4.3.2.1:1234",
			TLSInsecure: true,
		}

		err := a.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "invalid bind")
	})

	t.Run("wrong publish; empty port", func() {
		a := NodeNetworkDesign{
			Bind:        &addrport,
			Publish:     "4.3.2.1:",
			TLSInsecure: true,
		}

		err := a.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "empty port")
	})

	t.Run("wrong publish; missing port", func() {
		a := NodeNetworkDesign{
			Bind:        &addrport,
			Publish:     "4.3.2.1",
			TLSInsecure: true,
		}

		err := a.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "missing port in address")
	})

	t.Run("wrong publish; empty host", func() {
		a := NodeNetworkDesign{
			Bind:        &addrport,
			Publish:     ":4321",
			TLSInsecure: true,
		}

		err := a.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "empty host")
	})

	t.Run("empty publish", func() {
		a := NodeNetworkDesign{
			Bind:        &addrport,
			Publish:     "",
			TLSInsecure: true,
		}

		t.NoError(a.IsValid(nil))
	})
}

func (t *testNodeNetworkDesign) TestDecode() {
	t.Run("ok", func() {
		b := []byte(`
bind: 0.0.0.0:1234
publish: a.b.c.d:4321
tls_insecure: true
`)

		var a NodeNetworkDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("0.0.0.0:1234", a.Bind.String())
		t.Equal("a.b.c.d:4321", a.Publish)
		t.Equal(true, a.TLSInsecure)
	})

	t.Run("empty bind", func() {
		b := []byte(`
publish: a.b.c.d:4321
tls_insecure: true
`)

		var a NodeNetworkDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Nil(a.Bind)
		t.Equal("a.b.c.d:4321", a.Publish)
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
		t.Equal("", a.Publish)
		t.Equal(true, a.TLSInsecure)
	})

	t.Run("empty TLSInsecure", func() {
		b := []byte(`
bind: 0.0.0.0:1234
publish: a.b.c.d:4321
`)

		var a NodeNetworkDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("0.0.0.0:1234", a.Bind.String())
		t.Equal("a.b.c.d:4321", a.Publish)
		t.Equal(false, a.TLSInsecure)
	})
}

func TestNodeNetworkDesign(t *testing.T) {
	suite.Run(t, new(testNodeNetworkDesign))
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
	addrport := netip.MustParseAddrPort("1.2.3.4:4321")

	t.Run("ok", func() {
		a := NodeDesign{
			Address:    base.RandomAddress(""),
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  base.NetworkID(util.UUID().String()),
			Network: NodeNetworkDesign{
				Bind:        &addrport,
				Publish:     "4.3.2.1:1234",
				TLSInsecure: true,
			},
			Storage: NodeStorageDesign{
				Base:     "/tmp/a/b/c",
				Database: &url.URL{Scheme: LeveldbURIScheme, Path: "/a/b/c"},
			},
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
		t.Empty(a.Network.Publish)
	})

	t.Run("empty storage", func() {
		a := NodeDesign{
			Address:    base.RandomAddress(""),
			Privatekey: base.NewMPrivatekey(),
			NetworkID:  base.NetworkID(util.UUID().String()),
			Network: NodeNetworkDesign{
				Bind:        &addrport,
				Publish:     "4.3.2.1:1234",
				TLSInsecure: true,
			},
		}

		t.NoError(a.IsValid(nil))

		t.Equal(DefaultStorageBase+"/"+a.Address.String(), a.Storage.Base)
		t.Equal((&url.URL{
			Scheme: LeveldbURIScheme,
			Path:   filepath.Join(defaultStorageBase, a.Address.String(), "perm"),
		}).String(), a.Storage.Database.String())
	})
}

func (t *testNodeDesign) TestDecode() {
	t.Run("ok", func() {
		b := []byte(`
address: no0sas
privatekey: L5GTSKkRs9NPsXwYgACZdodNUJqCAWjz2BccuR4cAgxJumEZWjokmpr
network_id: hehe 1 2 3 4
network:
  bind: 0.0.0.0:1234
  publish: a.b.c.d:4321
  tls_insecure: true
storage:
  base: /tmp/a/b/c
  database: redis://
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("L5GTSKkRs9NPsXwYgACZdodNUJqCAWjz2BccuR4cAgxJumEZWjokmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Equal("0.0.0.0:1234", a.Network.Bind.String())
		t.Equal("a.b.c.d:4321", a.Network.Publish)
		t.Equal(true, a.Network.TLSInsecure)

		t.Equal("/tmp/a/b/c", a.Storage.Base)
		t.Equal("redis:", a.Storage.Database.String())
	})

	t.Run("empty network", func() {
		b := []byte(`
address: no0sas
privatekey: L5GTSKkRs9NPsXwYgACZdodNUJqCAWjz2BccuR4cAgxJumEZWjokmpr
network_id: hehe 1 2 3 4
storage:
  base: /tmp/a/b/c
  database: redis://
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("L5GTSKkRs9NPsXwYgACZdodNUJqCAWjz2BccuR4cAgxJumEZWjokmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Nil(a.Network.Bind)
		t.Equal("", a.Network.Publish)
		t.Equal(false, a.Network.TLSInsecure)

		t.Equal("/tmp/a/b/c", a.Storage.Base)
		t.Equal("redis:", a.Storage.Database.String())
	})

	t.Run("empty storage", func() {
		b := []byte(`
address: no0sas
privatekey: L5GTSKkRs9NPsXwYgACZdodNUJqCAWjz2BccuR4cAgxJumEZWjokmpr
network_id: hehe 1 2 3 4
network:
  bind: 0.0.0.0:1234
  publish: a.b.c.d:4321
  tls_insecure: true
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("L5GTSKkRs9NPsXwYgACZdodNUJqCAWjz2BccuR4cAgxJumEZWjokmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Equal("0.0.0.0:1234", a.Network.Bind.String())
		t.Equal("a.b.c.d:4321", a.Network.Publish)
		t.Equal(true, a.Network.TLSInsecure)

		t.Equal("", a.Storage.Base)
		t.Nil(a.Storage.Database)
	})
}

func (t *testNodeDesign) TestEncode() {
	t.Run("ok", func() {
		b := []byte(`
address: no0sas
privatekey: L5GTSKkRs9NPsXwYgACZdodNUJqCAWjz2BccuR4cAgxJumEZWjokmpr
network_id: hehe 1 2 3 4
network:
  bind: 0.0.0.0:1234
  publish: a.b.c.d:4321
  tls_insecure: true
storage:
  base: /tmp/a/b/c
  database: redis://
`)

		var a NodeDesign
		t.NoError(a.DecodeYAML(b, t.enc))

		t.Equal("no0sas", a.Address.String())
		t.Equal("L5GTSKkRs9NPsXwYgACZdodNUJqCAWjz2BccuR4cAgxJumEZWjokmpr", a.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(a.NetworkID))

		t.Equal("0.0.0.0:1234", a.Network.Bind.String())
		t.Equal("a.b.c.d:4321", a.Network.Publish)
		t.Equal(true, a.Network.TLSInsecure)

		t.Equal("/tmp/a/b/c", a.Storage.Base)
		t.Equal("redis:", a.Storage.Database.String())

		ub, err := yaml.Marshal(a)
		t.NoError(err)

		t.T().Logf("marshaled:\n%s", string(ub))

		var ua NodeDesign
		t.NoError(ua.DecodeYAML(ub, t.enc))

		t.Equal("no0sas", ua.Address.String())
		t.Equal("L5GTSKkRs9NPsXwYgACZdodNUJqCAWjz2BccuR4cAgxJumEZWjokmpr", ua.Privatekey.String())
		t.Equal("hehe 1 2 3 4", string(ua.NetworkID))

		t.Equal("0.0.0.0:1234", ua.Network.Bind.String())
		t.Equal("a.b.c.d:4321", ua.Network.Publish)
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
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: isaacoperation.SuffrageGenesisJoinPermissionFactHint, Instance: isaacoperation.SuffrageGenesisJoinPermissionFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: isaacoperation.GenesisNetworkPolicyFactHint, Instance: isaacoperation.GenesisNetworkPolicyFact{}}))

	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testGenesisOpertionsDesign) newSuffrageGenesisJoinPermissionFact() isaacoperation.SuffrageGenesisJoinPermissionFact {
	return isaacoperation.NewSuffrageGenesisJoinPermissionFact(
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
	suffact := t.newSuffrageGenesisJoinPermissionFact()
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

	usuffact := u.Facts[0].(isaacoperation.SuffrageGenesisJoinPermissionFact)
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
