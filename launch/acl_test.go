package launch

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v3"
)

type baseTestACL struct {
	suite.Suite
	enc *jsonenc.Encoder
}

func (t *baseTestACL) SetupSuite() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
}

func (t *baseTestACL) printYAML(name string, i interface{}) {
	b, _ := yaml.Marshal(i)
	t.T().Logf("%s:\n%s", name, string(b))
}

func (t *baseTestACL) compareACL(acl *ACL, om *util.YAMLOrderedMap) {
	om.Traverse(func(k string, v interface{}) bool {
		am, ok := v.(*util.YAMLOrderedMap)
		t.True(ok)

		bm, found := acl.m.Value(k)
		t.True(found)

		am.Traverse(func(amk string, amv interface{}) bool {
			i, found := bm[ACLScope(amk)]
			t.True(found)

			j, ok := amv.(string)
			t.True(ok)

			t.Equal(j, i.String())

			return true
		})

		return true
	})
}

func (t *testACL) checkallow(acl *ACL, user string, scope ACLScope, required, expected ACLPerm) bool {
	given, allow := acl.Allow(user, scope, required)

	t.Equal(expected, given, "expected=%s given=%s", expected, given)

	return allow
}

type testACL struct {
	baseTestACL
}

func (t *testACL) TestParseACLYAML() {
	t.Run("ok", func() {
		y := `
_default:
  bd: x
  cd: oo
  ad: o
0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu:
  am: oo
  cm: x
  bm: o
03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu:
  ce: o
  be: oo
  ae: x
`
		t.T().Logf("source:\n%s", y)

		acl, _ := NewACL(9, "")

		om, updated, err := loadACLFromYAML(acl, []byte(y), t.enc)
		t.NoError(err)
		t.NotNil(om)
		t.NotNil(acl)
		t.True(updated)

		t.printYAML("om -> yml", om)
		t.compareACL(acl, om)
	})

	t.Run("empty user string", func() {
		y := `
_default:
  bd: x
"":
  am: oo
`
		t.T().Logf("source:\n%s", y)

		acl, _ := NewACL(9, "")

		_, _, err := loadACLFromYAML(acl, []byte(y), t.enc)
		t.Error(err)
		t.ErrorContains(err, "empty user")
	})

	t.Run("not updated", func() {
		y := `
_default:
  bd: x
  cd: oo
  ad: o
03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu:
  ce: o
  be: oo
  ae: x
0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu:
  am: oo
  cm: x
  bm: o
`
		t.T().Logf("source:\n%s", y)

		acl, _ := NewACL(9, "")

		om, updated, err := loadACLFromYAML(acl, []byte(y), t.enc)
		t.NoError(err)
		t.NotNil(om)
		t.NotNil(acl)
		t.True(updated)

		uom, uupdated, uerr := loadACLFromYAML(acl, []byte(y), t.enc)
		t.NoError(uerr)
		t.False(uupdated)
		t.NotNil(uom)

		t.Equal(om.Len(), uom.Len())
		om.Traverse(func(k string, v interface{}) bool {
			i, found := uom.Get(k)
			t.True(found)
			t.Equal(v, i)

			return true
		})
	})
}

func (t *testACL) TestParseACLYAMLError() {
	t.Run("__default", func() {
		y := `
__default:
  bd: x
0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu:
  am: oo
`
		acl, _ := NewACL(9, "")

		_, _, err := loadACLFromYAML(acl, []byte(y), t.enc)
		t.Error(err)
		t.ErrorContains(err, "decode publickey")
	})

	t.Run("wrong publickey", func() {
		y := `
_default:
  bd: x
mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUu:
  am: oo
`
		acl, _ := NewACL(9, "")

		_, _, err := loadACLFromYAML(acl, []byte(y), t.enc)
		t.Error(err)
		t.ErrorContains(err, "decode publickey")
	})

	t.Run("wrong perm", func() {
		y := `
_default:
  bd: k
`
		acl, _ := NewACL(9, "")

		_, _, err := loadACLFromYAML(acl, []byte(y), t.enc)
		t.Error(err)
		t.ErrorContains(err, "unknown perm")
	})

	t.Run("empty perm", func() {
		y := `
_default:
  bd: ""
`
		acl, _ := NewACL(9, "")

		_, _, err := loadACLFromYAML(acl, []byte(y), t.enc)
		t.Error(err)
		t.ErrorContains(err, "empty perm")
	})
}

func (t *testACL) TestAllow() {
	y := `
_default:
  bd: x
  cd: oo
  ad: o

  am: x
  cm: x
  bm: x
  ce: o
  be: x
  ae: x
0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu:
  am: oo
  cm: x
  bm: o
`
	acl, _ := NewACL(9, "03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu")

	_, _, err := loadACLFromYAML(acl, []byte(y), t.enc)
	t.NoError(err)

	t.Run("default user", func() {
		t.False(t.checkallow(acl, defaultACLUser, ACLScope("bd"), NewAllowACLPerm(0), aclPermProhibit))
		t.True(t.checkallow(acl, defaultACLUser, ACLScope("cd"), NewAllowACLPerm(0), NewAllowACLPerm(1)))
		t.True(t.checkallow(acl, defaultACLUser, ACLScope("cd"), NewAllowACLPerm(1), NewAllowACLPerm(1)))
		t.False(t.checkallow(acl, defaultACLUser, ACLScope("ad"), NewAllowACLPerm(1), NewAllowACLPerm(0)))
		t.True(t.checkallow(acl, defaultACLUser, ACLScope("ad"), NewAllowACLPerm(0), NewAllowACLPerm(0)))

		t.False(t.checkallow(acl, defaultACLUser, ACLScope("am"), NewAllowACLPerm(0), aclPermProhibit))
		t.False(t.checkallow(acl, defaultACLUser, ACLScope("cm"), NewAllowACLPerm(0), aclPermProhibit))
		t.False(t.checkallow(acl, defaultACLUser, ACLScope("bm"), NewAllowACLPerm(0), aclPermProhibit))
		t.True(t.checkallow(acl, defaultACLUser, ACLScope("ce"), NewAllowACLPerm(0), NewAllowACLPerm(0)))
		t.False(t.checkallow(acl, defaultACLUser, ACLScope("ce"), NewAllowACLPerm(1), NewAllowACLPerm(0)))
		t.False(t.checkallow(acl, defaultACLUser, ACLScope("be"), NewAllowACLPerm(0), aclPermProhibit))
		t.False(t.checkallow(acl, defaultACLUser, ACLScope("ae"), NewAllowACLPerm(0), aclPermProhibit))
	})

	t.Run("0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu", func() {
		u := "0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu"

		t.False(t.checkallow(acl, u, ACLScope("cm"), NewAllowACLPerm(0), aclPermProhibit))
		t.True(t.checkallow(acl, u, ACLScope("am"), NewAllowACLPerm(0), NewAllowACLPerm(1)))
		t.True(t.checkallow(acl, u, ACLScope("am"), NewAllowACLPerm(1), NewAllowACLPerm(1)))
		t.False(t.checkallow(acl, u, ACLScope("bm"), NewAllowACLPerm(1), NewAllowACLPerm(0)))
		t.True(t.checkallow(acl, u, ACLScope("bm"), NewAllowACLPerm(0), NewAllowACLPerm(0)))

		t.Run("from default", func() {
			t.False(t.checkallow(acl, defaultACLUser, ACLScope("bd"), NewAllowACLPerm(0), aclPermProhibit))
			t.True(t.checkallow(acl, defaultACLUser, ACLScope("cd"), NewAllowACLPerm(0), NewAllowACLPerm(1)))
			t.True(t.checkallow(acl, defaultACLUser, ACLScope("cd"), NewAllowACLPerm(1), NewAllowACLPerm(1)))
			t.False(t.checkallow(acl, defaultACLUser, ACLScope("ad"), NewAllowACLPerm(1), NewAllowACLPerm(0)))
			t.True(t.checkallow(acl, defaultACLUser, ACLScope("ad"), NewAllowACLPerm(0), NewAllowACLPerm(0)))

			t.True(t.checkallow(acl, defaultACLUser, ACLScope("ce"), NewAllowACLPerm(0), NewAllowACLPerm(0)))
			t.False(t.checkallow(acl, defaultACLUser, ACLScope("be"), NewAllowACLPerm(0), aclPermProhibit))
			t.False(t.checkallow(acl, defaultACLUser, ACLScope("ae"), NewAllowACLPerm(0), aclPermProhibit))
		})
	})

	t.Run("unknown user", func() {
		u := "showme"

		t.False(t.checkallow(acl, u, ACLScope("bd"), NewAllowACLPerm(0), aclPermProhibit))
		t.True(t.checkallow(acl, u, ACLScope("cd"), NewAllowACLPerm(0), NewAllowACLPerm(1)))
		t.True(t.checkallow(acl, u, ACLScope("cd"), NewAllowACLPerm(1), NewAllowACLPerm(1)))
		t.False(t.checkallow(acl, u, ACLScope("ad"), NewAllowACLPerm(1), NewAllowACLPerm(0)))
		t.True(t.checkallow(acl, u, ACLScope("ad"), NewAllowACLPerm(0), NewAllowACLPerm(0)))

		t.False(t.checkallow(acl, u, ACLScope("am"), NewAllowACLPerm(0), aclPermProhibit))
		t.False(t.checkallow(acl, u, ACLScope("cm"), NewAllowACLPerm(0), aclPermProhibit))
		t.False(t.checkallow(acl, u, ACLScope("bm"), NewAllowACLPerm(0), aclPermProhibit))
		t.True(t.checkallow(acl, u, ACLScope("ce"), NewAllowACLPerm(0), NewAllowACLPerm(0)))
		t.False(t.checkallow(acl, u, ACLScope("be"), NewAllowACLPerm(0), aclPermProhibit))
		t.False(t.checkallow(acl, u, ACLScope("ae"), NewAllowACLPerm(0), aclPermProhibit))
	})

	t.Run("super", func() {
		u := "03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu"

		t.True(t.checkallow(acl, u, ACLScope("cm"), NewAllowACLPerm(0), aclPermSuper))
		t.True(t.checkallow(acl, u, ACLScope("am"), NewAllowACLPerm(0), aclPermSuper))
		t.True(t.checkallow(acl, u, ACLScope("am"), NewAllowACLPerm(1), aclPermSuper))
		t.True(t.checkallow(acl, u, ACLScope("bm"), NewAllowACLPerm(1), aclPermSuper))
		t.True(t.checkallow(acl, u, ACLScope("bm"), NewAllowACLPerm(0), aclPermSuper))
	})
}

func (t *testACL) TestSetUser() {
	y := `
_default:
  _default: x
`
	t.T().Logf("source:\n%s", y)

	acl, _ := NewACL(9, "")

	_, _, err := loadACLFromYAML(acl, []byte(y), t.enc)
	t.NoError(err)

	t.Run("unknown user", func() {
		u := "0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu"

		t.False(t.checkallow(acl, u, ACLScope("a"), NewAllowACLPerm(0), aclPermProhibit))
		t.False(t.checkallow(acl, u, ACLScope("a"), NewAllowACLPerm(1), aclPermProhibit))
	})

	t.Run("set new user", func() {
		u := "0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu"

		prev, updated, err := acl.setUser(u, map[ACLScope]ACLPerm{"b": NewAllowACLPerm(0)})
		t.NoError(err)
		t.True(updated)
		t.Nil(prev)

		t.printYAML("0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu", acl.m.Map())

		t.False(t.checkallow(acl, u, ACLScope("a"), NewAllowACLPerm(0), aclPermProhibit))
		t.False(t.checkallow(acl, u, ACLScope("a"), NewAllowACLPerm(1), aclPermProhibit))

		t.True(t.checkallow(acl, u, ACLScope("b"), NewAllowACLPerm(0), NewAllowACLPerm(0)))
		t.False(t.checkallow(acl, u, ACLScope("b"), NewAllowACLPerm(1), NewAllowACLPerm(0)))

		t.Run("unknown user", func() {
			u := "03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu"

			t.False(t.checkallow(acl, u, ACLScope("a"), NewAllowACLPerm(0), aclPermProhibit))
			t.False(t.checkallow(acl, u, ACLScope("a"), NewAllowACLPerm(1), aclPermProhibit))
			t.False(t.checkallow(acl, u, ACLScope("b"), NewAllowACLPerm(0), aclPermProhibit))
			t.False(t.checkallow(acl, u, ACLScope("b"), NewAllowACLPerm(1), aclPermProhibit))
		})
	})

	t.Run("set new user", func() {
		u := "03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu"

		prev, updated, err := acl.setUser(u, map[ACLScope]ACLPerm{"c": NewAllowACLPerm(1)})
		t.NoError(err)
		t.True(updated)
		t.Nil(prev)

		t.printYAML(u, acl.m.Map())

		t.False(t.checkallow(acl, u, ACLScope("a"), NewAllowACLPerm(0), aclPermProhibit))
		t.False(t.checkallow(acl, u, ACLScope("a"), NewAllowACLPerm(1), aclPermProhibit))
		t.False(t.checkallow(acl, u, ACLScope("b"), NewAllowACLPerm(0), aclPermProhibit))
		t.False(t.checkallow(acl, u, ACLScope("b"), NewAllowACLPerm(1), aclPermProhibit))

		t.True(t.checkallow(acl, u, ACLScope("c"), NewAllowACLPerm(0), NewAllowACLPerm(1)))
		t.True(t.checkallow(acl, u, ACLScope("c"), NewAllowACLPerm(1), NewAllowACLPerm(1)))

		t.Run("unknown user", func() {
			u := "kmbahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu"

			t.False(t.checkallow(acl, u, ACLScope("a"), NewAllowACLPerm(0), aclPermProhibit))
			t.False(t.checkallow(acl, u, ACLScope("a"), NewAllowACLPerm(1), aclPermProhibit))
			t.False(t.checkallow(acl, u, ACLScope("b"), NewAllowACLPerm(0), aclPermProhibit))
			t.False(t.checkallow(acl, u, ACLScope("b"), NewAllowACLPerm(1), aclPermProhibit))
			t.False(t.checkallow(acl, u, ACLScope("c"), NewAllowACLPerm(0), aclPermProhibit))
			t.False(t.checkallow(acl, u, ACLScope("c"), NewAllowACLPerm(1), aclPermProhibit))
		})
	})

	t.Run("set new user; empty map", func() {
		u := "03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu"

		prev, updated, err := acl.setUser(u, map[ACLScope]ACLPerm{})
		t.NoError(err)
		t.True(updated)
		t.NotNil(prev)

		t.printYAML("prev", prev)
		t.printYAML("03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu", acl.m.Map())
	})

	t.Run("override", func() {
		u := "0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu"

		prev, updated, err := acl.setUser(u, map[ACLScope]ACLPerm{"b": aclPermProhibit})
		t.NoError(err)
		t.True(updated)
		t.NotNil(prev)

		t.printYAML("prev", prev)
		t.printYAML("0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu", acl.m.Map())

		t.False(t.checkallow(acl, u, ACLScope("b"), NewAllowACLPerm(0), aclPermProhibit))
		t.False(t.checkallow(acl, u, ACLScope("b"), NewAllowACLPerm(1), aclPermProhibit))
	})

	t.Run("override; not updated", func() {
		u := "0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu"

		prev, updated, err := acl.setUser(u, map[ACLScope]ACLPerm{"b": aclPermProhibit})
		t.NoError(err)
		t.False(updated)
		t.NotNil(prev)

		t.printYAML("prev", prev)
		t.printYAML("0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu", acl.m.Map())
	})
}

func (t *testACL) TestSuperUser() {
	y := `
_default:
  a: o
  b: o
03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu:
  a: x
  b: x
`
	t.T().Logf("source:\n%s", y)

	superuser := "0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu"
	acl, _ := NewACL(9, superuser)

	t.Run("allow", func() {
		t.True(t.checkallow(acl, superuser, ACLScope("a"), NewAllowACLPerm(2), aclPermSuper))
		t.True(t.checkallow(acl, superuser, ACLScope("b"), NewAllowACLPerm(2), aclPermSuper))
		t.True(t.checkallow(acl, superuser, ACLScope("c"), NewAllowACLPerm(2), aclPermSuper))
	})

	t.Run("set user", func() {
		_, updated, err := acl.setUser(superuser, map[ACLScope]ACLPerm{"c": NewAllowACLPerm(0)})
		t.Error(err)
		t.False(updated)
		t.ErrorContains(err, "superuser")
	})
}

func TestACL(t *testing.T) {
	suite.Run(t, new(testACL))
}

type testACLPerm struct {
	suite.Suite
}

func (t *testACLPerm) TestYAMLMarshal() {
	t.Run("prohibit", func() {
		b, err := yaml.Marshal(aclPermProhibit)
		t.NoError(err)

		t.Equal("x\n", string(b))
	})

	t.Run("allow1", func() {
		b, err := yaml.Marshal(NewAllowACLPerm(0))
		t.NoError(err)

		t.Equal("o\n", string(b))
	})

	t.Run("allow2", func() {
		b, err := yaml.Marshal(NewAllowACLPerm(1))
		t.NoError(err)

		t.Equal("oo\n", string(b))
	})

	t.Run("unknown", func() {
		b, err := yaml.Marshal(aclPermSuper)
		t.NoError(err)
		t.Equal("s\n", string(b))
	})
}

func (t *testACLPerm) TestConvertACLPerm() {
	cases := []struct {
		name string
		s    string
		p    ACLPerm
		err  string
	}{
		{name: "prohibit", s: "x", p: aclPermProhibit},
		{name: "allow1", s: "o", p: NewAllowACLPerm(0)},
		{name: "allow2", s: "oo", p: NewAllowACLPerm(1)},
		{name: "empty", s: "", err: "empty perm string"},
		{name: "not o", s: "b", err: "unknown perm string"},
		{name: "uppercase o", s: "O", err: "unknown perm string"},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(c.name, func() {
			u, err := convertACLPerm(c.s)
			if len(c.err) > 0 {
				t.ErrorContains(err, c.err, "%d: %v", i, c.name)

				return
			}
			if err != nil {
				t.NoError(err)

				return
			}

			t.Equal(c.p, u, "%d: %v", i, c.name)
		})
	}
}

func TestACLPerm(t *testing.T) {
	suite.Run(t, new(testACLPerm))
}

type testYAMLACL struct {
	baseTestACL
}

func (t *testYAMLACL) TestNew() {
	t.Run("ok", func() {
		y := `
_default:
  bd: x
  cd: oo
  ad: o
0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu:
  am: oo
  cm: x
  bm: o
03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu:
  ce: o
  be: oo
  ae: x
`
		t.T().Logf("source:\n%s", y)

		acl, err := NewACL(9, "")
		t.NoError(err)

		yamlacl := NewYAMLACL(acl)
		updated, err := yamlacl.Import([]byte(y), t.enc)
		t.NoError(err)
		t.True(updated)

		m := yamlacl.Export()

		t.printYAML("new", m)
		t.compareACL(acl, m.(*util.YAMLOrderedMap))
	})

	t.Run("wrong user", func() {
		y := `
_default:
  bd: x
  cd: oo
  ad: o
mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUu:
  am: oo
  cm: x
  bm: o
03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu:
  ce: o
  be: oo
  ae: x
`
		acl, err := NewACL(9, "")
		t.NoError(err)

		yamlacl := NewYAMLACL(acl)
		updated, err := yamlacl.Import([]byte(y), t.enc)
		t.Error(err)
		t.False(updated)
		t.ErrorContains(err, "user")
	})

	t.Run("empty user", func() {
		y := `
_default:
  bd: x
  cd: oo
  ad: o
0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu:
03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu:
  ce: o
  be: oo
  ae: x
`
		acl, err := NewACL(9, "")
		t.NoError(err)

		yamlacl := NewYAMLACL(acl)
		updated, err := yamlacl.Import([]byte(y), t.enc)
		t.Error(err)
		t.False(updated)
		t.ErrorContains(err, "empty user perms")
	})
}

func (t *testYAMLACL) TestImport() {
	t.Run("ok", func() {
		y := `
_default:
  bd: x
  cd: oo
  ad: o
0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu:
  am: oo
  cm: x
  bm: o
03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu:
  ce: o
  be: oo
  ae: x
`
		acl, err := NewACL(9, "")
		t.NoError(err)

		yamlacl := NewYAMLACL(acl)
		updated, err := yamlacl.Import([]byte(y), t.enc)
		t.NoError(err)
		t.True(updated)

		m := yamlacl.Export()

		t.printYAML("new", m)
		t.compareACL(acl, m.(*util.YAMLOrderedMap))

		ny := `
_default:
  bd: x
  cd: oo
  ad: o
0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu:
  dm: oo
  em: x
  fm: o
`
		updated, err = yamlacl.Import([]byte(ny), t.enc)
		t.NoError(err)
		t.True(updated)

		m = yamlacl.Export()
		t.printYAML("imported", m)

		var om *util.YAMLOrderedMap
		t.NoError(yaml.Unmarshal([]byte(ny), &om))

		t.compareACL(yamlacl.ACL, om)
	})

	t.Run("not updated", func() {
		y := `
_default:
  bd: x
  cd: oo
  ad: o
0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu:
  am: oo
  cm: x
  bm: o
03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu:
  ce: o
  be: oo
  ae: x
`
		acl, err := NewACL(9, "")
		t.NoError(err)

		yamlacl := NewYAMLACL(acl)

		updated, err := yamlacl.Import([]byte(y), t.enc)
		t.NoError(err)
		t.True(updated)

		m := yamlacl.Export()

		t.printYAML("new", m)
		t.compareACL(acl, m.(*util.YAMLOrderedMap))

		ny := `
_default:
  bd: x
  cd: oo
  ad: o
03e26fda9b3f1c5c0d66c5538364aac6841002cccc8b1a3acc6863e4ec7990ea34mpu:
  ce: o
  be: oo
  ae: x
0326d53ddc61cdf6eef1deafc23987214342c216192ea233d5900d8f3e2a4ef2d9mpu:
  am: oo
  cm: x
  bm: o
`
		updated, err = yamlacl.Import([]byte(ny), t.enc)
		t.NoError(err)
		t.False(updated)

		var om *util.YAMLOrderedMap
		t.NoError(yaml.Unmarshal([]byte(y), &om))

		t.compareACL(yamlacl.ACL, om)
	})
}

func TestYAMLACL(t *testing.T) {
	suite.Run(t, new(testYAMLACL))
}
