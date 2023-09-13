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

type testACL struct {
	suite.Suite
	enc *jsonenc.Encoder
}

func (t *testACL) SetupSuite() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
}

func (t *testACL) printYAML(name string, i interface{}) {
	b, _ := yaml.Marshal(i)
	t.T().Logf("%s:\n%s", name, string(b))
}

func (t *testACL) compareACL(acl *ACL, om *util.YAMLOrderedMap) {
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

func (t *testACL) TestParseACLYAML() {
	t.Run("ok", func() {
		y := `
_default:
  bd: x
  cd: oo
  ad: o
mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUumpu:
  am: oo
  cm: x
  bm: o
embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu:
  ce: o
  be: oo
  ae: x
`
		t.T().Logf("source:\n%s", y)

		acl, _ := NewACL(9, "")

		om, err := parseACLYAML([]byte(y), acl, t.enc)
		t.NoError(err)
		t.NotNil(om)
		t.NotNil(acl)

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

		_, err := parseACLYAML([]byte(y), acl, t.enc)
		t.Error(err)
		t.ErrorContains(err, "empty user")
	})
}

func (t *testACL) TestParseACLYAMLError() {
	t.Run("__default", func() {
		y := `
__default:
  bd: x
mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUumpu:
  am: oo
`
		acl, _ := NewACL(9, "")

		_, err := parseACLYAML([]byte(y), acl, t.enc)
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

		_, err := parseACLYAML([]byte(y), acl, t.enc)
		t.Error(err)
		t.ErrorContains(err, "decode publickey")
	})

	t.Run("wrong perm", func() {
		y := `
_default:
  bd: k
`
		acl, _ := NewACL(9, "")

		_, err := parseACLYAML([]byte(y), acl, t.enc)
		t.Error(err)
		t.ErrorContains(err, "unknown perm")
	})

	t.Run("empty perm", func() {
		y := `
_default:
  bd: ""
`
		acl, _ := NewACL(9, "")

		_, err := parseACLYAML([]byte(y), acl, t.enc)
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
mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUumpu:
  am: oo
  cm: x
  bm: o
`
	acl, _ := NewACL(9, "embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu")

	_, err := parseACLYAML([]byte(y), acl, t.enc)
	t.NoError(err)

	t.Run("default user", func() {
		t.False(acl.Allow(defaultACLUser, ACLScope("bd"), NewAllowACLPerm(1)))
		t.True(acl.Allow(defaultACLUser, ACLScope("cd"), NewAllowACLPerm(1)))
		t.True(acl.Allow(defaultACLUser, ACLScope("cd"), NewAllowACLPerm(2)))
		t.False(acl.Allow(defaultACLUser, ACLScope("ad"), NewAllowACLPerm(2)))
		t.True(acl.Allow(defaultACLUser, ACLScope("ad"), NewAllowACLPerm(1)))

		t.False(acl.Allow(defaultACLUser, ACLScope("am"), NewAllowACLPerm(1)))
		t.False(acl.Allow(defaultACLUser, ACLScope("cm"), NewAllowACLPerm(1)))
		t.False(acl.Allow(defaultACLUser, ACLScope("bm"), NewAllowACLPerm(1)))
		t.True(acl.Allow(defaultACLUser, ACLScope("ce"), NewAllowACLPerm(1)))
		t.False(acl.Allow(defaultACLUser, ACLScope("ce"), NewAllowACLPerm(2)))
		t.False(acl.Allow(defaultACLUser, ACLScope("be"), NewAllowACLPerm(1)))
		t.False(acl.Allow(defaultACLUser, ACLScope("ae"), NewAllowACLPerm(1)))
	})

	t.Run("mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUumpu", func() {
		u := "mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUumpu"
		t.False(acl.Allow(u, ACLScope("cm"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("am"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("am"), NewAllowACLPerm(2)))
		t.False(acl.Allow(u, ACLScope("bm"), NewAllowACLPerm(2)))
		t.True(acl.Allow(u, ACLScope("bm"), NewAllowACLPerm(1)))

		t.Run("from default", func() {
			t.False(acl.Allow(defaultACLUser, ACLScope("bd"), NewAllowACLPerm(1)))
			t.True(acl.Allow(defaultACLUser, ACLScope("cd"), NewAllowACLPerm(1)))
			t.True(acl.Allow(defaultACLUser, ACLScope("cd"), NewAllowACLPerm(2)))
			t.False(acl.Allow(defaultACLUser, ACLScope("ad"), NewAllowACLPerm(2)))
			t.True(acl.Allow(defaultACLUser, ACLScope("ad"), NewAllowACLPerm(1)))

			t.True(acl.Allow(defaultACLUser, ACLScope("ce"), NewAllowACLPerm(1)))
			t.False(acl.Allow(defaultACLUser, ACLScope("be"), NewAllowACLPerm(1)))
			t.False(acl.Allow(defaultACLUser, ACLScope("ae"), NewAllowACLPerm(1)))
		})
	})

	t.Run("unknown user", func() {
		u := "showme"

		t.False(acl.Allow(u, ACLScope("bd"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("cd"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("cd"), NewAllowACLPerm(2)))
		t.False(acl.Allow(u, ACLScope("ad"), NewAllowACLPerm(2)))
		t.True(acl.Allow(u, ACLScope("ad"), NewAllowACLPerm(1)))

		t.False(acl.Allow(u, ACLScope("am"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("cm"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("bm"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("ce"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("be"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("ae"), NewAllowACLPerm(1)))
	})

	t.Run("super", func() {
		u := "embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu"

		t.True(acl.Allow(u, ACLScope("cm"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("am"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("am"), NewAllowACLPerm(2)))
		t.True(acl.Allow(u, ACLScope("bm"), NewAllowACLPerm(2)))
		t.True(acl.Allow(u, ACLScope("bm"), NewAllowACLPerm(1)))
	})
}

func (t *testACL) TestSetUser() {
	y := `
_default:
  _default: x
`
	t.T().Logf("source:\n%s", y)

	acl, _ := NewACL(9, "")

	_, err := parseACLYAML([]byte(y), acl, t.enc)
	t.NoError(err)

	t.Run("unknown user", func() {
		u := "mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUumpu"

		t.False(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(2)))
	})

	t.Run("set new user", func() {
		u := "mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUumpu"

		prev, err := acl.SetUser(u, map[ACLScope]ACLPerm{"b": NewAllowACLPerm(1)})
		t.NoError(err)
		t.Nil(prev)

		t.printYAML("mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUumpu", acl.m.Map())

		t.False(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(2)))

		t.True(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(2)))

		t.Run("unknown user", func() {
			u := "embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu"

			t.False(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
			t.False(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(2)))
			t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))
			t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(2)))
		})
	})

	t.Run("set new user", func() {
		u := "embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu"

		prev, err := acl.SetUser(u, map[ACLScope]ACLPerm{"c": NewAllowACLPerm(2)})
		t.NoError(err)
		t.Nil(prev)

		t.printYAML("embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu", acl.m.Map())

		t.False(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(2)))
		t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(2)))

		t.True(acl.Allow(u, ACLScope("c"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("c"), NewAllowACLPerm(2)))

		t.Run("unknown user", func() {
			u := "kmbahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu"

			t.False(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
			t.False(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(2)))
			t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))
			t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(2)))
			t.False(acl.Allow(u, ACLScope("c"), NewAllowACLPerm(1)))
			t.False(acl.Allow(u, ACLScope("c"), NewAllowACLPerm(2)))
		})
	})

	t.Run("set new user; empty map", func() {
		u := "embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu"

		prev, err := acl.SetUser(u, map[ACLScope]ACLPerm{})
		t.NoError(err)
		t.NotNil(prev)

		t.printYAML("prev", prev)
		t.printYAML("embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu", acl.m.Map())
	})

	t.Run("override", func() {
		u := "mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUumpu"

		prev, err := acl.SetUser(u, map[ACLScope]ACLPerm{"b": aclPermProhibit})
		t.NoError(err)
		t.NotNil(prev)

		t.printYAML("prev", prev)
		t.printYAML("mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUumpu", acl.m.Map())

		t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(2)))
	})
}

func (t *testACL) TestUpdateScope() {
	y := `
_default:
  a: x
  b: x
`
	t.T().Logf("source:\n%s", y)

	acl, _ := NewACL(9, "")

	_, err := parseACLYAML([]byte(y), acl, t.enc)
	t.NoError(err)

	t.Run("update", func() {
		u := defaultACLUser

		prev, err := acl.UpdateScope(u, ACLScope("a"), NewAllowACLPerm(2))
		t.NoError(err)
		t.printYAML("prev", prev)
		t.printYAML("after", acl.m.Map())

		t.True(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(2)))
	})

	t.Run("new", func() {
		u := "embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu"

		prev, err := acl.UpdateScope(u, ACLScope("b"), NewAllowACLPerm(1))
		t.NoError(err)
		t.printYAML("prev", prev)
		t.printYAML("after", acl.m.Map())

		t.True(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(2)))

		t.True(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(2)))
	})
}

func (t *testACL) TestRemoveUser() {
	y := `
_default:
  a: o
  b: o
embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu:
  a: x
  b: x
`
	t.T().Logf("source:\n%s", y)

	acl, _ := NewACL(9, "")

	_, err := parseACLYAML([]byte(y), acl, t.enc)
	t.NoError(err)

	t.Run("remove", func() {
		u := "embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu"

		t.False(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))

		prev, err := acl.RemoveUser(u)
		t.NoError(err)
		t.printYAML("prev", prev)
		t.printYAML("after", acl.m.Map())

		t.True(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))
	})

	t.Run("unknown", func() {
		u := "bmbahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu"

		t.True(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))

		prev, err := acl.RemoveUser(u)
		t.NoError(err)
		t.printYAML("prev", prev)
		t.printYAML("after", acl.m.Map())
	})
}

func (t *testACL) TestRemoveScope() {
	y := `
_default:
  a: o
  b: o
embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu:
  a: x
  b: x
`
	t.T().Logf("source:\n%s", y)

	acl, _ := NewACL(9, "")

	_, err := parseACLYAML([]byte(y), acl, t.enc)
	t.NoError(err)

	t.Run("remove", func() {
		u := "embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu"

		t.False(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))

		prev, err := acl.RemoveScope(u, ACLScope("a"))
		t.NoError(err)
		t.printYAML("prev", prev)
		t.printYAML("after", acl.m.Map())

		t.True(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))
	})

	t.Run("remove left", func() {
		u := "embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu"

		t.True(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
		t.False(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))

		prev, err := acl.RemoveScope(u, ACLScope("b"))
		t.NoError(err)
		t.printYAML("prev", prev)
		t.printYAML("after", acl.m.Map())

		t.True(acl.Allow(u, ACLScope("a"), NewAllowACLPerm(1)))
		t.True(acl.Allow(u, ACLScope("b"), NewAllowACLPerm(1)))
	})
}

func (t *testACL) TestSuperUser() {
	y := `
_default:
  a: o
  b: o
embahrNvSzYDMmXn37JYR8V756PVakPGaeyjDXDRisErmpu:
  a: x
  b: x
`
	t.T().Logf("source:\n%s", y)

	superuser := "mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUumpu"
	acl, _ := NewACL(9, superuser)

	t.Run("allow", func() {
		t.True(acl.Allow(superuser, ACLScope("a"), NewAllowACLPerm(3)))
		t.True(acl.Allow(superuser, ACLScope("b"), NewAllowACLPerm(3)))
		t.True(acl.Allow(superuser, ACLScope("c"), NewAllowACLPerm(3)))
	})

	t.Run("set user", func() {
		_, err := acl.SetUser(superuser, map[ACLScope]ACLPerm{"c": NewAllowACLPerm(1)})
		t.Error(err)
		t.ErrorContains(err, "superuser")
	})

	t.Run("remove user", func() {
		_, err := acl.RemoveUser(superuser)
		t.Error(err)
		t.ErrorContains(err, "superuser")
	})

	t.Run("update scope", func() {
		_, err := acl.UpdateScope(superuser, ACLScope("c"), NewAllowACLPerm(1))
		t.Error(err)
		t.ErrorContains(err, "superuser")
	})

	t.Run("remove scope", func() {
		_, err := acl.RemoveScope(superuser, ACLScope("c"))
		t.Error(err)
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
		b, err := yaml.Marshal(NewAllowACLPerm(1))
		t.NoError(err)

		t.Equal("o\n", string(b))
	})

	t.Run("allow2", func() {
		b, err := yaml.Marshal(NewAllowACLPerm(2))
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
		{name: "allow1", s: "o", p: NewAllowACLPerm(1)},
		{name: "allow2", s: "oo", p: NewAllowACLPerm(2)},
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
