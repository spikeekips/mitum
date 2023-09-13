package launch

import (
	"regexp"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"gopkg.in/yaml.v3"
)

type (
	ACLScope string
	ACLPerm  uint8
)

const (
	defaultACLScope ACLScope = "_default"
	defaultACLUser  string   = "_default"
)

const (
	aclPermProhibit ACLPerm = 1
	aclPermSuper    ACLPerm = 79
)

type ACL struct {
	m         util.LockedMap[ /* user */ string /* perm set */, map[ACLScope]ACLPerm]
	superuser string
	sync.RWMutex
}

func NewACL(mapsize uint64, superuser string) (*ACL, error) {
	m, err := util.NewLockedMap[string, map[ACLScope]ACLPerm](mapsize, nil)
	if err != nil {
		return nil, err
	}

	return &ACL{m: m, superuser: superuser}, nil
}

func (acl *ACL) Allow(user string, scope ACLScope, required ACLPerm) bool {
	if user == acl.superuser {
		return true
	}

	acl.RLock()
	defer acl.RUnlock()

	if required == aclPermProhibit {
		return false
	}

	if foundscope, allow := acl.allow(user, scope, required); foundscope {
		return allow
	}

	_, allow := acl.allow(defaultACLUser, scope, required)

	return allow
}

func (acl *ACL) SetUser(user string, m map[ACLScope]ACLPerm) (prev map[ACLScope]ACLPerm, _ error) {
	if user == acl.superuser {
		return nil, errors.Errorf("superuser")
	}

	acl.Lock()
	defer acl.Unlock()

	prev, _ = acl.m.Value(user)

	switch {
	case len(m) < 1:
		_ = acl.m.RemoveValue(user)
	default:
		_ = acl.m.SetValue(user, m)
	}

	return prev, nil
}

func (acl *ACL) RemoveUser(user string) (prev map[ACLScope]ACLPerm, _ error) {
	if user == acl.superuser {
		return nil, errors.Errorf("superuser")
	}

	acl.Lock()
	defer acl.Unlock()

	_, _ = acl.m.Remove(user, func(value map[ACLScope]ACLPerm, found bool) error {
		if found {
			prev = value
		}

		return nil
	})

	return prev, nil
}

func (acl *ACL) UpdateScope(user string, scope ACLScope, p ACLPerm) (prev ACLPerm, _ error) {
	if user == acl.superuser {
		return prev, errors.Errorf("superuser")
	}

	acl.Lock()
	defer acl.Unlock()

	_, _, _ = acl.m.Set(user, func(m map[ACLScope]ACLPerm, found bool) (map[ACLScope]ACLPerm, error) {
		prev = m[scope]

		switch {
		case !found:
			return map[ACLScope]ACLPerm{scope: p}, nil
		default:
			m[scope] = p
		}

		return m, nil
	})

	return prev, nil
}

func (acl *ACL) RemoveScope(user string, scope ACLScope) (prev ACLPerm, _ error) {
	if user == acl.superuser {
		return prev, errors.Errorf("superuser")
	}

	acl.Lock()
	defer acl.Unlock()

	var removed bool

	_, _, _ = acl.m.Set(user, func(value map[ACLScope]ACLPerm, found bool) (map[ACLScope]ACLPerm, error) {
		if !found {
			return nil, util.ErrLockedMapClosed
		}

		switch i, found := value[scope]; {
		case !found:
			return nil, util.ErrLockedMapClosed
		default:
			prev = i

			if removed = len(value) < 2; removed {
				return nil, util.ErrLockedMapClosed
			}

			delete(value, scope)

			return value, nil
		}
	})

	if removed {
		_ = acl.m.RemoveValue(user)
	}

	return prev, nil
}

func (*ACL) fromDefault(perms map[ACLScope]ACLPerm, required ACLPerm) (foundscope bool, allow bool) {
	if p, found := perms[defaultACLScope]; found {
		return true, p >= required
	}

	return false, false
}

func (acl *ACL) allow(user string, scope ACLScope, required ACLPerm) (foundscope bool, allow bool) {
	_ = acl.m.Get(user, func(perms map[ACLScope]ACLPerm, found bool) error {
		if !found {
			return nil
		}

		switch p, found := perms[scope]; {
		case !found:
			foundscope, allow = acl.fromDefault(perms, required)
		default:
			foundscope = true
			allow = p >= required
		}

		return nil
	})

	return foundscope, allow
}

func NewAllowACLPerm(c uint8) ACLPerm {
	return ACLPerm(c + 1)
}

func (p ACLPerm) IsValid([]byte) error {
	switch {
	case p == 0:
		return util.ErrInvalid.Errorf("empty perm")
	case p > aclPermSuper:
		return util.ErrInvalid.Errorf("overflowed perm")
	}

	return nil
}

func (p ACLPerm) String() string {
	switch p {
	case 0:
		return "<empty perm>"
	case aclPermProhibit:
		return "x" // NOTE prohibit
	case aclPermSuper:
		return "s" // NOTE super allow
	default:
		return strings.Repeat("o", int(p-1))
	}
}

func (p ACLPerm) MarshalText() ([]byte, error) {
	return []byte(p.String()), nil
}

var regexpACLPermString = regexp.MustCompile(`^[o][o]*$`)

func (p *ACLPerm) UnmarshalText(b []byte) error {
	switch t := string(b); {
	case t == "x":
		*p = aclPermProhibit
	case t == "s":
		*p = aclPermSuper
	case len(t) < 1:
		return errors.Errorf("empty perm string")
	case !regexpACLPermString.MatchString(t):
		return errors.Errorf("unknown perm string, %q", t)
	default:
		c := ACLPerm(strings.Count(t, "o"))
		if c > aclPermSuper {
			c = aclPermSuper
		}

		*p = c + 1
	}

	return nil
}

func parseACLYAML(
	b []byte,
	acl *ACL,
	enc encoder.Encoder,
) (
	om *util.YAMLOrderedMap,
	_ error,
) {
	switch i, err := util.UnmarshalWithYAMLOrderedMap(b); {
	case err != nil:
		return nil, err
	default:
		j, ok := i.(*util.YAMLOrderedMap)
		if !ok {
			return nil, errors.Errorf("expected map, but %T", i)
		}

		om = j
	}

	var ferr error

	om.Traverse(func(k string, v interface{}) bool {
		switch {
		case k == defaultACLUser:
		case len(k) < 1:
			ferr = errors.Errorf("empty user")

			return false
		default:
			if _, ferr = base.DecodePublickeyFromString(k, enc); ferr != nil {
				return false
			}
		}

		var m map[ACLScope]ACLPerm

		switch i, ok := v.(*util.YAMLOrderedMap); {
		case !ok:
			ferr = errors.Errorf("expected map, but %T", v)

			return false
		default:
			if m, ferr = convertACLUserYAML(i); ferr != nil {
				return false
			}
		}

		if len(m) > 0 {
			if _, err := acl.SetUser(k, m); err != nil {
				ferr = err

				return false
			}
		}

		return true
	})

	return om, ferr
}

func parseACLUserYAML(b []byte) (
	om *util.YAMLOrderedMap,
	m map[ACLScope]ACLPerm,
	_ error,
) {
	switch i, err := util.UnmarshalWithYAMLOrderedMap(b); {
	case err != nil:
		return nil, nil, err
	default:
		j, ok := i.(*util.YAMLOrderedMap)
		if !ok {
			return nil, nil, errors.Errorf("expected map, but %T", i)
		}

		om = j
	}

	i, err := convertACLUserYAML(om)

	return om, i, err
}

func convertACLUserYAML(om *util.YAMLOrderedMap) (
	m map[ACLScope]ACLPerm,
	_ error,
) {
	var ferr error

	om.Traverse(func(k string, v interface{}) bool {
		if len(k) < 1 {
			ferr = errors.Errorf("empty scope")

			return false
		}

		switch p, err := convertACLPerm(v); {
		case err != nil:
			ferr = err

			return false
		default:
			if m == nil {
				m = map[ACLScope]ACLPerm{}
			}

			m[ACLScope(k)] = p

			return true
		}
	})

	return m, ferr
}

func convertACLPermYAML(b []byte) (p ACLPerm, _ error) {
	var u interface{}

	if err := yaml.Unmarshal(b, &u); err != nil {
		return p, errors.WithStack(err)
	}

	return convertACLPerm(u)
}

func convertACLPerm(i interface{}) (p ACLPerm, _ error) {
	if i == nil {
		return p, errors.Errorf("empty perm")
	}

	switch t := i.(type) {
	case string:
		if err := p.UnmarshalText([]byte(t)); err != nil {
			return p, err
		}
	case uint8:
		p = ACLPerm(t)
	default:
		return p, errors.Errorf("unsupported perm type, %T", t)
	}

	return p, p.IsValid(nil)
}
