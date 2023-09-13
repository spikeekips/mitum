package launch

import (
	"context"
	"net"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"gopkg.in/yaml.v3"
)

type (
	ACLScope string
	ACLPerm  uint8
)

type ACLAllowFunc func(
	user string,
	scope ACLScope,
	required ACLPerm,
	extra *zerolog.Event,
) bool

const (
	defaultACLScope ACLScope = "_default"
	defaultACLUser  string   = "_default"
)

const (
	aclPermProhibit ACLPerm = 1
	aclPermSuper    ACLPerm = 79
)

var AllACLScopes = []ACLScope{
	DesignACLScope,
	StatesAllowConsensusACLScope,
	DiscoveryACLScope,
	ACLACLScope,
	HandoverACLScope,
	EventLoggingACLScope,
}

var ErrACLBlocked = util.NewIDError("acl blocked")

var ACLEventLoggerName EventLoggerName = "acl"

type ACL struct {
	m         util.LockedMap[ /* user */ string /* perm set */, map[ACLScope]ACLPerm]
	superuser string
}

func NewACL(mapsize uint64, superuser string) (*ACL, error) {
	m, err := util.NewLockedMap[string, map[ACLScope]ACLPerm](mapsize, nil)
	if err != nil {
		return nil, err
	}

	return &ACL{m: m, superuser: superuser}, nil
}

func (acl *ACL) Allow(user string, scope ACLScope, required ACLPerm) (ACLPerm, bool) {
	if required == aclPermProhibit {
		return aclPermProhibit, false
	}

	if user == acl.superuser {
		return aclPermSuper, true
	}

	if assigned, allow := acl.allow(user, scope, required); assigned >= aclPermProhibit {
		return assigned, allow
	}

	assigned, allow := acl.allow(defaultACLUser, scope, required)

	return assigned, allow
}

func (acl *ACL) SetUser(user string, m map[ACLScope]ACLPerm) (prev map[ACLScope]ACLPerm, updated bool, _ error) {
	if user == acl.superuser {
		return nil, false, errors.Errorf("superuser")
	}

	switch {
	case len(m) < 1:
		updated, _ = acl.m.Remove(user, func(i map[ACLScope]ACLPerm, found bool) error {
			if found {
				prev = i
			}

			return nil
		})
	default:
		_, _, _ = acl.m.Set(user, func(i map[ACLScope]ACLPerm, found bool) (map[ACLScope]ACLPerm, error) {
			if !found {
				updated = true

				return m, nil
			}

			prev = i

			if compareACLUserValues(prev, m) {
				return nil, util.ErrLockedMapClosed
			}

			updated = true

			return m, nil
		})
	}

	return prev, updated, nil
}

func (acl *ACL) RemoveUser(user string) (prev map[ACLScope]ACLPerm, removed bool, _ error) {
	if user == acl.superuser {
		return nil, false, errors.Errorf("superuser")
	}

	removed, _ = acl.m.Remove(user, func(value map[ACLScope]ACLPerm, found bool) error {
		if found {
			prev = value
		}

		return nil
	})

	return prev, removed, nil
}

func (acl *ACL) SetScope(user string, scope ACLScope, p ACLPerm) (prev ACLPerm, updated bool, _ error) {
	if user == acl.superuser {
		return prev, false, errors.Errorf("superuser")
	}

	_, _, _ = acl.m.Set(user, func(m map[ACLScope]ACLPerm, found bool) (map[ACLScope]ACLPerm, error) {
		prev = m[scope]

		switch {
		case !found:
			updated = true

			return map[ACLScope]ACLPerm{scope: p}, nil
		case prev == p:
			return nil, util.ErrLockedSetIgnore
		default:
			m[scope] = p
			updated = true

			return m, nil
		}
	})

	return prev, updated, nil
}

func (acl *ACL) RemoveScope(user string, scope ACLScope) (prev ACLPerm, removed bool, _ error) {
	if user == acl.superuser {
		return prev, false, errors.Errorf("superuser")
	}

	_, _, _, _ = acl.m.SetOrRemove(user,
		func(value map[ACLScope]ACLPerm, found bool) (map[ACLScope]ACLPerm, bool, error) {
			if !found {
				return nil, false, util.ErrLockedMapClosed
			}

			switch i, found := value[scope]; {
			case !found:
				return nil, false, util.ErrLockedMapClosed
			default:
				prev = i
				removed = true

				if len(value) < 2 {
					return nil, true, nil
				}

				delete(value, scope)

				return value, false, nil
			}
		})

	return prev, removed, nil
}

func (*ACL) fromDefault(perms map[ACLScope]ACLPerm, required ACLPerm) (assigned ACLPerm, allow bool) {
	if p, found := perms[defaultACLScope]; found {
		return p, p >= required
	}

	return assigned, false
}

func (acl *ACL) allow(user string, scope ACLScope, required ACLPerm) (assigned ACLPerm, allow bool) {
	_ = acl.m.Get(user, func(perms map[ACLScope]ACLPerm, found bool) error {
		if !found {
			return nil
		}

		switch p, found := perms[scope]; {
		case !found:
			assigned, allow = acl.fromDefault(perms, required)
		default:
			assigned = p
			allow = p >= required
		}

		return nil
	})

	return assigned, allow
}

func NewAllowACLPerm(c uint8) ACLPerm {
	return aclPermProhibit + ACLPerm(c) + 1
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

func loadACLFromYAML(
	acl *ACL,
	b []byte,
	enc encoder.Encoder,
) (
	om *util.YAMLOrderedMap,
	updated bool,
	_ error,
) {
	switch i, err := parseACLYAML(b, enc); {
	case err != nil:
		return nil, false, err
	default:
		om = i
	}

	var ferr error

	if om.Len() == acl.m.Len() {
		var isnew bool

		om.Traverse(func(k string, v interface{}) bool {
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

			switch i, found := acl.m.Value(k); {
			case !found,
				!compareACLUserValues(i, m):
				updated = true

				return false
			default:
				return true
			}
		})

		switch {
		case ferr != nil:
			return nil, false, ferr
		case !isnew:
			return om, updated, nil
		default:
			acl.m.Empty()
		}
	}

	om.Traverse(func(k string, v interface{}) bool {
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
			if _, _, err := acl.SetUser(k, m); err != nil {
				ferr = err

				return false
			}
		}

		return true
	})

	return om, true, ferr
}

func compareACLUserValues(a, b map[ACLScope]ACLPerm) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if j, found := b[i]; !found || a[i] != j {
			return false
		}
	}

	return true
}

func parseACLYAML(b []byte, enc encoder.Encoder) (om *util.YAMLOrderedMap, _ error) {
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

		if _, ok := v.(*util.YAMLOrderedMap); !ok {
			ferr = errors.Errorf("expected map, but %T", v)

			return false
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

type ACLUser interface {
	ACLUser() base.Publickey
}

func NewACLAllowFunc(acl *ACL, el zerolog.Logger) ACLAllowFunc {
	return func(user string, scope ACLScope, required ACLPerm, extra *zerolog.Event) bool {
		assigned, allow := acl.Allow(user, scope, required)

		el.Debug().
			Str("user", user).
			Interface("scope", scope).
			Stringer("required", required).
			Stringer("assigned", assigned).
			Bool("allowed", allow).
			Func(func(e *zerolog.Event) {
				if extra != nil {
					e.Dict("extra", extra)
				}
			}).
			Msg("acl")

		return allow
	}
}

func ACLNetworkHandlerFunc[T quicstreamheader.RequestHeader](
	aclallow ACLAllowFunc,
	scope ACLScope,
	required ACLPerm,
	networkID base.NetworkID,
) quicstreamheader.HandlerFunc[T] {
	return func(handler quicstreamheader.Handler[T]) quicstreamheader.Handler[T] {
		return func(
			ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header T,
		) (context.Context, error) {
			var h ACLUser

			switch i, ok := (interface{})(header).(ACLUser); {
			case !ok:
				return ctx, errors.Errorf("invalid acl header")
			default:
				h = i
			}

			if err := isaacnetwork.QuicstreamHandlerVerifyNode(
				ctx, addr, broker,
				h.ACLUser(), networkID,
			); err != nil {
				return ctx, err
			}

			extra := zerolog.Dict().
				Stringer("addr", addr).
				Interface("header", header)

			if !aclallow(h.ACLUser().String(), scope, required, extra) {
				return ctx, ErrACLBlocked.WithStack()
			}

			if handler == nil {
				return ctx, nil
			}

			return handler(ctx, addr, broker, header)
		}
	}
}
