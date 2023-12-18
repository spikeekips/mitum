package launch

import (
	"context"
	"net"
	"regexp"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type (
	ACLScope string
	ACLPerm  uint8
)

type ACLAllowFunc func(
	_ context.Context,
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

const (
	ReadAllowACLPerm  = ACLPerm(2)
	WriteAllowACLPerm = ACLPerm(3)
)

var AllACLScopes = []ACLScope{
	DesignACLScope,
	StatesAllowConsensusACLScope,
	DiscoveryACLScope,
	ACLACLScope,
	HandoverACLScope,
	EventLoggingACLScope,
}

var ErrACLAccessDenied = util.NewIDError("access denied")

var ACLEventLogger EventLoggerName = "acl"

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

func (acl *ACL) Allow(user string, scope ACLScope, required ACLPerm) (ACLPerm, bool) {
	acl.RLock()
	defer acl.RUnlock()

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

func (acl *ACL) setUser(user string, m map[ACLScope]ACLPerm) (prev map[ACLScope]ACLPerm, updated bool, _ error) {
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

type YAMLACL struct {
	*ACL
	om *util.YAMLOrderedMap
}

func NewYAMLACL(acl *ACL) *YAMLACL {
	return &YAMLACL{ACL: acl, om: util.NewYAMLOrderedMap()}
}

func (acl *YAMLACL) Import(b []byte, enc encoder.Encoder) (updated bool, _ error) {
	if len(b) < 1 {
		return false, nil
	}

	if _, _, err := parseACLYAML(b, enc); err != nil {
		return false, err
	}

	acl.Lock()
	defer acl.Unlock()

	switch om, updated, err := loadACLFromYAML(acl.ACL, b, enc); {
	case err != nil:
		return false, err
	case !updated:
		return false, nil
	default:
		acl.om = om

		return true, nil
	}
}

func (acl *YAMLACL) Export() interface{} {
	return acl.om
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
	var nom *util.YAMLOrderedMap

	switch i, j, err := parseACLYAML(b, enc); {
	case err != nil:
		return nil, false, err
	default:
		om = i
		nom = j
	}

	var ferr error
	isnew := om.Len() != acl.m.Len()

	nom.Traverse(func(k string, v interface{}) bool {
		var m map[ACLScope]ACLPerm

		switch m, ferr = util.AssertInterfaceValue[map[ACLScope]ACLPerm](v); {
		case ferr != nil:
			return false
		case !isnew:
			if i, found := acl.m.Value(k); !found || !compareACLUserValues(i, m) {
				isnew = true

				return false
			}
		}

		return true
	})

	if ferr != nil {
		return nil, false, ferr
	}

	if !isnew {
		_ = acl.m.Traverse(func(key string, v map[ACLScope]ACLPerm) bool {
			if _, found := nom.Get(key); !found {
				isnew = true

				return false
			}

			return true
		})
	}

	switch {
	case !isnew:
		return om, false, nil
	default:
		acl.m.Empty()
	}

	nom.Traverse(func(k string, v interface{}) bool {
		switch i, err := util.AssertInterfaceValue[map[ACLScope]ACLPerm](v); {
		case err != nil:
			ferr = err

			return false
		case len(i) < 1:
		default:
			if _, _, err := acl.setUser(k, i); err != nil {
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

func parseACLYAML(b []byte, enc encoder.Encoder) (
	om *util.YAMLOrderedMap,
	nom *util.YAMLOrderedMap,
	_ error,
) {
	switch i, err := util.UnmarshalWithYAMLOrderedMap(b); {
	case err != nil:
		return nil, nil, err
	default:
		if err := util.SetInterfaceValue(i, &om); err != nil {
			return nil, nil, err
		}
	}

	nom = util.NewYAMLOrderedMap()

	var ferr error

	om.Traverse(func(k string, v interface{}) bool {
		switch {
		case k == defaultACLUser:
		case len(k) < 1:
			ferr = errors.Errorf("empty user")

			return false
		case v == nil:
			ferr = errors.Errorf("empty user perms")

			return false
		default:
			if _, err := base.DecodePublickeyFromString(k, enc); err != nil {
				ferr = errors.WithMessage(err, "user")

				return false
			}
		}

		switch i, ok := v.(*util.YAMLOrderedMap); {
		case !ok:
			ferr = errors.Errorf("expected map, but %T", v)

			return false
		default:
			switch m, err := convertACLUserYAML(i); {
			case err != nil:
				ferr = err

				return false
			default:
				_ = nom.Set(k, m)

				return true
			}
		}
	})

	return om, nom, ferr
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

func NewACLAllowFunc(acl *ACL, el *zerolog.Logger) ACLAllowFunc {
	return func(ctx context.Context, user string, scope ACLScope, required ACLPerm, extra *zerolog.Event) bool {
		assigned, allow := acl.Allow(user, scope, required)

		l := quicstream.ConnectionLoggerFromContext(ctx, el)
		l.Debug().
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

func ACLNetworkHandler[T quicstreamheader.RequestHeader](
	aclallow ACLAllowFunc,
	scope ACLScope,
	required ACLPerm,
	networkID base.NetworkID,
) quicstreamheader.Handler[T] {
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

		if !aclallow(ctx, h.ACLUser().String(), scope, required, extra) {
			return ctx, ErrACLAccessDenied.WithStack()
		}

		return ctx, nil
	}
}
