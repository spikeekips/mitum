package launch

import (
	"context"

	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameDiscoveryFlag      = ps.Name("discovery-flag")
	DiscoveryFlagContextKey = ps.ContextKey("discovery-flag")
	DiscoveryContextKey     = ps.ContextKey("discovery")
)

func PDiscoveryFlag(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare discovery flag")

	var flag []ConnInfoFlag
	if err := ps.LoadFromContextOK(ctx, DiscoveryFlagContextKey, &flag); err != nil {
		return ctx, e(err, "")
	}

	discoveries := util.EmptyLocked()

	if len(flag) > 0 {
		v := make([]quicstream.UDPConnInfo, len(flag))

		for i := range flag {
			ci, err := flag[i].ConnInfo()
			if err != nil {
				return ctx, e(err, "invalid member discovery, %q", flag[i])
			}

			v[i] = ci
		}

		_ = discoveries.SetValue(v)
	}

	ctx = context.WithValue(ctx, DiscoveryContextKey, discoveries) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func GetDiscoveriesFromLocked(l *util.Locked) []quicstream.UDPConnInfo {
	switch i, isnil := l.Value(); {
	case isnil, i == nil:
		return nil
	default:
		return i.([]quicstream.UDPConnInfo) //nolint:forcetypeassert //...
	}
}
