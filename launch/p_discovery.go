package launch

import (
	"context"

	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameDiscoveryFlag      = ps.Name("discovery-flag")
	DiscoveryFlagContextKey = util.ContextKey("discovery-flag")
	DiscoveryContextKey     = util.ContextKey("discovery")
)

func PDiscoveryFlag(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare discovery flag")

	var flag []ConnInfoFlag
	if err := util.LoadFromContextOK(ctx, DiscoveryFlagContextKey, &flag); err != nil {
		return ctx, e(err, "")
	}

	discoveries := util.EmptyLocked([]quicstream.UDPConnInfo{})

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

func GetDiscoveriesFromLocked(l *util.Locked[[]quicstream.UDPConnInfo]) []quicstream.UDPConnInfo {
	switch i, isempty := l.Value(); {
	case isempty:
		return nil
	default:
		return i
	}
}
