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
	if err := ps.LoadsFromContextOK(ctx,
		DiscoveryFlagContextKey, &flag,
	); err != nil {
		return ctx, e(err, "")
	}

	var discoveries []quicstream.UDPConnInfo

	if len(flag) > 0 {
		discoveries = make([]quicstream.UDPConnInfo, len(flag))

		for i := range flag {
			ci, err := flag[i].ConnInfo()
			if err != nil {
				return ctx, e(err, "invalid member discovery, %q", flag[i])
			}

			discoveries[i] = ci
		}
	}

	ctx = context.WithValue(ctx, DiscoveryContextKey, discoveries) //revive:disable-line:modifies-parameter

	return ctx, nil
}
