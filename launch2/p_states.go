package launch2

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameStates         = ps.PName("states")
	PNameBallotbox      = ps.PName("ballotbox")
	BallotboxContextKey = ps.ContextKey("ballotbox")
	StatesContextKey    = ps.ContextKey("states")
)

func PBallotbox(ctx context.Context) (context.Context, error) {
	var policy base.NodePolicy
	var db isaac.Database

	if err := ps.LoadsFromContext(ctx,
		NodePolicyContextKey, &policy,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return ctx, err
	}

	ballotbox := isaacstates.NewBallotbox(
		func(blockheight base.Height) (base.Suffrage, bool, error) {
			return isaac.GetSuffrageFromDatabase(db, blockheight)
		},
		policy.Threshold(),
	)

	ctx = context.WithValue(ctx, BallotboxContextKey, ballotbox) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func PStates(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func PCloseStates(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
