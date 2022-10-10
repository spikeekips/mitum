package launch

import (
	"context"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameProposalMaker      = ps.Name("proposal-maker")
	ProposalMakerContextKey = util.ContextKey("proposal-maker")
)

func PProposalMaker(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare proposal maker")

	var log *logging.Logging
	var local base.LocalNode
	var params base.LocalParams
	var pool *isaacdatabase.TempPool

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		PoolDatabaseContextKey, &pool,
	); err != nil {
		return ctx, e(err, "")
	}

	opf, err := proposalMakderGetOperationsFunc(ctx)
	if err != nil {
		return ctx, e(err, "")
	}

	pm := isaac.NewProposalMaker(
		local,
		params,
		opf,
		pool,
	)

	_ = pm.SetLogging(log)

	ctx = context.WithValue(ctx, ProposalMakerContextKey, pm) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func proposalMakderGetOperationsFunc(ctx context.Context) (
	func(context.Context, base.Height) ([]util.Hash, error),
	error,
) {
	var log *logging.Logging
	var local base.LocalNode
	var params *isaac.LocalParams
	var db isaac.Database
	var pool *isaacdatabase.TempPool

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
		PoolDatabaseContextKey, &pool,
	); err != nil {
		return nil, err
	}

	operationfilterf := IsSupportedProposalOperationFactHintFunc()

	return func(ctx context.Context, height base.Height) ([]util.Hash, error) {
		policy := db.LastNetworkPolicy()
		if policy == nil { // NOTE Usually it means empty block data
			return nil, nil
		}

		n := policy.MaxOperationsInProposal()
		if n < 1 {
			return nil, nil
		}

		hs, err := pool.NewOperationHashes(
			ctx,
			height,
			n,
			func(meta isaac.PoolOperationRecordMeta) (bool, error) {
				// NOTE filter genesis operations
				if !operationfilterf(meta.Hint()) {
					return false, nil
				}

				switch found, err := db.ExistsKnownOperation(meta.Operation()); {
				case err != nil:
					return false, err
				case found:
					log.Log().Trace().
						Stringer("operation", meta.Operation()).
						Msg("already processed; known operation")

					return false, nil
				}

				switch found, err := db.ExistsInStateOperation(meta.Fact()); {
				case err != nil:
					return false, err
				case found:
					log.Log().Trace().Stringer("operation", meta.Fact()).Msg("already processed; in state")

					return false, nil
				}

				// NOTE if bad operation and it is failed to be processed;
				// it can be included in next proposal; it should be
				// excluded.
				// NOTE if operation has not enough fact signs, it will
				// ignored. It must be filtered for not this kind of
				// operations.
				switch found, err := db.ExistsInStateOperation(meta.Fact()); {
				case err != nil:
					return false, err
				case found:
					return false, nil
				}

				var expire time.Duration
				switch ht := meta.Hint(); {
				case ht.Type() == isaacoperation.SuffrageCandidateFactHint.Type(),
					ht.Type() == isaacoperation.SuffrageJoinFactHint.Type():
					expire = params.ValidProposalSuffrageOperationsExpire()
				default:
					expire = params.ValidProposalOperationExpire()
				}

				if localtime.UTCNow().After(meta.AddedAt().Add(expire)) {
					return false, nil
				}

				return true, nil
			},
		)
		if err != nil {
			return nil, err
		}

		return hs, nil
	}, nil
}
