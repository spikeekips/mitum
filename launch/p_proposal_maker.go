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

func PProposalMaker(pctx context.Context) (context.Context, error) {
	e := util.StringError(" prepare proposal maker")

	var log *logging.Logging
	var local base.LocalNode
	var isaacparams *isaac.Params
	var pool *isaacdatabase.TempPool

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		ISAACParamsContextKey, &isaacparams,
		PoolDatabaseContextKey, &pool,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	opf, err := proposalMakderGetOperationsFunc(pctx)
	if err != nil {
		return pctx, e.Wrap(err)
	}

	pm := isaac.NewProposalMaker(
		local,
		isaacparams.NetworkID(),
		opf,
		pool,
	)

	_ = pm.SetLogging(log)

	return context.WithValue(pctx, ProposalMakerContextKey, pm), nil
}

func proposalMakderGetOperationsFunc(pctx context.Context) (
	func(context.Context, base.Height) ([][2]util.Hash, error),
	error,
) {
	var log *logging.Logging
	var local base.LocalNode
	var params *LocalParams
	var db isaac.Database
	var pool *isaacdatabase.TempPool

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
		PoolDatabaseContextKey, &pool,
	); err != nil {
		return nil, err
	}

	operationfilterf := IsSupportedProposalOperationFactHintFunc()

	return func(ctx context.Context, height base.Height) ([][2]util.Hash, error) {
		policy := db.LastNetworkPolicy()
		if policy == nil { // NOTE Usually it means empty block data
			return nil, nil
		}

		n := policy.MaxOperationsInProposal()
		if n < 1 {
			return nil, nil
		}

		hs, err := pool.OperationHashes(
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
					expire = params.MISC.ValidProposalSuffrageOperationsExpire()
				default:
					expire = params.MISC.ValidProposalOperationExpire()
				}

				if localtime.Now().UTC().After(meta.AddedAt().Add(expire)) {
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
