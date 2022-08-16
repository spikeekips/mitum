package launch2

import (
	"bytes"
	"context"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameProposalMaker      = ps.PName("proposal-maker")
	ProposalMakerContextKey = ps.ContextKey("proposal-maker")
)

var (
	suffrageCandidateFactHintTypeBytes = isaacoperation.SuffrageCandidateFactHint.Type().Bytes()
	suffrageJoinFactHintTypeBytes      = isaacoperation.SuffrageJoinFactHint.Type().Bytes()
)

func PProposalMaker(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare proposal maker")

	var log *logging.Logging
	var local base.LocalNode
	var policy base.NodePolicy
	var pool *isaacdatabase.TempPool

	if err := ps.LoadsFromContextOK(ctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		NodePolicyContextKey, &policy,
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
		policy,
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
	var nodepolicy *isaac.NodePolicy
	var db isaac.Database
	var pool *isaacdatabase.TempPool

	if err := ps.LoadsFromContextOK(ctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		NodePolicyContextKey, &nodepolicy,
		CenterDatabaseContextKey, &db,
		PoolDatabaseContextKey, &pool,
	); err != nil {
		return nil, err
	}

	operationfilterf := launch.IsSupportedProposalOperationFactHintFunc()

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
			func(operationhash, facthash util.Hash, header isaac.PoolOperationHeader) (bool, error) {
				// NOTE filter genesis operations
				if !operationfilterf(header.HintBytes()) {
					return false, nil
				}

				switch found, err := db.ExistsKnownOperation(operationhash); {
				case err != nil:
					return false, err
				case found:
					log.Log().Trace().Stringer("operation", operationhash).Msg("already processed; known operation")

					return false, nil
				}

				switch found, err := db.ExistsInStateOperation(facthash); {
				case err != nil:
					return false, err
				case found:
					log.Log().Trace().Stringer("operation", facthash).Msg("already processed; in state")

					return false, nil
				}

				// NOTE if bad operation and it is failed to be processed;
				// it can be included in next proposal; it should be
				// excluded.
				// NOTE if operation has not enough fact signs, it will
				// ignored. It must be filtered for not this kind of
				// operations.
				switch found, err := db.ExistsInStateOperation(facthash); {
				case err != nil:
					return false, err
				case found:
					return false, nil
				}

				addedat, err := util.BytesToInt64(header.AddedAt())
				if err != nil {
					return false, nil
				}

				var expire time.Duration
				switch ht := header.HintBytes(); {
				case bytes.HasPrefix(ht, suffrageCandidateFactHintTypeBytes),
					bytes.HasPrefix(ht, suffrageJoinFactHintTypeBytes):
					expire = nodepolicy.ValidProposalSuffrageOperationsExpire()
				default:
					expire = nodepolicy.ValidProposalOperationExpire()
				}

				if addedat < localtime.UTCNow().Add(expire*-1).UnixNano() {
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
