package main

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/launch2"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/ps"
)

func (cmd *RunCommand) prepare(pctx context.Context) (func() error, error) {
	stop := func() error {
		return nil
	}

	e := util.StringErrorFunc("failed to prepare")

	if err := ps.LoadsFromContext(pctx,
		launch2.EncodersContextKey, &cmd.encs,
		launch2.EncoderContextKey, &cmd.enc,
		launch2.DesignContextKey, &cmd.design,
		launch2.LocalContextKey, &cmd.local,
		launch2.NodePolicyContextKey, &cmd.nodePolicy,
		launch2.DiscoveryContextKey, &cmd.discoveries,
		launch2.NodeInfoContextKey, &cmd.nodeinfo,
		launch2.FSNodeInfoContextKey, &cmd.nodeInfo,
		launch2.LeveldbStorageContextKey, &cmd.st,
		launch2.CenterDatabaseContextKey, &cmd.db,
		launch2.PermanentDatabaseContextKey, &cmd.perm,
		launch2.PoolDatabaseContextKey, &cmd.pool,
		launch2.ProposalMakerContextKey, &cmd.proposalMaker,
		launch2.QuicstreamClientContextKey, &cmd.client,
		launch2.QuicstreamHandlersContextKey, &cmd.handlers,
		launch2.QuicstreamServerContextKey, &cmd.quicstreamserver,
		launch2.SyncSourceCheckerContextKey, &cmd.syncSourceChecker,
		launch2.SyncSourcePoolContextKey, &cmd.syncSourcePool,
		launch2.SuffrageCandidateLimiterSetContextKey, &cmd.suffrageCandidateLimiterSet,
		launch2.LastSuffrageProofWatcherContextKey, &cmd.lastSuffrageProofWatcher,
		launch2.MemberlistContextKey, &cmd.memberlist,
		launch2.BallotboxContextKey, &cmd.ballotbox,
	); err != nil {
		return stop, e(err, "")
	}

	//if err := cmd.prepareSyncSourceChecker(); err != nil {
	//	return stop, e(err, "")
	//}

	//if err := cmd.prepareSuffrage(); err != nil {
	//	return stop, e(err, "")
	//}

	if err := cmd.prepareStates(); err != nil {
		return stop, err
	}

	//if err := cmd.prepareNetwork(); err != nil {
	//	return stop, e(err, "")
	//}

	cmd.syncSourcesRetryInterval = time.Second * 3 //nolint:gomnd //...

	return stop, nil
}

func (cmd *RunCommand) prepareNetwork() error {
	// cmd.client = launch.NewNetworkClient(cmd.encs, cmd.enc, time.Second*2) //nolint:gomnd //...
	// cmd.handlers = cmd.networkHandlers()
	// cmd.networkHandlers()

	//quicconfig := launch.DefaultQuicConfig()
	//quicconfig.AcceptToken = func(clientAddr net.Addr, token *quic.Token) bool {
	//	return true // FIXME NOTE manage blacklist
	//}
	//
	//cmd.quicstreamserver = quicstream.NewServer(
	//	cmd.design.Network.Bind,
	//	launch.GenerateNewTLSConfig(),
	//	quicconfig,
	//	cmd.handlers.Handler,
	//)
	//_ = cmd.quicstreamserver.SetLogging(log)

	// return cmd.prepareMemberlist()
	return nil
}

func (cmd *RunCommand) prepareLastSuffrageProofWatcher() {
	builder := isaac.NewSuffrageStateBuilder(
		cmd.nodePolicy.NetworkID(),
		cmd.getLastSuffrageProofFunc(),
		cmd.getSuffrageProofFunc(),
		cmd.getLastSuffrageCandidateFunc(),
	)

	cmd.lastSuffrageProofWatcher = isaac.NewLastConsensusNodesWatcher(
		func() (base.SuffrageProof, base.State, bool, error) {
			proof, found, err := cmd.db.LastSuffrageProof()
			if err != nil {
				return nil, nil, false, err
			}

			st, _, err := cmd.db.State(isaac.SuffrageCandidateStateKey)
			if err != nil {
				return nil, nil, false, err
			}

			return proof, st, found, nil
		},
		builder.Build,
		func(ctx context.Context, proof base.SuffrageProof, st base.State) {
			if len(cmd.discoveries) < 1 {
				return
			}

			switch _, found, err := isaac.IsNodeInLastConsensusNodes(cmd.local, proof, st); {
			case err != nil:
				log.Log().Error().Err(err).Msg("failed to check node in consensus nodes")

				return
			case !found:
				log.Log().Debug().Msg("local is not in consensus nodes; will leave from memberlist")

				if err := cmd.memberlist.Leave(time.Second * 30); err != nil { //nolint:gomnd // long enough to leave
					log.Log().Error().Err(err).Msg("failed to leave from memberlist")
				}

				return
			case cmd.memberlist.IsJoined():
				return
			}

			_ = util.Retry(
				ctx,
				func() (bool, error) {
					l := log.Log().With().Interface("discoveries", cmd.discoveries).Logger()

					switch err := cmd.memberlist.Join(cmd.discoveries); {
					case err != nil:
						l.Error().Err(err).Msg("trying to join to memberlist, but failed")

						return true, err
					default:
						l.Debug().Msg("joined to memberlist")

						return false, nil
					}
				},
				3,             //nolint:gomnd //...
				time.Second*3, //nolint:gomnd //...
			)
		},
	)
}

func (cmd *RunCommand) prepareSyncSourceChecker() error {
	sources := make([]isaacnetwork.SyncSource, len(cmd.design.SyncSources))
	for i := range cmd.design.SyncSources {
		sources[i] = cmd.design.SyncSources[i].Source
	}

	switch {
	case len(sources) < 1:
		log.Log().Warn().Msg("empty initial sync sources; connected memberlist members will be used")
	default:
		log.Log().Debug().Interface("sync_sources", sources).Msg("initial sync sources found")
	}

	cmd.syncSourceChecker = isaacnetwork.NewSyncSourceChecker(
		cmd.local,
		cmd.nodePolicy.NetworkID(),
		cmd.client,
		cmd.nodePolicy.SyncSourceCheckerInterval(),
		cmd.enc,
		sources,
		cmd.updateSyncSources,
	)
	_ = cmd.syncSourceChecker.SetLogging(log)

	cmd.syncSourcePool = isaac.NewSyncSourcePool(nil)

	return nil
}

func (cmd *RunCommand) prepareSuffrage() error {
	set := hint.NewCompatibleSet()
	if err := set.Add(
		isaac.FixedSuffrageCandidateLimiterRuleHint,
		base.SuffrageCandidateLimiterFunc(func(
			rule base.SuffrageCandidateLimiterRule,
		) (base.SuffrageCandidateLimiter, error) {
			i, ok := rule.(isaac.FixedSuffrageCandidateLimiterRule)
			if !ok {
				return nil, errors.Errorf("expected FixedSuffrageCandidateLimiterRule, not %T", rule)
			}

			return isaac.NewFixedSuffrageCandidateLimiter(i), nil
		}),
	); err != nil {
		return err
	}

	if err := set.Add(
		isaac.MajoritySuffrageCandidateLimiterRuleHint,
		base.SuffrageCandidateLimiterFunc(func(
			rule base.SuffrageCandidateLimiterRule,
		) (base.SuffrageCandidateLimiter, error) {
			i, ok := rule.(isaac.MajoritySuffrageCandidateLimiterRule)
			if !ok {
				return nil, errors.Errorf("expected MajoritySuffrageCandidateLimiterRule, not %T", rule)
			}

			proof, found, err := cmd.db.LastSuffrageProof()

			switch {
			case err != nil:
				return nil, errors.WithMessagef(err, "failed to get last suffrage for MajoritySuffrageCandidateLimiter")
			case !found:
				return nil, errors.Errorf("last suffrage not found for MajoritySuffrageCandidateLimiter")
			}

			suf, err := proof.Suffrage()
			if err != nil {
				return nil, errors.WithMessagef(err, "failed to get suffrage for MajoritySuffrageCandidateLimiter")
			}

			return isaac.NewMajoritySuffrageCandidateLimiter(
				i,
				func() (uint64, error) {
					return uint64(suf.Len()), nil
				},
			), nil
		}),
	); err != nil {
		return err
	}

	cmd.suffrageCandidateLimiterSet = set

	return nil
}
