package main

import (
	"context"
	"net"
	"os"
	"time"

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

func (cmd *runCommand) prepare() (func() error, error) {
	stop := func() error {
		return nil
	}

	e := util.StringErrorFunc("failed to prepare")

	if err := cmd.prepareEncoder(); err != nil {
		return stop, e(err, "")
	}

	if err := cmd.prepareDesigns(); err != nil {
		return stop, e(err, "")
	}

	if err := cmd.prepareLocal(); err != nil {
		return stop, e(err, "")
	}

	cmd.nodeinfo = isaacnetwork.NewNodeInfoUpdater(cmd.local, version)
	_ = cmd.nodeinfo.SetConsensusState(isaacstates.StateBooting)
	_ = cmd.nodeinfo.SetConnInfo(network.ConnInfoToString(
		cmd.design.Network.PublishString,
		cmd.design.Network.TLSInsecure,
	))
	_ = cmd.nodeinfo.SetNodePolicy(cmd.nodePolicy)

	if err := cmd.prepareFlags(); err != nil {
		return stop, e(err, "")
	}

	if err := cmd.prepareDatabase(); err != nil {
		return stop, e(err, "")
	}

	cmd.proposalMaker = cmd.prepareProposalMaker()
	_ = cmd.proposalMaker.SetLogging(log)

	if err := cmd.prepareNetwork(); err != nil {
		return stop, e(err, "")
	}

	if err := cmd.prepareSyncSourceChecker(); err != nil {
		return stop, e(err, "")
	}

	if err := cmd.prepareSuffrage(); err != nil {
		return stop, e(err, "")
	}

	if err := cmd.prepareStates(); err != nil {
		return nil, err
	}

	cmd.syncSourcesRetryInterval = time.Second * 3 //nolint:gomnd //...

	return stop, nil
}

func (cmd *runCommand) prepareFlags() error {
	switch {
	case len(cmd.Discovery) < 1:
	default:
		cmd.discoveries = make([]quicstream.UDPConnInfo, len(cmd.Discovery))

		for i := range cmd.Discovery {
			ci, err := cmd.Discovery[i].ConnInfo()
			if err != nil {
				return errors.WithMessagef(err, "invalid member discovery, %q", cmd.Discovery[i])
			}

			cmd.discoveries[i] = ci
		}
	}

	return nil
}

func (cmd *runCommand) prepareDatabase() error {
	e := util.StringErrorFunc("failed to prepare database")

	nodeinfo, err := launch.CheckLocalFS(cmd.nodePolicy.NetworkID(), cmd.design.Storage.Base, cmd.enc)

	switch {
	case err == nil:
		if err = isaacblock.CleanBlockTempDirectory(launch.LocalFSDataDirectory(cmd.design.Storage.Base)); err != nil {
			return e(err, "")
		}
	case errors.Is(err, os.ErrNotExist):
		if err = launch.CleanStorage(
			cmd.design.Storage.Database.String(),
			cmd.design.Storage.Base,
			cmd.encs, cmd.enc,
		); err != nil {
			return e(err, "")
		}

		nodeinfo, err = launch.CreateLocalFS(
			launch.CreateDefaultNodeInfo(cmd.nodePolicy.NetworkID(), version), cmd.design.Storage.Base, cmd.enc)
		if err != nil {
			return e(err, "")
		}
	default:
		return e(err, "")
	}

	st, db, perm, pool, err := launch.LoadDatabase(
		nodeinfo, cmd.design.Storage.Database.String(), cmd.design.Storage.Base, cmd.encs, cmd.enc)
	if err != nil {
		return e(err, "")
	}

	_ = db.SetLogging(log)

	if err := db.Start(); err != nil {
		return e(err, "")
	}

	cmd.st = st
	cmd.nodeInfo = nodeinfo
	cmd.db = db
	cmd.perm = perm
	cmd.pool = pool

	// NOTE update node info
	cmd.updateNodeInfoWithNewBlock()

	return nil
}

func (cmd *runCommand) prepareNetwork() error {
	cmd.client = launch.NewNetworkClient(cmd.encs, cmd.enc, time.Second*2) //nolint:gomnd //...
	cmd.handlers = cmd.networkHandlers()

	quicconfig := launch.DefaultQuicConfig()
	quicconfig.AcceptToken = func(clientAddr net.Addr, token *quic.Token) bool {
		return true // FIXME NOTE manage blacklist
	}

	cmd.quicstreamserver = quicstream.NewServer(
		cmd.design.Network.Bind,
		launch.GenerateNewTLSConfig(),
		quicconfig,
		cmd.handlers.Handler,
	)
	_ = cmd.quicstreamserver.SetLogging(log)

	return cmd.prepareMemberlist()
}

func (cmd *runCommand) prepareLastSuffrageProofWatcher() {
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

func (cmd *runCommand) prepareSyncSourceChecker() error {
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

func (cmd *runCommand) prepareSuffrage() error {
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
