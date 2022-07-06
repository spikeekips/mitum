package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
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

	if err := cmd.prepareFlags(); err != nil {
		return stop, e(err, "")
	}

	if err := cmd.prepareDatabase(); err != nil {
		return stop, e(err, "")
	}

	if err := cmd.prepareNetwork(); err != nil {
		return stop, e(err, "")
	}

	if err := cmd.prepareSyncSourceChecker(); err != nil {
		return stop, e(err, "")
	}

	switch i, err := cmd.prepareProfiling(); {
	case err != nil:
		return stop, e(err, "")
	default:
		stop = i
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

	nodeinfo, err := launch.CheckLocalFS(networkID, cmd.design.Storage.Base, cmd.enc)

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
			launch.CreateDefaultNodeInfo(networkID, version), cmd.design.Storage.Base, cmd.enc)
		if err != nil {
			return e(err, "")
		}
	default:
		return e(err, "")
	}

	db, perm, pool, err := launch.LoadDatabase(
		nodeinfo, cmd.design.Storage.Database.String(), cmd.design.Storage.Base, cmd.encs, cmd.enc)
	if err != nil {
		return e(err, "")
	}

	_ = db.SetLogging(logging)

	if err := db.Start(); err != nil {
		return e(err, "")
	}

	cmd.nodeInfo = nodeinfo
	cmd.db = db
	cmd.perm = perm
	cmd.pool = pool

	return nil
}

func (cmd *runCommand) prepareProfiling() (func() error, error) {
	return launch.StartProfile(
		filepath.Join(cmd.design.Storage.Base, fmt.Sprintf("cpu-%s.pprof", util.ULID().String())),
		filepath.Join(cmd.design.Storage.Base, fmt.Sprintf("mem-%s.pprof", util.ULID().String())),
	)
}

func (cmd *runCommand) prepareNetwork() error {
	cmd.client = launch.NewNetworkClient(cmd.encs, cmd.enc, time.Second*2) //nolint:gomnd //...
	cmd.handlers = cmd.networkHandlers()

	cmd.quicstreamserver = quicstream.NewServer(
		cmd.design.Network.Bind,
		launch.GenerateNewTLSConfig(),
		launch.DefaultQuicConfig(),
		cmd.handlers.Handler,
	)
	_ = cmd.quicstreamserver.SetLogging(logging)

	return cmd.prepareMemberlist()
}

func (cmd *runCommand) prepareSuffrageStateBuilder() {
	cmd.suffrageStateBuilder = isaacstates.NewSuffrageStateBuilder(
		networkID,
		cmd.getLastSuffrageProofFunc(),
		cmd.getSuffrageProofFunc(),
	)
}

func (cmd *runCommand) prepareSyncSourceChecker() error {
	sources := make([]isaacnetwork.SyncSource, len(cmd.design.SyncSources))
	for i := range cmd.design.SyncSources {
		sources[i] = cmd.design.SyncSources[i].Source
	}

	switch {
	case len(sources) < 1:
		log.Warn().Msg("empty initial sync sources; connected memberlist members will be used")
	default:
		log.Debug().Interface("sync_sources", sources).Msg("initial sync sources found")
	}

	cmd.syncSourceChecker = isaacnetwork.NewSyncSourceChecker(
		cmd.local,
		cmd.nodePolicy.NetworkID(),
		cmd.client,
		time.Second*30, //nolint:gomnd //... // FIXME config
		cmd.enc,
		sources,
		cmd.updateSyncSources,
	)
	_ = cmd.syncSourceChecker.SetLogging(logging)

	cmd.syncSourcePool = isaac.NewSyncSourcePool(nil)

	return nil
}
