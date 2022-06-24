package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/network/quictransport"
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

	switch i, err := cmd.prepareProfiling(); {
	case err != nil:
		return stop, e(err, "")
	default:
		stop = i
	}

	if err := cmd.prepareStates(); err != nil {
		return nil, err
	}

	return stop, nil
}

func (cmd *runCommand) prepareFlags() error {
	switch {
	case len(cmd.SyncNode) < 1:
		return errors.Errorf("empty SyncNode")
	default:
		for i := range cmd.SyncNode {
			if _, err := cmd.SyncNode[i].ConnInfo(); err != nil {
				return errors.WithMessagef(err, "invalid SyncNode, %q", cmd.SyncNode[i])
			}
		}
	}

	switch {
	case len(cmd.Discovery) < 1:
	default:
		cmd.discoveries = make([]quictransport.ConnInfo, len(cmd.Discovery))

		for i := range cmd.Discovery {
			ci, err := cmd.Discovery[i].ConnInfo()
			if err != nil {
				return errors.WithMessagef(err, "invalid Discovery, %q", cmd.Discovery[i])
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
	var last util.Hash

	cmd.suffrageStateBuilder = isaacstates.NewSuffrageStateBuilder(
		networkID,
		func(ctx context.Context) (base.SuffrageProof, bool, error) {
			sn := cmd.SyncNode[0] // FIXME use multiple sync nodes

			ci, err := sn.ConnInfo()
			if err != nil {
				return nil, false, err
			}

			proof, updated, err := cmd.client.LastSuffrageProof(ctx, ci, last)
			switch {
			case err != nil:
				return proof, updated, err
			case !updated:
				return proof, updated, nil
			default:
				if err := proof.IsValid(networkID); err != nil {
					return nil, updated, err
				}

				last = proof.Map().Manifest().Suffrage()

				return proof, updated, nil
			}
		},
		func(ctx context.Context, suffrageheight base.Height) (base.SuffrageProof, bool, error) {
			sn := cmd.SyncNode[0]

			ci, err := sn.ConnInfo()
			if err != nil {
				return nil, false, err
			}

			proof, found, err := cmd.client.SuffrageProof(ctx, ci, suffrageheight)

			return proof, found, err
		},
	)
}
