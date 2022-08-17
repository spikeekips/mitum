package launch

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameNodeInfo      = ps.PName("nodeinfo")
	NodeInfoContextKey = ps.ContextKey("nodeinfo")
)

func PNodeInfo(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare nodeinfo")

	var log *logging.Logging
	var version util.Version
	var local base.LocalNode
	var policy *isaac.NodePolicy
	var design NodeDesign
	var db isaac.Database

	if err := ps.LoadsFromContextOK(ctx,
		LoggingContextKey, &log,
		VersionContextKey, &version,
		DesignContextKey, &design,
		LocalContextKey, &local,
		NodePolicyContextKey, &policy,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return ctx, e(err, "")
	}

	nodeinfo := isaacnetwork.NewNodeInfoUpdater(local, version)
	_ = nodeinfo.SetConsensusState(isaacstates.StateBooting)
	_ = nodeinfo.SetConnInfo(network.ConnInfoToString(
		design.Network.PublishString,
		design.Network.TLSInsecure,
	))
	_ = nodeinfo.SetNodePolicy(policy)

	ctx = context.WithValue(ctx, NodeInfoContextKey, nodeinfo) //revive:disable-line:modifies-parameter

	if err := UpdateNodeInfoWithNewBlock(db, nodeinfo); err != nil {
		log.Log().Error().Err(err).Msg("failed to update nodeinfo")
	}

	return ctx, nil
}

func UpdateNodeInfoWithNewBlock(
	db isaac.Database,
	nodeinfo *isaacnetwork.NodeInfoUpdater,
) error {
	switch m, found, err := db.LastBlockMap(); {
	case err != nil:
		return err
	case !found:
		return errors.Errorf("last BlockMap not found")
	case !nodeinfo.SetLastManifest(m.Manifest()):
		return nil
	}

	switch proof, found, err := db.LastSuffrageProof(); {
	case err != nil:
		return errors.Errorf("last SuffrageProof not found")
	case found && nodeinfo.SetSuffrageHeight(proof.SuffrageHeight()):
		suf, err := proof.Suffrage()
		if err != nil {
			return errors.Errorf("failed suffrage from proof")
		}

		_ = nodeinfo.SetConsensusNodes(suf.Nodes())
	}

	_ = nodeinfo.SetNetworkPolicy(db.LastNetworkPolicy())

	return nil
}
