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
	PNameNodeInfo      = ps.Name("nodeinfo")
	NodeInfoContextKey = util.ContextKey("nodeinfo")
)

func PNodeInfo(pctx context.Context) (context.Context, error) {
	e := util.StringError("prepare nodeinfo")

	var log *logging.Logging
	var version util.Version
	var local base.LocalNode
	var isaacparams *isaac.Params
	var design NodeDesign
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		VersionContextKey, &version,
		DesignContextKey, &design,
		LocalContextKey, &local,
		ISAACParamsContextKey, &isaacparams,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	nodeinfo := isaacnetwork.NewNodeInfoUpdater(design.NetworkID, local, version)
	_ = nodeinfo.SetConsensusState(isaacstates.StateBooting)
	_ = nodeinfo.SetConnInfo(network.ConnInfoToString(
		design.Network.PublishString,
		design.Network.TLSInsecure,
	))
	_ = nodeinfo.SetLocalParams(isaacparams)

	nctx := context.WithValue(pctx, NodeInfoContextKey, nodeinfo)

	if err := UpdateNodeInfoWithNewBlock(db, nodeinfo); err != nil {
		log.Log().Error().Err(err).Msg("failed to update nodeinfo")
	}

	return nctx, nil
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
			return errors.Errorf("suffrage from proof")
		}

		_ = nodeinfo.SetConsensusNodes(suf.Nodes())
	}

	_ = nodeinfo.SetNetworkPolicy(db.LastNetworkPolicy())

	return nil
}
