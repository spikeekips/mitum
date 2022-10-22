package isaacstates

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
)

type BootingHandler struct {
	*baseHandler
	lastManifest         func() (base.Manifest, bool, error)
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc
}

type NewBootingHandlerType struct {
	*BootingHandler
}

func NewNewBootingHandlerType(
	local base.LocalNode,
	params *isaac.LocalParams,
	lastManifest func() (base.Manifest, bool, error),
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc,
) *NewBootingHandlerType {
	return &NewBootingHandlerType{
		BootingHandler: &BootingHandler{
			baseHandler:          newBaseHandler(StateBooting, local, params),
			lastManifest:         lastManifest,
			nodeInConsensusNodes: nodeInConsensusNodes,
		},
	}
}

func (h *NewBootingHandlerType) new() (handler, error) {
	return &BootingHandler{
		baseHandler:          h.baseHandler.new(),
		lastManifest:         h.lastManifest,
		nodeInConsensusNodes: h.nodeInConsensusNodes,
	}, nil
}

func (st *BootingHandler) enter(from StateType, i switchContext) (func(), error) { //nolint:unparam //...
	e := util.StringErrorFunc("failed to enter booting state")

	if _, err := st.baseHandler.enter(from, i); err != nil {
		return nil, e(err, "")
	}

	var manifest base.Manifest

	switch m, found, err := st.lastManifest(); {
	case err != nil:
		return nil, e(err, "")
	case !found:
		// NOTE empty block map item, moves to syncing
		st.Log().Debug().Msg("empty block map item; moves to syncing")

		return nil, newSyncingSwitchContext(StateBooting, base.GenesisHeight)
	default:
		manifest = m
	}

	avp := st.lastVoteproofs().ACCEPT()

	switch {
	case avp == nil:
		return nil, e(nil, "empty last accept voteproof")
	default:
		if err := compareManifestWithACCEPTVoteproof(manifest, avp); err != nil {
			st.Log().Debug().
				Interface("voteproof", avp).
				Interface("manifest", manifest).
				Msg("manifest and last accept voteproof do not match")

			return nil, e(err, "")
		}
	}

	// NOTE if node not in suffrage, moves to syncing
	switch suf, found, err := st.nodeInConsensusNodes(st.local, manifest.Height()+1); {
	case errors.Is(err, storage.ErrNotFound):
		st.Log().Debug().Interface("height", manifest.Height()+1).Msg("suffrage not found; moves to syncing")

		return nil, newSyncingSwitchContext(StateBooting, manifest.Height())
	case err != nil:
		return nil, e(err, "")
	case suf == nil || suf.Len() < 1:
		return nil, e(nil, "empty suffrage for last manifest, %d", manifest.Height()+1)
	case !found:
		st.Log().Debug().Msg("local not in consensus node; moves to syncing")

		return nil, newSyncingSwitchContext(StateBooting, manifest.Height())
	}

	st.Log().Debug().Msg("moves to joining")

	return nil, newJoiningSwitchContext(StateBooting, avp)
}

type bootingSwitchContext struct {
	baseSwitchContext
}

func newBootingSwitchContext(from StateType) bootingSwitchContext {
	return bootingSwitchContext{
		baseSwitchContext: newBaseSwitchContext(StateBooting, switchContextOKFuncCheckFrom(from)),
	}
}

func compareManifestWithACCEPTVoteproof(manifest base.Manifest, vp base.ACCEPTVoteproof) error {
	e := util.StringErrorFunc("failed to compare manifest with accept voteproof")

	switch {
	case manifest.Height() != vp.Point().Height():
		return e(nil, "height does not match; %d != %d", manifest.Height(), vp.Point().Height())
	case !manifest.Hash().Equal(vp.BallotMajority().NewBlock()):
		return e(nil, "hash does not match")
	default:
		return nil
	}
}
