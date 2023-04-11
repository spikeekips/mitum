package isaacstates

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
)

type BootingHandlerArgs struct {
	LastManifestFunc         func() (base.Manifest, bool, error)
	NodeInConsensusNodesFunc isaac.NodeInConsensusNodesFunc
}

func NewBootingHandlerArgs() *BootingHandlerArgs {
	return &BootingHandlerArgs{
		NodeInConsensusNodesFunc: func(base.Node, base.Height) (base.Suffrage, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("NodeInConsensusNodesFunc")
		},
		LastManifestFunc: func() (base.Manifest, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("LastManifestFunc")
		},
	}
}

type BootingHandler struct {
	*baseHandler
	args *BootingHandlerArgs
}

type NewBootingHandlerType struct {
	*BootingHandler
}

func NewNewBootingHandlerType(
	local base.LocalNode,
	params *isaac.LocalParams,
	args *BootingHandlerArgs,
) *NewBootingHandlerType {
	return &NewBootingHandlerType{
		BootingHandler: &BootingHandler{
			baseHandler: newBaseHandlerType(StateBooting, local, params),
			args:        args,
		},
	}
}

func (h *NewBootingHandlerType) new() (handler, error) {
	return &BootingHandler{
		baseHandler: h.baseHandler.new(),
		args:        h.args,
	}, nil
}

func (st *BootingHandler) enter(from StateType, i switchContext) (func(), error) { //nolint:unparam //...
	e := util.StringErrorFunc("enter booting state")

	if _, err := st.baseHandler.enter(from, i); err != nil {
		return nil, e(err, "")
	}

	var manifest base.Manifest

	switch m, found, err := st.args.LastManifestFunc(); {
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
	switch suf, found, err := st.args.NodeInConsensusNodesFunc(st.local, manifest.Height()); {
	case errors.Is(err, storage.ErrNotFound):
		st.Log().Debug().Interface("height", manifest.Height()).Msg("suffrage not found; moves to syncing")

		return nil, newSyncingSwitchContext(StateBooting, manifest.Height())
	case err != nil:
		return nil, e(err, "")
	case suf == nil || suf.Len() < 1:
		return nil, e(nil, "empty suffrage for last manifest, %d", manifest.Height())
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
		baseSwitchContext: newBaseSwitchContext(from, StateBooting),
	}
}

func compareManifestWithACCEPTVoteproof(manifest base.Manifest, vp base.ACCEPTVoteproof) error {
	e := util.StringErrorFunc("compare manifest with accept voteproof")

	switch {
	case manifest.Height() != vp.Point().Height():
		return e(nil, "height does not match; %d != %d", manifest.Height(), vp.Point().Height())
	case !manifest.Hash().Equal(vp.BallotMajority().NewBlock()):
		return e(nil, "hash does not match")
	default:
		return nil
	}
}
