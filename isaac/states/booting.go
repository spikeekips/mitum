package isaacstates

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type BootingHandler struct {
	*baseHandler
	lastManifest func() (base.Manifest, bool, error)
	getSuffrage  isaac.GetSuffrageByBlockHeight
}

func NewBootingHandler(
	local base.LocalNode,
	policy isaac.NodePolicy,
	lastManifest func() (base.Manifest, bool, error),
	getSuffrage isaac.GetSuffrageByBlockHeight,
) *BootingHandler {
	return &BootingHandler{
		baseHandler:  newBaseHandler(StateBooting, local, policy, nil),
		lastManifest: lastManifest,
		getSuffrage:  getSuffrage,
	}
}

func (st *BootingHandler) enter(i switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to enter booting state")

	_, err := st.baseHandler.enter(i)
	if err != nil {
		return nil, e(err, "")
	}

	var manifest base.Manifest
	switch i, found, err := st.lastManifest(); {
	case err != nil:
		return nil, e(err, "")
	case !found:
		// NOTE empty block data, moves to syncing
		st.Log().Debug().Msg("empty block data; moves to syncing")

		return nil, newSyncingSwitchContext(StateEmpty, base.GenesisHeight)
	default:
		manifest = i
	}

	avp := st.lastVoteproof().ACCEPT()
	switch {
	case avp == nil:
		return nil, e(nil, "empty last accept voteproof")
	default:
		if err := compareManifestWithACCEPTVoteproof(manifest, avp); err != nil {
			st.Log().Debug().Msg("manifest and last accept voteproof do not match")

			return nil, e(err, "")
		}
	}

	// BLOCK if node is candidate, moves to joining
	// NOTE if node not in suffrage, moves to syncing
	switch suf, found, err := st.getSuffrage(manifest.Height() + 1); {
	case err != nil:
		return nil, e(err, "")
	case !found:
		return nil, e(nil, "empty suffrage for last manifest, %d", manifest.Height())
	case !suf.Exists(st.local.Address()):
		st.Log().Debug().Msg("local not in suffrage; moves to syncing")

		return nil, newSyncingSwitchContext(StateEmpty, manifest.Height())
	}

	st.Log().Debug().Msg("moves to joining")

	return nil, newJoiningSwitchContext(StateEmpty, avp)
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
