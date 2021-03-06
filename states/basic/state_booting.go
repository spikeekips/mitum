package basicstates

import (
	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util/logging"
)

type BootingState struct {
	*logging.Logging
	*BaseState
	storage  storage.Storage
	blockFS  *storage.BlockFS
	policy   *isaac.LocalPolicy
	suffrage base.Suffrage
}

func NewBootingState(
	st storage.Storage,
	blockFS *storage.BlockFS,
	policy *isaac.LocalPolicy,
	suffrage base.Suffrage,
) *BootingState {
	return &BootingState{
		Logging: logging.NewLogging(func(c logging.Context) logging.Emitter {
			return c.Str("module", "basic-booting-state")
		}),
		BaseState: NewBaseState(base.StateBooting),
		storage:   st,
		blockFS:   blockFS,
		policy:    policy,
		suffrage:  suffrage,
	}
}

func (st *BootingState) Enter(sctx StateSwitchContext) (func() error, error) {
	callback := EmptySwitchFunc
	if i, err := st.BaseState.Enter(sctx); err != nil {
		return nil, err
	} else if i != nil {
		callback = i
	}

	if err := storage.CheckBlock(st.storage, st.blockFS, st.policy.NetworkID()); err != nil {
		st.Log().Error().Err(err).Msg("something wrong to check blocks")

		if !xerrors.Is(err, storage.NotFoundError) {
			return nil, err
		}

		st.Log().Debug().Msg("empty blocks found; cleaning up")
		// NOTE empty block
		if err := storage.Clean(st.storage, st.blockFS, false); err != nil {
			return nil, err
		}

		if len(st.suffrage.Nodes()) < 2 { // NOTE suffrage nodes has local node itself
			st.Log().Debug().Msg("empty blocks; no other nodes in suffrage; can not sync")

			return nil, xerrors.Errorf("empty blocks, but no other nodes; can not sync")
		} else {
			st.Log().Debug().Msg("empty blocks; will sync")

			return func() error {
				if err := callback(); err != nil {
					return err
				}

				return NewStateSwitchContext(base.StateBooting, base.StateSyncing)
			}, nil
		}
	}

	return func() error {
		if err := callback(); err != nil {
			return err
		}

		st.Log().Debug().Msg("block checked; moves to joining")

		return NewStateSwitchContext(base.StateBooting, base.StateJoining)
	}, nil
}
