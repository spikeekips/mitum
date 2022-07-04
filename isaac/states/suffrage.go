package isaacstates

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

// SuffrageStateBuilder tries to sync suffrage states from remote nodes. It will
// rebuild the entire suffrage states history. SuffrageProof from getSuffrageProof
// should be valid(IsValid()).
type SuffrageStateBuilder struct {
	lastSuffrageProof func(context.Context) (base.SuffrageProof, bool, error)
	getSuffrageProof  func(_ context.Context, suffrageheight base.Height) (base.SuffrageProof, bool, error)
	networkID         base.NetworkID
	batchlimit        uint64
	retryInterval     time.Duration
}

func NewSuffrageStateBuilder(
	networkID base.NetworkID,
	lastSuffrageProof func(context.Context) (base.SuffrageProof, bool, error),
	getSuffrageProof func(context.Context, base.Height) (base.SuffrageProof, bool, error),
) *SuffrageStateBuilder {
	return &SuffrageStateBuilder{
		networkID:         networkID,
		lastSuffrageProof: lastSuffrageProof,
		getSuffrageProof:  getSuffrageProof,
		batchlimit:        333, //nolint:gomnd //...
		retryInterval:     time.Second * 3,
	}
}

// Build builds latest suffrage states from localstate.
func (s *SuffrageStateBuilder) Build(ctx context.Context, localstate base.State) (base.Suffrage, error) {
	e := util.StringErrorFunc("failed to build suffrage states")

	if s.batchlimit < 1 {
		return nil, e(nil, "invalid numbatches, %d", s.batchlimit)
	}

	fromheight := base.GenesisHeight
	var currentsuf base.Suffrage

	if localstate != nil {
		switch i, err := isaac.NewSuffrageFromState(localstate); {
		case err != nil:
			return nil, e(err, "invalid localstate")
		default:
			currentsuf = i

			v, _ := base.LoadSuffrageState(localstate)
			fromheight = v.Height() + 1
		}
	}

	var suf base.Suffrage
	var last base.State

	switch proof, updated, err := s.retryLastSuffrageProof(ctx); {
	case err != nil:
		return nil, e(err, "")
	case !updated:
		i, err := isaac.NewSuffrageFromState(localstate)
		if err != nil {
			return nil, e(err, "invalid localstate")
		}

		return i, nil
	default:
		if err := proof.IsValid(s.networkID); err != nil {
			return nil, e(err, "")
		}

		if localstate != nil {
			if proof.State().Height() <= localstate.Height() {
				return currentsuf, nil
			}

			v, _ := base.LoadSuffrageState(localstate)

			if proof.SuffrageHeight() <= v.Height() {
				return currentsuf, nil
			}
		}

		i, err := isaac.NewSuffrageFromState(proof.State())
		if err != nil {
			return nil, e(err, "")
		}

		last = proof.State()
		suf = i
	}

	if err := s.buildBatch(ctx, localstate, last, fromheight); err != nil {
		return nil, e(err, "")
	}

	return suf, nil
}

func (s *SuffrageStateBuilder) buildBatch(
	ctx context.Context, localstate, last base.State, from base.Height,
) error {
	e := util.StringErrorFunc("failed to build by batch")

	lastv, _ := base.LoadSuffrageState(last)
	lastheight := lastv.Height()

	newprev := localstate
	var previous base.State
	var proofs []base.SuffrageProof
	var provelock sync.Mutex

	if err := util.BatchWork(
		ctx,
		uint64((lastheight-from).Int64())+1,
		s.batchlimit,
		func(_ context.Context, last uint64) error {
			previous = newprev

			switch r := (last + 1) % s.batchlimit; {
			case r == 0:
				proofs = make([]base.SuffrageProof, s.batchlimit)
			default:
				proofs = make([]base.SuffrageProof, r)
			}

			return nil
		},
		func(_ context.Context, i, last uint64) error {
			height := base.Height(int64(i)) + from

			proof, found, err := s.retryGetSuffrageProof(ctx, height)

			switch {
			case err != nil:
				return err
			case !found:
				return util.ErrNotFound.Errorf("suffrage proof not found, %d", height)
			}

			return func() error {
				provelock.Lock()
				defer provelock.Unlock()

				if err := s.prove(proof, proofs, previous); err != nil {
					return err
				}

				if uint64((proof.SuffrageHeight() - from).Int64()) == last {
					newprev = proof.State()
				}

				return nil
			}()
		},
	); err != nil {
		return e(err, "")
	}

	return nil
}

func (*SuffrageStateBuilder) prove(
	proof base.SuffrageProof,
	proofs []base.SuffrageProof,
	previous base.State,
) error {
	prevheight := base.NilHeight

	if previous != nil {
		i, _ := base.LoadSuffrageState(previous)
		prevheight = i.Height()
	}

	height := proof.SuffrageHeight()

	index := (height - prevheight - 1).Int64()
	if index >= int64(len(proofs)) {
		return errors.Errorf("wrong height")
	}

	proofs[index] = proof

	if index == 0 {
		if err := proof.Prove(previous); err != nil {
			return err
		}
	}

	if index > 0 && proofs[index-1] != nil {
		if err := proof.Prove(proofs[index-1].State()); err != nil {
			return err
		}
	}

	// revive:disable-next-line:optimize-operands-order
	if index+1 < int64(len(proofs)) && proofs[index+1] != nil {
		if err := proofs[index+1].Prove(proof.State()); err != nil {
			return err
		}
	}

	return nil
}

func (s *SuffrageStateBuilder) retryLastSuffrageProof(
	ctx context.Context,
) (proof base.SuffrageProof, found bool, _ error) {
	if err := isaac.RetrySyncSource(
		ctx,
		func() (bool, error) {
			i, j, err := s.lastSuffrageProof(ctx)

			if err == nil {
				proof = i
				found = j

				return false, nil
			}

			return false, err
		},
		-1,
		s.retryInterval,
	); err != nil {
		return proof, false, err
	}

	return proof, found, nil
}

func (s *SuffrageStateBuilder) retryGetSuffrageProof(
	ctx context.Context, height base.Height,
) (proof base.SuffrageProof, found bool, _ error) {
	if err := isaac.RetrySyncSource(
		ctx,
		func() (bool, error) {
			i, j, err := s.getSuffrageProof(ctx, height)

			if err == nil {
				proof = i
				found = j

				return false, nil
			}

			return false, err
		},
		-1,
		s.retryInterval,
	); err != nil {
		return proof, false, err
	}

	return proof, found, nil
}
