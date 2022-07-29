package isaac

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type (
	GetLastSuffrageProofFromRemoteFunc func(context.Context) (base.SuffrageProof, bool, error)
	GetSuffrageProofFromRemoteFunc     func(_ context.Context, suffrageheight base.Height) (
		base.SuffrageProof, bool, error)
	GetLastSuffrageCandidateStateRemoteFunc func(context.Context) (base.State, bool, error)
)

// SuffrageStateBuilder tries to sync suffrage states from remote nodes. It will
// rebuild the entire suffrage states history. SuffrageProof from getSuffrageProof
// should be valid(IsValid()).
type SuffrageStateBuilder struct {
	lastSuffrageProof          GetLastSuffrageProofFromRemoteFunc
	getSuffrageProof           GetSuffrageProofFromRemoteFunc
	lastSuffrageCandidateState GetLastSuffrageCandidateStateRemoteFunc
	networkID                  base.NetworkID
	batchlimit                 uint64
}

func NewSuffrageStateBuilder(
	networkID base.NetworkID,
	lastSuffrageProof GetLastSuffrageProofFromRemoteFunc,
	getSuffrageProof GetSuffrageProofFromRemoteFunc,
	lastSuffrageCandidateState GetLastSuffrageCandidateStateRemoteFunc,
) *SuffrageStateBuilder {
	return &SuffrageStateBuilder{
		networkID:                  networkID,
		lastSuffrageProof:          lastSuffrageProof,
		getSuffrageProof:           getSuffrageProof,
		lastSuffrageCandidateState: lastSuffrageCandidateState,
		batchlimit:                 333, //nolint:gomnd //...
	}
}

// Build builds latest suffrage states from localstate.
func (s *SuffrageStateBuilder) Build(
	ctx context.Context, localstate base.State,
) (base.SuffrageProof, base.State, error) {
	e := util.StringErrorFunc("failed to build suffrage states")

	if s.batchlimit < 1 {
		return nil, nil, e(nil, "invalid numbatches, %d", s.batchlimit)
	}

	fromheight := base.GenesisHeight

	if localstate != nil {
		if _, err := NewSuffrageFromState(localstate); err != nil {
			return nil, nil, e(err, "invalid localstate")
		}

		v, _ := base.LoadSuffrageNodesStateValue(localstate)
		fromheight = v.Height() + 1
	}

	var last base.SuffrageProof

	switch proof, updated, err := s.lastSuffrageProof(ctx); {
	case err != nil:
		return nil, nil, e(err, "")
	case !updated:
		if localstate != nil {
			if _, err := NewSuffrageFromState(localstate); err != nil {
				return nil, nil, e(err, "invalid localstate")
			}
		}

		return nil, nil, nil
	default:
		if err := proof.IsValid(s.networkID); err != nil {
			return nil, nil, e(err, "")
		}

		if localstate != nil {
			if proof.State().Height() <= localstate.Height() {
				return nil, nil, nil
			}

			v, _ := base.LoadSuffrageNodesStateValue(localstate)

			if proof.SuffrageHeight() <= v.Height() {
				return nil, nil, nil
			}
		}

		if _, err := NewSuffrageFromState(proof.State()); err != nil {
			return nil, nil, e(err, "")
		}

		last = proof
	}

	if err := s.buildBatch(ctx, localstate, last.State(), fromheight); err != nil {
		return nil, nil, e(err, "")
	}

	switch cst, _, err := s.lastSuffrageCandidateState(ctx); {
	case err != nil:
		return nil, nil, e(err, "")
	default:
		return last, cst, nil
	}
}

func (s *SuffrageStateBuilder) buildBatch(
	ctx context.Context, localstate, last base.State, from base.Height,
) error {
	e := util.StringErrorFunc("failed to build by batch")

	lastv, _ := base.LoadSuffrageNodesStateValue(last)
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

			proof, found, err := s.getSuffrageProof(ctx, height)

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
		i, _ := base.LoadSuffrageNodesStateValue(previous)
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
