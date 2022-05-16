package isaacstates

import (
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

// SuffrageStateBuilder tries to sync suffrage states from remote nodes. It will
// rebuild the entire suffrage states history. SuffrageProof from getSuffrageProof
// should be valid(IsValid()).
type SuffrageStateBuilder struct {
	lastproof         isaac.SuffrageProof
	lastSuffrageProof func(context.Context) (isaac.SuffrageProof, error)
	getSuffrageProof  func(context.Context, base.Height) (isaac.SuffrageProof, bool, error)
	networkID         base.NetworkID
	numbatches        int64
}

func NewSuffrageStateBuilder(
	networkID base.NetworkID,
	lastSuffrageProof func(context.Context) (isaac.SuffrageProof, error),
	getSuffrageProof func(context.Context, base.Height) (isaac.SuffrageProof, bool, error),
) *SuffrageStateBuilder {
	return &SuffrageStateBuilder{
		networkID:         networkID,
		lastSuffrageProof: lastSuffrageProof,
		getSuffrageProof:  getSuffrageProof,
		numbatches:        33, //nolint:gomnd //...
	}
}

// Build builds latest suffrage states from localstate.
func (s *SuffrageStateBuilder) Build(ctx context.Context, localstate base.State) (base.Suffrage, error) {
	e := util.StringErrorFunc("failed to build suffrage states")

	if s.numbatches < 1 {
		return nil, e(nil, "invalid numbatches, %d", s.numbatches)
	}

	fromheight := base.GenesisHeight
	var currentsuf base.Suffrage

	if localstate != nil {
		switch i, err := isaac.NewSuffrageFromState(localstate); {
		case err != nil:
			return nil, e(err, "invalid localstate")
		default:
			currentsuf = i
			fromheight = localstate.Height() + 1
		}
	}

	var last base.State

	switch proof, err := s.lastSuffrageProof(ctx); {
	case err != nil:
		return nil, e(err, "")
	case proof == nil:
		return nil, e(util.ErrNotFound.Call(), "last suffrage proof not found")
	default:
		if err := proof.IsValid(s.networkID); err != nil {
			return nil, e(err, "")
		}

		if localstate != nil {
			if proof.State().Height() <= localstate.Height() {
				return currentsuf, nil
			}
		}

		// NOTE proof will be Prove() later
		s.lastproof = proof
		last = proof.State()
	}

	suf, err := s.build(ctx, localstate, last, fromheight.Int64())
	if err != nil {
		return nil, e(err, "")
	}

	return suf, nil
}

func (s *SuffrageStateBuilder) build(
	ctx context.Context, localstate, last base.State, fromheight int64,
) (base.Suffrage, error) {
	var diff int64

	switch {
	case localstate == nil && last.Height() == base.GenesisHeight:
		diff = 1
	case localstate == nil:
		diff = last.Height().Int64()
	default:
		diff = last.Height().Int64() - localstate.Height().Int64()
	}

	laststate := localstate

	for i := int64(0); i < (diff/s.numbatches + 1); i++ {
		from := fromheight + (i * s.numbatches)
		to := fromheight + ((i + 1) * s.numbatches) - 1

		if to > last.Height().Int64() {
			to = last.Height().Int64()
		}

		j, err := s.buildBatch(ctx, base.Height(from), base.Height(to), laststate)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}

		laststate = j
	}

	suf, err := isaac.NewSuffrageFromState(laststate)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	return suf, nil
}

func (s *SuffrageStateBuilder) buildBatch(ctx context.Context, from, to base.Height, previous base.State) (laststate base.State, _ error) {
	e := util.StringErrorFunc("failed to build by batch")

	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

	proofch := make(chan isaac.SuffrageProof, 1)
	donech := make(chan error, 1)

	go func() {
		var last base.State
		proofs := make([]isaac.SuffrageProof, (to - from + 1).Int64())

	end:
		for proof := range proofch {
			if err := s.prove(from, proof, proofs, previous); err != nil {
				donech <- err

				break end
			}

			if proof.State().Height() == to {
				last = proof.State()
			}
		}

		laststate = last

		donech <- nil
	}()

	for i := from; i <= to; i++ {
		height := i

		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			switch proof, found, err := s.getSuffrageProof(ctx, height); {
			case err != nil:
				return err
			case !found:
				return util.ErrNotFound.Errorf("suffrage proof not found, %d", height)
			default:
				proofch <- proof

				return nil
			}
		}); err != nil {
			return nil, e(err, "")
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return nil, e(err, "")
	}

	close(proofch)

	if err := <-donech; err != nil {
		return nil, e(err, "")
	}

	return laststate, nil
}

func (*SuffrageStateBuilder) prove(
	from base.Height,
	proof isaac.SuffrageProof,
	proofs []isaac.SuffrageProof,
	previous base.State,
) error {
	height := proof.State().Height()

	index := (height - from).Int64()
	if index >= int64(len(proofs)) {
		return errors.Errorf("wrong height")
	}

	proofs[index] = proof

	if index == 0 {
		if err := proof.Prove(previous); err != nil {
			return errors.Wrap(err, "")
		}
	}

	if index > 0 && proofs[index-1] != nil {
		if err := proof.Prove(proofs[index-1].State()); err != nil {
			return errors.Wrap(err, "")
		}
	}

	// revive:disable-next-line:optimize-operands-order
	if index+1 < int64(len(proofs)) && proofs[index+1] != nil {
		if err := proofs[index+1].Prove(proof.State()); err != nil {
			return errors.Wrap(err, "")
		}
	}

	return nil
}
