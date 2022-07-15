package isaacstates

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	GetLastSuffrageProofFromRemoteFunc func(context.Context) (base.SuffrageProof, bool, error)
	GetSuffrageProofFromRemoteFunc     func(_ context.Context, suffrageheight base.Height) (
		base.SuffrageProof, bool, error)
)

// SuffrageStateBuilder tries to sync suffrage states from remote nodes. It will
// rebuild the entire suffrage states history. SuffrageProof from getSuffrageProof
// should be valid(IsValid()).
type SuffrageStateBuilder struct {
	lastSuffrageProof GetLastSuffrageProofFromRemoteFunc
	getSuffrageProof  GetSuffrageProofFromRemoteFunc
	networkID         base.NetworkID
	batchlimit        uint64
}

func NewSuffrageStateBuilder(
	networkID base.NetworkID,
	lastSuffrageProof GetLastSuffrageProofFromRemoteFunc,
	getSuffrageProof GetSuffrageProofFromRemoteFunc,
) *SuffrageStateBuilder {
	return &SuffrageStateBuilder{
		networkID:         networkID,
		lastSuffrageProof: lastSuffrageProof,
		getSuffrageProof:  getSuffrageProof,
		batchlimit:        333, //nolint:gomnd //...
	}
}

// Build builds latest suffrage states from localstate.
func (s *SuffrageStateBuilder) Build(ctx context.Context, localstate base.State) (base.SuffrageProof, error) {
	e := util.StringErrorFunc("failed to build suffrage states")

	if s.batchlimit < 1 {
		return nil, e(nil, "invalid numbatches, %d", s.batchlimit)
	}

	fromheight := base.GenesisHeight

	if localstate != nil {
		if _, err := isaac.NewSuffrageFromState(localstate); err != nil {
			return nil, e(err, "invalid localstate")
		}

		v, _ := base.LoadSuffrageState(localstate)
		fromheight = v.Height() + 1
	}

	var last base.SuffrageProof

	switch proof, updated, err := s.lastSuffrageProof(ctx); {
	case err != nil:
		return nil, e(err, "")
	case !updated:
		if _, err := isaac.NewSuffrageFromState(localstate); err != nil {
			return nil, e(err, "invalid localstate")
		}

		return nil, nil
	default:
		if err := proof.IsValid(s.networkID); err != nil {
			return nil, e(err, "")
		}

		if localstate != nil {
			if proof.State().Height() <= localstate.Height() {
				return nil, nil
			}

			v, _ := base.LoadSuffrageState(localstate)

			if proof.SuffrageHeight() <= v.Height() {
				return nil, nil
			}
		}

		if _, err := isaac.NewSuffrageFromState(proof.State()); err != nil {
			return nil, e(err, "")
		}

		last = proof
	}

	if err := s.buildBatch(ctx, localstate, last.State(), fromheight); err != nil {
		return nil, e(err, "")
	}

	return last, nil
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

type LastSuffrageProofWatcher struct { // FIXME rename to LastConsensusNodesWatcher
	*util.ContextDaemon
	*logging.Logging
	getFromLocal  func() (base.SuffrageProof, bool, error)
	getFromRemote func(context.Context, base.State) (base.SuffrageProof, error)
	whenUpdated   func(context.Context, base.SuffrageProof)
	last          *util.Locked
	interval      time.Duration
}

func NewLastSuffrageProofWatcher(
	getFromLocal func() (base.SuffrageProof, bool, error),
	getFromRemote func(context.Context, base.State) (base.SuffrageProof, error),
	whenUpdated func(context.Context, base.SuffrageProof),
) *LastSuffrageProofWatcher {
	if whenUpdated == nil {
		whenUpdated = func(context.Context, base.SuffrageProof) {} // revive:disable-line:modifies-parameter
	}

	u := &LastSuffrageProofWatcher{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "suffrage-state-updater")
		}),
		getFromLocal:  getFromLocal,
		getFromRemote: getFromRemote,
		whenUpdated:   whenUpdated,
		interval:      time.Second * 3, //nolint:gomnd //...
		last:          util.EmptyLocked(),
	}

	u.ContextDaemon = util.NewContextDaemon(u.start)

	return u
}

func (u *LastSuffrageProofWatcher) Last() (base.SuffrageProof, error) {
	e := util.StringErrorFunc("failed to get last suffrageproof")

	var local base.SuffrageProof

	switch proof, found, err := u.getFromLocal(); {
	case err != nil:
		return nil, e(err, "")
	case found:
		local = proof
	}

	switch i, isnil := u.last.Value(); {
	case isnil, i == nil:
		return local, nil
	default:
		remote := i.(base.SuffrageProof) //nolint:forcetypeassert //...

		if local == nil {
			return remote, nil
		}

		// NOTE local proof will be used if same height
		if remote.Map().Manifest().Height() > local.Map().Manifest().Height() {
			return remote, nil
		}

		return local, nil
	}
}

func (u *LastSuffrageProofWatcher) start(ctx context.Context) error {
	last := base.NilHeight // NOTE suffrage height

	if err := u.checkRemote(ctx); err != nil {
		u.Log().Error().Err(err).Msg("failed to check remote")
	}

	last = u.checkUpdated(ctx, last)

	ticker := time.NewTicker(u.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := u.checkRemote(ctx); err != nil {
				u.Log().Error().Err(err).Msg("failed to check remote")
			}

			last = u.checkUpdated(ctx, last)
		}
	}
}

func (u *LastSuffrageProofWatcher) checkRemote(ctx context.Context) error {
	var known base.State

	if i, isnil := u.last.Value(); !isnil {
		known = i.(base.SuffrageProof).State() //nolint:forcetypeassert //...
	}

	switch proof, err := u.getFromRemote(ctx, known); {
	case err != nil:
		return err
	case proof == nil:
		return nil
	default:
		if u.update(proof) {
			u.Log().Debug().Interface("proof", proof).Msg("new suffrage proof found from remote")
		}

		return nil
	}
}

func (u *LastSuffrageProofWatcher) update(proof base.SuffrageProof) bool {
	var updated bool

	_, _ = u.last.Set(func(i interface{}) (interface{}, error) {
		if i == nil {
			return proof, nil
		}

		old := i.(base.SuffrageProof) //nolint:forcetypeassert //...

		if proof.Map().Manifest().Height() <= old.Map().Manifest().Height() {
			return nil, util.ErrLockedSetIgnore.Errorf("old")
		}

		updated = true

		return proof, nil
	})

	return updated
}

func (u *LastSuffrageProofWatcher) checkUpdated(ctx context.Context, last base.Height) base.Height {
	switch proof, err := u.Last(); {
	case err != nil:
		return last
	case proof == nil:
		return last
	case last >= proof.SuffrageHeight():
		return last
	default:
		go u.whenUpdated(ctx, proof)

		return proof.SuffrageHeight()
	}
}
