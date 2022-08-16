package isaac

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type LastConsensusNodesWatcher struct {
	*util.ContextDaemon
	*logging.Logging
	getFromLocal  func() (_ base.SuffrageProof, candidatestate base.State, _ bool, _ error)
	getFromRemote func(context.Context, base.State) (_ base.SuffrageProof, candidatestate base.State, _ error)
	whenUpdated   func(context.Context, base.SuffrageProof, base.State)
	last          *util.Locked
	interval      time.Duration
}

func NewLastConsensusNodesWatcher(
	getFromLocal func() (base.SuffrageProof, base.State, bool, error),
	getFromRemote func(context.Context, base.State) (base.SuffrageProof, base.State, error),
	whenUpdated func(context.Context, base.SuffrageProof, base.State),
) *LastConsensusNodesWatcher {
	u := &LastConsensusNodesWatcher{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "suffrage-state-updater")
		}),
		getFromLocal:  getFromLocal,
		getFromRemote: getFromRemote,
		interval:      time.Second * 3, //nolint:gomnd //...
		last:          util.EmptyLocked(),
	}

	u.SetWhenUpdated(whenUpdated)
	u.ContextDaemon = util.NewContextDaemon(u.start)

	return u
}

func (u *LastConsensusNodesWatcher) Start() error {
	if _, _, err := u.getFromRemote(context.Background(), nil); err != nil {
		return err
	}

	return u.ContextDaemon.Start()
}

func (u *LastConsensusNodesWatcher) Last() (base.SuffrageProof, base.State, error) {
	e := util.StringErrorFunc("failed to get last suffrageproof and candidate state")

	var localproof base.SuffrageProof
	var localcandidates base.State

	switch proof, candidates, found, err := u.getFromLocal(); {
	case err != nil:
		return nil, nil, e(err, "")
	case found:
		localproof = proof
		localcandidates = candidates
	}

	switch proof, candidates := u.lastValue(); {
	case u.compare(localproof, proof, localcandidates, candidates):
		return proof, candidates, nil
	default:
		return localproof, localcandidates, nil
	}
}

func (u *LastConsensusNodesWatcher) Exists(node base.Node) (base.Suffrage, bool, error) {
	proof, st, err := u.Last()
	if err != nil {
		return nil, false, err
	}

	return IsNodeInLastConsensusNodes(node, proof, st)
}

func (u *LastConsensusNodesWatcher) SetWhenUpdated(
	whenUpdated func(context.Context, base.SuffrageProof, base.State),
) {
	if whenUpdated == nil {
		whenUpdated = func(context.Context, base.SuffrageProof, base.State) {} // revive:disable-line:modifies-parameter
	}

	u.whenUpdated = whenUpdated
}

func (u *LastConsensusNodesWatcher) lastValue() (base.SuffrageProof, base.State) {
	i, isnil := u.last.Value()
	if isnil || i == nil {
		return nil, nil
	}

	j := i.([2]interface{}) //nolint:forcetypeassert //...

	var proof base.SuffrageProof
	var st base.State

	if j[0] != nil {
		proof = j[0].(base.SuffrageProof) //nolint:forcetypeassert //...
	}

	if j[1] != nil {
		st = j[1].(base.State) //nolint:forcetypeassert //...
	}

	return proof, st
}

func (u *LastConsensusNodesWatcher) start(ctx context.Context) error {
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

func (u *LastConsensusNodesWatcher) checkRemote(ctx context.Context) error {
	var known base.State

	if suf, _ := u.lastValue(); suf != nil {
		known = suf.State()
	}

	switch proof, candidates, err := u.getFromRemote(ctx, known); {
	case err != nil:
		return err
	case proof == nil:
		return nil
	default:
		if u.update(proof, candidates) {
			u.Log().Debug().Interface("proof", proof).Msg("new suffrage proof found from remote")
		}

		return nil
	}
}

func (*LastConsensusNodesWatcher) newerHeight(proof base.SuffrageProof, candidates base.State) base.Height {
	height := base.NilHeight

	if proof != nil {
		height = proof.Map().Manifest().Height()
	}

	if candidates != nil && candidates.Height() > height {
		height = candidates.Height()
	}

	return height
}

func (u *LastConsensusNodesWatcher) compare(
	aproof, bproof base.SuffrageProof, acandidates, bcandidates base.State,
) bool {
	aheight := u.newerHeight(aproof, acandidates)
	bheight := u.newerHeight(bproof, bcandidates)

	return bheight > aheight
}

func (u *LastConsensusNodesWatcher) update(proof base.SuffrageProof, candidates base.State) bool {
	var updated bool

	_, _ = u.last.Set(func(i interface{}) (interface{}, error) {
		if i == nil {
			return [2]interface{}{proof, candidates}, nil
		}

		j := i.([2]interface{}) //nolint:forcetypeassert //...

		oldproof := j[0].(base.SuffrageProof) //nolint:forcetypeassert //...
		oldcandidates := j[1].(base.State)    //nolint:forcetypeassert //...

		updated = u.compare(oldproof, proof, oldcandidates, candidates)
		if !updated {
			return nil, util.ErrLockedSetIgnore.Errorf("old")
		}

		return [2]interface{}{proof, candidates}, nil
	})

	return updated
}

func (u *LastConsensusNodesWatcher) checkUpdated(ctx context.Context, last base.Height) base.Height {
	proof, candidates, err := u.Last()
	if err != nil {
		return last
	}

	switch height := u.newerHeight(proof, candidates); {
	case last >= height:
		return last
	default:
		go u.whenUpdated(ctx, proof, candidates)

		return height
	}
}

func IsNodeInLastConsensusNodes(node base.Node, proof base.SuffrageProof, st base.State) (base.Suffrage, bool, error) {
	if proof == nil {
		return nil, false, nil
	}

	suf, err := proof.Suffrage()
	if err != nil {
		return nil, false, err
	}

	if suf.ExistsPublickey(node.Address(), node.Publickey()) {
		return suf, true, nil
	}

	if st == nil {
		return suf, false, nil
	}

	candidates, err := base.LoadNodesFromSuffrageCandidatesState(st)
	if err != nil {
		return suf, false, err
	}

	for i := range candidates {
		n := candidates[i]

		if n.Address().Equal(node.Address()) && n.Publickey().Equal(node.Publickey()) {
			return suf, true, nil
		}
	}

	return suf, false, nil
}
