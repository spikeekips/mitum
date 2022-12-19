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
	getFromLocal  func() (_ base.Height, _ base.SuffrageProof, candidatestate base.State, _ bool, _ error)
	getFromRemote func(context.Context, base.State) (
		lastheight base.Height, _ []base.SuffrageProof, lastcandidatestate base.State, _ error)
	whenUpdatedf func(
		_ context.Context, previous base.SuffrageProof, updated base.SuffrageProof, updatedstate base.State)
	lastheight          *util.Locked[base.Height]
	last                *util.Locked[[]base.SuffrageProof]
	lastcandidates      base.State
	checkRemoteInterval time.Duration
}

func NewLastConsensusNodesWatcher(
	getFromLocal func() (base.Height, base.SuffrageProof, base.State, bool, error),
	getFromRemote func(context.Context, base.State) (base.Height, []base.SuffrageProof, base.State, error),
	whenUpdatedf func(context.Context, base.SuffrageProof, base.SuffrageProof, base.State),
	checkInterval time.Duration,
) (*LastConsensusNodesWatcher, error) {
	if whenUpdatedf == nil {
		whenUpdatedf = func( // revive:disable-line:modifies-parameter
			context.Context, base.SuffrageProof, base.SuffrageProof, base.State) {
		}
	}

	u := &LastConsensusNodesWatcher{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "last-consensus-nodes-watcher")
		}),
		getFromLocal:        getFromLocal,
		getFromRemote:       getFromRemote,
		checkRemoteInterval: checkInterval,
		lastheight:          util.NewLocked(base.NilHeight),
		last:                util.EmptyLocked([]base.SuffrageProof{}),
		whenUpdatedf:        whenUpdatedf,
	}

	switch height, proof, st, found, err := u.getFromLocal(); {
	case err != nil:
		return nil, err
	case !found:
	default:
		_ = u.lastheight.SetValue(height)
		_ = u.last.SetValue([]base.SuffrageProof{proof})
		u.lastcandidates = st
	}

	u.ContextDaemon = util.NewContextDaemon(u.start)

	return u, nil
}

func (u *LastConsensusNodesWatcher) Last() (base.SuffrageProof, base.State, error) {
	switch height, p, c, found, err := u.getFromLocal(); {
	case err != nil:
		return nil, nil, err
	case !found:
		var proof base.SuffrageProof
		var candidates base.State

		_, _ = u.lastheight.Set(func(old base.Height, isempty bool) (base.Height, error) {
			proofs, _ := u.last.Value()
			if len(proofs) > 0 {
				proof = proofs[len(proofs)-1]
			}

			candidates = u.lastcandidates

			return base.NilHeight, util.ErrLockedSetIgnore.Call()
		})

		return proof, candidates, nil
	default:
		var proof base.SuffrageProof

		proofs, candidates, _, _ := u.update(height, []base.SuffrageProof{p}, c)
		if len(proofs) > 0 {
			proof = proofs[len(proofs)-1]
		}

		return proof, candidates, nil
	}
}

func (u *LastConsensusNodesWatcher) Exists(node base.Node) (base.Suffrage, bool, error) {
	proof, st, err := u.Last()
	if err != nil {
		return nil, false, err
	}

	return IsNodeInLastConsensusNodes(node, proof, st)
}

func (u *LastConsensusNodesWatcher) GetSuffrage(height base.Height) (base.Suffrage, bool, error) {
	var proof base.SuffrageProof

	_, _ = u.lastheight.Set(func(lastheight base.Height, _ bool) (base.Height, error) {
		switch {
		case height > lastheight:
			return base.NilHeight, util.ErrLockedSetIgnore.Call()
		case lastheight <= base.NilHeight:
			return base.NilHeight, util.ErrLockedSetIgnore.Call()
		}

		l3, _ := u.last.Value()

		switch {
		case len(l3) < 1:
			return base.NilHeight, util.ErrLockedSetIgnore.Call()
		case height == lastheight:
			proof = l3[len(l3)-1]

			return base.NilHeight, util.ErrLockedSetIgnore.Call()
		}

		for i := len(l3) - 1; i >= 0; i-- {
			j := l3[i]

			if height >= j.Map().Manifest().Height() {
				proof = j

				break
			}
		}

		return base.NilHeight, util.ErrLockedSetIgnore.Call()
	})

	if proof != nil {
		suf, err := proof.Suffrage()

		return suf, true, err
	}

	return nil, false, nil
}

func (u *LastConsensusNodesWatcher) SetWhenUpdated(
	whenUpdated func(context.Context, base.SuffrageProof, base.SuffrageProof, base.State),
) {
	u.whenUpdatedf = whenUpdated
}

func (u *LastConsensusNodesWatcher) start(ctx context.Context) error {
	if err := u.check(ctx); err != nil {
		u.Log().Error().Err(err).Msg("failed to check remote")
	}

	ticker := time.NewTicker(u.checkRemoteInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := u.check(ctx); err != nil {
				u.Log().Error().Err(err).Msg("failed to check")
			}
		}
	}
}

func (u *LastConsensusNodesWatcher) check(ctx context.Context) error {
	var last base.SuffrageProof
	var laststate base.State

	switch i, _, err := u.Last(); {
	case err != nil:
		return err
	case i != nil:
		last = i
		laststate = i.State()
	}

	switch height, proof, candidates, err := u.getFromRemote(ctx, laststate); {
	case err != nil:
		return err
	default:
		newproofs, newcandidates, lastheightUpdated, nodesUpdated := u.update(height, proof, candidates)

		if lastheightUpdated {
			u.Log().Debug().Interface("height", height).Msg("new height found from remote")

			defer func() {
				// NOTE empty updated SuffrageProof when new updated last height
				go u.whenUpdatedf(ctx, last, nil, nil)
			}()
		}

		if nodesUpdated {
			u.Log().Debug().Interface("proofs", newproofs).Msg("new suffrage proof found from remote")

			go u.whenUpdatedf(ctx, last, newproofs[len(newproofs)-1], newcandidates)
		}

		return nil
	}
}

func (u *LastConsensusNodesWatcher) update(
	height base.Height,
	proofs []base.SuffrageProof,
	candidates base.State,
) (
	lastproofs []base.SuffrageProof,
	lastcandidates base.State,
	lastheightUpdated, nodesUpdated bool,
) {
	_, _ = u.lastheight.Set(func(old base.Height, isempty bool) (base.Height, error) {
		defer func() {
			lastproofs, _ = u.last.Value()
			lastcandidates = u.lastcandidates
		}()

		if isempty {
			if len(proofs) > 0 {
				nodesUpdated = true

				_ = u.last.SetValue(proofs)
			}

			if candidates != nil {
				nodesUpdated = true

				u.lastcandidates = candidates
			}

			lastheightUpdated = true

			return height, nil
		}

		if newl3 := u.compareProofs(proofs); len(newl3) > 0 {
			nodesUpdated = true

			_ = u.last.SetValue(newl3)
		}

		if u.isNewCandiates(candidates) {
			nodesUpdated = true

			u.lastcandidates = candidates
		}

		if height > old {
			lastheightUpdated = true

			return height, nil
		}

		return base.NilHeight, util.ErrLockedSetIgnore.Call()
	})

	return lastproofs, lastcandidates, lastheightUpdated, nodesUpdated
}

func (u *LastConsensusNodesWatcher) compareProofs(proofs []base.SuffrageProof) []base.SuffrageProof {
	if len(proofs) < 1 {
		return nil
	}

	l3, _ := u.last.Value()

	if len(l3) > 0 {
		top := l3[len(l3)-1].Map().Manifest().Height()

		proofs = util.FilterSlice(proofs, func(i base.SuffrageProof) bool { //revive:disable-line:modifies-parameter
			return i.Map().Manifest().Height() > top
		})
	}

	if len(proofs) < 1 {
		return nil
	}

	var newl3 []base.SuffrageProof

	switch {
	case len(proofs) >= 3: //nolint:gomnd //...
		newl3 = proofs[len(proofs)-3:]
	case len(proofs) < 3: //nolint:gomnd //...
		newl3 = proofs

		for i := len(l3) - 1; i >= 0; i-- {
			j := l3[i]

			if newl3[0].SuffrageHeight() == j.SuffrageHeight()+1 {
				k := make([]base.SuffrageProof, len(newl3)+1)
				k[0] = j
				copy(k[1:], newl3)

				newl3 = k

				if len(newl3) == 3 { //nolint:gomnd //...
					break
				}

				continue
			}

			break
		}
	}

	return newl3
}

func (u *LastConsensusNodesWatcher) isNewCandiates(candidates base.State) bool {
	switch last := u.lastcandidates; {
	case candidates == nil:
	case last == nil,
		candidates.Height() > last.Height():
		return true
	}

	return false
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
