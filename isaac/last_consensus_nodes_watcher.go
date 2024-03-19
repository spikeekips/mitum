package isaac

import (
	"context"
	"time"

	"github.com/pkg/errors"
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
	nwhenUpdatedf := whenUpdatedf
	if nwhenUpdatedf == nil {
		nwhenUpdatedf = func(
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
		last:                util.EmptyLocked[[]base.SuffrageProof](),
		whenUpdatedf:        nwhenUpdatedf,
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

		_ = u.lastheight.Get(func(base.Height, bool) error {
			proofs, _ := u.last.Value()
			if len(proofs) > 0 {
				proof = proofs[len(proofs)-1]
			}

			candidates = u.lastcandidates

			return nil
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

	_ = u.lastheight.Get(func(lastheight base.Height, _ bool) error {
		switch {
		case lastheight <= base.NilHeight,
			height > lastheight:
			return nil
		}

		l3, _ := u.last.Value()

		switch {
		case len(l3) < 1:
			return nil
		case height == lastheight:
			proof = l3[len(l3)-1]

			return nil
		}

		for i := len(l3) - 1; i >= 0; i-- {
			j := l3[i]

			if height >= j.Map().Manifest().Height() {
				proof = j

				break
			}
		}

		return nil
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
			return errors.WithStack(ctx.Err())
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

	switch height, proofs, candidates, err := u.getFromRemote(ctx, laststate); {
	case err != nil:
		return err
	default:
		newproofs, newcandidates, lastheightUpdated, nodesUpdated := u.update(height, proofs, candidates)

		var newproof base.SuffrageProof
		var newcandidatesst base.State

		if nodesUpdated {
			u.Log().Debug().Interface("proofs", newproofs).Msg("new suffrage proof found from remote")

			newproof = newproofs[len(newproofs)-1]
			newcandidatesst = newcandidates
		}

		if lastheightUpdated {
			u.Log().Debug().Interface("height", height).Msg("new height found from remote")
		}

		go u.whenUpdatedf(ctx, last, newproof, newcandidatesst)

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
	_, _ = u.lastheight.Set(func(old base.Height, _ bool) (base.Height, error) {
		defer func() {
			lastproofs, _ = u.last.Value()
			lastcandidates = u.lastcandidates
		}()

		var lheight base.Height
		var lproofs []base.SuffrageProof
		var lcandidates base.State

		switch h, p, c, err := u.loadLast(old); {
		case err != nil:
			return height, err
		default:
			lheight = h
			lproofs = p
			lcandidates = c
		}

		switch newl3 := u.compareProofs(lproofs, proofs); {
		case len(newl3) > 0:
			nodesUpdated = true

			_ = u.last.SetValue(newl3)
		default:
			_ = u.last.SetValue(lproofs)
		}

		switch {
		case u.isNewCandiates(lcandidates, candidates):
			nodesUpdated = true

			u.lastcandidates = candidates
		default:
			u.lastcandidates = lcandidates
		}

		switch {
		case height > lheight:
			lastheightUpdated = true

			return height, nil
		case lheight > old:
			return lheight, nil
		default:
			return old, util.ErrLockedSetIgnore
		}
	})

	return lastproofs, lastcandidates, lastheightUpdated, nodesUpdated
}

func (*LastConsensusNodesWatcher) compareProofs(a, b []base.SuffrageProof) []base.SuffrageProof {
	if len(b) < 1 {
		return nil
	}

	nb := b

	if len(a) > 0 {
		top := a[len(a)-1].Map().Manifest().Height()

		nb = util.FilterSlice(nb, func(i base.SuffrageProof) bool {
			return i.Map().Manifest().Height() > top
		})
	}

	if len(nb) < 1 {
		return nil
	}

	var newl3 []base.SuffrageProof

	switch {
	case len(nb) >= 3: //nolint:gomnd //...
		newl3 = nb[len(nb)-3:]
	case len(nb) < 3: //nolint:gomnd //...
		newl3 = nb

		for i := len(a) - 1; i >= 0; i-- {
			j := a[i]

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

func (*LastConsensusNodesWatcher) isNewCandiates(a, b base.State) bool {
	switch {
	case b == nil:
	case a == nil, b.Height() > a.Height():
		return true
	}

	return false
}

func (u *LastConsensusNodesWatcher) loadLast(
	lastHeight base.Height,
) (base.Height, []base.SuffrageProof, base.State, error) {
	height := lastHeight
	proofs, _ := u.last.Value()
	candidates := u.lastcandidates

	var localheight base.Height

	switch i, localproofs, localcandidates, found, err := u.getFromLocal(); {
	case err != nil:
		return height, nil, nil, err
	case !found:
		localheight = i
	default:
		localheight = i

		if n := u.compareProofs(proofs, []base.SuffrageProof{localproofs}); len(n) > 0 {
			proofs = []base.SuffrageProof{localproofs}
		}

		if u.isNewCandiates(candidates, localcandidates) {
			candidates = localcandidates
		}
	}

	if localheight > height {
		height = localheight
	}

	return height, proofs, candidates, nil
}

func IsNodeInLastConsensusNodes(
	node base.Node, proof base.SuffrageProof, candidatesst base.State,
) (base.Suffrage, bool, error) {
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

	if candidatesst == nil {
		return suf, false, nil
	}

	candidates, err := base.LoadNodesFromSuffrageCandidatesState(candidatesst)
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

func IsNodeAddressInLastConsensusNodes(
	node base.Address, proof base.SuffrageProof, candidatesst base.State,
) (base.Suffrage, bool, error) {
	if proof == nil {
		return nil, false, nil
	}

	suf, err := proof.Suffrage()
	if err != nil {
		return nil, false, err
	}

	if suf.Exists(node) {
		return suf, true, nil
	}

	if candidatesst == nil {
		return suf, false, nil
	}

	candidates, err := base.LoadNodesFromSuffrageCandidatesState(candidatesst)
	if err != nil {
		return suf, false, err
	}

	for i := range candidates {
		n := candidates[i]

		if n.Address().Equal(node) {
			return suf, true, nil
		}
	}

	return suf, false, nil
}
