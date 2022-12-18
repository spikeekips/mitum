package isaacstates

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testLastConsensusNodesWatcher struct {
	baseTestSuffrageStateBuilder
}

func (t *testLastConsensusNodesWatcher) TestLocalAhead() {
	proofs := t.newProofs(2)

	localproof := proofs[1]
	remoteproof := proofs[0]

	localcandidates := []base.SuffrageCandidateStateValue{
		isaac.NewSuffrageCandidateStateValue(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+3,
		),
		isaac.NewSuffrageCandidateStateValue(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+5,
		),
	}

	localstv := isaac.NewSuffrageCandidatesStateValue(localcandidates)
	localcandidatest := base.NewBaseState(
		localproof.Map().Manifest().Height(),
		isaac.SuffrageCandidateStateKey,
		localstv,
		nil,
		[]util.Hash{valuehash.RandomSHA256()},
	)

	remotecandidates := []base.SuffrageCandidateStateValue{
		isaac.NewSuffrageCandidateStateValue(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+3,
		),
		isaac.NewSuffrageCandidateStateValue(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+5,
		),
	}

	remotestv := isaac.NewSuffrageCandidatesStateValue(remotecandidates)
	remotecandidatest := base.NewBaseState(
		localproof.Map().Manifest().Height()+1,
		isaac.SuffrageCandidateStateKey,
		remotestv,
		nil,
		[]util.Hash{valuehash.RandomSHA256()},
	)

	updatedch := make(chan struct{})
	called := make(chan struct{}, 2)

	u, err := isaac.NewLastConsensusNodesWatcher(
		func() (base.Height, base.SuffrageProof, base.State, bool, error) {
			return localproof.Map().Manifest().Height(), localproof, localcandidatest, true, nil
		},
		func(context.Context, base.State) (base.Height, []base.SuffrageProof, base.State, error) {
			called <- struct{}{}

			return remoteproof.Map().Manifest().Height(), []base.SuffrageProof{remoteproof}, remotecandidatest, nil
		},
		func(_ context.Context, _ base.SuffrageProof, proof base.SuffrageProof, _ base.State) {
			updatedch <- struct{}{}
		},
		time.Millisecond*300,
	)
	t.NoError(err)

	t.NoError(u.Start())
	defer u.Stop()

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("failed to get from remote"))
	case <-called:
	}

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("failed to update"))
	case <-updatedch:
	}

	proof, candidatest, err := u.Last()
	t.NoError(err)

	base.EqualSuffrageProof(t.Assert(), localproof, proof)

	nodes := candidatest.Value().(base.SuffrageCandidatesStateValue).Nodes()
	t.Equal(len(localcandidates), len(nodes))

	for i := range remotecandidates {
		base.EqualSuffrageCandidateStateValue(t.Assert(), remotecandidates[i], nodes[i])
	}
}

func (t *testLastConsensusNodesWatcher) TestRemoteAhead() {
	proofs := t.newProofs(2)

	localproof := proofs[0]
	remoteproof := proofs[1]

	localstv := isaac.NewSuffrageCandidatesStateValue([]base.SuffrageCandidateStateValue{
		isaac.NewSuffrageCandidateStateValue(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+3,
		),
	})
	localcandidatest := base.NewBaseState(
		localproof.Map().Manifest().Height(),
		isaac.SuffrageCandidateStateKey,
		localstv,
		nil,
		[]util.Hash{valuehash.RandomSHA256()},
	)

	remotecandidates := []base.SuffrageCandidateStateValue{
		isaac.NewSuffrageCandidateStateValue(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+3,
		),
		isaac.NewSuffrageCandidateStateValue(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+5,
		),
	}

	remotestv := isaac.NewSuffrageCandidatesStateValue(remotecandidates)
	remotecandidatest := base.NewBaseState(
		remoteproof.Map().Manifest().Height(),
		isaac.SuffrageCandidateStateKey,
		remotestv,
		nil,
		[]util.Hash{valuehash.RandomSHA256()},
	)

	updatedch := make(chan struct{})
	called := make(chan struct{}, 2)

	u, err := isaac.NewLastConsensusNodesWatcher(
		func() (base.Height, base.SuffrageProof, base.State, bool, error) {
			return localproof.Map().Manifest().Height(), localproof, localcandidatest, true, nil
		},
		func(context.Context, base.State) (base.Height, []base.SuffrageProof, base.State, error) {
			called <- struct{}{}

			return remoteproof.Map().Manifest().Height(), []base.SuffrageProof{remoteproof}, remotecandidatest, nil
		},
		func(_ context.Context, _ base.SuffrageProof, proof base.SuffrageProof, _ base.State) {
			if proof != nil {
				updatedch <- struct{}{}
			}
		},
		time.Millisecond*300,
	)
	t.NoError(err)

	t.NoError(u.Start())
	defer u.Stop()

	<-called
	<-updatedch

	proof, candidatest, err := u.Last()
	t.NoError(err)

	base.EqualSuffrageProof(t.Assert(), remoteproof, proof)

	nodes := candidatest.Value().(base.SuffrageCandidatesStateValue).Nodes()
	t.Equal(len(remotecandidates), len(nodes))

	for i := range remotecandidates {
		base.EqualSuffrageCandidateStateValue(t.Assert(), remotecandidates[i], nodes[i])
	}
}

func (t *testLastConsensusNodesWatcher) TestSameButLocalFirst() {
	localproofs := t.newProofs(2)
	remoteproofs := t.newProofs(2)

	localproof := localproofs[1]
	remoteproof := remoteproofs[1]

	localcandidates := []base.SuffrageCandidateStateValue{
		isaac.NewSuffrageCandidateStateValue(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+3,
		),
		isaac.NewSuffrageCandidateStateValue(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+5,
		),
	}

	localstv := isaac.NewSuffrageCandidatesStateValue(localcandidates)
	localcandidatest := base.NewBaseState(
		localproof.Map().Manifest().Height(),
		isaac.SuffrageCandidateStateKey,
		localstv,
		nil,
		[]util.Hash{valuehash.RandomSHA256()},
	)

	remotestv := isaac.NewSuffrageCandidatesStateValue([]base.SuffrageCandidateStateValue{
		isaac.NewSuffrageCandidateStateValue(
			base.RandomNode(),
			remoteproof.Map().Manifest().Height()+1,
			remoteproof.Map().Manifest().Height()+3,
		),
	})
	remotecandidatest := base.NewBaseState(
		remoteproof.Map().Manifest().Height(),
		isaac.SuffrageCandidateStateKey,
		remotestv,
		nil,
		[]util.Hash{valuehash.RandomSHA256()},
	)

	updatedch := make(chan struct{})
	called := make(chan struct{}, 33)

	u, err := isaac.NewLastConsensusNodesWatcher(
		func() (base.Height, base.SuffrageProof, base.State, bool, error) {
			return localproof.Map().Manifest().Height(), localproof, localcandidatest, true, nil
		},
		func(context.Context, base.State) (base.Height, []base.SuffrageProof, base.State, error) {
			called <- struct{}{}

			return remoteproof.Map().Manifest().Height(), []base.SuffrageProof{remoteproof}, remotecandidatest, nil
		},
		func(_ context.Context, _ base.SuffrageProof, proof base.SuffrageProof, _ base.State) {
			if proof != nil {
				updatedch <- struct{}{}
			}
		},
		time.Millisecond*300,
	)
	t.NoError(err)

	t.NoError(u.Start())
	defer u.Stop()

	<-called

	select {
	case <-time.After(time.Second):
	case <-updatedch:
		t.NoError(errors.Errorf("update, unexpected"))
	}

	proof, candidatest, err := u.Last()
	t.NoError(err)

	base.EqualSuffrageProof(t.Assert(), localproof, proof)

	nodes := candidatest.Value().(base.SuffrageCandidatesStateValue).Nodes()
	t.Equal(len(localcandidates), len(nodes))

	for i := range localcandidates {
		base.EqualSuffrageCandidateStateValue(t.Assert(), localcandidates[i], nodes[i])
	}
}

func (t *testLastConsensusNodesWatcher) TestLast3() {
	p := t.newProofs(7)
	proofs := make([]base.SuffrageProof, len(p))

	for i := range p {
		j := p[i]

		proofs[j.Map().Manifest().Height()] = j
	}

	localproof := proofs[0]

	var remoteproofs0, remoteproofs1 []base.SuffrageProof
	for i := range proofs {
		switch {
		case i < 2:
			continue
		case len(remoteproofs0) < 3:
			remoteproofs0 = append(remoteproofs0, proofs[i])
		default:
			remoteproofs1 = append(remoteproofs1, proofs[i])
		}
	}

	lastheight0 := remoteproofs0[len(remoteproofs0)-1].Map().Manifest().Height()
	lastheight1 := remoteproofs1[len(remoteproofs1)-1].Map().Manifest().Height()

	called := make(chan base.Height, 2)
	updatedch := make(chan base.Height)

	u, err := isaac.NewLastConsensusNodesWatcher(
		func() (base.Height, base.SuffrageProof, base.State, bool, error) {
			return localproof.Map().Manifest().Height(), localproof, nil, true, nil
		},
		func(context.Context, base.State) (base.Height, []base.SuffrageProof, base.State, error) {
			switch height := <-called; {
			case height == lastheight0:
				return lastheight0, remoteproofs0, nil, nil
			case height == lastheight1:
				return lastheight1, remoteproofs1, nil, nil
			default:
				return base.NilHeight, nil, nil, errors.Errorf("findme")
			}
		},
		func(_ context.Context, _ base.SuffrageProof, proof base.SuffrageProof, _ base.State) {
			if proof == nil {
				return
			}

			updatedch <- proof.Map().Manifest().Height()
		},
		time.Millisecond*300,
	)
	t.NoError(err)

	called <- lastheight0

	t.NoError(u.Start())
	defer u.Stop()

	called <- lastheight0

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to wait new proofs"))
	case height := <-updatedch:
		t.Equal(lastheight0, height)
	}

	t.T().Log("remote updated:", len(remoteproofs0))

	for i := range remoteproofs0 {
		p := remoteproofs0[i]

		rsuf, found, err := u.GetSuffrage(p.Map().Manifest().Height())
		t.NoError(err)
		t.True(found)

		suf, err := p.Suffrage()
		t.NoError(err)

		nodes := suf.Nodes()

		for j := range nodes {
			t.True(rsuf.Exists(nodes[j].Address()))
		}
	}

	for i := range remoteproofs1 {
		p := remoteproofs1[i]

		_, found, err := u.GetSuffrage(p.Map().Manifest().Height())
		t.NoError(err)
		t.False(found)
	}

	t.T().Log("new remote updated:", len(remoteproofs1))
	called <- lastheight1

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to wait new proofs"))
	case height := <-updatedch:
		t.Equal(lastheight1, height)
	}

	for i := range remoteproofs1 {
		p := remoteproofs1[i]

		rsuf, found, err := u.GetSuffrage(p.Map().Manifest().Height())
		t.NoError(err)
		t.True(found)

		suf, err := p.Suffrage()
		t.NoError(err)

		nodes := suf.Nodes()

		for j := range nodes {
			t.True(rsuf.Exists(nodes[j].Address()))
		}
	}

	for i := range remoteproofs0[2:] { // NOTE last item of remoteproofs0
		p := remoteproofs0[2:][i]

		rsuf, found, err := u.GetSuffrage(p.Map().Manifest().Height())
		t.NoError(err)
		t.True(found)

		suf, err := p.Suffrage()
		t.NoError(err)

		nodes := suf.Nodes()

		for j := range nodes {
			t.True(rsuf.Exists(nodes[j].Address()))
		}
	}
}

func (t *testLastConsensusNodesWatcher) TestLast3FromLocal() {
	proofs := t.newProofs(1)

	localproof := proofs[0]

	u, err := isaac.NewLastConsensusNodesWatcher(
		func() (base.Height, base.SuffrageProof, base.State, bool, error) {
			return localproof.Map().Manifest().Height(), localproof, nil, true, nil
		},
		func(context.Context, base.State) (base.Height, []base.SuffrageProof, base.State, error) {
			return base.NilHeight, nil, nil, errors.Errorf("findme")
		},
		func(_ context.Context, _ base.SuffrageProof, proof base.SuffrageProof, _ base.State) {},
		time.Millisecond*300,
	)
	t.NoError(err)

	rsuf, found, err := u.GetSuffrage(localproof.Map().Manifest().Height())
	t.NoError(err)
	t.True(found)

	suf, err := localproof.Suffrage()
	t.NoError(err)

	nodes := suf.Nodes()

	for j := range nodes {
		t.True(rsuf.Exists(nodes[j].Address()))
	}
}

func TestLastConsensusNodesWatcher(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testLastConsensusNodesWatcher))
}
