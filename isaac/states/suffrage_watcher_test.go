package isaacstates

import (
	"context"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testLastSuffrageProofWatcher struct {
	baseTestSuffrageStateBuilder
}

func (t *testLastSuffrageProofWatcher) TestLocalAhead() {
	proofs := t.newProofs(2)

	localproof := proofs[1]
	remoteproof := proofs[0]

	localcandidates := []base.SuffrageCandidate{
		isaac.NewSuffrageCandidate(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+3,
		),
		isaac.NewSuffrageCandidate(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+5,
		),
	}

	localstv := isaac.NewSuffrageCandidateStateValue(localcandidates)
	localcandidatest := base.NewBaseState(
		localproof.Map().Manifest().Height(),
		isaac.SuffrageCandidateStateKey,
		localstv,
		nil,
		[]util.Hash{valuehash.RandomSHA256()},
	)

	remotestv := isaac.NewSuffrageCandidateStateValue([]base.SuffrageCandidate{
		isaac.NewSuffrageCandidate(
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
	called := make(chan struct{}, 2)

	u := isaac.NewLastConsensusNodesWatcher(
		func() (base.SuffrageProof, base.State, bool, error) {
			return localproof, localcandidatest, true, nil
		},
		func(context.Context, base.State) (base.SuffrageProof, base.State, error) {
			called <- struct{}{}

			return remoteproof, remotecandidatest, nil
		},
		func(context.Context, base.SuffrageProof, base.State) {
			updatedch <- struct{}{}
		},
	)
	t.NoError(u.Start())
	defer u.Stop()

	<-called
	<-updatedch

	proof, candidatest, err := u.Last()
	t.NoError(err)

	base.EqualSuffrageProof(t.Assert(), localproof, proof)

	nodes := candidatest.Value().(base.SuffrageCandidateStateValue).Nodes()
	t.Equal(len(localcandidates), len(nodes))

	for i := range localcandidates {
		base.EqualSuffrageCandidate(t.Assert(), localcandidates[i], nodes[i])
	}
}

func (t *testLastSuffrageProofWatcher) TestRemoteAhead() {
	proofs := t.newProofs(2)

	localproof := proofs[0]
	remoteproof := proofs[1]

	localstv := isaac.NewSuffrageCandidateStateValue([]base.SuffrageCandidate{
		isaac.NewSuffrageCandidate(
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

	remotecandidates := []base.SuffrageCandidate{
		isaac.NewSuffrageCandidate(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+3,
		),
		isaac.NewSuffrageCandidate(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+5,
		),
	}

	remotestv := isaac.NewSuffrageCandidateStateValue(remotecandidates)
	remotecandidatest := base.NewBaseState(
		remoteproof.Map().Manifest().Height(),
		isaac.SuffrageCandidateStateKey,
		remotestv,
		nil,
		[]util.Hash{valuehash.RandomSHA256()},
	)

	updatedch := make(chan struct{})
	called := make(chan struct{}, 2)

	u := isaac.NewLastConsensusNodesWatcher(
		func() (base.SuffrageProof, base.State, bool, error) {
			return localproof, localcandidatest, true, nil
		},
		func(context.Context, base.State) (base.SuffrageProof, base.State, error) {
			called <- struct{}{}

			return remoteproof, remotecandidatest, nil
		},
		func(context.Context, base.SuffrageProof, base.State) {
			updatedch <- struct{}{}
		},
	)
	t.NoError(u.Start())
	defer u.Stop()

	<-called
	<-updatedch

	proof, candidatest, err := u.Last()
	t.NoError(err)

	base.EqualSuffrageProof(t.Assert(), remoteproof, proof)

	nodes := candidatest.Value().(base.SuffrageCandidateStateValue).Nodes()
	t.Equal(len(remotecandidates), len(nodes))

	for i := range remotecandidates {
		base.EqualSuffrageCandidate(t.Assert(), remotecandidates[i], nodes[i])
	}
}

func (t *testLastSuffrageProofWatcher) TestSameButLocalFirst() {
	localproofs := t.newProofs(2)
	remoteproofs := t.newProofs(2)

	localproof := localproofs[1]
	remoteproof := remoteproofs[1]

	localcandidates := []base.SuffrageCandidate{
		isaac.NewSuffrageCandidate(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+3,
		),
		isaac.NewSuffrageCandidate(
			base.RandomNode(),
			localproof.Map().Manifest().Height()+1,
			localproof.Map().Manifest().Height()+5,
		),
	}

	localstv := isaac.NewSuffrageCandidateStateValue(localcandidates)
	localcandidatest := base.NewBaseState(
		localproof.Map().Manifest().Height(),
		isaac.SuffrageCandidateStateKey,
		localstv,
		nil,
		[]util.Hash{valuehash.RandomSHA256()},
	)

	remotestv := isaac.NewSuffrageCandidateStateValue([]base.SuffrageCandidate{
		isaac.NewSuffrageCandidate(
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
	called := make(chan struct{}, 2)

	u := isaac.NewLastConsensusNodesWatcher(
		func() (base.SuffrageProof, base.State, bool, error) {
			return localproof, localcandidatest, true, nil
		},
		func(context.Context, base.State) (base.SuffrageProof, base.State, error) {
			called <- struct{}{}

			return remoteproof, remotecandidatest, nil
		},
		func(context.Context, base.SuffrageProof, base.State) {
			updatedch <- struct{}{}
		},
	)
	t.NoError(u.Start())
	defer u.Stop()

	<-called
	<-updatedch

	proof, candidatest, err := u.Last()
	t.NoError(err)

	base.EqualSuffrageProof(t.Assert(), localproof, proof)

	nodes := candidatest.Value().(base.SuffrageCandidateStateValue).Nodes()
	t.Equal(len(localcandidates), len(nodes))

	for i := range localcandidates {
		base.EqualSuffrageCandidate(t.Assert(), localcandidates[i], nodes[i])
	}
}

func TestLastSuffrageProofWatcher(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testLastSuffrageProofWatcher))
}
