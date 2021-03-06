package isaac

import (
	"sync"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/ballot"
	"github.com/spikeekips/mitum/base/operation"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util/valuehash"
)

type ProposalMaker struct {
	sync.Mutex
	local    *network.LocalNode
	storage  storage.Storage
	policy   *LocalPolicy
	proposed ballot.Proposal
}

func NewProposalMaker(
	local *network.LocalNode,
	st storage.Storage,
	policy *LocalPolicy,
) *ProposalMaker {
	return &ProposalMaker{local: local, storage: st, policy: policy}
}

func (pm *ProposalMaker) seals() ([]valuehash.Hash, error) {
	founds := map[ /* Operation.Fact().Hash() */ string]struct{}{}

	maxOperations := pm.policy.MaxOperationsInProposal()

	var facts int
	var seals, uselessSeals []valuehash.Hash
	if err := pm.storage.StagedOperationSeals(
		func(sl operation.Seal) (bool, error) {
			var ofs []valuehash.Hash
			for _, op := range sl.Operations() {
				fh := op.Fact().Hash()
				if _, found := founds[fh.String()]; found {
					continue
				} else if found, err := pm.storage.HasOperationFact(fh); err != nil {
					return false, err
				} else if found {
					continue
				}

				ofs = append(ofs, fh)
				if uint(facts+len(ofs)) > maxOperations {
					break
				}

				founds[fh.String()] = struct{}{}
			}

			switch {
			case uint(facts+len(ofs)) > maxOperations:
				return false, nil
			case len(ofs) > 0:
				facts += len(ofs)
				seals = append(seals, sl.Hash())
			default:
				uselessSeals = append(uselessSeals, sl.Hash())
			}

			return true, nil
		},
		true,
	); err != nil {
		return nil, err
	}

	if len(uselessSeals) > 0 {
		if err := pm.storage.UnstagedOperationSeals(uselessSeals); err != nil {
			return nil, err
		}
	}

	return seals, nil
}

func (pm *ProposalMaker) Proposal(
	height base.Height,
	round base.Round,
	voteproof base.Voteproof,
) (ballot.Proposal, error) {
	pm.Lock()
	defer pm.Unlock()

	if pm.proposed != nil {
		if pm.proposed.Height() == height && pm.proposed.Round() == round {
			return pm.proposed, nil
		}
	}

	seals, err := pm.seals()
	if err != nil {
		return nil, err
	}

	pr := ballot.NewProposalV0(
		pm.local.Address(),
		height,
		round,
		seals,
		voteproof,
	)
	if err := pr.Sign(pm.local.Privatekey(), pm.policy.NetworkID()); err != nil {
		return nil, err
	}

	pm.proposed = pr

	return pr, nil
}
