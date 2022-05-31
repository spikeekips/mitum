package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type runCommand struct {
	Address              launch.AddressFlag `arg:"" name:"local address" help:"node address"`
	local                base.LocalNode
	db                   isaac.Database
	perm                 isaac.PermanentDatabase
	proposalSelector     *isaac.BaseProposalSelector
	pool                 *isaacdatabase.TempPool
	getSuffrage          func(blockheight base.Height) (base.Suffrage, bool, error)
	getSuffrageBooting   func(blockheight base.Height) (base.Suffrage, bool, error)
	getManifest          func(height base.Height) (base.Manifest, error)
	getLastManifest      func() (base.Manifest, bool, error)
	newProposalProcessor newProposalProcessorFunc
	getProposal          func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error)
	nodePolicy           isaac.NodePolicy
}

func (cmd *runCommand) Run() error {
	encs, enc, err := launch.PrepareEncoders()
	if err != nil {
		return errors.Wrap(err, "")
	}

	local, err := prepareLocal(cmd.Address.Address())
	if err != nil {
		return errors.Wrap(err, "failed to prepare cmd.local")
	}

	cmd.local = local

	localfsroot := defaultLocalFSRoot(cmd.local.Address())

	if err = cmd.prepareDatabase(localfsroot, encs, enc); err != nil {
		return errors.Wrap(err, "")
	}

	cmd.nodePolicy = isaac.DefaultNodePolicy(networkID)
	log.Info().
		Interface("node_policy", cmd.nodePolicy).
		Msg("node policy loaded")

	// FIXME implement isaacstates.NewSuffrageStateBuilder(cmd.nodePolicy.NetworkID(), )

	cmd.getSuffrage = cmd.getSuffrageFunc()
	// FIXME cmd.getSuffrageBooting   func(blockheight base.Height) (base.Suffrage, bool, error) {
	//}
	cmd.getManifest = cmd.getManifestFunc()
	cmd.proposalSelector = cmd.proposalSelectorFunc()
	cmd.getLastManifest = cmd.getLastManifestFunc()
	cmd.newProposalProcessor = cmd.newProposalProcessorFunc(localfsroot, enc)
	cmd.getProposal = cmd.getProposalFunc()

	states, err := cmd.states()
	if err != nil {
		return errors.Wrap(err, "")
	}

	log.Debug().Msg("states loaded")

	if err := <-states.Wait(context.Background()); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (cmd *runCommand) prepareDatabase(localfsroot string, encs *encoder.Encoders, enc encoder.Encoder) error {
	e := util.StringErrorFunc("failed to prepare database")

	nodeinfo, err := launch.CheckLocalFS(networkID, localfsroot, enc)
	if err != nil {
		return e(err, "")
	}

	if err = isaacblock.CleanBlockTempDirectory(launch.LocalFSDataDirectory(localfsroot)); err != nil {
		return e(err, "")
	}

	db, perm, pool, err := launch.LoadDatabase(nodeinfo, defaultPermanentDatabaseURI(), localfsroot, encs, enc)
	if err != nil {
		return e(err, "")
	}

	_ = db.SetLogging(logging)

	if err := db.Start(); err != nil {
		return e(err, "")
	}

	cmd.db = db
	cmd.perm = perm
	cmd.pool = pool

	return nil
}

func (cmd *runCommand) getSuffrageFunc() func(blockheight base.Height) (base.Suffrage, bool, error) {
	return func(blockheight base.Height) (base.Suffrage, bool, error) {
		proof, found, err := cmd.db.SuffrageProofByBlockHeight(blockheight.Prev())

		switch {
		case err != nil:
			return nil, false, errors.Wrap(err, "")
		case !found:
			return nil, false, nil
		default:
			suf, err := proof.Suffrage()
			if err != nil {
				return nil, true, errors.Wrap(err, "")
			}

			return suf, true, nil
		}
	}
}

func (cmd *runCommand) getManifestFunc() func(height base.Height) (base.Manifest, error) {
	return func(height base.Height) (base.Manifest, error) {
		switch m, found, err := cmd.db.Map(height); {
		case err != nil:
			return nil, errors.Wrap(err, "")
		case !found:
			return nil, nil
		default:
			return m.Manifest(), nil
		}
	}
}

func (cmd *runCommand) proposalMaker() *isaac.ProposalMaker {
	return isaac.NewProposalMaker(
		cmd.local,
		cmd.nodePolicy,
		func(ctx context.Context) ([]util.Hash, error) {
			policy := cmd.db.LastNetworkPolicy()
			n := policy.MaxOperationsInProposal()
			if n < 1 {
				return nil, nil
			}

			hs, err := cmd.pool.NewOperationHashes(
				ctx,
				n,
				func(facthash util.Hash) (bool, error) {
					// FIXME if bad operation and it is failed to be processed;
					// it can be included in next proposal; it should be
					// excluded.
					// FIXME if operation has not enough fact signs, it will
					// ignored. It must be filtered for not this kind of
					// operations.
					switch found, err := cmd.db.ExistsInStateOperation(facthash); {
					case err != nil:
						return false, errors.Wrap(err, "")
					case !found:
						return false, nil
					}

					return true, nil
				},
			)
			if err != nil {
				return nil, errors.Wrap(err, "")
			}

			return hs, nil
		},
		cmd.pool,
	)
}

func (cmd *runCommand) proposalSelectorFunc() *isaac.BaseProposalSelector {
	return isaac.NewBaseProposalSelector(
		cmd.local,
		cmd.nodePolicy,
		isaac.NewBlockBasedProposerSelector(
			func(height base.Height) (util.Hash, error) {
				switch m, err := cmd.getManifest(height); {
				case err != nil:
					return nil, errors.Wrap(err, "")
				case m == nil:
					return nil, nil
				default:
					return m.Hash(), nil
				}
			},
		),
		cmd.proposalMaker(),
		cmd.getSuffrage,
		func() []base.Address { return nil },
		func(context.Context, base.Point, base.Address) (
			base.ProposalSignedFact, error,
		) {
			// FIXME set request
			return nil, nil
		},
		cmd.pool,
	)
}

func (cmd *runCommand) getLastManifestFunc() func() (base.Manifest, bool, error) {
	return func() (base.Manifest, bool, error) {
		switch m, found, err := cmd.db.LastMap(); {
		case err != nil || !found:
			return nil, found, errors.Wrap(err, "")
		default:
			return m.Manifest(), true, nil
		}
	}
}

func (cmd *runCommand) newProposalProcessorFunc(localfsroot string, enc encoder.Encoder) newProposalProcessorFunc {
	return func(proposal base.ProposalSignedFact, previous base.Manifest) (
		isaac.ProposalProcessor, error,
	) {
		return isaac.NewDefaultProposalProcessor(
			proposal,
			previous,
			launch.NewBlockWriterFunc(cmd.local, networkID, launch.LocalFSDataDirectory(localfsroot), enc, cmd.db),
			cmd.db.State,
			nil,
			nil,
			cmd.pool.SetLastVoteproofs,
		)
	}
}

func (cmd *runCommand) states() (*isaacstates.States, error) {
	box := isaacstates.NewBallotbox(cmd.getSuffrage, cmd.nodePolicy.Threshold())
	voteFunc := func(bl base.Ballot) (bool, error) {
		voted, err := box.Vote(bl)
		if err != nil {
			return false, errors.Wrap(err, "")
		}

		return voted, nil
	}

	pps := isaac.NewProposalProcessors(cmd.newProposalProcessor, cmd.getProposal)
	_ = pps.SetLogging(logging)

	states := isaacstates.NewStates(box)
	_ = states.SetLogging(logging)

	whenNewBlockSaved := func(height base.Height) {
		box.Count()
	}

	states.
		SetHandler(isaacstates.NewBrokenHandler(cmd.local, cmd.nodePolicy)).
		SetHandler(isaacstates.NewStoppedHandler(cmd.local, cmd.nodePolicy)).
		SetHandler(isaacstates.NewBootingHandler(cmd.local, cmd.nodePolicy, cmd.getLastManifest, cmd.getSuffrage)).
		SetHandler(
			isaacstates.NewJoiningHandler(
				cmd.local, cmd.nodePolicy, cmd.proposalSelector, cmd.getLastManifest, cmd.getSuffrage, voteFunc,
			),
		).
		SetHandler(
			isaacstates.NewConsensusHandler(
				cmd.local, cmd.nodePolicy, cmd.proposalSelector,
				cmd.getManifest, cmd.getSuffrage, voteFunc, whenNewBlockSaved,
				pps,
			)).
		SetHandler(isaacstates.NewSyncingHandler(cmd.local, cmd.nodePolicy, cmd.proposalSelector, nil))

	// NOTE load last init, accept voteproof and last majority voteproof
	switch ivp, avp, found, err := cmd.pool.LastVoteproofs(); {
	case err != nil:
		return nil, errors.Wrap(err, "")
	case !found:
	default:
		_ = states.LastVoteproofsHandler().Set(ivp)
		_ = states.LastVoteproofsHandler().Set(avp)
	}

	return states, nil
}

func (cmd *runCommand) getProposalFunc() func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
	return func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
		switch pr, found, err := cmd.pool.Proposal(facthash); {
		case err != nil:
			return nil, errors.Wrap(err, "")
		case !found:
			// FIXME if not found, request to remote node
			return nil, nil
		default:
			return pr, nil
		}
	}
}
