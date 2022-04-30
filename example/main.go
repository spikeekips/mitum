package main

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/isaac/blockdata"
	"github.com/spikeekips/mitum/isaac/database"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	mitumlogging "github.com/spikeekips/mitum/util/logging"
)

var (
	networkID     = base.NetworkID([]byte("mitum-example-node"))
	envKeyFSRootf = "MITUM_FS_ROOT"
)

var (
	logging *mitumlogging.Logging
	log     *zerolog.Logger
)

func init() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
}

type newProposalProcessorFunc func(proposal base.ProposalSignedFact, previous base.Manifest) (
	isaac.ProposalProcessor, error)

func main() {
	logging = mitumlogging.Setup(os.Stderr, zerolog.DebugLevel, "json", false)
	log = mitumlogging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
		return lctx.Str("module", "main")
	}).SetLogging(logging).Log()

	var cli struct {
		Init initCommand `cmd:"" help:"init node"`
		Run  runCommand  `cmd:"" help:"run node"`
	}

	kctx := kong.Parse(&cli)

	log.Info().Str("command", kctx.Command()).Msg("start command")

	if err := func() error {
		defer log.Info().Msg("stopped")

		return kctx.Run()
	}(); err != nil {
		log.Error().Err(err).Msg("stopped by error")

		kctx.FatalIfErrorf(err)
	}
}

type initCommand struct {
	Address launch.AddressFlag `arg:"" name:"address" help:"node address"`
	local   base.LocalNode
}

func (cmd *initCommand) Run() error {
	encs, enc, err := launch.PrepareEncoders()
	if err != nil {
		return errors.Wrap(err, "")
	}

	local, err := prepareLocal(cmd.Address.Address())
	if err != nil {
		return errors.Wrap(err, "failed to prepare local")
	}
	cmd.local = local

	dbroot, found := os.LookupEnv(envKeyFSRootf)
	if !found {
		dbroot = filepath.Join(os.TempDir(), "mitum-example-"+cmd.local.Address().String())
	}

	if err = launch.CleanDatabase(dbroot); err != nil {
		return errors.Wrap(err, "")
	}

	if err = launch.InitializeDatabase(dbroot); err != nil {
		return errors.Wrap(err, "")
	}

	perm, err := loadPermanentDatabase(launch.DBRootPermDirectory(dbroot), encs, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	db, pool, err := launch.PrepareDatabase(perm, dbroot, encs, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	g := launch.NewGenesisBlockGenerator(cmd.local, networkID, enc, db, pool, launch.DBRootDataDirectory(dbroot))
	_ = g.SetLogging(logging)
	if _, err := g.Generate(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

type runCommand struct {
	Address              launch.AddressFlag `arg:"" name:"address" help:"node address"`
	local                base.LocalNode
	nodePolicy           isaac.NodePolicy
	db                   isaac.Database
	perm                 isaac.PermanentDatabase
	pool                 *database.TempPool
	getSuffrage          func(blockheight base.Height) (base.Suffrage, bool, error)
	getManifest          func(height base.Height) (base.Manifest, error)
	proposalSelector     *isaac.BaseProposalSelector
	getLastManifest      func() (base.Manifest, bool, error)
	newProposalProcessor newProposalProcessorFunc
	getProposal          func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error)
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

	dbroot, found := os.LookupEnv(envKeyFSRootf)
	if !found {
		dbroot = filepath.Join(os.TempDir(), "mitum-example-"+cmd.local.Address().String())
	}

	if err = cmd.prepareDatabase(dbroot, encs, enc); err != nil {
		return errors.Wrap(err, "")
	}

	cmd.nodePolicy = isaac.DefaultNodePolicy(networkID)
	log.Info().
		Interface("node_policy", cmd.nodePolicy).
		Msg("node policy loaded")

	cmd.getSuffrage = cmd.getSuffrageFunc()
	cmd.getManifest = cmd.getManifestFunc()
	cmd.proposalSelector = cmd.proposalSelectorFunc()
	cmd.getLastManifest = cmd.getLastManifestFunc()
	cmd.newProposalProcessor = cmd.newProposalProcessorFunc(dbroot, enc)
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

func (cmd *runCommand) prepareDatabase(dbroot string, encs *encoder.Encoders, enc encoder.Encoder) error {
	if err := launch.CheckDatabase(dbroot); err != nil {
		return errors.Wrap(err, "")
	}

	if err := blockdata.CleanBlockDataTempDirectory(launch.DBRootDataDirectory(dbroot)); err != nil {
		return errors.Wrap(err, "")
	}

	perm, err := loadPermanentDatabase(launch.DBRootPermDirectory(dbroot), encs, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}
	cmd.perm = perm

	db, pool, err := launch.PrepareDatabase(perm, dbroot, encs, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}
	_ = db.SetLogging(logging)

	cmd.db = db
	cmd.pool = pool

	if err := db.Start(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (cmd *runCommand) getSuffrageFunc() func(blockheight base.Height) (base.Suffrage, bool, error) {
	return func(blockheight base.Height) (base.Suffrage, bool, error) {
		st, found, err := cmd.db.Suffrage(blockheight.Prev())
		switch {
		case err != nil:
			return nil, false, errors.Wrap(err, "")
		case !found:
			return nil, false, nil
		}

		suf, err := isaac.NewSuffrage(st.Value().(base.SuffrageStateValue).Nodes())
		if err != nil {
			return nil, true, errors.Wrap(err, "")
		}

		return suf, true, nil
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
			// BLOCK set request
			return nil, nil
		},
		cmd.pool,
	)
}

func (cmd *runCommand) getLastManifestFunc() func() (base.Manifest, bool, error) {
	return func() (base.Manifest, bool, error) {
		switch m, found, err := cmd.db.LastMap(); {
		case err != nil || !found:
			return nil, found, err
		default:
			return m.Manifest(), true, nil
		}
	}
}

func (cmd *runCommand) newProposalProcessorFunc(dbroot string, enc encoder.Encoder) newProposalProcessorFunc {
	return func(proposal base.ProposalSignedFact, previous base.Manifest) (
		isaac.ProposalProcessor, error,
	) {
		return isaac.NewDefaultProposalProcessor(
			proposal,
			previous,
			launch.NewBlockDataWriterFunc(cmd.local, networkID, launch.DBRootDataDirectory(dbroot), enc, cmd.db),
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
				cmd.local, cmd.nodePolicy, cmd.proposalSelector, cmd.getManifest, cmd.getSuffrage, voteFunc, pps,
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
			// BLOCK if not found, request to remote node
			return nil, nil
		default:
			return pr, nil
		}
	}
}

func prepareLocal(address base.Address) (base.LocalNode, error) {
	// NOTE make privatekey, based on node address
	b := make([]byte, base.PrivatekeyMinSeedSize)
	copy(b, address.Bytes())

	priv, err := base.NewMPrivatekeyFromSeed(string(b))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create privatekey from node address")
	}

	log.Info().
		Stringer("address", address).
		Stringer("privatekey", priv).
		Stringer("publickey", priv.Publickey()).
		Msg("keypair generated")

	return isaac.NewLocalNode(priv, address), nil
}

func loadPermanentDatabase(_ string, encs *encoder.Encoders, enc encoder.Encoder) (isaac.PermanentDatabase, error) {
	// uri := launch.DBRootPermDirectory(dbroot)
	uri := "redis://"

	return launch.LoadPermanentDatabase(uri, encs, enc)
}
