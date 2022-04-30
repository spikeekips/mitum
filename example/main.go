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

	dbroot, found := os.LookupEnv(envKeyFSRootf)
	if !found {
		dbroot = filepath.Join(os.TempDir(), "mitum-example-"+local.Address().String())
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

	g := launch.NewGenesisBlockGenerator(local, networkID, enc, db, pool, launch.DBRootDataDirectory(dbroot))
	_ = g.SetLogging(logging)
	if _, err := g.Generate(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

type runCommand struct {
	Address launch.AddressFlag `arg:"" name:"address" help:"node address"`
	perm    isaac.PermanentDatabase
	db      isaac.Database
	pool    *database.TempPool
}

func (cmd *runCommand) Run() error {
	encs, enc, err := launch.PrepareEncoders()
	if err != nil {
		return errors.Wrap(err, "")
	}

	local, err := prepareLocal(cmd.Address.Address())
	if err != nil {
		return errors.Wrap(err, "failed to prepare local")
	}

	dbroot, found := os.LookupEnv(envKeyFSRootf)
	if !found {
		dbroot = filepath.Join(os.TempDir(), "mitum-example-"+local.Address().String())
	}

	if err := cmd.prepareDatabase(dbroot, encs, enc); err != nil {
		return errors.Wrap(err, "")
	}

	nodePolicy := isaac.DefaultNodePolicy(networkID)
	log.Info().
		Interface("node_policy", nodePolicy).
		Msg("node policy loaded")

	getSuffrage := func(blockheight base.Height) (base.Suffrage, bool, error) {
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

	box := isaacstates.NewBallotbox(getSuffrage, nodePolicy.Threshold())

	getManifest := func(height base.Height) (base.Manifest, error) {
		switch m, found, err := cmd.db.Map(height); {
		case err != nil:
			return nil, errors.Wrap(err, "")
		case !found:
			return nil, nil
		default:
			return m.Manifest(), nil
		}
	}

	proposalMaker := isaac.NewProposalMaker(
		local,
		nodePolicy,
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

	proposerSelector := isaac.NewBaseProposalSelector(
		local,
		nodePolicy,
		isaac.NewBlockBasedProposerSelector(
			func(height base.Height) (util.Hash, error) {
				switch m, err := getManifest(height); {
				case err != nil:
					return nil, errors.Wrap(err, "")
				case m == nil:
					return nil, nil
				default:
					return m.Hash(), nil
				}
			},
		),
		proposalMaker,
		getSuffrage,
		func() []base.Address { return nil },
		func(context.Context, base.Point, base.Address) (
			base.ProposalSignedFact, error,
		) {
			// BLOCK set request
			return nil, nil
		},
		cmd.pool,
	)

	getLastManifest := func() (base.Manifest, bool, error) {
		switch m, found, err := cmd.db.LastMap(); {
		case err != nil || !found:
			return nil, found, err
		default:
			return m.Manifest(), true, nil
		}
	}

	voteFunc := func(bl base.Ballot) (bool, error) {
		voted, err := box.Vote(bl)
		if err != nil {
			return false, errors.Wrap(err, "")
		}

		return voted, nil
	}

	newProposalProcessor := func(proposal base.ProposalSignedFact, previous base.Manifest) (
		isaac.ProposalProcessor, error,
	) {
		return isaac.NewDefaultProposalProcessor(
			proposal,
			previous,
			launch.NewBlockDataWriterFunc(local, networkID, launch.DBRootDataDirectory(dbroot), enc, cmd.db),
			cmd.db.State,
			nil,
			nil,
			cmd.pool.SetLastVoteproofs,
		)
	}

	getProposal := func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
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

	pps := isaac.NewProposalProcessors(newProposalProcessor, getProposal)
	_ = pps.SetLogging(logging)

	states := isaacstates.NewStates(box)
	_ = states.SetLogging(logging)

	states.
		SetHandler(isaacstates.NewBrokenHandler(local, nodePolicy)).
		SetHandler(isaacstates.NewStoppedHandler(local, nodePolicy)).
		SetHandler(isaacstates.NewBootingHandler(local, nodePolicy, getLastManifest, getSuffrage)).
		SetHandler(
			isaacstates.NewJoiningHandler(local, nodePolicy, proposerSelector, getLastManifest, getSuffrage, voteFunc)).
		SetHandler(
			isaacstates.NewConsensusHandler(
				local, nodePolicy, proposerSelector, getManifest, getSuffrage, voteFunc, pps,
			)).
		SetHandler(isaacstates.NewSyncingHandler(local, nodePolicy, proposerSelector, nil))

	// NOTE load last init, accept voteproof and last majority voteproof
	switch ivp, avp, found, err := cmd.pool.LastVoteproofs(); {
	case err != nil:
		return errors.Wrap(err, "")
	case !found:
	default:
		_ = states.LastVoteproofsHandler().Set(ivp)
		_ = states.LastVoteproofsHandler().Set(avp)
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
