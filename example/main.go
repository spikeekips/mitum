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
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
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

	root, found := os.LookupEnv(envKeyFSRootf)
	if !found {
		root = filepath.Join(os.TempDir(), "mitum-example-"+local.Address().String())
	}

	if err := launch.InitializeDatabase(root); err != nil {
		return errors.Wrap(err, "")
	}

	db, pool, err := launch.PrepareDatabase(root, encs, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	g := launch.NewGenesisBlockGenerator(local, networkID, enc, db, pool, launch.FSRootDataDirectory(root))
	_ = g.SetLogging(logging)
	if _, err := g.Generate(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

type runCommand struct {
	Address launch.AddressFlag `arg:"" name:"address" help:"node address"`
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

	root, found := os.LookupEnv(envKeyFSRootf)
	if !found {
		root = filepath.Join(os.TempDir(), "mitum-example-"+local.Address().String())
	}

	db, pool, err := launch.PrepareDatabase(root, encs, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	nodePolicy := defaultNodePolicy()
	log.Info().
		Interface("node_policy", nodePolicy).
		Msg("node policy loaded")

		// BLOCK hehehe
		// BLOCK
		// ================================================================================
		// ================================================================================
		// ================================================================================
		// ================================================================================
	getSuffrage := func(blockheight base.Height) base.Suffrage {
		blockheight--
		if blockheight < base.GenesisHeight {
			blockheight = base.GenesisHeight
		}

		st, found, err := db.Suffrage(blockheight)
		switch {
		case err != nil:
			return nil
		case !found:
			return nil
		}

		suf, err := isaac.NewSuffrage(st.Value().(base.SuffrageStateValue).Nodes())
		if err != nil {
			return nil
		}

		return suf
	}

	getManifest := func(height base.Height) (base.Manifest, error) {
		switch m, found, err := db.Map(height); {
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
			policy := db.LastNetworkPolicy()
			n := policy.MaxOperationsInProposal()
			if n < 1 {
				return nil, nil
			}

			hs, err := pool.NewOperationHashes(
				ctx,
				n, // BLOCK set ops limit in proposal
				func(facthash util.Hash) (bool, error) {
					switch found, err := db.ExistsInStateOperation(facthash); {
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
		pool,
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
		func(height base.Height) (base.Suffrage, bool, error) { // BLOCK replace  getSuffrage
			i, found, err := db.Suffrage(height)
			switch {
			case err != nil:
				return nil, false, errors.Wrap(err, "")
			case !found:
				return nil, false, nil
			case !base.IsSuffrageState(i):
				return nil, false, errors.Errorf("invalid suffrage state")
			}

			suf, err := i.Value().(base.SuffrageStateValue).Suffrage()
			if err != nil {
				return nil, false, errors.Wrap(err, "")
			}

			return suf, true, nil
		},
		func() []base.Address { return nil },
		func(context.Context, base.Point, base.Address) (base.ProposalSignedFact, error) { return nil, nil }, // BLOCK set request
		pool,
	)

	getLastManifest := func() (base.Manifest, bool, error) {
		switch m, found, err := db.LastMap(); {
		case err != nil || !found:
			return nil, found, err
		default:
			return m.Manifest(), true, nil
		}
	}

	newProposalProcessor := func(proposal base.ProposalSignedFact, previous base.Manifest) (isaac.ProposalProcessor, error) {
		return isaac.NewDefaultProposalProcessor(
			proposal,
			previous,
			launch.NewBlockDataWriterFunc(local, networkID, launch.FSRootDataDirectory(root), enc, db),
			db.State,
			nil,
			nil,
			pool.SetLastVoteproofs,
		)
	}

	pps := isaac.NewProposalProcessors(newProposalProcessor, nil)

	box := isaacstates.NewBallotbox(getSuffrage, nodePolicy.Threshold())
	states := isaacstates.NewStates(box)
	_ = states.SetLogging(logging)

	states.
		SetHandler(isaacstates.NewBrokenHandler(local, nodePolicy)).
		SetHandler(isaacstates.NewStoppedHandler(local, nodePolicy)).
		SetHandler(isaacstates.NewBootingHandler(local, nodePolicy, getLastManifest, getSuffrage)).
		SetHandler(isaacstates.NewJoiningHandler(local, nodePolicy, proposerSelector, getLastManifest, getSuffrage)).
		SetHandler(isaacstates.NewConsensusHandler(local, nodePolicy, proposerSelector, getManifest, getSuffrage, pps))

	// NOTE load last init, accept voteproof and last majority voteproof
	switch ivp, avp, found, err := pool.LastVoteproofs(); {
	case err != nil:
		return errors.Wrap(err, "")
	case !found:
	default:
		_ = states.LastVoteproofsHandler().Set(ivp)
		_ = states.LastVoteproofsHandler().Set(avp)
	}

	log.Debug().Msg("states loaded")

	select {
	case err := <-states.Wait(context.Background()):
		if err != nil {
			return errors.Wrap(err, "")
		}
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

func defaultNodePolicy() isaac.NodePolicy {
	return isaac.DefaultNodePolicy(networkID)
}
