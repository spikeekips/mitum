package main

import (
	"context"
	"os"
	"path/filepath"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/isaac/blockdata"
	"github.com/spikeekips/mitum/isaac/database"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	mitumlogging "github.com/spikeekips/mitum/util/logging"
)

var (
	networkID = base.NetworkID([]byte("mitum-example-node"))
	encs      *encoder.Encoders
	enc       *jsonenc.Encoder
	db        *database.Default
	dataroot  string
	local     isaac.LocalNode
	address   base.Address
)

var (
	logging *mitumlogging.Logging
	log     *zerolog.Logger
)

func init() {
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

	if err := prepareEncoders(); err != nil {
		kctx.FatalIfErrorf(err)
	}

	log.Info().Str("command", kctx.Command()).Msg("start command")

	err := func() error {
		defer log.Info().Msg("stopped")

		return kctx.Run()
	}()
	if err != nil {
		log.Error().Err(err).Msg("stopped by error")
	}

	kctx.FatalIfErrorf(err)
}

type initCommand struct {
	Address addressFlag `arg:"" name:"address" help:"node address"`
}

func (cmd *initCommand) Run() error {
	address = cmd.Address.address

	if err := prepareLocal(); err != nil {
		return errors.Wrap(err, "failed to prepare local")
	}

	log.Debug().Stringer("node_address", address).Msg("address parsed")

	if err := prepareDatabase(); err != nil {
		return errors.Wrap(err, "failed to prepare database")
	}

	// BLOCK new update suffrage operation to local is only suffrage node
	joinFact := isaacoperation.NewSuffrageGenesisJoinPermissionFact(local.Address(), local.Publickey(), networkID)
	if err := joinFact.IsValid(networkID); err != nil {
		return errors.Wrap(err, "")
	}

	joinOp := isaacoperation.NewSuffrageGenesisJoin(joinFact)
	if err := joinOp.Sign(local.Privatekey(), networkID); err != nil {
		return errors.Wrap(err, "")
	}

	proposal, err := newGenesisProposal(address, local.Privatekey(), []util.Hash{joinOp.Fact().Hash()})
	if err != nil {
		return errors.Wrap(err, "")
	}
	log.Debug().Interface("proposal", proposal).Msg("proposal created for genesis")

	ivp, err := newGenesisINITVoetproof(proposal.Fact().Hash())
	if err != nil {
		return errors.Wrap(err, "")
	}
	log.Debug().Interface("init_voteproof", ivp).Msg("init voteproof created for genesis")

	// NOTE create genesis proposal

	dbw, err := db.NewBlockWriteDatabase(base.GenesisHeight)
	if err != nil {
		return errors.Wrap(err, "")
	}

	// NOTE fs writer
	var fswriter *blockdata.LocalFSWriter
	{
		i, err := blockdata.NewLocalFSWriter(dataroot, base.GenesisHeight, enc, local, networkID)
		if err != nil {
			return errors.Wrap(err, "")
		}
		fswriter = i
	}

	pp, err := isaac.NewDefaultProposalProcessor(
		proposal,
		nil,
		func(pr base.ProposalSignedFact, getStateFunc base.GetStateFunc) (isaac.BlockDataWriter, error) {
			return blockdata.NewWriter(pr, getStateFunc, dbw, db.MergeBlockWriteDatabase, fswriter), nil
		},
		func(key string) (base.State, bool, error) {
			return nil, false, nil
		},
		func(_ context.Context, facthash util.Hash) (base.Operation, error) {
			switch {
			case facthash.Equal(joinOp.Fact().Hash()):
				return joinOp, nil
			default:
				return nil, util.NotFoundError.Errorf("operation not found")
			}
		},
		func(base.Height, hint.Hint) (base.OperationProcessor, bool, error) {
			return nil, false, nil
		},
	)
	if err != nil {
		return errors.Wrap(err, "")
	}

	var avp base.ACCEPTVoteproof
	switch m, err := pp.Process(context.Background(), ivp); {
	case err != nil:
		return errors.Wrap(err, "")
	default:
		if err := m.IsValid(networkID); err != nil {
			return errors.Wrap(err, "")
		}

		log.Info().Interface("manifest", m).Msg("genesis block generated")

		vp, err := newGenesisACCEPTVoteproof(proposal.Fact().Hash(), m.Hash())
		if err != nil {
			return errors.Wrap(err, "")
		}

		avp = vp
		log.Debug().Interface("accept_voteproof", avp).Msg("accept voteproof created for genesis")
	}

	if err := pp.Save(context.Background(), avp); err != nil {
		return errors.Wrap(err, "")
	}

	if err := closeDatabase(); err != nil {
		return errors.Wrap(err, "")
	}

	fsreader, err := blockdata.NewLocalFSReader(dataroot, base.GenesisHeight, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	switch blockdatamap, found, err := fsreader.Map(); {
	case err != nil:
		return errors.Wrap(err, "")
	case !found:
		return errors.Errorf("blockdatamap not found")
	default:
		if err := blockdatamap.IsValid(networkID); err != nil {
			return errors.Wrap(err, "")
		}

		log.Info().Interface("blockdatamap", blockdatamap).Msg("genesis block generated")
	}

	return nil
}

type runCommand struct {
	Address string `arg:"" name:"address" help:"node address"`
}

func (cmd *runCommand) Run() error {
	return nil
}

type addressFlag struct {
	address base.Address
}

func (f *addressFlag) UnmarshalText(b []byte) error {
	e := util.StringErrorFunc("failed to parse address")

	address, err := base.ParseStringAddress(string(b))
	if err != nil {
		return e(err, "")
	}

	if err := address.IsValid(nil); err != nil {
		return e(err, "")
	}

	f.address = address

	return nil
}

func prepareEncoders() error {
	encs = encoder.NewEncoders()
	enc = jsonenc.NewEncoder()

	if err := encs.AddHinter(enc); err != nil {
		return err
	}

	if err := launch.LoadHinters(enc); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func prepareDatabase() error {
	e := util.StringErrorFunc("failed to prepare database")

	root := filepath.Join(os.TempDir(), "mitum-example-"+address.String())
	switch _, err := os.Stat(root); {
	case err == nil:
		if err := os.RemoveAll(root); err != nil {
			return e(err, "")
		}
	case os.IsNotExist(err):
	default:
		return e(err, "")
	}

	if err := os.MkdirAll(root, 0o700); err != nil {
		return e(err, "")
	}

	permroot := filepath.Join(root, "perm")
	temproot := filepath.Join(root, "temp")
	dataroot = filepath.Join(root, "data")

	if err := os.MkdirAll(dataroot, 0o700); err != nil {
		return e(err, "failed to make blockdata root")
	}

	// NOTE db
	perm, err := database.NewLeveldbPermanent(permroot, encs, enc)
	if err != nil {
		return e(err, "")
	}

	i, err := database.NewDefault(root, encs, enc, perm, func(height base.Height) (isaac.BlockWriteDatabase, error) {
		newroot, err := database.NewTempDirectory(temproot, height)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}

		return database.NewLeveldbBlockWrite(height, newroot, encs, enc)
	})
	if err != nil {
		return e(err, "")
	}

	db = i
	_ = db.SetLogging(logging)

	return nil
}

func generatePrivatekey(address base.Address) (base.Privatekey, error) {
	b := make([]byte, base.PrivatekeyMinSeedSize)
	copy(b, []byte(address.Bytes()))

	return base.NewMPrivatekeyFromSeed(string(b))
}

func newGenesisProposal(proposer base.Address, priv base.Privatekey, ops []util.Hash) (base.ProposalSignedFact, error) {
	e := util.StringErrorFunc("failed to make genesis proposal")

	fact := isaac.NewProposalFact(base.GenesisPoint, proposer, ops)
	signed := isaac.NewProposalSignedFact(fact)

	if err := signed.Sign(local.Privatekey(), networkID); err != nil {
		return nil, e(err, "")
	}

	if err := signed.IsValid(networkID); err != nil {
		return nil, e(err, "")
	}

	return signed, nil
}

func prepareLocal() error {
	// NOTE make privatekey, based on node address
	priv, err := generatePrivatekey(address)
	if err != nil {
		return errors.Wrap(err, "failed to create privatekey from node address")
	}
	log.Debug().Stringer("privatekey", priv).Stringer("publickey", priv.Publickey()).Msg("keypair generated")

	local = isaac.NewLocalNode(priv, address)

	return nil
}

func newGenesisINITVoetproof(proposal util.Hash) (base.INITVoteproof, error) {
	e := util.StringErrorFunc("failed to make genesis init voteproof")

	fact := isaac.NewINITBallotFact(base.GenesisPoint, nil, proposal)
	if err := fact.IsValid(nil); err != nil {
		return nil, e(err, "")
	}

	sf := isaac.NewINITBallotSignedFact(local.Address(), fact)
	if err := sf.Sign(local.Privatekey(), networkID); err != nil {
		return nil, e(err, "")
	}
	if err := sf.IsValid(networkID); err != nil {
		return nil, e(err, "")
	}

	vp := isaac.NewINITVoteproof(fact.Point().Point)
	vp.SetResult(base.VoteResultMajority).
		SetMajority(fact).
		SetSignedFacts([]base.BallotSignedFact{sf}).
		SetThreshold(base.Threshold(100)).
		Finish()

	if err := vp.IsValid(networkID); err != nil {
		return nil, e(err, "")
	}

	return vp, nil
}

func newGenesisACCEPTVoteproof(proposal, newblock util.Hash) (base.ACCEPTVoteproof, error) {
	e := util.StringErrorFunc("failed to make genesis accept voteproof")

	fact := isaac.NewACCEPTBallotFact(base.GenesisPoint, proposal, newblock)
	if err := fact.IsValid(nil); err != nil {
		return nil, e(err, "")
	}

	sf := isaac.NewACCEPTBallotSignedFact(local.Address(), fact)
	if err := sf.Sign(local.Privatekey(), networkID); err != nil {
		return nil, e(err, "")
	}
	if err := sf.IsValid(networkID); err != nil {
		return nil, e(err, "")
	}

	vp := isaac.NewACCEPTVoteproof(fact.Point().Point)
	vp.SetResult(base.VoteResultMajority).
		SetMajority(fact).
		SetSignedFacts([]base.BallotSignedFact{sf}).
		SetThreshold(base.Threshold(100)).
		Finish()
	if err := vp.IsValid(networkID); err != nil {
		return nil, e(err, "")
	}

	return vp, nil
}

func closeDatabase() error {
	if err := db.MergeAllPermanent(); err != nil {
		return errors.Wrap(err, "failed to merge temps")
	}

	if err := db.Close(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}
