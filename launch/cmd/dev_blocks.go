package launchcmd

import (
	"context"
	"os"
	"runtime/pprof"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

type GenerateBlocksCommand struct { //nolint:govet //...
	GenesisDesign string `arg:"" name:"genesis design" help:"genesis design" type:"filepath"`
	launch.DesignFlag
	Height                  base.Height   `arg:""`
	SleepPerHeights         uint64        `name:"sleep-per-heights" help:"sleep per heights"`
	SleepDurationPerHeights time.Duration `name:"sleep-duration-per-heights" help:"duration for sleep per heights"`
	launch.PrivatekeyFlags
}

func (cmd *GenerateBlocksCommand) Run(pctx context.Context) error {
	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return err
	}

	if cmd.Height < 1 {
		return errors.Errorf("<height> should be over 0")
	}

	log.Log().Debug().
		Interface("design", cmd.DesignFlag).
		Interface("height", cmd.Height).
		Msg("flags")

	nctx := util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		launch.DesignFlagContextKey:        cmd.DesignFlag,
		launch.DevFlagsContextKey:          launch.DevFlags{},
		launch.GenesisDesignFileContextKey: cmd.GenesisDesign,
		launch.PrivatekeyContextKey:        string(cmd.PrivatekeyFlags.Flag.Body()),
	})

	pps := launch.DefaultINITPS()

	_ = pps.
		AddOK(launch.PNameStartStorage, launch.PStartStorage, launch.PCloseStorage, launch.PNameGenerateGenesis).
		AddOK(ps.Name("generate-blocks"), cmd.pGenerateBlocks, nil, launch.PNameStartStorage)

	_ = pps.SetLogging(log)

	log.Log().Debug().Interface("process", pps.Verbose()).Msg("process ready")

	nctx, err := pps.Run(nctx)

	switch i, merr := os.Create("./mem.pprof"); {
	case merr != nil:
		return errors.WithStack(merr)
	default:
		_ = pprof.WriteHeapProfile(i)
		_ = i.Close()
	}

	defer func() {
		log.Log().Debug().Interface("process", pps.Verbose()).Msg("process will be closed")

		if _, err = pps.Close(nctx); err != nil {
			log.Log().Error().Err(err).Msg("failed to close")
		}
	}()

	return err
}

func (cmd *GenerateBlocksCommand) pGenerateBlocks(pctx context.Context) (context.Context, error) {
	if err := cmd.generateBlocks(pctx); err != nil {
		return pctx, err
	}

	return pctx, nil
}

func (cmd *GenerateBlocksCommand) generateBlocks(pctx context.Context) error {
	var log *logging.Logging
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		launch.LoggingContextKey, &log,
		launch.CenterDatabaseContextKey, &db,
	); err != nil {
		return err
	}

	var previous base.Manifest

	switch i, found, err := db.LastBlockMap(); {
	case err != nil:
		return err
	case !found:
		return util.ErrNotFound.Errorf("genesis block")
	default:
		previous = i.Manifest()
	}

	sleep := func(uint64) {}
	if cmd.SleepPerHeights > 0 && cmd.SleepDurationPerHeights > 0 {
		sleep = func(i uint64) {
			if i%cmd.SleepPerHeights == 0 {
				<-time.After(cmd.SleepDurationPerHeights)
			}
		}
	}

	var n uint64

	for i := base.GenesisHeight + 1; i <= cmd.Height; i++ {
		sleep(n)

		switch m, err := cmd.generateBlock(pctx, i, previous); {
		case err != nil:
			return err
		default:
			previous = m
		}

		log.Log().Debug().Interface("height", i).Msg("block generated")

		if i%99 == 0 {
			if err := db.MergeAllPermanent(); err != nil {
				return err
			}
		}

		n++
	}

	return db.MergeAllPermanent()
}

func (cmd *GenerateBlocksCommand) generateBlock(
	pctx context.Context,
	height base.Height,
	previous base.Manifest,
) (base.Manifest, error) {
	var proposal base.ProposalSignFact

	switch i, err := cmd.newProposal(pctx, height, previous.Hash()); {
	case err != nil:
		return nil, err
	default:
		proposal = i
	}

	var writer *isaacblock.Writer

	switch i, err := cmd.newWriter(pctx, proposal); {
	case err != nil:
		return nil, err
	default:
		writer = i
	}

	switch i, err := cmd.initVoetproof(pctx, height, proposal.Fact().Hash(), previous.Hash()); {
	case err != nil:
		return nil, err
	default:
		if err := writer.SetINITVoteproof(context.Background(), i); err != nil {
			return nil, err
		}
	}

	var manifest base.Manifest

	switch i, err := writer.Manifest(context.Background(), previous); {
	case err != nil:
		return nil, err
	default:
		manifest = i
	}

	switch i, err := cmd.acceptVoteproof(pctx, height, proposal.Fact().Hash(), manifest.Hash()); {
	case err != nil:
		return nil, err
	default:
		if err := writer.SetACCEPTVoteproof(context.Background(), i); err != nil {
			return nil, err
		}
	}

	if _, err := writer.Save(context.Background()); err != nil {
		return nil, err
	}

	return manifest, nil
}

func (*GenerateBlocksCommand) newWriter(
	pctx context.Context,
	proposal base.ProposalSignFact,
) (*isaacblock.Writer, error) {
	var encs *encoder.Encoders
	var design launch.NodeDesign
	var local base.LocalNode
	var isaacparams *isaac.Params
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &encs,
		launch.DesignContextKey, &design,
		launch.LocalContextKey, &local,
		launch.ISAACParamsContextKey, &isaacparams,
		launch.CenterDatabaseContextKey, &db,
	); err != nil {
		return nil, err
	}

	dbw, err := db.NewBlockWriteDatabaseForSync(proposal.Point().Height())
	if err != nil {
		return nil, err
	}

	fswriter, err := isaacblock.NewLocalFSWriter(
		launch.LocalFSDataDirectory(design.Storage.Base),
		proposal.Point().Height(),
		encs.JSON(), encs.Default(),
		local,
		isaacparams.NetworkID(),
	)
	if err != nil {
		return nil, err
	}

	return isaacblock.NewWriter(
		proposal,
		func(key string) (base.State, bool, error) { return nil, false, nil },
		dbw,
		db.MergeBlockWriteDatabase,
		fswriter,
		1,
	), nil
}

func (*GenerateBlocksCommand) newProposal(
	pctx context.Context,
	height base.Height,
	previous util.Hash,
) (base.ProposalSignFact, error) {
	var local base.LocalNode
	var isaacparams *isaac.Params

	if err := util.LoadFromContextOK(pctx,
		launch.LocalContextKey, &local,
		launch.ISAACParamsContextKey, &isaacparams,
	); err != nil {
		return nil, err
	}

	fact := isaac.NewProposalFact(base.NewPoint(height, 0), local.Address(), previous, nil)
	sign := isaac.NewProposalSignFact(fact)

	if err := sign.Sign(local.Privatekey(), isaacparams.NetworkID()); err != nil {
		return nil, err
	}

	if err := sign.IsValid(isaacparams.NetworkID()); err != nil {
		return nil, err
	}

	return sign, nil
}

func (*GenerateBlocksCommand) initVoetproof( //nolint:dupl //...
	pctx context.Context,
	height base.Height,
	proposal, previous util.Hash,
) (base.INITVoteproof, error) {
	var local base.LocalNode
	var isaacparams *isaac.Params

	if err := util.LoadFromContextOK(pctx,
		launch.LocalContextKey, &local,
		launch.ISAACParamsContextKey, &isaacparams,
	); err != nil {
		return nil, err
	}

	fact := isaac.NewINITBallotFact(base.NewPoint(height, 0), previous, proposal, nil)
	if err := fact.IsValid(nil); err != nil {
		return nil, err
	}

	sf := isaac.NewINITBallotSignFact(fact)
	if err := sf.NodeSign(local.Privatekey(), isaacparams.NetworkID(), local.Address()); err != nil {
		return nil, err
	}

	if err := sf.IsValid(isaacparams.NetworkID()); err != nil {
		return nil, err
	}

	vp := isaac.NewINITVoteproof(fact.Point().Point)
	vp.
		SetMajority(fact).
		SetSignFacts([]base.BallotSignFact{sf}).
		SetThreshold(base.MaxThreshold).
		Finish()

	if err := vp.IsValid(isaacparams.NetworkID()); err != nil {
		return nil, err
	}

	return vp, nil
}

func (*GenerateBlocksCommand) acceptVoteproof( //nolint:dupl //...
	pctx context.Context,
	height base.Height,
	proposal, newblock util.Hash,
) (base.ACCEPTVoteproof, error) {
	var local base.LocalNode
	var isaacparams *isaac.Params

	if err := util.LoadFromContextOK(pctx,
		launch.LocalContextKey, &local,
		launch.ISAACParamsContextKey, &isaacparams,
	); err != nil {
		return nil, err
	}

	fact := isaac.NewACCEPTBallotFact(base.NewPoint(height, 0), proposal, newblock, nil)
	if err := fact.IsValid(nil); err != nil {
		return nil, err
	}

	sf := isaac.NewACCEPTBallotSignFact(fact)
	if err := sf.NodeSign(local.Privatekey(), isaacparams.NetworkID(), local.Address()); err != nil {
		return nil, err
	}

	if err := sf.IsValid(isaacparams.NetworkID()); err != nil {
		return nil, err
	}

	vp := isaac.NewACCEPTVoteproof(fact.Point().Point)
	vp.
		SetMajority(fact).
		SetSignFacts([]base.BallotSignFact{sf}).
		SetThreshold(base.MaxThreshold).
		Finish()

	if err := vp.IsValid(isaacparams.NetworkID()); err != nil {
		return nil, err
	}

	return vp, nil
}
