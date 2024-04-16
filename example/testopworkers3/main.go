package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	"github.com/spikeekips/mitum/util/valuehash"
)

var maxConcurrency int64 = 1 << 14

var designString = `
address: testsas
privatekey: 09e151a12a283cd46ac02a425559460e88746c49697e41c4edb9b3122f83b399mpr
network_id: mitum contest; Sat 26 Dec 2020 05:29:13 AM KST
storage:
    base: ./data
parameters:
`

var flagDefaults = kong.Vars{
	"log_out":         "stderr",
	"log_format":      "terminal",
	"log_level":       "debug",
	"log_force_color": "false",
	"max_concurrency": strconv.FormatInt(maxConcurrency, 10),
}

type CLI struct { //nolint:govet //...
	*logging.Logging `kong:"-"`
	launch.BaseFlags
	Operations  uint64 `name:"ops" help:"number of operations" default:"333"`
	Concurrency int64  `name:"concurrency" help:"concurrency; default is number of operation <= ${max_concurrency}"`
	PProf       bool   `name:"pprof"`
	Workers     uint64 `name:"workers" help:"number of workers" default:"3"`
	startPoint  base.Height
	local       base.LocalNode
	networkID   base.NetworkID
	keys        []base.Privatekey
	c           int64
	workers     []string
}

func main() {
	cli := &CLI{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "test-process-operations")
		}),
	}

	kctx := kong.Parse(cli, flagDefaults)

	var pctx context.Context

	switch i, pps, err := cli.prepare(kctx); {
	case err != nil:
		kctx.FatalIfErrorf(err)
	default:
		defer func() {
			_, _ = pps.Close(pctx)
		}()

		pctx = i
	}

	cli.Log().Debug().
		Interface("flags", os.Args).
		Int64("concurrency_flag", cli.Concurrency).
		Int64("concurrency", cli.c).
		Msg("flags")

	if err := func() error {
		defer cli.Log().Debug().Msg("stopped")

		return errors.WithStack(cli.run(pctx))
	}(); err != nil {
		cli.Log().Error().Err(err).Msg("stopped by error")

		kctx.FatalIfErrorf(err)
	}
}

func (cli *CLI) prepare(kctx *kong.Context) (context.Context, *ps.PS, error) {
	if cli.Workers < 1 {
		return nil, nil, errors.Errorf("Workers should be at least 1")
	}

	pps := ps.NewPS("cmd-main")

	_ = pps.
		AddOK(ps.NameINIT, launch.PINIT, nil)

	_ = pps.POK(ps.NameINIT).
		PostAddOK(launch.PNameLogging, launch.PLogging).
		PostAddOK(launch.PNameLoggingWithCLI, launch.PLoggingWithCLI)

	_ = pps.
		AddOK(launch.PNameEncoder, launch.PEncoder, nil).
		AddOK(launch.PNameDesign, func(pctx context.Context) (context.Context, error) {
			var encs *encoder.Encoders

			if err := util.LoadFromContextOK(pctx,
				launch.EncodersContextKey, &encs,
			); err != nil {
				return pctx, err
			}

			var design launch.NodeDesign

			if err := design.DecodeYAML([]byte(designString), encs.JSON()); err != nil {
				return pctx, err
			}

			return util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
				launch.DesignContextKey:       design,
				launch.DesignStringContextKey: designString,
			}), nil
		}, nil, launch.PNameEncoder).
		AddOK(launch.PNameLocal, launch.PLocal, nil, launch.PNameDesign).
		AddOK(launch.PNameBlockItemReaders, launch.PBlockItemReaders, nil, launch.PNameDesign).
		AddOK(launch.PNameStorage, launch.PStorage, nil, launch.PNameLocal).
		AddOK(launch.PNameStartStorage, launch.PStartStorage, launch.PCloseStorage, launch.PNameStorage).
		AddOK(launch.PNameStates, func(pctx context.Context) (context.Context, error) {
			return pctx, nil
		}, nil, launch.PNameStartStorage)

	_ = pps.POK(launch.PNameEncoder).
		PostAddOK(launch.PNameAddHinters, launch.PAddHinters).
		PostAddOK(ps.Name("test-add-hinter"), func(pctx context.Context) (context.Context, error) {
			hinters := []encoder.DecodeDetail{
				{Hint: OpFactHint, Instance: OpFact{}},
				{Hint: OpHint, Instance: Op{}},
				{Hint: AccountStateValueHint, Instance: AccountStateValue{}},
			}

			var encs *encoder.Encoders
			if err := util.LoadFromContextOK(pctx, launch.EncodersContextKey, &encs); err != nil {
				return pctx, err
			}

			for i := range hinters {
				if err := encs.AddDetail(hinters[i]); err != nil {
					return pctx, errors.Wrap(err, "add hinter to encoders")
				}
			}

			return pctx, nil
		})

	_ = pps.POK(launch.PNameDesign).
		PostAddOK(launch.PNameCheckDesign, launch.PCheckDesign).
		PostAddOK(ps.Name("update design"), func(pctx context.Context) (context.Context, error) {
			var isaacparams *isaac.Params

			if err := util.LoadFromContextOK(pctx,
				launch.ISAACParamsContextKey, &isaacparams,
			); err != nil {
				return pctx, err
			}

			_ = isaacparams.SetStateCacheSize(int(cli.Operations) + 33)         //nolint:gomnd //...
			_ = isaacparams.SetOperationPoolCacheSize(int(cli.Operations) + 33) //nolint:gomnd //...

			return pctx, nil
		}).
		PostAddOK(launch.PNameINITObjectCache, launch.PINITObjectCache)

	_ = pps.POK(launch.PNameBlockItemReaders).
		PreAddOK(launch.PNameBlockItemReadersDecompressFunc, launch.PBlockItemReadersDecompressFunc).
		PostAddOK(launch.PNameRemotesBlockItemReaderFunc, launch.PRemotesBlockItemReaderFunc)

	_ = pps.POK(launch.PNameStorage).
		PreAddOK(launch.PNameCleanStorage, launch.PCleanStorage).
		PreAddOK(launch.PNameCreateLocalFS, launch.PCreateLocalFS).
		PreAddOK(launch.PNameLoadDatabase, launch.PLoadDatabase).
		PostAddOK(launch.PNameCheckLeveldbStorage, launch.PCheckLeveldbStorage).
		PostAddOK(launch.PNameLoadFromDatabase, launch.PLoadFromDatabase).
		PostAddOK(launch.PNameCheckBlocksOfStorage, launch.PCheckBlocksOfStorage).
		PostAddOK(launch.PNameSuffrageCandidateLimiterSet, launch.PSuffrageCandidateLimiterSet).
		PostAddOK(launch.PNamePatchBlockItemReaders, launch.PPatchBlockItemReaders)

	_ = pps.POK(launch.PNameStates).
		PostAddOK(launch.PNameQuicstreamClient, launch.PQuicstreamClient).
		PostAddOK(launch.PNameSyncSourceChecker, func(pctx context.Context) (context.Context, error) {
			syncSourcePool := isaac.NewSyncSourcePool(nil)

			return util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
				launch.SyncSourcePoolContextKey: syncSourcePool,
			}), nil
		}).
		PostAddOK(launch.PNameOperationProcessorsMap, launch.POperationProcessorsMap)

	cli.startPoint = base.GenesisHeight

	var designFlag launch.DesignFlag
	var devFlags launch.DevFlags

	pctx := util.ContextWithValues(context.Background(), map[util.ContextKey]interface{}{
		launch.VersionContextKey:     util.MustNewVersion("v0.0.1"),
		launch.FlagsContextKey:       cli.BaseFlags,
		launch.KongContextContextKey: kctx,
		launch.DesignFlagContextKey:  designFlag,
		launch.PrivatekeyContextKey:  "",
		launch.DevFlagsContextKey:    devFlags,
		launch.MemberlistContextKey:  &quicmemberlist.Memberlist{},
	})

	switch i, err := pps.Run(pctx); {
	case err != nil:
		return pctx, nil, err
	default:
		pctx = i
	}

	var log *logging.Logging
	var isaacparams *isaac.Params

	if err := util.LoadFromContextOK(pctx,
		launch.LoggingContextKey, &log,
		launch.ISAACParamsContextKey, &isaacparams,
	); err != nil {
		return nil, nil, err
	}

	_ = cli.SetLogging(log)

	cli.Log().Debug().Interface("main_process", pps.Verbose()).Msg("processed")

	cli.networkID = isaacparams.NetworkID()

	switch {
	case cli.Concurrency > 0:
		cli.c = cli.Concurrency
	default:
		cli.c = int64(cli.Operations)
		if cli.c > maxConcurrency {
			cli.c = maxConcurrency
		}
	}

	cli.makeWorkerNames()

	return pctx, pps, nil
}

func (cli *CLI) run(pctx context.Context) error {
	var mp base.BlockMap

	switch i, err := cli.createAccounts(pctx); {
	case err != nil:
		return errors.WithMessage(err, "create accounts")
	default:
		mp = i
	}

	if err := cli.transfer(pctx, mp.Manifest()); err != nil {
		return errors.WithMessage(err, "transfer")
	}

	return nil
}

func (cli *CLI) createAccounts(pctx context.Context) (base.BlockMap, error) {
	if err := cli.createAccountKeys(pctx); err != nil {
		return nil, errors.WithMessage(err, "keys")
	}

	var ops []Op

	switch i, err := cli.createAccountOperations(pctx); {
	case err != nil:
		return nil, errors.WithMessage(err, "create account operations")
	default:
		ops = i
	}

	var pr base.ProposalSignFact

	switch i, err := cli.newProposal(pctx, cli.startPoint, valuehash.RandomSHA256(), ops); {
	case err != nil:
		return nil, errors.WithMessage(err, "create account proposal")
	default:
		pr = i
	}

	switch done, err := cli.pprof("create-accounts"); {
	case err != nil:
		return nil, err
	default:
		defer done()

		switch i, j, err := cli.process(pctx, pr, nil); {
		case err != nil:
			return nil, errors.WithMessage(err, "process proposal")
		default:
			return i, j()
		}
	}
}

func (cli *CLI) createAccountKeys(pctx context.Context) error {
	started := time.Now()

	if err := util.LoadFromContextOK(pctx,
		launch.LocalContextKey, &cli.local,
	); err != nil {
		return err
	}

	cli.keys = make([]base.Privatekey, (cli.Operations))

	wk, _ := util.NewBaseJobWorker(context.Background(), cli.c)

	go func() {
		for i := range cli.keys {
			i := i

			if err := wk.NewJob(func(context.Context, uint64) error {
				cli.keys[i] = base.NewMPrivatekey()

				return nil
			}); err != nil {
				break
			}
		}

		wk.Done()
	}()

	if err := wk.Wait(); err != nil {
		return err
	}

	cli.Log().Debug().
		Stringer("elapsed", time.Since(started)).
		Int("accounts", len(cli.keys)).
		Msg("account keys generated")

	return nil
}

func (cli *CLI) createAccountOperations(pctx context.Context) ([]Op, error) {
	cli.Log().Debug().Msg("create operations")
	defer logging.TimeElapsed()(cli.Log().Info(), "create operations generated")

	var pool isaac.NewOperationPool

	if err := util.LoadFromContextOK(pctx, launch.PoolDatabaseContextKey, &pool); err != nil {
		return nil, err
	}

	token := []byte("create-accounts")

	ops := make([]Op, len(cli.keys))

	wk, _ := util.NewBaseJobWorker(context.Background(), cli.c)

	go func() {
		for i := range ops {
			i := i

			if err := wk.NewJob(func(ctx context.Context, _ uint64) error {
				fact := NewOpFact(token, 1, nil, cli.keys[i].Publickey())
				op := NewOp(fact)
				if err := op.Sign(cli.local.Privatekey(), cli.networkID); err != nil {
					return err
				}

				_, _ = pool.SetOperation(ctx, op)

				ops[i] = op

				return nil
			}); err != nil {
				break
			}
		}

		wk.Done()
	}()

	if err := wk.Wait(); err != nil {
		return nil, err
	}

	return ops, nil
}

func (cli *CLI) newProposal(
	pctx context.Context,
	height base.Height,
	previousBlock util.Hash,
	ops []Op,
) (base.ProposalSignFact, error) {
	started := time.Now()

	var pool isaac.ProposalPool

	if err := util.LoadFromContextOK(pctx, launch.PoolDatabaseContextKey, &pool); err != nil {
		return nil, err
	}

	oppair := make([][2]util.Hash, len(ops))
	for i := range ops {
		oppair[i] = [2]util.Hash{ops[i].Hash(), ops[i].Fact().Hash()}
	}

	fact := isaac.NewProposalFact(
		base.NewPoint(height, 0),
		cli.local.Address(),
		previousBlock,
		oppair,
	)

	sf := isaac.NewProposalSignFact(fact)
	if err := sf.Sign(cli.local.Privatekey(), cli.networkID); err != nil {
		return nil, err
	}

	switch isset, err := pool.SetProposal(sf); {
	case err != nil:
		return nil, err
	case !isset:
		return nil, errors.Errorf("not set")
	}

	cli.Log().Debug().
		Stringer("elapsed", time.Since(started)).
		Msg("proposal generated")

	return sf, nil
}

func (cli *CLI) processor(
	pctx context.Context,
	proposal base.ProposalSignFact,
	previous base.Manifest,
) (isaac.ProposalProcessor, func() error, error) {
	var encs *encoder.Encoders
	var design launch.NodeDesign
	var isaacparams *isaac.Params
	var db isaac.Database
	var pool isaac.NewOperationPool
	var oprs *hint.CompatibleSet[isaac.NewOperationProcessorInternalFunc]

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &encs,
		launch.DesignContextKey, &design,
		launch.ISAACParamsContextKey, &isaacparams,
		launch.CenterDatabaseContextKey, &db,
		launch.PoolDatabaseContextKey, &pool,
		launch.OperationProcessorsMapContextKey, &oprs,
	); err != nil {
		return nil, nil, err
	}

	fsroot := filepath.Join(
		launch.LocalFSDataDirectory(design.Storage.Base),
		proposal.Point().Height().String(),
	)

	if err := os.MkdirAll(fsroot, 0o700); err != nil {
		return nil, nil, errors.WithMessage(err, "create fs root directory")
	}

	args := NewProposalProcessorArgs()
	args.NewJobWorker = func(ctx context.Context) (util.JobWorker, error) {
		return util.NewBaseJobWorker(ctx, cli.c)
	}
	args.SignBlockMapItem = func(bm isaacblock.BlockMap) error {
		return bm.Sign(cli.local.Address(), cli.local.Privatekey(), cli.networkID)
	}

	var bdb isaac.BlockWriteDatabase

	switch i, err := db.NewBlockWriteDatabase(proposal.Point().Height()); {
	case err != nil:
		return nil, nil, err
	default:
		bdb = i
	}

	if i, ok := bdb.(isaac.StateCacheSetter); ok {
		i.SetStateCache(util.NewLFUGCache[string, [2]interface{}](isaacparams.StateCacheSize()))
	}

	writerSaveBlockItemFuncs := NewWriterSaveBlockItemFuncs(bdb, fsroot, encs.Default())

	args.SaveBlockItemFuncs = func(ctx context.Context, t base.BlockItemType) (writerSaveBlockItemFunc, writerCloseSaveFunc, error) {
		return writerSaveBlockItemFuncs(ctx, t)
	}

	var newManagerFunc func(context.Context) (*ProcessManager, error)

	var previousManifest, previousSuffrage util.Hash
	if previous != nil {
		previousManifest = previous.Hash()
		previousSuffrage = previous.Suffrage()
	}

	switch i, err := cli.newManagerFunc(db, bdb, fsroot, encs, pool, previousManifest, previousSuffrage, proposal); {
	case err != nil:
		return nil, nil, err
	default:
		newManagerFunc = i
	}

	var manager *ProcessManager

	switch i, err := newManagerFunc(context.Background()); {
	case err != nil:
		return nil, nil, err
	default:
		manager = i
	}

	if err := manager.ReadyWorkers(); err != nil {
		return nil, nil, err
	}

	switch pp, err := NewProposalProcessor(
		context.Background(),
		args,
		proposal,
		manager,
	); {
	case err != nil:
		return nil, nil, err
	default:
		pp.SetLogging(cli.Logging)

		return pp,
			func() error {
				return db.MergeBlockWriteDatabase(bdb)
			},
			nil
	}
}

func (cli *CLI) newManagerFunc(
	db isaac.Database,
	bdb isaac.BlockWriteDatabase,
	fsroot string,
	encs *encoder.Encoders,
	pool isaac.NewOperationPool,
	previousManifest, previousSuffrage util.Hash,
	proposal base.ProposalSignFact,
) (
	func(context.Context) (*ProcessManager, error),
	error,
) {
	var pm *ProcessManager

	workerpool, f := cli.newWorkerReadyFunc(db, bdb, fsroot, encs, pool, &pm)

	var saveStatesTreeFuncs, saveOperationsTreeNodeFuncs writerSaveTreeNodeFuncs

	switch i, err := newWriterSaveTreeFuncs(
		fsroot,
		encs.Default(),
		base.BlockItemStatesTree,
	); {
	case err != nil:
		return nil, err
	default:
		saveStatesTreeFuncs = i
	}

	switch i, err := newWriterSaveTreeFuncs(
		fsroot,
		encs.Default(),
		base.BlockItemOperationsTree,
	); {
	case err != nil:
		return nil, err
	default:
		saveOperationsTreeNodeFuncs = i
	}

	return func(ctx context.Context) (*ProcessManager, error) {
		args := NewProcessManagerArgs(cli.workers)
		args.IsInStateOperationFunc = func(_ context.Context, fact util.Hash) (bool, error) {
			return db.ExistsInStateOperation(fact)
		}
		args.NewJobWorker = func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, cli.c)
		}
		args.WorkerReady = func(ctx context.Context, worker string, args processWorkerPrepareArgs) error {
			return f(ctx, worker, args)
		}
		args.WorkerCancel = func(_ context.Context, worker string) error {
			w, _ := workerpool.Value(worker)
			w.Cancel()

			return nil
		}
		args.WorkerDone = func(_ context.Context, worker string) error {
			w, _ := workerpool.Value(worker)

			return w.Done()
		}
		args.WorkerNewOperations = func(ct_ context.Context, worker string) (
			workerNewOperationsFunc,
			func() error,
			func(error),
			error,
		) {
			w, _ := workerpool.Value(worker)

			return w.NewOperations()
		}
		args.IsEnoughWorkers = func(workers uint64) bool {
			return workers == cli.Workers
		}

		args.NewManifestFunc = func(context.Context, base.Manifest) error {
			return nil
		}
		args.SaveStatesTreeNodeFuncs = saveStatesTreeFuncs
		args.SaveOperationsTreeNodeFuncs = saveOperationsTreeNodeFuncs

		switch i, err := NewProcessManager(
			ctx,
			args,
			previousManifest, previousSuffrage,
			proposal,
		); {
		case err != nil:
			return nil, err
		default:
			pm = i
			pm.SetLogging(cli.Logging)

			return pm, nil
		}
	}, nil
}

func (cli *CLI) makeWorkerNames() {
	l := len(strconv.FormatUint(cli.Workers, 10))
	if l < 2 {
		l = 2
	}

	f := fmt.Sprintf("w%%0%dd", l)

	cli.workers = make([]string, cli.Workers)
	for i := range cli.workers {
		cli.workers[i] = fmt.Sprintf(f, i)
	}
}

func (cli *CLI) process(
	pctx context.Context,
	pr base.ProposalSignFact,
	previous base.Manifest,
) (base.BlockMap, func() error, error) {
	cli.Log().Debug().Msg("processing")

	var pps isaac.ProposalProcessor
	var ppdone func() error

	switch i, j, err := cli.processor(pctx, pr, previous); {
	case err != nil:
		return nil, nil, err
	default:
		defer i.Cancel()

		pps = i
		ppdone = j
	}

	var ivp base.INITVoteproof

	switch i, err := cli.newINITVoteproof(pr.Point(), pr); {
	case err != nil:
		return nil, nil, err
	default:
		ivp = i
	}

	defer logging.TimeElapsed()(cli.Log().Info(), "processing done")

	var m base.Manifest

	proposalProcessingElapsed := logging.TimeElapsed()

	switch i, err := pps.Process(context.Background(), ivp); {
	case err != nil:
		return nil, nil, err
	default:
		m = i

		proposalProcessingElapsed(cli.Log().Debug().Interface("manifest", m), "proposal processed")
	}

	saveElapsed := logging.TimeElapsed()

	var avp base.ACCEPTVoteproof

	switch i, err := cli.newACCEPTVoteproof(pr.Point(), pr.Fact().Hash(), m.Hash()); {
	case err != nil:
		return nil, nil, err
	default:
		avp = i
	}

	var mp base.BlockMap

	switch i, err := pps.Save(context.Background(), avp); {
	case err != nil:
		return nil, nil, err
	default:
		mp = i

		saveElapsed(cli.Log().Info().Interface("blockmap", mp), "proposal saved")
	}

	return mp, ppdone, nil
}

func (cli *CLI) newINITVoteproof(point base.Point, proposal base.ProposalSignFact) (base.INITVoteproof, error) {
	fact := isaac.NewINITBallotFact(
		point,
		proposal.ProposalFact().PreviousBlock(),
		proposal.ProposalFact().Hash(),
		nil,
	)
	if err := fact.IsValid(nil); err != nil {
		return nil, err
	}

	sf := isaac.NewINITBallotSignFact(fact)
	if err := sf.NodeSign(cli.local.Privatekey(), cli.networkID, cli.local.Address()); err != nil {
		return nil, err
	}

	if err := sf.IsValid(cli.networkID); err != nil {
		return nil, err
	}

	vp := isaac.NewINITVoteproof(fact.Point().Point)
	vp.
		SetMajority(fact).
		SetSignFacts([]base.BallotSignFact{sf}).
		SetThreshold(base.MaxThreshold).
		Finish()

	if err := vp.IsValid(cli.networkID); err != nil {
		return nil, err
	}

	return vp, nil
}

func (cli *CLI) newACCEPTVoteproof(point base.Point, proposal, newBlock util.Hash) (base.ACCEPTVoteproof, error) {
	fact := isaac.NewACCEPTBallotFact(point, proposal, newBlock, nil)
	if err := fact.IsValid(nil); err != nil {
		return nil, err
	}

	sf := isaac.NewACCEPTBallotSignFact(fact)
	if err := sf.NodeSign(cli.local.Privatekey(), cli.networkID, cli.local.Address()); err != nil {
		return nil, err
	}

	if err := sf.IsValid(cli.networkID); err != nil {
		return nil, err
	}

	vp := isaac.NewACCEPTVoteproof(fact.Point().Point)
	vp.
		SetMajority(fact).
		SetSignFacts([]base.BallotSignFact{sf}).
		SetThreshold(base.MaxThreshold).
		Finish()

	if err := vp.IsValid(cli.networkID); err != nil {
		return nil, err
	}

	return vp, nil
}

func (cli *CLI) transfer(pctx context.Context, m base.Manifest) error {
	var ops []Op

	switch i, err := cli.createTransferOperations(pctx); {
	case err != nil:
		return errors.WithMessage(err, "transfer operations")
	default:
		ops = i
	}

	var pr base.ProposalSignFact

	switch i, err := cli.newProposal(pctx, m.Height()+1, m.Hash(), ops); {
	case err != nil:
		return errors.WithMessage(err, "transfer proposal")
	default:
		pr = i
	}

	switch done, err := cli.pprof("transfer"); {
	case err != nil:
		return err
	default:
		defer done()

		switch _, j, err := cli.process(pctx, pr, m); {
		case err != nil:
			return errors.WithMessage(err, "process proposal")
		default:
			return j()
		}
	}
}

func (cli *CLI) createTransferOperations(pctx context.Context) ([]Op, error) {
	cli.Log().Debug().Msg("transfer operations")
	defer logging.TimeElapsed()(cli.Log().Info(), "transfer operations generated")

	var pool isaac.NewOperationPool

	if err := util.LoadFromContextOK(pctx, launch.PoolDatabaseContextKey, &pool); err != nil {
		return nil, err
	}

	token := []byte("transfer")

	ops := make([]Op, cli.Operations)

	wk, _ := util.NewBaseJobWorker(context.Background(), cli.c)

	go func() {
		for i := range ops {
			i := i

			issuer := cli.keys[i]

			var receiver base.Privatekey

			switch {
			case i == len(ops)-1:
				receiver = cli.keys[0]
			default:
				receiver = cli.keys[i+1]
			}

			if err := wk.NewJob(func(ctx context.Context, _ uint64) error {
				fact := NewOpFact(token, 1, issuer.Publickey(), receiver.Publickey())
				op := NewOp(fact)
				if err := op.Sign(issuer, cli.networkID); err != nil {
					return err
				}

				_, _ = pool.SetOperation(ctx, op)

				ops[i] = op

				return nil
			}); err != nil {
				break
			}
		}

		wk.Done()
	}()

	if err := wk.Wait(); err != nil {
		return nil, err
	}

	return ops, nil
}

func (cli *CLI) pprof(name string) (func(), error) {
	if !cli.PProf {
		return func() {}, nil
	}

	f, err := os.Create("./cpu-" + name + "-" + util.RFC3339(time.Now()) + ".pprof")
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		return nil, errors.WithStack(err)
	}

	return func() {
		pprof.StopCPUProfile()

		_ = f.Close()
	}, nil
}

func (cli *CLI) newWorkerReadyFunc(
	db isaac.Database,
	bdb isaac.BlockWriteDatabase,
	fsroot string,
	encs *encoder.Encoders,
	pool isaac.NewOperationPool,
	ppm **ProcessManager,
) (
	util.LockedMap[string, *ProcessWorker],
	func(context.Context, string, processWorkerPrepareArgs) error,
) {
	workerpool := util.NewSingleLockedMap[string, *ProcessWorker]()

	return workerpool,
		func(ctx context.Context, local string, margs processWorkerPrepareArgs) error {
			pm := *ppm

			workerroot := filepath.Join(fsroot, local)
			if err := os.MkdirAll(workerroot, 0o700); err != nil {
				return errors.WithMessage(err, "create worker root directory")
			}

			saveOperationFuncs := newWriterSaveOperationsFuncs(
				bdb,
				workerroot,
				encs.Default(),
				"operations.json.gz",
			)

			saveStatesFuncs := newWriterSaveStatesFuncs(
				bdb,
				workerroot,
				encs.Default(),
				"states.json.gz",
			)

			args := NewProcessWorkerArgs(cli.networkID)
			args.GetOperationFunc = func(ctx context.Context, operationhash util.Hash) (Operation, bool, error) {
				switch i, found, err := pool.Operation(ctx, operationhash); {
				case err != nil:
					return nil, false, err
				case !found:
					return nil, false, nil
				default:
					return i.(Operation), true, nil
				}
			}
			args.GetStateFunc = func(_ context.Context, key string) (base.State, bool, error) {
				return db.State(key)
			}
			args.NewJobWorker = func(ctx context.Context) (util.JobWorker, error) {
				return util.NewBaseJobWorker(ctx, cli.c)
			}
			args.IsEnoughWorkers = pm.args.IsEnoughWorkers
			args.StateOwners = func(_ context.Context, key string) ([]string, error) {
				return pm.StateOwners(key)
			}
			args.BroadcastInsertOpFunc = func(_ context.Context, worker string, opindex uint64, fact util.Hash, values []StateMergeValue, owners map[string][]string) error {
				w, _ := workerpool.Value(worker)

				return w.InsertValues(local, opindex, fact, values, owners)
			}
			args.BroadcastInsertValuesDoneFunc = func(context.Context) error {
				workerpool.Traverse(func(worker string, w *ProcessWorker) bool {
					if worker == local {
						return true
					}

					err := w.WorkerInsertValuesDone(local)

					cli.Log().Trace().
						Str("local", local).
						Str("worker", worker).
						Err(err).
						Msg("broadcast insert values done")

					return true
				})

				return nil
			}

			args.BroadcastPhaseDoneFunc = func(_ context.Context, phase int, mergedOps, unmergedOps []uint64) error {
				workerpool.Traverse(func(worker string, w *ProcessWorker) bool {
					if worker == local {
						return true
					}

					err := w.WorkerPhaseDone(local, phase, mergedOps, unmergedOps)

					cli.Log().Trace().
						Str("local", local).
						Str("worker", worker).
						Err(err).
						Int("phase", phase).
						Int("merged_ops", len(mergedOps)).
						Int("unmerged_ops", len(unmergedOps)).
						Msg("broadcast worker phase done")

					return true
				})

				return nil
			}

			args.BroadcastCanceled = func() error {
				return pm.args.WorkerCancel(context.Background(), local)
			}
			args.SaveOperationFuncs = saveOperationFuncs
			args.SaveStateFuncs = saveStatesFuncs
			args.UpdateMergeDoneFunc = func(context.Context) (
				func(base.State) error,
				func(uint64, string) error,
				func() error,
				error,
			) {
				us, ur, done, err := pm.UpdateMerged(local)
				if err != nil {
					return nil, nil, nil, err
				}

				return func(st base.State) error {
						return us(st.Key(), st.Hash())
					},
					ur,
					done,
					nil
			}
			args.UpdateStoreDoneFunc = func(_ context.Context, chunks map[base.BlockItemType]string) error {
				return pm.WorkerStoreDone(local, chunks)
			}

			var pw *ProcessWorker

			switch i, err := NewProcessWorker(
				context.Background(),
				args,
				local,
				margs.Height,
			); {
			case err != nil:
				return err
			default:
				pw = i
				// pw.SetLogging(cli.Logging)

				workerpool.SetValue(local, pw)

				return nil
			}
		}
}
