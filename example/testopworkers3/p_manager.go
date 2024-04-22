package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
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
	"github.com/spikeekips/mitum/util/valuehash"
)

var maxConcurrency int64 = 1 << 14

var pManagerDesignString = `
address: managersas
privatekey: 09e151a12a283cd46ac02a425559460e88746c49697e41c4edb9b3122f83b399mpr
network_id: mitum contest; Sat 26 Dec 2020 05:29:13 AM KST
storage:
    base: ./data/manager
network:
  bind: 0.0.0.0:%d
  publish: localhost:%d
  tls_insecure: true
`

type PManager struct { //nolint:govet //...
	*logging.Logging `kong:"-"`
	Operations       uint64 `name:"ops" help:"number of operations" default:"333"`
	Concurrency      int64  `name:"concurrency" help:"concurrency; default is number of operation <= ${max_concurrency}"`
	Port             int    `arg:"" name:"network port"`
	Workers          uint64 `name:"workers" help:"number of workers" default:"3"`
	PProf            bool   `name:"pprof"`
	local            base.LocalNode
	networkID        base.NetworkID
	keys             []base.Privatekey
	c                int64
	workers          []string
}

func (cmd *PManager) Run(pctx context.Context) error {
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &cmd.Logging); err != nil {
		return err
	}

	cmd.Log().Debug().
		Int("port", cmd.Port).
		Uint64("operations", cmd.Operations).
		Int64("concurrency", cmd.Concurrency).
		Bool("pprof", cmd.PProf).
		Uint64("workers", cmd.Workers).
		Msg("flags")

	switch i, pps, err := cmd.prepare(pctx); {
	case err != nil:
		return err
	default:
		defer func() {
			_, _ = pps.Close(pctx)
		}()

		pctx = i
	}

	return cmd.run(pctx)
}

func (cmd *PManager) prepare(pctx context.Context) (context.Context, *ps.PS, error) {
	if cmd.Port <= 1024 {
		return nil, nil, errors.Errorf("invalid port, %d", cmd.Port)
	}

	if cmd.Workers < 1 {
		return nil, nil, errors.Errorf("--workers should be at least 1")
	}

	designString := fmt.Sprintf(
		pManagerDesignString,
		cmd.Port,
		cmd.Port,
	)

	cmd.Log().Info().Str("string", designString).Msg("design")

	pps, pctx := preparePS(
		pctx,
		"cmd-manager",
		designString,
		cmd.Operations,
	)

	switch i, err := pps.Run(pctx); {
	case err != nil:
		return pctx, nil, err
	default:
		pctx = i
	}

	var isaacparams *isaac.Params

	if err := util.LoadFromContextOK(pctx,
		launch.ISAACParamsContextKey, &isaacparams,
	); err != nil {
		return nil, nil, err
	}

	cmd.Log().Debug().Interface("main_process", pps.Verbose()).Msg("processed")

	cmd.networkID = isaacparams.NetworkID()

	switch {
	case cmd.Concurrency > 0:
		cmd.c = cmd.Concurrency
	default:
		cmd.c = int64(cmd.Operations)
		if cmd.c > maxConcurrency {
			cmd.c = maxConcurrency
		}
	}

	cmd.makeWorkerNames()

	return pctx, pps, nil
}

func (cmd *PManager) run(pctx context.Context) error {
	var mp base.BlockMap

	switch i, err := cmd.createAccounts(pctx); {
	case err != nil:
		return errors.WithMessage(err, "create accounts")
	default:
		mp = i
	}

	if err := cmd.transfer(pctx, mp.Manifest()); err != nil {
		return errors.WithMessage(err, "transfer")
	}

	return nil
}

func (cmd *PManager) createAccounts(pctx context.Context) (base.BlockMap, error) {
	if err := cmd.createAccountKeys(pctx); err != nil {
		return nil, errors.WithMessage(err, "keys")
	}

	var ops []Op

	switch i, err := cmd.createAccountOperations(pctx); {
	case err != nil:
		return nil, errors.WithMessage(err, "create account operations")
	default:
		ops = i
	}

	var pr base.ProposalSignFact

	switch i, err := cmd.newProposal(pctx, base.GenesisHeight, valuehash.RandomSHA256(), ops); {
	case err != nil:
		return nil, errors.WithMessage(err, "create account proposal")
	default:
		pr = i
	}

	switch done, err := cmd.pprof("create-accounts"); {
	case err != nil:
		return nil, err
	default:
		defer done()

		switch i, j, err := cmd.process(pctx, pr, nil); {
		case err != nil:
			return nil, errors.WithMessage(err, "process proposal")
		default:
			return i, j()
		}
	}
}

func (cmd *PManager) createAccountKeys(pctx context.Context) error {
	started := time.Now()

	if err := util.LoadFromContextOK(pctx,
		launch.LocalContextKey, &cmd.local,
	); err != nil {
		return err
	}

	cmd.keys = make([]base.Privatekey, (cmd.Operations))

	wk, _ := util.NewBaseJobWorker(context.Background(), cmd.c)

	go func() {
		for i := range cmd.keys {
			i := i

			if err := wk.NewJob(func(context.Context, uint64) error {
				cmd.keys[i] = base.NewMPrivatekey()

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

	cmd.Log().Debug().
		Stringer("elapsed", time.Since(started)).
		Int("accounts", len(cmd.keys)).
		Msg("account keys generated")

	return nil
}

func (cmd *PManager) createAccountOperations(pctx context.Context) ([]Op, error) {
	cmd.Log().Debug().Msg("create operations")
	defer logging.TimeElapsed()(cmd.Log().Info(), "create operations generated")

	var pool isaac.NewOperationPool

	if err := util.LoadFromContextOK(pctx, launch.PoolDatabaseContextKey, &pool); err != nil {
		return nil, err
	}

	token := []byte("create-accounts")

	ops := make([]Op, len(cmd.keys))

	wk, _ := util.NewBaseJobWorker(context.Background(), cmd.c)

	go func() {
		for i := range ops {
			i := i

			if err := wk.NewJob(func(ctx context.Context, _ uint64) error {
				fact := NewOpFact(token, 1, nil, cmd.keys[i].Publickey())
				op := NewOp(fact)
				if err := op.Sign(cmd.local.Privatekey(), cmd.networkID); err != nil {
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

func (cmd *PManager) newProposal(
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
		cmd.local.Address(),
		previousBlock,
		oppair,
	)

	sf := isaac.NewProposalSignFact(fact)
	if err := sf.Sign(cmd.local.Privatekey(), cmd.networkID); err != nil {
		return nil, err
	}

	switch isset, err := pool.SetProposal(sf); {
	case err != nil:
		return nil, err
	case !isset:
		return nil, errors.Errorf("not set")
	}

	cmd.Log().Debug().
		Stringer("elapsed", time.Since(started)).
		Msg("proposal generated")

	return sf, nil
}

func (cmd *PManager) processor(
	pctx context.Context,
	proposal base.ProposalSignFact,
	previous base.Manifest,
) (isaac.ProposalProcessor, func() error, error) {
	var encs *encoder.Encoders
	var design launch.NodeDesign
	var isaacparams *isaac.Params
	var db isaac.Database
	var pool isaac.NewOperationPool

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &encs,
		launch.DesignContextKey, &design,
		launch.ISAACParamsContextKey, &isaacparams,
		launch.CenterDatabaseContextKey, &db,
		launch.PoolDatabaseContextKey, &pool,
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
		return util.NewBaseJobWorker(ctx, cmd.c)
	}
	args.SignBlockMapItem = func(bm isaacblock.BlockMap) error {
		return bm.Sign(cmd.local.Address(), cmd.local.Privatekey(), cmd.networkID)
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

	switch i, err := cmd.newManagerFunc(db, bdb, fsroot, encs, pool, previousManifest, previousSuffrage, proposal); {
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
		pp.SetLogging(cmd.Logging)

		return pp,
			func() error {
				return db.MergeBlockWriteDatabase(bdb)
			},
			nil
	}
}

func (cmd *PManager) newManagerFunc(
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

	workerpool, f := cmd.newWorkerReadyFunc(db, bdb, fsroot, encs, pool, &pm)

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
		args := NewProcessManagerArgs(cmd.workers)
		args.IsInStateOperationFunc = func(_ context.Context, fact util.Hash) (bool, error) {
			return db.ExistsInStateOperation(fact)
		}
		args.NewJobWorker = func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, cmd.c)
		}
		args.WorkerReady = func(ctx context.Context, worker string, args processWorkerReadyArgs) error {
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
			return workers == cmd.Workers
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
			pm.SetLogging(cmd.Logging)

			return pm, nil
		}
	}, nil
}

func (cmd *PManager) makeWorkerNames() {
	l := len(strconv.FormatUint(cmd.Workers, 10))
	if l < 2 {
		l = 2
	}

	f := fmt.Sprintf("w%%0%dd", l)

	cmd.workers = make([]string, cmd.Workers)
	for i := range cmd.workers {
		cmd.workers[i] = fmt.Sprintf(f, i)
	}
}

func (cmd *PManager) process(
	pctx context.Context,
	pr base.ProposalSignFact,
	previous base.Manifest,
) (base.BlockMap, func() error, error) {
	cmd.Log().Debug().Msg("processing")

	var pps isaac.ProposalProcessor
	var ppdone func() error

	switch i, j, err := cmd.processor(pctx, pr, previous); {
	case err != nil:
		return nil, nil, err
	default:
		defer i.Cancel()

		pps = i
		ppdone = j
	}

	var ivp base.INITVoteproof

	switch i, err := cmd.newINITVoteproof(pr.Point(), pr); {
	case err != nil:
		return nil, nil, err
	default:
		ivp = i
	}

	defer logging.TimeElapsed()(cmd.Log().Info(), "processing done")

	var m base.Manifest

	proposalProcessingElapsed := logging.TimeElapsed()

	switch i, err := pps.Process(context.Background(), ivp); {
	case err != nil:
		return nil, nil, err
	default:
		m = i

		proposalProcessingElapsed(cmd.Log().Debug().Interface("manifest", m), "proposal processed")
	}

	saveElapsed := logging.TimeElapsed()

	var avp base.ACCEPTVoteproof

	switch i, err := cmd.newACCEPTVoteproof(pr.Point(), pr.Fact().Hash(), m.Hash()); {
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

		saveElapsed(cmd.Log().Info().Interface("blockmap", mp), "proposal saved")
	}

	return mp, ppdone, nil
}

func (cmd *PManager) newINITVoteproof(point base.Point, proposal base.ProposalSignFact) (base.INITVoteproof, error) {
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
	if err := sf.NodeSign(cmd.local.Privatekey(), cmd.networkID, cmd.local.Address()); err != nil {
		return nil, err
	}

	if err := sf.IsValid(cmd.networkID); err != nil {
		return nil, err
	}

	vp := isaac.NewINITVoteproof(fact.Point().Point)
	vp.
		SetMajority(fact).
		SetSignFacts([]base.BallotSignFact{sf}).
		SetThreshold(base.MaxThreshold).
		Finish()

	if err := vp.IsValid(cmd.networkID); err != nil {
		return nil, err
	}

	return vp, nil
}

func (cmd *PManager) newACCEPTVoteproof(point base.Point, proposal, newBlock util.Hash) (base.ACCEPTVoteproof, error) {
	fact := isaac.NewACCEPTBallotFact(point, proposal, newBlock, nil)
	if err := fact.IsValid(nil); err != nil {
		return nil, err
	}

	sf := isaac.NewACCEPTBallotSignFact(fact)
	if err := sf.NodeSign(cmd.local.Privatekey(), cmd.networkID, cmd.local.Address()); err != nil {
		return nil, err
	}

	if err := sf.IsValid(cmd.networkID); err != nil {
		return nil, err
	}

	vp := isaac.NewACCEPTVoteproof(fact.Point().Point)
	vp.
		SetMajority(fact).
		SetSignFacts([]base.BallotSignFact{sf}).
		SetThreshold(base.MaxThreshold).
		Finish()

	if err := vp.IsValid(cmd.networkID); err != nil {
		return nil, err
	}

	return vp, nil
}

func (cmd *PManager) transfer(pctx context.Context, m base.Manifest) error {
	var ops []Op

	switch i, err := cmd.createTransferOperations(pctx); {
	case err != nil:
		return errors.WithMessage(err, "transfer operations")
	default:
		ops = i
	}

	var pr base.ProposalSignFact

	switch i, err := cmd.newProposal(pctx, m.Height()+1, m.Hash(), ops); {
	case err != nil:
		return errors.WithMessage(err, "transfer proposal")
	default:
		pr = i
	}

	switch done, err := cmd.pprof("transfer"); {
	case err != nil:
		return err
	default:
		defer done()

		switch _, j, err := cmd.process(pctx, pr, m); {
		case err != nil:
			return errors.WithMessage(err, "process proposal")
		default:
			return j()
		}
	}
}

func (cmd *PManager) createTransferOperations(pctx context.Context) ([]Op, error) {
	cmd.Log().Debug().Msg("transfer operations")
	defer logging.TimeElapsed()(cmd.Log().Info(), "transfer operations generated")

	var pool isaac.NewOperationPool

	if err := util.LoadFromContextOK(pctx, launch.PoolDatabaseContextKey, &pool); err != nil {
		return nil, err
	}

	token := []byte("transfer")

	ops := make([]Op, cmd.Operations)

	wk, _ := util.NewBaseJobWorker(context.Background(), cmd.c)

	go func() {
		for i := range ops {
			i := i

			issuer := cmd.keys[i]

			var receiver base.Privatekey

			switch {
			case i == len(ops)-1:
				receiver = cmd.keys[0]
			default:
				receiver = cmd.keys[i+1]
			}

			if err := wk.NewJob(func(ctx context.Context, _ uint64) error {
				fact := NewOpFact(token, 1, issuer.Publickey(), receiver.Publickey())
				op := NewOp(fact)
				if err := op.Sign(issuer, cmd.networkID); err != nil {
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

func (cmd *PManager) pprof(name string) (func(), error) {
	if !cmd.PProf {
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

func (cmd *PManager) newWorkerReadyFunc(
	db isaac.Database,
	bdb isaac.BlockWriteDatabase,
	fsroot string,
	encs *encoder.Encoders,
	pool isaac.NewOperationPool,
	ppm **ProcessManager,
) (
	util.LockedMap[string, *ProcessWorker],
	func(context.Context, string, processWorkerReadyArgs) error,
) {
	workerpool := util.NewSingleLockedMap[string, *ProcessWorker]()

	return workerpool,
		func(ctx context.Context, local string, margs processWorkerReadyArgs) error {
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

			args := NewProcessWorkerArgs(cmd.networkID)
			args.NOps = margs.NOps
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
				return util.NewBaseJobWorker(ctx, cmd.c)
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

					cmd.Log().Trace().
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

					cmd.Log().Trace().
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
				// pw.SetLogging(cmd.Logging)

				workerpool.SetValue(local, pw)

				return nil
			}
		}
}
