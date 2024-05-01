package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
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
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	"github.com/spikeekips/mitum/util/valuehash"
)

var pStandaloneManagerDesignString = `
address: managersas
privatekey: 09e151a12a283cd46ac02a425559460e88746c49697e41c4edb9b3122f83b399mpr
network_id: mitum contest; Sat 26 Dec 2020 05:29:13 AM KST
storage:
    base: ./data/manager
`

type CommandManagerStandalone struct { //nolint:govet //...
	baseCommandManager
	Workers uint64 `name:"workers" help:"number of workers" default:"3"`
	workers []string
}

func (cmd *CommandManagerStandalone) Run(pctx context.Context) error {
	if err := cmd.baseCommandManager.prepare(pctx); err != nil {
		return err
	}

	cmd.Log().Debug().
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

func (cmd *CommandManagerStandalone) prepare(pctx context.Context) (context.Context, *ps.PS, error) {
	if cmd.Workers < 1 {
		return nil, nil, errors.Errorf("--workers should be at least 1")
	}

	cmd.Log().Info().Str("string", pStandaloneManagerDesignString).Msg("design")

	pps, pctx := preparePS(
		pctx,
		"cmd-manager",
		pStandaloneManagerDesignString,
		cmd.Operations,
	)

	switch i, err := pps.Run(pctx); {
	case err != nil:
		return pctx, nil, err
	default:
		pctx = i
	}

	if err := cmd.loadDesign(pctx); err != nil {
		return nil, nil, err
	}

	cmd.makeWorkerNames()

	return pctx, pps, nil
}

func (cmd *CommandManagerStandalone) run(pctx context.Context) error {
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

func (cmd *CommandManagerStandalone) createAccounts(pctx context.Context) (base.BlockMap, error) {
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

	switch done, err := cmd.cpuPProf("create-accounts"); {
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

func (cmd *CommandManagerStandalone) processor(
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

func (cmd *CommandManagerStandalone) newManagerFunc(
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

func (cmd *CommandManagerStandalone) makeWorkerNames() {
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

func (cmd *CommandManagerStandalone) process(
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

		saveElapsed(cmd.Log().Info().Interface("blockmap", mp), "saved")
	}

	return mp, ppdone, nil
}

func (cmd *CommandManagerStandalone) transfer(pctx context.Context, m base.Manifest) error {
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

	switch done, err := cmd.cpuPProf("transfer"); {
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

func (cmd *CommandManagerStandalone) newWorkerReadyFunc(
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
				i, found := cmd.opsPool.Get(operationhash.String())

				return i.(Operation), found, nil
			}
			args.GetStateFunc = func(_ context.Context, key string) (base.State, bool, error) {
				i, found := cmd.statesPool.Get(key)

				return i, found, nil
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
						cmd.statesPool.Set(st.Key(), st, 0)

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

type baseCommand struct {
	*logging.Logging `kong:"-"`
	PProf            bool   `name:"pprof"`
	Operations       uint64 `name:"ops" help:"number of operations" default:"${default_operations}"`
	Concurrency      int64  `name:"concurrency" help:"concurrency; default is number of operation <= ${max_concurrency}"`
	local            base.LocalNode
	networkID        base.NetworkID
	c                int64
	encs             *encoder.Encoders
	connectionPool   *quicstream.ConnectionPool
	localCI          quicstream.ConnInfo
	cis              util.LockedMap[string, quicstream.ConnInfo]
	statesPool       util.GCache[string, base.State]
	opsPool          util.GCache[string, base.Operation]
}

func (cmd *baseCommand) prepare(pctx context.Context) error {
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &cmd.Logging); err != nil {
		return err
	}

	switch {
	case cmd.Concurrency > 0:
		cmd.c = cmd.Concurrency
	default:
		cmd.c = int64(cmd.Operations)
		if cmd.c > maxConcurrency {
			cmd.c = maxConcurrency
		}
	}

	switch i, err := util.NewShardedMap[string, quicstream.ConnInfo](1<<9, nil); {
	case err != nil:
		return err
	default:
		cmd.cis = i
	}

	cmd.Log().Debug().
		Int64("concurrency", cmd.c).
		Uint64("operations", cmd.Operations).
		Bool("pprof", cmd.PProf).
		Msg("flags")

	if cmd.PProf {
		switch port, err := availableTCPPort(); {
		case err != nil:
			return err
		default:
			go func() {
				cmd.Log().Info().Str("port", port).Msg("http pprof")

				if err := http.ListenAndServe("localhost:"+port, nil); err != nil {
					cmd.Log().Error().Err(errors.WithStack(err)).Msg("failed http pprof")
				}
			}()
		}
	}

	return nil
}

func (cmd *baseCommand) loadDesign(pctx context.Context) error {
	var design launch.NodeDesign
	var isaacparams *isaac.Params

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &cmd.encs,
		launch.DesignContextKey, &design,
		launch.LocalContextKey, &cmd.local,
		launch.ISAACParamsContextKey, &isaacparams,
		launch.ConnectionPoolContextKey, &cmd.connectionPool,
	); err != nil {
		return err
	}

	cmd.networkID = isaacparams.NetworkID()
	cmd.localCI = design.Network.PublishConnInfo()

	cmd.statesPool = util.NewLFUGCache[string, base.State](isaacparams.StateCacheSize() * 3)
	cmd.opsPool = util.NewLFUGCache[string, base.Operation](int(cmd.Operations) * 2)

	if err := design.LocalParams.Network.SetDefaultHandlerTimeout(time.Hour); err != nil {
		return err
	}

	return nil
}

func (cmd *baseCommand) cpuPProf(name string) (func(), error) {
	if !cmd.PProf {
		return func() {}, nil
	}

	return cpuPProf(name)
}

func (cmd *baseCommand) requestWorkersErrCallback(
	ctx context.Context,
	name string,
	brokerf quicstreamheader.BrokerFunc,
	errf func(worker string, _ error),
) error {
	if cmd.cis.Len() < 1 {
		return nil
	}

	var ew util.JobWorker

	switch i, err := util.NewErrCallbackJobWorker(
		ctx,
		int64(cmd.cis.Len()),
		func(error) {},
	); {
	case err != nil:
		return err
	default:
		defer i.Close()

		ew = i
	}

	_ = cmd.cis.Traverse(func(worker string, ci quicstream.ConnInfo) bool {
		if err := ew.NewJob(func(ctx context.Context, _ uint64) error {
			return cmd.request(ctx, name, brokerf, ci)
		}); err != nil {
			errf(worker, err)
		}

		return true
	})

	ew.Done()

	return ew.Wait()
}

func (cmd *baseCommand) requestWorkers(
	ctx context.Context,
	name string,
	brokerf quicstreamheader.BrokerFunc,
) error {
	if cmd.cis.Len() < 1 {
		return nil
	}

	var ew util.JobWorker

	switch i, err := util.NewErrCallbackJobWorker(
		ctx,
		int64(cmd.cis.Len()),
		func(error) {},
	); {
	case err != nil:
		return err
	default:
		defer i.Close()

		ew = i
	}

	var gerr error

	_ = cmd.cis.Traverse(func(worker string, ci quicstream.ConnInfo) bool {
		if err := ew.NewJob(func(ctx context.Context, _ uint64) error {
			return cmd.request(ctx, name, brokerf, ci)
		}); err != nil {
			gerr = err
		}

		return gerr == nil
	})

	if gerr != nil {
		return gerr
	}

	ew.Done()

	return ew.Wait()
}

func (cmd *baseCommand) requestWorker(
	ctx context.Context,
	name,
	worker string,
	brokerf quicstreamheader.BrokerFunc,
) error {
	switch ci, found := cmd.cis.Value(worker); {
	case !found:
		return util.ErrNotFound.Errorf("unknown worker ci, %q (%v)", worker, cmd.cis.Map())
	case network.EqualConnInfo(ci, cmd.localCI):
		return errors.Errorf("local is not remote worker")
	default:
		return cmd.request(ctx, name, brokerf, ci)
	}
}

func (cmd *baseCommand) request(
	ctx context.Context,
	name string,
	brokerf quicstreamheader.BrokerFunc,
	ci quicstream.ConnInfo,
) error {
	openStream := openStreamFunc(cmd.encs, cmd.connectionPool.Dial)

	if err := requestHandlerFunc(ctx, openStream, brokerf, ci); err != nil {
		cmd.Log().Error().
			Err(err).
			Str("request", name).
			Interface("conn_info", ci).
			Msg("request failed")

		return err
	}

	return nil
}

type baseCommandManager struct {
	baseCommand
	keys []base.Privatekey
}

func (cmd *baseCommandManager) prepare(pctx context.Context) error {
	if err := cmd.baseCommand.prepare(pctx); err != nil {
		return err
	}

	cmd.keys = make([]base.Privatekey, (cmd.Operations))

	return nil
}

func (cmd *baseCommandManager) createAccountKeys(pctx context.Context) error {
	started := time.Now()

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

func (cmd *baseCommandManager) createAccountOperations(pctx context.Context) ([]Op, error) {
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

				cmd.opsPool.Set(op.Hash().String(), op, 0)

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

func (cmd *baseCommandManager) newProposal(
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

func (cmd *baseCommandManager) newINITVoteproof(point base.Point, proposal base.ProposalSignFact) (base.INITVoteproof, error) {
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

func (cmd *baseCommandManager) newACCEPTVoteproof(point base.Point, proposal, newBlock util.Hash) (base.ACCEPTVoteproof, error) {
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

func (cmd *baseCommandManager) createTransferOperations(pctx context.Context) ([]Op, error) {
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

				cmd.opsPool.Set(op.Hash().String(), op, 0)

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

func cpuPProf(name string) (func(), error) {
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

func availableTCPPort() (string, error) {
	switch addr, err := net.ResolveTCPAddr("tcp", "localhost:0"); {
	case err != nil:
		return "", errors.WithStack(err)
	default:
		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return "", errors.WithStack(err)
		}

		defer func() {
			_ = l.Close()
		}()

		return fmt.Sprintf("%d", l.Addr().(*net.TCPAddr).Port), nil //nolint:forcetypeassert //...
	}
}
