package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	"github.com/spikeekips/mitum/util/valuehash"
)

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

type CommandManager struct { //nolint:govet //...
	baseCommandManager
	Port           int                   `arg:"" name:"port"`
	Workers        []quicstream.ConnInfo `arg:"" name:"worker"`
	localCI        quicstream.ConnInfo
	workerCIs      map[string]quicstream.ConnInfo
	connectionPool *quicstream.ConnectionPool
}

func (cmd *CommandManager) Run(pctx context.Context) error {
	if err := cmd.baseCommandManager.prepare(pctx); err != nil {
		return err
	}

	cmd.Log().Debug().
		Interface("workers", cmd.Workers).
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

func (cmd *CommandManager) prepare(pctx context.Context) (context.Context, *ps.PS, error) {
	if len(cmd.Workers) < 1 {
		return nil, nil, errors.Errorf("--workers should be at least 1")
	}

	if util.IsDuplicatedSlice(cmd.Workers, func(i quicstream.ConnInfo) (bool, string) {
		a := i.Addr()
		if a == nil {
			return true, ""
		}

		return true, a.String()
	}) {
		return nil, nil, errors.Errorf("duplicated workers found")
	}

	cmd.workerCIs = map[string]quicstream.ConnInfo{}

	if err := util.TraverseSliceV1(cmd.Workers, func(_ int, i quicstream.ConnInfo) (bool, error) {
		if err := i.IsValid(nil); err != nil {
			return false, err
		}

		cmd.workerCIs[i.String()] = i

		return true, nil
	}); err != nil {
		return nil, nil, err
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

	if err := cmd.loadDesign(pctx); err != nil {
		return nil, nil, err
	}

	return pctx, pps, nil
}

func (cmd *CommandManager) loadDesign(pctx context.Context) error {
	if err := cmd.baseCommandManager.loadDesign(pctx); err != nil {
		return err
	}

	var design launch.NodeDesign

	if err := util.LoadFromContextOK(pctx,
		launch.DesignContextKey, &design,
		launch.ConnectionPoolContextKey, &cmd.connectionPool,
	); err != nil {
		return err
	}

	cmd.localCI = design.Network.PublishConnInfo()

	return nil
}

func (cmd *CommandManager) run(pctx context.Context) error {
	var mp base.BlockMap

	switch i, err := cmd.createAccounts(pctx); {
	case err != nil:
		return errors.WithMessage(err, "create accounts")
	default:
		mp = i
	}

	_ = mp

	if err := cmd.syncStates(base.GenesisHeight); err != nil {
		return err
	}

	//if err := cmd.transfer(pctx, mp.Manifest()); err != nil {
	//	return errors.WithMessage(err, "transfer")
	//}

	return nil
}

func (cmd *CommandManager) createAccounts(pctx context.Context) (base.BlockMap, error) {
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

	if err := cmd.syncOperations(ops); err != nil {
		return nil, err
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

func (cmd *CommandManager) process(
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

func (cmd *CommandManager) processor(
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

func (cmd *CommandManager) newManagerFunc(
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
	height := proposal.Point().Height()

	var pm *ProcessManager

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
		workers := make([]string, len(cmd.Workers))
		for i := range cmd.Workers {
			workers[i] = cmd.Workers[i].String()
		}

		args := NewProcessManagerArgs(workers)
		args.IsInStateOperationFunc = func(_ context.Context, fact util.Hash) (bool, error) {
			return db.ExistsInStateOperation(fact)
		}
		args.NewJobWorker = func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, cmd.c)
		}
		args.WorkerReady = func(ctx context.Context, worker string, args processWorkerReadyArgs) error {
			return cmd.requestWorker(
				ctx,
				worker,
				newWorkerBrokerReadyFunc(args),
			)
		}
		args.WorkerCancel = func(_ context.Context, worker string) error {
			return cmd.requestWorker(
				ctx,
				worker,
				newWorkerBrokerCancelFunc(height),
			)
		}
		args.WorkerDone = func(_ context.Context, worker string) error {
			return cmd.requestWorker(
				ctx,
				worker,
				newWorkerBrokerDoneFunc(height),
			)
		}
		args.WorkerNewOperations = func(ctx context.Context, worker string) (
			workerNewOperationsFunc,
			func() error,
			func(error),
			error,
		) {
			return cmd.workerNewOperation(ctx, height, worker)
		}
		args.IsEnoughWorkers = func(workers uint64) bool {
			return workers == uint64(len(cmd.Workers))
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

func (cmd *CommandManager) workerNewOperation(
	ctx context.Context,
	height base.Height,
	worker string,
) (
	workerNewOperationsFunc,
	func() error,
	func(error),
	error,
) {
	ch := make(chan [3]interface{})

	insert := func(opindex uint64, op, fact util.Hash) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		ch <- [3]interface{}{opindex, op, fact}

		return nil
	}

	_, done, cancel := NewContextDoneFunc(
		ctx,
		func(context.Context) error {
			return cmd.requestWorker(
				ctx,
				worker,
				newWorkerBrokerNewOperationsFunc(
					height,
					func(f func(opindex uint64, op, fact util.Hash) error) error {
						for {
							select {
							case <-ctx.Done():
								return ctx.Err()
							case i := <-ch:
								if err := f(i[0].(uint64), i[1].(util.Hash), i[2].(util.Hash)); err != nil {
									return err
								}
							}
						}

						return nil
					},
				),
			)
		},
	)

	return insert, func() error {
		close(ch)

		return done()
	}, cancel, nil
}

func (cmd *CommandManager) syncStates(height base.Height) error {
	return cmd.requestWorkers(
		context.Background(),
		newWorkerBrokerSyncStatesFunc(height),
	)
}

func (cmd *CommandManager) syncOperations(ops []Op) error {
	return cmd.requestWorkers(
		context.Background(),
		newWorkerBrokerSetOperationsFunc(
			func(f func(base.Operation) error) error {
				for i := range ops {
					if err := f(ops[i]); err != nil {
						return err
					}
				}

				return nil
			},
		),
	)
}
