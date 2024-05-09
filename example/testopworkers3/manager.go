package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	mathrand "math/rand"
	"slices"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	errProcessManagerWorkersMergeDone       = util.NewIDError("process manager workers merge done")
	errProcessManagerWorkersStoreDone       = util.NewIDError("process manager workers store done")
	errProcessManagerStoreDone              = util.NewIDError("process manager store done")
	errProcessManagerWorkersWorkersCanceled = util.NewIDError("process manager; workers canceled")
	errProcessManagerWorkersDone            = util.NewIDError("process manager done")
)

type (
	opOwnersFunc            func(opindex uint64, op, fact util.Hash) ([]string, error)
	opOwnersFunc2           func([][2]util.Hash) ([][][3]interface{} /* [][]{opindex uint8, op, fact util.Hash} */, error)
	stateOwnersFunc         func(stateKey string) ([]string, error)
	workerNewOperationsFunc func(opindex uint64, op, fact util.Hash) error
)

type processWorkerReadyArgs struct {
	Manager     string
	Height      base.Height
	Workers     []string
	NStateShare uint8
	NOps        uint64
	FullOps     bool
}

type ProcessManagerArgs struct {
	Local               string
	Workers             []string
	NOpShare            uint8
	NStateShare         uint8
	NewJobWorker        func(context.Context) (util.JobWorker, error)
	WorkerReady         func(_ context.Context, worker string, _ processWorkerReadyArgs) error
	WorkerCancel        func(_ context.Context, worker string) error
	WorkerDone          func(_ context.Context, worker string) error
	WorkerNewOperations func(_ context.Context, worker string) (
		_ workerNewOperationsFunc,
		done func() error,
		cancel func(error),
		_ error,
	)
	IsEnoughWorkers             func(workers uint64) bool // NOTE include local worker
	IsInStateOperationFunc      func(_ context.Context, fact util.Hash) (bool, error)
	NewOpOwnersFunc2            func(workers uint64, share uint8) (opOwnersFunc2, error)
	NewStateOwnersFunc          func(workers []string, share uint8) (stateOwnersFunc, error)
	MapSize                     uint64
	MinStatesSize               uint64
	WorkerCancelTimeout         time.Duration
	NewManifestFunc             func(context.Context, base.Manifest) error
	SaveStatesTreeNodeFuncs     writerSaveTreeNodeFuncs
	SaveOperationsTreeNodeFuncs writerSaveTreeNodeFuncs
}

func NewProcessManagerArgs(workers []string) *ProcessManagerArgs {
	return &ProcessManagerArgs{
		Workers:     workers,
		NOpShare:    1,
		NStateShare: 1,
		NewJobWorker: func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, 1<<6)
		},
		WorkerReady: func(context.Context, string, processWorkerReadyArgs) error {
			return util.ErrNotImplemented.Errorf("WorkerReady")
		},
		WorkerCancel: func(context.Context, string) error {
			panic(util.ErrNotImplemented.Errorf("WorkerCancel"))
		},
		WorkerDone: func(context.Context, string) error {
			return util.ErrNotImplemented.Errorf("WorkerDone")
		},
		WorkerNewOperations: func(context.Context, string) (
			workerNewOperationsFunc,
			func() error,
			func(error),
			error,
		) {
			return nil, nil, nil, util.ErrNotImplemented.Errorf("WorkerNewOperations")
		},

		IsInStateOperationFunc: func(context.Context, util.Hash) (bool, error) {
			return false, util.ErrNotImplemented.Errorf("IsInStateOperationFunc")
		},
		NewOpOwnersFunc2:    newRegionalOpOwnersFunc,
		NewStateOwnersFunc:  serialStateOwnersFunc, // djb2StateOwnersFunc
		MapSize:             1 << 9,
		MinStatesSize:       1 << 18,
		WorkerCancelTimeout: time.Second * 3,
		NewManifestFunc: func(context.Context, base.Manifest) error {
			return util.ErrNotImplemented.Errorf("NewManifestFunc")
		},
		SaveStatesTreeNodeFuncs: func(context.Context) (writerSaveTreeNodeFunc, writerCloseSaveFunc, error) {
			return nil, nil, util.ErrNotImplemented.Errorf("SaveStatesTreeNodeFuncs")
		},
		SaveOperationsTreeNodeFuncs: func(context.Context) (writerSaveTreeNodeFunc, writerCloseSaveFunc, error) {
			return nil, nil, util.ErrNotImplemented.Errorf("SaveOperationsTreeNodeFuncs")
		},
	}
}

type ProcessManager struct {
	*logging.Logging
	args                      *ProcessManagerArgs
	previousManifest          util.Hash
	previousSuffrage          util.Hash
	proposal                  base.ProposalSignFact
	ctx                       func() context.Context
	ctxCancel                 func(error)
	workers                   []string
	mergeDoneCtx              func() context.Context
	mergeDoneCtxCancel        func(error)
	workersStoreDoneCtx       func() context.Context
	workersStoreDoneCtxCancel func(error)
	storeDoneCtx              func() context.Context
	storeDoneCtxCancel        func(error)
	ops                       [][2]util.Hash /* {operation hash, fact hash} */
	workersMergeDone          *util.Locked[map[ /* worker */ string] /* phase */ struct{}]
	workersStoreDone          *util.Locked[map[ /* worker */ string]struct{}]
	storeJobWorker            util.JobWorker
	newOpOwnersFunc2          opOwnersFunc2
	stateOwners               util.LockedMap[ /* state key */ string /* workers */, []string]
	newStateOwnersFunc        func(string) ([]string, error)
	cleanf                    func()
	states                    util.LockedMap[ /* state key */ string /* state hash */, util.Hash]
	reasons                   util.LockedMap[ /* opindex */ uint64 /* reason */, string]
	addFailedWorker           func(worker string, _ error) bool
	isWorkersFailed           func() error
	isFailedWorker            func(worker string) bool
	failedWorkers             func() []string
	blockFiles                util.LockedMap[base.BlockItemType, []string]
	statesTreeWriter          func(create bool) (*baseFixedTreeWriter[string], error)
	operationsTreeWriter      func(create bool) (*baseFixedTreeWriter[uint64], error)
	manifest                  *util.Locked[base.Manifest]
}

func NewProcessManager(
	ctx context.Context,
	args *ProcessManagerArgs,
	previousManifest, previousSuffrage util.Hash,
	proposal base.ProposalSignFact,
) (*ProcessManager, error) {
	ops := proposal.ProposalFact().Operations()

	if err := validateProcessManagerArgs(args, len(ops) < 1); err != nil {
		return nil, err
	}

	switch i, err := args.NewJobWorker(ctx); {
	case err != nil:
		return nil, err
	default:
		i.Close()
	}

	pmctx, ctxCancel := context.WithCancelCause(ctx)
	mergeDoneCtx, mergeDoneCtxCancel := context.WithCancelCause(pmctx)
	workersStoreDoneCtx, workersStoreDoneCtxCancel := context.WithCancelCause(pmctx)
	storeDoneCtx, storeDoneCtxCancel := context.WithCancelCause(pmctx)

	pm := &ProcessManager{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "process-manager")
		}),
		args:                      args,
		previousManifest:          previousManifest,
		previousSuffrage:          previousSuffrage,
		proposal:                  proposal,
		ctx:                       func() context.Context { return pmctx },
		ctxCancel:                 ctxCancel,
		workers:                   args.Workers,
		mergeDoneCtx:              func() context.Context { return mergeDoneCtx },
		mergeDoneCtxCancel:        mergeDoneCtxCancel,
		workersStoreDoneCtx:       func() context.Context { return workersStoreDoneCtx },
		workersStoreDoneCtxCancel: workersStoreDoneCtxCancel,
		storeDoneCtx:              func() context.Context { return storeDoneCtx },
		storeDoneCtxCancel:        storeDoneCtxCancel,
		ops:                       ops,
	}

	pm.workersMergeDone = util.NewLocked[map[string]struct{}](map[string]struct{}{})
	pm.workersStoreDone = util.NewLocked[map[string]struct{}](map[string]struct{}{})
	pm.manifest = util.EmptyLocked[base.Manifest]()

	pm.storeJobWorker, _ = args.NewJobWorker(pmctx)

	workerMapSize := uint64(len(args.Workers))

	switch i, err := util.NewLockedMap[string, []string](workerMapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pm.stateOwners = i
	}

	switch i, err := util.NewShardedMap[string, util.Hash](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pm.states = i
	}

	switch i, err := util.NewShardedMap[uint64, string](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pm.reasons = i
	}

	switch i, err := util.NewLockedMap[base.BlockItemType, []string](workerMapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pm.blockFiles = i
	}

	switch i, err := args.NewStateOwnersFunc(pm.workers, args.NStateShare); {
	case err != nil:
		return nil, err
	default:
		pm.newStateOwnersFunc = newStateOwnersFunc(i)
	}

	pm.newStatesTreeWriterFunc()
	pm.newOperationsTreeWriterFunc()

	switch i, err := args.NewOpOwnersFunc2(uint64(len(pm.workers)), args.NOpShare); {
	case err != nil:
		return nil, err
	default:
		pm.newOpOwnersFunc2 = i
	}

	clearFailedWorkers := pm.attachFailedWorkers()

	pm.cleanf = func() {
		pm.ctxCancel(nil)

		clearFailedWorkers()

		pm.storeJobWorker.Close()
		pm.stateOwners.Close()
		pm.states.Close()
		pm.workersMergeDone.EmptyValue()
		pm.workersStoreDone.EmptyValue()

		if i, err := pm.statesTreeWriter(false); err == nil && i != nil {
			i.Cancel()
		}

		if i, err := pm.operationsTreeWriter(false); err == nil && i != nil {
			i.Cancel()
		}
	}

	return pm, nil
}

func (pm *ProcessManager) Cancel() {
	pm.ctxCancel(nil)
	pm.cleanf()
}

func (pm *ProcessManager) ReadyWorkers() error {
	if len(pm.ops) < 1 {
		return nil
	}

	if err := pm.readyWorkers(); err != nil {
		pm.ctxCancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) StateOwners(stateKey string) (owners []string, _ error) {
	if err := pm.stateOwners.GetOrCreate(
		stateKey,
		func(i []string, _ bool) error {
			owners = i

			return nil
		},
		func() ([]string, error) {
			return pm.newStateOwnersFunc(stateKey)
		},
	); err != nil {
		return nil, err
	}

	return owners, nil
}

func (pm *ProcessManager) Process() error {
	if err := pm.process(); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) WorkerStoreDone(worker string, chunks map[base.BlockItemType]string) error {
	if err := pm.workerStoreDone(worker, chunks); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) ManifestDone() (base.Manifest, error) {
	if err := pm.mergeDone(); err != nil {
		pm.cancel(err)

		return nil, err
	}

	return pm.getOrCreateManifest()
}

func (pm *ProcessManager) StoreDone() error {
	if err := pm.workersStoreCtxDone(); err != nil {
		pm.ctxCancel(err)

		return err
	}

	if err := pm.storeDone(); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) Done() (bf map[base.BlockItemType]BlockItemFile, _ error) {
	defer pm.Cancel()

	if err := pm.mergeDone(); err != nil {
		pm.ctxCancel(err)

		return nil, err
	}

	if err := pm.workersStoreCtxDone(); err != nil {
		pm.ctxCancel(err)

		return nil, err
	}

	if err := pm.storeDone(); err != nil {
		pm.ctxCancel(err)

		return nil, err
	}

	if err := pm.workersDone(); err != nil {
		pm.ctxCancel(err)

		return nil, err
	}

	switch i, err := pm.newBlockFiles(); {
	case err != nil:
		pm.ctxCancel(err)

		return nil, err
	default:
		bf = i
	}

	pm.ctxCancel(errProcessManagerWorkersDone)

	return bf, nil
}

func (pm *ProcessManager) UpdateMerged(worker string) (
	updateState func(stateKey string, stateHash util.Hash) error,
	updateReason func(opindex uint64, reason string) error,
	done func() error,
	_ error,
) {
	return pm.updateState,
		pm.updatedReason,
		func() error {
			return pm.workerMergeDone(worker)
		},
		nil
}

func (pm *ProcessManager) WorkerCanceled(worker string) error {
	if pm.addFailedWorker(worker, context.Canceled) {
		if err := pm.isWorkersFailed(); err != nil {
			pm.ctxCancel(errProcessManagerWorkersWorkersCanceled)
		}
	}

	return nil
}

func (pm *ProcessManager) updateState(stateKey string, stateHash util.Hash) error {
	var writer *baseFixedTreeWriter[string]

	switch i, err := pm.statesTreeWriter(true); {
	case err != nil:
		return err
	default:
		writer = i
	}

	if _, _, err := pm.states.Set(
		stateKey,
		func(_ util.Hash, found bool) (util.Hash, error) {
			if found {
				return nil, util.ErrLockedSetIgnore
			}

			if err := writer.Add(stateKey, fixedtree.NewBaseNode(stateHash.String())); err != nil {
				return nil, err
			}

			return stateHash, nil
		},
	); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) updatedReason(
	opindex uint64,
	reason string,
) error {
	var writer *baseFixedTreeWriter[uint64]

	switch i, err := pm.operationsTreeWriter(true); {
	case err != nil:
		return err
	default:
		writer = i
	}

	if _, _, err := pm.reasons.Set(opindex, func(_ string, found bool) (string, error) {
		switch {
		case found, len(reason) < 1:
			return "", util.ErrLockedSetIgnore
		default:
			if err := writer.Add(
				opindex,
				base.NewNotInStateOperationFixedtreeNode(pm.ops[opindex][1], reason),
			); err != nil {
				return "", err
			}

			return reason, nil
		}
	}); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) cancel(err error) {
	pm.ctxCancel(err)

	pm.Cancel()
}

func (pm *ProcessManager) process() error {
	defer logging.TimeElapsedMsg(pm.Log().Debug, "process")()

	go func() {
		<-pm.ctx().Done()

		switch err := context.Cause(pm.ctx()); {
		case errors.Is(err, errProcessManagerWorkersDone),
			errors.Is(err, errProcessManagerWorkersWorkersCanceled):
		default:
			pm.cancelWorkers()
		}
	}()

	manifestElapsed := logging.TimeElapsed()

	switch i := len(pm.ops); {
	case i < 1:
		pm.mergeDoneCtxCancel(errProcessManagerWorkersMergeDone)
		pm.workersStoreDoneCtxCancel(errProcessManagerWorkersStoreDone)
	default:
		if err := pm.newNonEmptyNewOperationFunc(); err != nil {
			pm.ctxCancel(err)

			return err
		}
	}

	if err := pm.mergeDone(); err != nil {
		return err
	}

	defer manifestElapsed(pm.Log().Info(), "manifest done")

	if _, err := pm.getOrCreateManifest(); err != nil {
		pm.cancel(err)

		return err
	}

	if err := pm.storeJobWorker.Wait(); err != nil {
		pm.cancel(err)

		return err
	}

	pm.storeDoneCtxCancel(errProcessManagerStoreDone)

	pm.Log().Debug().Msg("store done")

	return nil
}

func (pm *ProcessManager) readyWorkers() error {
	var ew util.JobWorker

	switch i, err := pm.args.NewJobWorker(pm.ctx()); {
	case err != nil:
		return err
	default:
		defer i.Close()

		ew = i
	}

	args := processWorkerReadyArgs{
		Manager:     pm.args.Local,
		Height:      pm.proposal.Point().Height(),
		Workers:     pm.workers,
		NStateShare: pm.args.NStateShare,
		NOps:        uint64(len(pm.ops)),
	}

	if err := util.TraverseSliceV1(pm.workers, func(_ int, worker string) (bool, error) {
		return true, ew.NewJob(func(ctx context.Context, _ uint64) error {
			if err := pm.args.WorkerReady(ctx, worker, args); err != nil {
				if errors.Is(err, util.ErrNotImplemented) {
					return err
				}

				pm.Log().Error().Err(err).Str("worker", worker).Msg("worker failed ready")

				pm.addFailedWorker(worker, err)
			}

			return nil
		})
	}); err != nil {
		return err
	}

	ew.Done()

	if err := ew.Wait(); err != nil {
		return err
	}

	if err := pm.isWorkersFailed(); err != nil {
		return err
	}

	switch faileds := pm.failedWorkers(); {
	case len(faileds) < 1:
	default:
		pm.workers = util.Filter2Slices(pm.workers, faileds, func(a, b string) bool { return a == b })
	}

	return nil
}

func (pm *ProcessManager) mergeDone() error {
	<-pm.mergeDoneCtx().Done()

	switch err := context.Cause(pm.mergeDoneCtx()); {
	case errors.Is(err, errProcessManagerWorkersMergeDone):
	case err != nil:
		return err
	}

	return nil
}

func (pm *ProcessManager) workersStoreCtxDone() error {
	<-pm.workersStoreDoneCtx().Done()

	switch err := context.Cause(pm.workersStoreDoneCtx()); {
	case errors.Is(err, errProcessManagerWorkersStoreDone):
	case err != nil:
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) storeDone() error {
	<-pm.storeDoneCtx().Done()

	switch err := context.Cause(pm.storeDoneCtx()); {
	case errors.Is(err, errProcessManagerStoreDone):
	case err != nil:
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) workerMergeDone(worker string) error {
	l := pm.Log().With().Str("worker", worker).Logger()

	var count uint64

	if _, err := pm.workersMergeDone.Set(func(m map[string]struct{}, _ bool) (map[string]struct{}, error) {
		if m == nil {
			return nil, util.ErrLockedSetIgnore // NOTE already done
		}

		if _, workerFound := m[worker]; workerFound {
			return nil, errors.Errorf("worker, %q already merge done", worker)
		}

		m[worker] = struct{}{}
		count = uint64(len(m))

		return m, nil
	}); err != nil {
		return err
	}

	if count < 1 {
		l.Debug().Msg("already merge done")

		return nil
	}

	if pm.args.IsEnoughWorkers(count) {
		l.Debug().Msg("enough workers merge done")

		pm.mergeDoneCtxCancel(errProcessManagerWorkersMergeDone)
	}

	return nil
}

func (pm *ProcessManager) workerStoreDone(worker string, chunks map[base.BlockItemType]string) error {
	l := pm.Log().With().Str("worker", worker).Logger()

	var count uint64

	if _, err := pm.workersStoreDone.Set(func(m map[string]struct{}, _ bool) (map[string]struct{}, error) {
		if m == nil {
			return nil, util.ErrLockedSetIgnore // NOTE already done
		}

		if _, workerFound := m[worker]; workerFound {
			return nil, errors.Errorf("worker, %q already store done", worker)
		}

		m[worker] = struct{}{}
		count = uint64(len(m))

		return m, nil
	}); err != nil {
		return err
	}

	if count < 1 {
		l.Debug().Msg("already store done")

		return nil
	}

	for t := range chunks {
		pm.setWorkerBlockFiles(worker, t, chunks[t])
	}

	if pm.args.IsEnoughWorkers(count) {
		l.Debug().Msg("enough workers store done")

		pm.workersStoreDoneCtxCancel(errProcessManagerWorkersStoreDone)
	}

	return nil
}

func (pm *ProcessManager) newOperationFunc(
	worker string,
	ops [][3]interface{},
) error {
	defer logging.TimeElapsedMsg(func() *zerolog.Event { return pm.Log().Debug().Str("worker", worker).Int("ops", len(ops)) }, "worker new operations")()

	var newOp workerNewOperationsFunc
	var done func() error
	cancel := func(error) {}

	switch i, j, k, err := pm.args.WorkerNewOperations(pm.ctx(), worker); {
	case err != nil:
		return err
	default:
		newOp = i
		done = j
		cancel = k
	}

	if err := util.TraverseSliceV1(ops, func(_ int, v [3]interface{}) (bool, error) {
		opindex, op, fact := v[0].(uint64), v[1].(util.Hash), v[2].(util.Hash)

		if err := newOp(opindex, op, fact); err != nil {
			return false, err
		}

		return true, nil
	}); err != nil {
		cancel(err)

		return err
	}

	if err := done(); err != nil {
		cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) newNonEmptyNewOperationFunc() error {
	defer logging.TimeElapsedMsg(pm.Log().Debug, "new operations")()

	var ws [][][3]interface{}

	switch i, err := pm.newOpOwnersFunc2(pm.ops); {
	case err != nil:
		return err
	default:
		ws = i
	}

	var ew util.JobWorker

	switch i, err := pm.args.NewJobWorker(pm.ctx()); {
	case err != nil:
		return err
	default:
		defer i.Close()

		ew = i
	}

	for i := range ws {
		owner := pm.workers[i]
		wops := ws[i]

		if err := ew.NewJob(func(ctx context.Context, _ uint64) error {
			if err := pm.newOperationFunc(owner, wops); err != nil {
				if pm.addFailedWorker(owner, err) {
					if ferr := pm.isWorkersFailed(); ferr != nil {
						return ferr
					}
				}

				return err
			}

			return nil
		}); err != nil {
			return err
		}
	}

	ew.Done()

	if err := ew.Wait(); err != nil {
		return err
	}

	return pm.isWorkersFailed()
}

func (pm *ProcessManager) getOrCreateManifest() (manifest base.Manifest, _ error) {
	if err := pm.manifest.GetOrCreate(
		func(i base.Manifest, _ bool) error {
			manifest = i

			return nil
		},
		func() (base.Manifest, error) {
			switch i, err := pm.newManifest(); {
			case err != nil:
				return nil, err
			default:
				pm.storeJobWorker.Done()

				return i, nil
			}
		},
	); err != nil {
		return nil, err
	}

	return manifest, nil
}

func (pm *ProcessManager) newManifest() (base.Manifest, error) {
	var statesTree, opsTree util.Hash

	var treeew util.JobWorker

	switch i, err := pm.args.NewJobWorker(context.Background()); {
	case err != nil:
		return nil, err
	default:
		defer i.Close()

		treeew = i
	}

	if err := treeew.NewJob(func(context.Context, uint64) error {
		switch i, err := pm.newStatesTree(); {
		case err != nil:
			return err
		default:
			statesTree = i

			return nil
		}
	}); err != nil {
		return nil, err
	}

	if err := treeew.NewJob(func(context.Context, uint64) error {
		switch i, err := pm.newOpsTree(); {
		case err != nil:
			return err
		default:
			opsTree = i

			return nil
		}
	}); err != nil {
		return nil, err
	}

	treeew.Done()

	if err := treeew.Wait(); err != nil {
		return nil, err
	}

	var suffrage, prev util.Hash

	if pm.proposal.Point().Height() > base.GenesisHeight {
		suffrage = pm.previousSuffrage
		prev = pm.previousManifest
	}

	manifest := isaac.NewManifest(
		pm.proposal.Point().Height(),
		prev,
		pm.proposal.Fact().Hash(),
		opsTree,
		statesTree,
		suffrage,
		pm.proposal.ProposalFact().ProposedAt(),
	)

	pm.Log().Debug().
		Interface("manifest", manifest).
		Msg("manifest created")

	if err := pm.args.NewManifestFunc(pm.ctx(), manifest); err != nil {
		return nil, err
	}

	return manifest, nil
}

func (pm *ProcessManager) newStatesTree() (root util.Hash, _ error) {
	if len(pm.ops) < 1 {
		return nil, nil
	}

	var writer *baseFixedTreeWriter[string]

	switch i, err := pm.statesTreeWriter(false); {
	case err != nil:
		return nil, err
	case i == nil:
		return nil, nil
	default:
		writer = i
	}

	switch i, err := writer.Tree(); {
	case err != nil:
		return nil, err
	default:
		root = i
	}

	if root != nil {
		if err := pm.storeJobWorker.NewJob(func(context.Context, uint64) error {
			switch i, err := writer.Save(); {
			case err != nil:
				return err
			default:
				pm.setWorkerBlockFiles("", base.BlockItemStatesTree, i)

				return nil
			}
		}); err != nil {
			return nil, err
		}
	}

	return root, nil
}

func (pm *ProcessManager) newOpsTree() (root util.Hash, _ error) {
	if len(pm.ops) < 1 {
		return nil, nil
	}

	var writer *baseFixedTreeWriter[uint64]

	switch i, err := pm.operationsTreeWriter(true); {
	case err != nil:
		return nil, err
	case i == nil:
		return nil, nil
	default:
		writer = i
	}

	if err := util.TraverseSliceV1(pm.ops, func(i int, v [2]util.Hash) (bool, error) {
		opindex := uint64(i)
		_, fact := v[0], v[1]

		if pm.reasons.Exists(opindex) {
			return true, nil
		}

		if err := writer.Add(
			opindex,
			base.NewInStateOperationFixedtreeNode(fact, ""),
		); err != nil {
			return false, err
		}

		return true, nil
	}); err != nil {
	}

	switch i, err := writer.Tree(); {
	case err != nil:
		return nil, err
	default:
		root = i
	}

	if root != nil {
		if err := pm.storeJobWorker.NewJob(func(context.Context, uint64) error {
			switch i, err := writer.Save(); {
			case err != nil:
				return err
			default:
				pm.setWorkerBlockFiles("", base.BlockItemOperationsTree, i)

				return nil
			}
		}); err != nil {
			return nil, err
		}
	}

	return root, nil
}

func (pm *ProcessManager) newBlockFiles() (map[base.BlockItemType]BlockItemFile, error) {
	if pm.blockFiles.Len() < 1 {
		return nil, nil
	}

	bf := map[base.BlockItemType]BlockItemFile{}

	_ = pm.blockFiles.Traverse(func(t base.BlockItemType, chunks []string) bool {
		bf[t] = NewBlockItemFile(chunks)

		return true
	})

	return bf, nil
}

func (pm *ProcessManager) cancelWorkers() {
	var ew util.JobWorker

	switch i, err := pm.args.NewJobWorker(context.Background()); {
	case err != nil:
		return
	default:
		defer i.Close()

		ew = i
	}

	_ = util.TraverseSliceV1(pm.workers, func(_ int, worker string) (bool, error) {
		_ = ew.NewJob(func(ctx context.Context, _ uint64) error {
			wctx, cancel := context.WithTimeout(ctx, pm.args.WorkerCancelTimeout)
			defer cancel()

			if err := pm.args.WorkerCancel(wctx, worker); err != nil {
				pm.Log().Error().Err(err).Str("worker", worker).Msg("failed to cancel worker")
			}

			return nil
		})

		return true, nil
	})

	ew.Done()

	if err := ew.Wait(); err != nil {
		pm.Log().Error().Err(err).Msg("failed to cancel workers")
	}
}

func (pm *ProcessManager) attachFailedWorkers() func() {
	faileds := util.NewLocked(map[string]struct{}{})

	pm.addFailedWorker = func(worker string, err error) (added bool) {
		pm.Log().Error().Err(err).Str("worker", worker).Msg("worker failed")

		_, _ = faileds.Set(func(m map[string]struct{}, _ bool) (map[string]struct{}, error) {
			if m == nil { // NOTE ProcessManager cleaned
				return nil, util.ErrLockedSetIgnore
			}

			if _, found := m[worker]; found {
				return nil, util.ErrLockedSetIgnore
			}

			m[worker] = struct{}{}
			added = true

			return m, nil
		})

		return added
	}

	share := uint64(pm.args.NOpShare)

	if i := uint64(pm.args.NStateShare); i < share {
		share = i
	}

	pm.isWorkersFailed = func() error {
		return faileds.Get(func(m map[string]struct{}, _ bool) error {
			if i := uint64(len(m)); i >= share {
				return errors.Errorf("over share workers failed, %d", i)
			}

			return nil
		})
	}

	pm.isFailedWorker = func(worker string) (found bool) {
		_ = faileds.Get(func(m map[string]struct{}, _ bool) error {
			_, found = m[worker]

			return nil
		})

		return found
	}

	pm.failedWorkers = func() (workers []string) {
		add, done := util.CompactAppendSlice[string](len(pm.workers))

		_ = faileds.Get(func(m map[string]struct{}, _ bool) error {
			for i := range m {
				add(i)
			}

			return nil
		})

		return done()
	}

	return func() {
		_ = faileds.EmptyValue()
	}
}

func (pm *ProcessManager) workersDone() error {
	var ew util.JobWorker

	switch i, err := pm.args.NewJobWorker(pm.ctx()); {
	case err != nil:
		return err
	default:
		defer i.Close()

		ew = i
	}

	if err := util.TraverseSliceV1(pm.workers, func(_ int, worker string) (bool, error) {
		if pm.isFailedWorker(worker) {
			return true, nil
		}

		return true, ew.NewJob(func(ctx context.Context, _ uint64) error {
			if err := pm.args.WorkerDone(ctx, worker); err != nil {
				if pm.addFailedWorker(worker, err) {
					return pm.isWorkersFailed()
				}
			}

			return nil
		})
	}); err != nil {
		return err
	}

	ew.Done()

	if err := ew.Wait(); err != nil {
		return err
	}

	return nil
}

func (pm *ProcessManager) newStatesTreeWriterFunc() {
	var writer *baseFixedTreeWriter[string]
	var once sync.Once
	var gerr error

	pm.statesTreeWriter = func(create bool) (*baseFixedTreeWriter[string], error) {
		once.Do(func() {
			if !create {
				return
			}

			writer, gerr = NewBaseFixedTreeWriter[string](
				pm.ctx(),
				pm.args.NewJobWorker,
				base.StateFixedtreeHint,
				pm.args.SaveStatesTreeNodeFuncs,
				pm.args.MinStatesSize,
			)

			if gerr == nil {
				writer.SetLogging(pm.Logging)
			}
		})

		if gerr != nil {
			return nil, gerr
		}

		return writer, nil
	}
}

func (pm *ProcessManager) newOperationsTreeWriterFunc() {
	var writer *baseFixedTreeWriter[uint64]
	var once sync.Once
	var gerr error

	pm.operationsTreeWriter = func(create bool) (*baseFixedTreeWriter[uint64], error) {
		once.Do(func() {
			if !create {
				return
			}

			writer, gerr = NewBaseFixedTreeWriter[uint64](
				pm.ctx(),
				pm.args.NewJobWorker,
				base.OperationFixedtreeHint,
				pm.args.SaveOperationsTreeNodeFuncs,
				pm.args.MinStatesSize,
			)

			if gerr == nil {
				writer.SetLogging(pm.Logging)
			}
		})

		if gerr != nil {
			return nil, gerr
		}

		return writer, nil
	}
}

func (pm *ProcessManager) setWorkerBlockFiles(worker string, t base.BlockItemType, chunk string) {
	_, _, _ = pm.blockFiles.Set(t, func(c []string, _ bool) ([]string, error) {
		var wc string

		switch {
		case len(worker) < 1:
			wc = chunk
		default:
			wc = fmt.Sprintf("%s-%s", worker, chunk)
		}

		if len(c) < 1 {
			return []string{wc}, nil
		}

		n := make([]string, len(c)+1)
		copy(n, c)
		n[len(c)] = wc

		return n, nil
	})
}

func validateProcessManagerArgs(
	args *ProcessManagerArgs,
	emptyOps bool,
) error {
	if len(args.Workers) < 1 {
		return errors.Errorf("empty workers")
	}

	switch {
	case args.NOpShare < 1:
		return errors.Errorf("zero operation share")
	case int(args.NOpShare) > len(args.Workers):
		args.NOpShare = uint8(len(args.Workers))
	}

	switch {
	case args.NStateShare < 1:
		return errors.Errorf("zero state share")
	case int(args.NStateShare) > len(args.Workers):
		args.NStateShare = uint8(len(args.Workers))
	}

	if emptyOps {
		args.Workers = args.Workers[:args.NStateShare]
	}

	if args.IsEnoughWorkers == nil {
		args.IsEnoughWorkers = newIsEnoughWorkers(uint64(len(args.Workers)), uint64(args.NStateShare))
	}

	return nil
}

func fullOpOwnersFunc(workers []string, share uint8) (opOwnersFunc, error) {
	return func(uint64, util.Hash, util.Hash) ([]string, error) {
		return workers, nil
	}, nil
}

func newRegionalOpOwnersFunc(nworkers uint64, share uint8) (opOwnersFunc2, error) {
	if nworkers < 1 {
		return nil, errors.Errorf("empty workers")
	}

	nshare := uint64(share)
	if nshare < 1 {
		return nil, errors.Errorf("zero share")
	}

	return func(ops [][2]util.Hash) ([][][3]interface{}, error) {
		switch nops := uint64(len(ops)); {
		case nops < 1:
			return nil, errors.Errorf("empty ops")
		case nops < nworkers:
			nworkers = nops
		}

		ns := nshare

		if ns > nworkers {
			ns = nworkers
		}

		return regionalOpOwnersFunc(nworkers, ns, ops)
	}, nil
}

func regionalOpOwnersFunc(nworkers uint64, share uint64, ops [][2]util.Hash) ([][][3]interface{} /* [][]{opindex uint8, op, fact util.Hash} */, error) {
	nd := uint64(len(ops)) / nworkers

	nl := nworkers
	if uint64(len(ops))%nworkers > 0 {
		nl++
	}

	ws := make([][][3]interface{}, nworkers)

	for i := uint64(0); i < nl; i++ {
		s := i * nd
		e := (i + 1) * nd
		if e >= uint64(len(ops)) {
			e = uint64(len(ops))
		}

		o := ops[s:e]
		n := make([][3]interface{}, len(o))

		for j := range o {
			n[j] = [3]interface{}{s + uint64(j), o[j][0], o[j][1]}
		}

		w := i % nworkers
		ws[w] = ConcatSlice(ws[w], n)
	}

	if share > 1 {
		nws := make([][][3]interface{}, nworkers)
		for i := range ws {
			a := ws[i]
			n := make([][3]interface{}, len(a))
			copy(n, a)

			nws[i] = n
		}

		for i := range ws {
			a := ws[i]

			for j := uint64(0); j < share-1; j++ {
				bi := (uint64(i) + j + 1) % nworkers
				b := nws[bi]

				n := make([][3]interface{}, len(a)+len(b))
				copy(n, b)
				copy(n[len(b):], a)
				nws[bi] = n
			}
		}

		ws = nws
	}

	return ws, nil
}

func serialOpOwnersFunc(workers []string, share uint8) (opOwnersFunc, error) {
	w := uint64(len(workers))
	if w < 1 {
		return nil, errors.Errorf("empty workers")
	}

	n := uint64(share)
	if n < 1 {
		return nil, errors.Errorf("zero share")
	}

	if n > w {
		n = w
	}

	return func(opindex uint64, _, _ util.Hash) ([]string, error) {
		add, done := util.CompactAppendSlice[string](int(n))

		for i := uint64(0); i < n; i++ {
			add(workers[((opindex*n)+i)%w])
		}

		return done(), nil
	}, nil
}

func serialStateOwnersFunc(workers []string, share uint8) (stateOwnersFunc, error) {
	w := uint64(len(workers))
	if w < 1 {
		return nil, errors.Errorf("empty workers")
	}

	n := uint64(share)
	if n < 1 {
		return nil, errors.Errorf("zero share")
	}

	if n > w {
		n = w
	}

	last := util.NewLocked(uint64(0))

	return func(stateKey string) (owners []string, _ error) {
		_, _ = last.Set(func(l uint64, _ bool) (uint64, error) {
			add, done := util.CompactAppendSlice[string](int(n))

			var i uint64

			for i = 0; i < n; i++ {
				add(workers[(l+i)%w])
			}

			owners = done()

			return l + i, nil
		})

		return owners, nil
	}, nil
}

func djb2StateOwnersFunc(workers []string, share uint8) (stateOwnersFunc, error) {
	w := uint64(len(workers))
	if w < 1 {
		return nil, errors.Errorf("empty workers")
	}

	n := uint64(share)
	if n < 1 {
		return nil, errors.Errorf("zero share")
	}

	if n > w {
		n = w
	}

	seed := uint32(0)

	return func(stateKey string) ([]string, error) {
		index := stringdjb2(seed, stateKey) % w

		add, done, _ := util.CompactAppendSliceV1[string](n)

		for i := uint64(0); i < n; i++ {
			add(workers[(index+i)%w])
		}

		var owners []string

		_ = done(func(s []string) error {
			owners = slices.Clone(s)

			return nil
		})

		return owners, nil
	}, nil
}

func newStateOwnersFunc(f stateOwnersFunc) stateOwnersFunc {
	return func(stateKey string) ([]string, error) {
		switch owners, err := f(stateKey); {
		case err != nil:
			return nil, err
		case len(owners) < 1:
			return nil, util.ErrNotFound.Errorf("state owners")
		default:
			return owners, nil
		}
	}
}

func newIsEnoughWorkers(workers, nshare uint64) func(uint64) bool {
	i := workers - nshare

	return func(n uint64) bool {
		return n > i
	}
}

func newDjb2Seed() uint32 {
	max := big.NewInt(0).SetUint64(uint64(math.MaxUint32))

	switch rnd, err := rand.Int(rand.Reader, max); {
	case err != nil:
		return mathrand.Uint32()
	default:
		return uint32(rnd.Uint64())
	}
}

// stringdjb2 derived from
// https://github.com/patrickmn/go-cache/blob/master/sharded.go .
func stringdjb2(seed uint32, k string) uint64 {
	var (
		l = uint32(len(k))
		d = 5381 + seed + l
		i = uint32(0)
	)

	if l >= 4 { //nolint:gomnd //...
		for i < l-4 {
			d = (d * 33) ^ uint32(k[i])   //nolint:gomnd //...
			d = (d * 33) ^ uint32(k[i+1]) //nolint:gomnd //...
			d = (d * 33) ^ uint32(k[i+2]) //nolint:gomnd //...
			d = (d * 33) ^ uint32(k[i+3]) //nolint:gomnd //...
			i += 4
		}
	}

	switch l - i {
	case 1:
	case 2:
		d = (d * 33) ^ uint32(k[i]) //nolint:gomnd //...
	case 3: //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i])   //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i+1]) //nolint:gomnd //...
	case 4: //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i])   //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i+1]) //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i+2]) //nolint:gomnd //...
	}

	return uint64(d ^ (d >> 16)) //nolint:gomnd //...
}

func ConcatSlice[S ~[]E, E any](s ...S) S {
	size := 0
	for _, s := range s {
		size += len(s)
		if size < 0 {
			panic("len out of range")
		}
	}
	newslice := slices.Grow[S](nil, size)
	for _, s := range s {
		newslice = append(newslice, s...)
	}
	return newslice
}
