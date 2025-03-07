package main

import (
	"context"
	"iter"
	"net"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/quicstream"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type workerCommand struct {
	baseCommand
	Manager string `arg:""`

	local         string
	workerOptions *dop5.WorkerOptions
	manager       *net.UDPAddr
}

func (cmd *workerCommand) Run(log *zerolog.Logger) error {
	cmd.local = cmd.Bind

	if err := cmd.baseCommand.prepare(cmd.local, log); err != nil {
		return err
	}
	defer cmd.baseCommand.closef()

	if err := cmd.prepare(); err != nil {
		return err
	}

	cmd.log.Debug().Msg("prepared")

	return cmd.ready()
}

func (cmd *workerCommand) prepare() error {
	switch addr, err := net.ResolveUDPAddr("udp", cmd.Manager); {
	case err != nil:
		return err
	case addr.Port < 1:
		return errors.Errorf("invalid manager addr, %q", cmd.Manager)
	default:
		cmd.manager = addr
	}

	cmd.workerOptions = dop5.NewWorkerOptions(cmd.local)

	return nil
}

func (cmd *workerCommand) ready() error {
	return nil
}

func (cmd *workerCommand) startWorker(
	ctx context.Context,
	workID string,
	height base.Height,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	maxOpsSize uint64,
) error {
	defer cmd.removeHandler([]byte(workID))

	var initialInsert dop5.WorkerInitialInsertFunc
	var workerInsertSMVs dop5.WorkerProcessWorkerInsertSMVsFunc
	var workerUnmerged dop5.WorkerUnmergedFunc
	var wait dop5.WorkerWaitFunc
	var workerClose dutil.InErrorFunc[error]

	switch ii, wi, wu, w, c, err := dop5.NewFuncWorker(
		ctx,
		cmd.workerOptions,
		cmd.getState,
		cmd.newFuncCreateState(height),
		cmd.newFuncReportCancelWork(workID),
		workers, opsWorkers,
		opsShare, statesShare,
		maxOpsSize,
	); {
	case err != nil:
		return err
	default:
		defer c(nil)

		initialInsert, workerInsertSMVs, workerUnmerged, wait, workerClose = ii, wi, wu, w, c
	}

	addHandler, _, handler, closeHandler := quicstream.NewFuncPrefixStreamHandler()
	defer closeHandler()

	cmd.mustAddHandler(workID, handler)

	mustAddHandler := newFuncMustAddHandler(addHandler)

	var maxItems uint64

	mustAddHandler("manager_send_ops", cmd.newHandlerInitialInsert(
		workID, workers, statesShare,
		func(i uint64) {
			atomic.AddUint64(&maxItems, i)
		},
		initialInsert,
	).StreamHandler(cmd.newAcceptSubStream))

	_, _, _, _, _ = workerInsertSMVs, workerUnmerged, wait, workerClose, maxItems

	_ = workerClose(nil) // FIXME add handler

	return nil
}

func (cmd *workerCommand) getState(stateKey string) (st base.State, _ bool, _ error) {
	found, err := redisGetState(
		cmd.ctx,
		cmd.enc,
		cmd.rdb,
		stateKey,
		&st,
	)

	return st, found, err
}

func (cmd *workerCommand) newFuncCreateState(height base.Height) dop5.CreateStateFunc {
	return func(stateKey string, value base.StateValue, prev util.Hash, ops []util.Hash) (
		base.State,
		error,
	) {
		return base.NewBaseState(
				height,
				stateKey,
				value,
				prev,
				ops,
			),
			nil
	}
}

func (cmd *workerCommand) newFuncReportCancelWork(workID string) dutil.InErrorFunc[error] {
	return func(err error) error {
		_, rerr := cmd.requestWork(workID, "manager_worker_error")(cmd.ctx, cmd.manager, nil, nil, requestHandlerWorkerErr(cmd.local, err))

		return rerr
	}
}

func (cmd *workerCommand) newHandlerInitialInsert(
	workID string,
	workers []string,
	statesShare uint64,
	maxItemsf func(uint64),
	initialInsert dop5.WorkerInitialInsertFunc,
) quicstream.AcceptSubStreamHandler {
	return acceptHandlerSendOps(
		func(maxItems uint64, seq iter.Seq2[uint64, [2]util.Hash]) error {
			maxItemsf(maxItems)

			return initialInsert(
				newFuncGetStateOwnersMurmur3(workers, statesShare),
				cmd.newWorkerDistributeSMVs,
				cmd.getOperationFact,
				seq,
				cmd.newFuncReportStatesOwners(workID),
			)
		},
	)
}

func (cmd *workerCommand) newWorkerDistributeSMVs(ctx context.Context, worker string) (
	dop5.WorkerProcessInsertSMVsFunc,
	dutil.In2ErrorFunc[[]string, []string],
	error,
) {
	// FIXME
	return func(opindex uint64, facth util.Hash, values []dop5.SMV) error {
			return nil
		},
		func(subs, affected []string) error {
			return nil
		},
		nil
}

func (cmd *workerCommand) getOperationFact(ctx context.Context, opindex uint64, oph, facth util.Hash) (
	fact dop5.OperationFact,
	_ bool,
	_ string,
	_ error,
) {
	found, err := redisGetOperationFact(
		ctx,
		cmd.enc,
		cmd.rdb,
		facth,
		&fact,
	)

	return fact, found, "", err
}

func (cmd *workerCommand) newFuncReportStatesOwners(workID string) dop5.WorkerReportStatesOwnersFunc {
	request := cmd.requestWork(workID, "manager_worker_owners")

	return func(
		ctx context.Context,
		_ string,
		owners []string,
	) error {
		_, err := request(ctx, cmd.manager, nil, nil, requestHandlerWorkerOwners(cmd.local, owners))

		return err
	}
}
