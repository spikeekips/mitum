package main

import (
	"context"
	"crypto/tls"
	stderrors "errors"
	"fmt"
	"net"
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	"github.com/spikeekips/mitum/example/test364_5/quicstream"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	defaultSubStreamItems    uint64 = 1 << 9
	defaultMaxSubStreamsSize uint16 = 1 << 8
)

type requestWorkerFunc = func(context.Context,
	string, /* worker */
	*tls.Config, *quic.Config, quicstream.StreamHandler) (context.Context, error)

type managerCommand struct {
	baseCommand `cmd:""`
	Ops         uint64   `arg:"" help:"number of operations"`
	OpsShare    uint64   `arg:"" help:"ops share"`
	StatesShare uint64   `arg:"" help:"states share"`
	Workers     []string `arg:"" default:""`

	managerOptions *dop5.ManagerOptions
	privs          []base.Privatekey
	pubs           []base.Publickey
}

func (cmd *managerCommand) Run(log *zerolog.Logger) error {
	if err := cmd.baseCommand.prepare("manager", log); err != nil {
		return err
	}
	defer cmd.baseCommand.closef()

	if err := cmd.prepare(); err != nil {
		return err
	}

	cmd.log.Debug().Msg("prepared")

	return cmd.process()
}

func (cmd *managerCommand) prepare() error {
	switch {
	case cmd.Ops < 1:
		return errors.Errorf("zero <ops>")
	case len(cmd.Workers) < 1:
		return errors.Errorf("empty workers")
	}

	switch {
	case cmd.OpsShare < 1:
		return errors.Errorf("zero <ops share>")
	case cmd.OpsShare < uint64(len(cmd.Workers)):
		cmd.OpsShare = uint64(len(cmd.Workers))
	}

	switch {
	case cmd.StatesShare < 1:
		return errors.Errorf("zero <states share>")
	case cmd.StatesShare < uint64(len(cmd.Workers)):
		cmd.StatesShare = uint64(len(cmd.Workers))
	}

	for s := range slices.Values(cmd.Workers) {
		switch addr, err := net.ResolveUDPAddr("udp", s); {
		case err != nil:
			return err
		case addr.Port < 1:
			return errors.Errorf("invalid worker addr, %q", s)
		default:
			cmd.workerAddrs.SetValue(s, addr)
		}
	}

	cmd.log.Debug().
		Interface("addrs", cmd.workerAddrs.Map()).
		Msg("workers")

	if err := cmd.preparePrivs(); err != nil {
		return err
	}

	cmd.managerOptions = dop5.NewManagerOptions()

	return nil
}

func (cmd *managerCommand) preparePrivs() error {
	cmd.log.Debug().Uint64("n", cmd.Ops).Msg("> privs")

	cmd.privs = make([]base.Privatekey, cmd.Ops)
	cmd.pubs = make([]base.Publickey, cmd.Ops)

	for i := range cmd.Ops {
		priv, err := base.NewMPrivatekeyFromSeed(
			valuehash.NewSHA256([]byte(fmt.Sprintf("%d", i))).String(),
		)
		if err != nil {
			return err
		}

		cmd.privs[i] = priv
		cmd.pubs[i] = priv.Publickey()
	}

	cmd.log.Debug().Int("n", len(cmd.privs)).Msg("< privs")

	return nil
}

func (cmd *managerCommand) prepareCreateOps() ([]dop5.OperationFact, [][2]util.Hash) {
	defer logging.TimeElapsedMsg(func() *zerolog.Event { return cmd.log.Debug().Uint64("n", cmd.Ops) }, "create ops created")()

	facts := make([]dop5.OperationFact, cmd.Ops)
	ophs := make([][2]util.Hash, cmd.Ops)

	for i := range cmd.Ops {
		fact := dop5.NewAccountOpFact(
			[]byte("c"),
			1,
			nil,
			cmd.pubs[i],
		)
		facts[i] = fact
		ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}
	}

	return facts, ophs
}

func (cmd *managerCommand) prepareTransferOps() ([]dop5.OperationFact, [][2]util.Hash) {
	defer logging.TimeElapsedMsg(func() *zerolog.Event { return cmd.log.Debug().Uint64("n", cmd.Ops) }, "transfer ops created")()

	facts := make([]dop5.OperationFact, cmd.Ops)
	ophs := make([][2]util.Hash, cmd.Ops)

	for i := range cmd.Ops {
		fact := dop5.NewAccountOpFact(
			[]byte("d"),
			1,
			cmd.pubs[i],
			cmd.pubs[cmd.Ops%uint64(i+1)],
		)
		facts[i] = fact
		ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}
	}

	return facts, ophs
}

func (cmd *managerCommand) process() error {
	defer func() {
		requestStopWorker := cmd.requestWorker("manager_stop_worker")

		cmd.requestWorkers(cmd.ctx, func(ctx context.Context, worker string) error {
			_, err := requestStopWorker(ctx, worker, nil, nil, requestHandlerStopWorker())

			return err
		})
	}()

	var prevHash util.Hash

	switch h, err := cmd.process1st(); {
	case err != nil:
		return err
	default:
		prevHash = h
	}

	if err := cmd.process2nd(prevHash); err != nil {
		return err
	}

	cmd.log.Debug().Msg("processed")

	return nil
}

func (cmd *managerCommand) process1st() (util.Hash, error) {
	defer logging.TimeElapsedMsg(cmd.log.Debug, "process 1st")()

	facts, ophs := cmd.prepareCreateOps()
	if err := cmd.saveOps(facts); err != nil {
		return nil, err
	}

	switch h, w, err := cmd.processHeight(cmd.ctx, 33, nil, ophs); {
	case err != nil:
		return nil, err
	default:
		if err := w(); err != nil {
			return nil, err
		}

		return h, nil
	}
}

func (cmd *managerCommand) process2nd(prevHash util.Hash) error {
	defer logging.TimeElapsedMsg(cmd.log.Debug, "process 2nd")()

	facts, ophs := cmd.prepareTransferOps()
	if err := cmd.saveOps(facts); err != nil {
		return err
	}

	switch _, w, err := cmd.processHeight(cmd.ctx, 34, prevHash, ophs); {
	case err != nil:
		return err
	default:
		return w()
	}
}

func (cmd *managerCommand) saveOps(facts []dop5.OperationFact) error {
	defer logging.TimeElapsedMsg(func() *zerolog.Event { return cmd.log.Debug().Int("n", len(facts)) }, "save fact")()

	return redisSaveOperationFacts(
		cmd.ctx,
		cmd.enc,
		cmd.rdb,
		facts,
		1<<9, 1<<9,
	)
}

func (cmd *managerCommand) processHeight(
	ctx context.Context,
	height base.Height,
	prevManiest util.Hash,
	ophs [][2]util.Hash,
) (util.Hash, dutil.ErrorFunc, error) {
	workID := fmt.Sprintf("%s-%s", height, util.UUID())

	defer logging.TimeElapsedMsg(func() *zerolog.Event { return cmd.log.Debug().Str("work_id", workID) }, "process height")()

	return cmd.startManager(
		dutil.SetContextValueLogger(ctx, cmd.log),
		height,
		prevManiest,
		workID,
		ophs,
	)
}

func (cmd *managerCommand) startManager(
	ctx context.Context,
	height base.Height,
	prevManiest util.Hash,
	workID string,
	ophs [][2]util.Hash,
) (util.Hash, dutil.ErrorFunc, error) {
	defer logging.TimeElapsedMsg(func() *zerolog.Event {
		return cmd.log.Debug().
			Str("work_id", workID).
			Interface("height", height).
			Stringer("prev_hash", prevManiest).
			Int("ophs", len(ophs))
	}, "start manager")()

	var start dutil.In2ErrorFunc[dop5.ManagerStartWorkerFunc, dop5.NewManagerWorkerSendOpFunc]
	var workerOwners dutil.In2ErrorFunc[string, []string]
	var workerResult dop5.ManagerInsertWorkerResultFunc
	var workerErr dutil.In2ErrorFunc[string, error]
	var wait dop5.ManagerWaitFunc

	switch s, o, r, e, w, err := dop5.NewFuncManager(
		ctx,
		cmd.managerOptions,
		cmd.newFuncCancelWorker(workID),
		cmd.Workers,
		cmd.OpsShare,
		cmd.StatesShare,
		ophs,
	); {
	case err != nil:
		return nil, nil, err
	default:
		start, workerOwners, workerResult, workerErr, wait = s, o, r, e, w
	}

	addHandler, _, handler, closef := quicstream.NewFuncPrefixStreamHandler()

	cmd.mustAddHandler(workID, handler)

	mustAddHandler := newFuncMustAddHandler(addHandler)

	mustAddHandler("manager_worker_owners", acceptHandlerWorkerOwners(func(worker string, owners []string) error {
		return workerOwners(worker, owners)
	}))

	mustAddHandler("manager_worker_result", acceptHandlerWorkerResult(workerResult).
		StreamHandler(cmd.newAcceptSubStream))

	mustAddHandler("manager_worker_error", acceptHandlerWorkerErr(workerErr))

	closeManager := func() error {
		cmd.removeHandler([]byte(workID))
		closef()

		return nil
	}

	defer logging.TimeElapsedMsg(cmd.log.Debug, "manager: start")()

	if err := start(
		cmd.newFuncStartWorkers(workID),
		cmd.newFuncSendOp(workID),
	); err != nil {
		_ = closeManager()

		return nil, nil, err
	}

	requestWorkerInsertDone := cmd.requestWorkWorker(workID, "manager_worker_insert_done")
	requestWorkerClose := cmd.requestWorkWorker(workID, "manager_worker_close")

	var manifestHash util.Hash

	if err := wait(
		func(ctx context.Context, worker string) error {
			_, err := requestWorkerInsertDone(ctx, worker, nil, nil, requestHandlerWorkerInsertDone())

			return err
		},
		func(opsTree, statesTree *fixedtree.Tree) (base.Manifest, error) {
			var opsr, statesr util.Hash
			if opsTree != nil {
				opsr = opsTree.Root()
			}
			if statesTree != nil {
				statesr = statesTree.Root()
			}

			manifest := isaac.NewManifest(
				height,
				prevManiest,
				valuehash.RandomSHA256(),
				opsr,
				statesr,
				valuehash.RandomSHA256(),
				time.Now(),
			)

			return manifest, nil
		},
		func(ctx context.Context, worker string) error {
			var cerr error

			_, err := requestWorkerClose(ctx, worker, nil, nil, requestHandlerWorkerClose(func(err error) error {
				cerr = err

				return nil
			}))

			return stderrors.Join(cerr, err)
		},
	); err != nil {
		_ = closeManager()

		return nil, nil, err
	}

	requestWaitWorkerClosed := cmd.requestWorkWorker(workID, "manager_wait_worker_closed")

	return manifestHash,
		func() error {
			return stderrors.Join(
				cmd.requestWorkers(cmd.ctx, func(ctx context.Context, worker string) error {
					_, err := requestWaitWorkerClosed(ctx, worker, nil, nil, requestHandlerWaitWorkerClosed())

					return err
				}),
				closeManager(),
			)
		},
		nil
}

func (cmd *managerCommand) newFuncCancelWorker(workID string) dop5.ManagerCancelWorkerFunc {
	request := cmd.requestWorkWorker(workID, "manager_cancel_worker")

	return func(ctx context.Context, worker string, merr error) error {
		_, err := request(ctx, worker, nil, nil, requestHandlerCancelWorker(merr))

		return err
	}
}

func (cmd *managerCommand) newFuncStartWorkers(workID string) dop5.ManagerStartWorkerFunc {
	return func(
		ctx context.Context,
		worker string,
		workers, opsWorkers []string,
		opsShare, statesShare uint64,
		maxOpsSize uint64,
	) error {
		newJob, jobDone, jobWait, _ := dutil.NewFuncJobWorker(ctx, uint64(len(workers)))

		handler := requestHandlerStartWorker(workID, workers, opsWorkers, opsShare, statesShare, maxOpsSize)

		for worker := range slices.Values(workers) {
			addr, err := cmd.workerAddr(worker)
			if err != nil {
				return err
			}

			if err := newJob(func(ctx context.Context) error {
				_, err := cmd.request("manager_start_worker")(ctx, addr, nil, nil, handler)

				return err
			}); err != nil {
				return err
			}
		}

		jobDone()

		return jobWait(nil)
	}
}

func (cmd *managerCommand) newFuncSendOp(workID string) dop5.NewManagerWorkerSendOpFunc {
	return func(ctx context.Context, worker string, maxItems uint64) (
		dop5.OpindexOpHashesFunc,
		dutil.InErrorFunc[error],
		error,
	) {
		addr, err := cmd.workerAddr(worker)
		if err != nil {
			return nil, nil, err
		}

		sendOp, sendDone, handler := requestHandlerSendOps(
			ctx, maxItems, defaultSubStreamItems, defaultMaxSubStreamsSize)

		requestDone, requestWait := dutil.NewContextDone(ctx)

		go func() {
			_, err := cmd.requestWork(workID, "manager_send_ops")(ctx, addr, nil, nil, handler.StreamHandler(cmd.openSubStreams))
			_ = requestDone(err)
		}()

		return sendOp, func(err error) error {
			if serr := sendDone(err); serr != nil {
				return serr
			}

			return requestWait()
		}, nil
	}
}

func (cmd *managerCommand) requestWorkers(
	ctx context.Context,
	job dutil.In2ErrorFunc[context.Context, string],
) error {
	newJob, jobDone, jobWait, _ := dutil.NewFuncJobWorker(ctx, uint64(len(cmd.Workers)))

	for worker := range slices.Values(cmd.Workers) {
		if err := newJob(func(ctx context.Context) error {
			return job(ctx, worker)
		}); err != nil {
			return err
		}
	}

	jobDone()

	return jobWait(nil)
}
