package main

import (
	"context"
	"crypto/tls"
	stderrors "errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
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

	if err := cmd.waitWorkers(); err != nil {
		return err
	}

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

	cmd.managerOptions = dop5.NewManagerOptions()

	if err := cmd.rdb.FlushAll(cmd.ctx).Err(); err != nil {
		return err
	}

	return nil
}

func (cmd *managerCommand) preparePrivs() error {
	defer logging.TimeElapsedMsg(func() *zerolog.Event { return cmd.log.Debug().Uint64("n", cmd.Ops) }, "create privs")()

	cmd.pubs = make([]base.Publickey, cmd.Ops)

	newJob, jobDone, jobWait, _ := dutil.NewFuncJobWorker(cmd.ctx, cmd.Ops)

	for i := range cmd.Ops {
		if err := newJob(func(context.Context) error {
			_, k := btcec.PrivKeyFromBytes([]byte(strconv.FormatInt(int64(i), 10)))

			cmd.pubs[i] = base.NewMPublickey(k)

			return nil
		}); err != nil {
			return jobWait(err)
		}
	}

	jobDone()

	return jobWait(nil)
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
	if err := cmd.preparePrivs(); err != nil {
		return err
	}

	os.Exit(1)

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

	return cmd.stopWorkers()
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
	workID := fmt.Sprintf("%s", height)

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
		cmd.newFuncCancelWork(workID),
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
		cmd.newFuncStartWorkers(workID, height),
		cmd.newFuncSendOp(workID),
	); err != nil {
		_ = closeManager()

		return nil, nil, err
	}

	requestWorkerInsertDone := cmd.requestWorkWorker(workID, "manager_worker_insert_done")
	requestCloseWork := cmd.requestWorkWorker(workID, "manager_close_work")

	var manifest base.Manifest

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

			manifest = isaac.NewManifest(
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

			_, _ = requestCloseWork(ctx, worker, nil, nil, requestHandlerCloseWork(func(err error) error {
				cerr = err

				return nil
			}))

			return cerr
		},
	); err != nil {
		_ = closeManager()

		return nil, nil, err
	}

	requestWaitWorkerClosed := cmd.requestWorkWorker(workID, "_manager_wait_worker_closed")

	return manifest.Hash(),
		func() error {
			// NOTE check all worker closes work.
			return stderrors.Join(
				cmd.requestWorkers(cmd.ctx, func(ctx context.Context, worker string) error {
					_, _ = requestWaitWorkerClosed(ctx, worker, nil, nil, requestHandlerWaitWorkerClosed())

					return nil
				}),
				closeManager(),
			)
		},
		nil
}

func (cmd *managerCommand) newFuncCancelWork(workID string) dop5.ManagerCancelWorkFunc {
	request := cmd.requestWorkWorker(workID, "manager_cancel_work")

	return func(ctx context.Context, worker string, merr error) error {
		_, err := request(ctx, worker, nil, nil, requestHandlerCancelWork(merr))

		return err
	}
}

func (cmd *managerCommand) newFuncStartWorkers(workID string, height base.Height) dop5.ManagerStartWorkerFunc {
	return func(
		ctx context.Context,
		worker string,
		workers, opsWorkers []string,
		opsShare, statesShare uint64,
		maxOpsSize uint64,
	) error {
		addr, err := cmd.workerAddr(worker)
		if err != nil {
			return err
		}

		_, err = cmd.request("manager_start_worker")(ctx, addr, nil, nil,
			requestHandlerStartWorker(workID, height, workers, opsWorkers, opsShare, statesShare, maxOpsSize),
		)

		return err
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

func (cmd *managerCommand) waitWorkers() error {
	cmd.log.Debug().Msg("waits workers")

	done, wait := dutil.NewContextDone(cmd.ctx)

	pinged := dutil.NewSingleLockedMap[string, struct{}]()
	cmd.mustAddHandler("_worker_ping_manager", acceptHandlerWorkerPingManager(func(worker string, i uint64) error {
		if slices.Index(cmd.Workers, worker) < 0 {
			return errors.Errorf("unknown worker, %q", worker)
		}

		if created, _ := pinged.Set(worker, func(_ struct{}, found bool) (struct{}, error) {
			if found {
				return struct{}{}, dutil.ErrLockedSetIgnore
			}

			return struct{}{}, nil
		}); created {
			cmd.log.Debug().Str("worker", worker).Msg("worker ping")
		}

		if pinged.Len() == len(cmd.Workers) {
			done(nil)
			cmd.log.Debug().Msg("all worker ping")
		}

		return nil
	}))

	return wait()
}

func (cmd *managerCommand) stopWorkers() error {
	requestStopWorker := cmd.requestWorker("_manager_stop_worker")

	return cmd.requestWorkers(cmd.ctx, func(ctx context.Context, worker string) error {
		_, err := requestStopWorker(ctx, worker, nil, nil, requestHandlerStopWorker())

		return err
	})
}
