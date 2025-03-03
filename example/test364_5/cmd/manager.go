package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"slices"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	"github.com/spikeekips/mitum/example/test364_5/quicstream"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

var handlerPrefixesString = map[string]struct{}{
	"manager_start_worker": {},
}

var workHandlerPrefixesString = map[string]struct{}{
	"manager_start_worker":       {},
	"manager_send_ops":           {},
	"manager_worker_owners":      {},
	"manager_worker_result":      {},
	"manager_worker_error":       {},
	"manager_worker_insert_done": {},
	"manager_worker_close":       {},
	"manager_cancel_worker":      {},
}

var (
	defaultSubStreamItems    uint64 = 1 << 9
	defaultMaxSubStreamsSize uint16 = 1 << 8
)

type requestWorkerFunc = func(context.Context, string /* worker */, *tls.Config, *quic.Config, quicstream.StreamHandler) (context.Context, error)

type managerCommand struct {
	Bind              string   `default:"${manager_bind}" help:"by default, ${manager_bind}"`
	Workers           []string `placeholder:"UDPAddr"`
	OpsShare          uint64   `default:"${ops_share}" help:"by default, ${ops_share}"`
	StatesShare       uint64   `default:"${states_share}" help:"by default, ${states_share}"`
	ctx               context.Context
	log               *zerolog.Logger
	closefs           []dutil.ErrorFunc
	tlsConfig         *tls.Config
	quicConfig        *quic.Config
	dialf             quicstream.DialCloseFunc
	request           func(prefix string) quicstream.RequestFunc
	requestWork       func(workID, prefix string) quicstream.RequestFunc
	requestWorkWorker func(workID, prefix string) requestWorkerFunc
	mustAddHandler    func(string, quicstream.StreamHandler)
	removeHandler     func([]byte) bool
	managerOptions    *dop5.ManagerOptions
	workers           map[string]*net.UDPAddr
	openSubStreams    quicstream.NewOpenSubStreamsFunc
}

func (cmd *managerCommand) Run(log *zerolog.Logger) error {
	cmd.log = log

	ctx, ctxCancel := dutil.ContextWithCancelCause(context.Background())
	defer func() {
		for _, f := range slices.Backward(cmd.closefs) {
			_ = f()
		}

		ctxCancel(nil)
	}()

	cmd.ctx = ctx

	if err := cmd.newQuicStream(cmd.ctx); err != nil {
		return err
	}

	cmd.managerOptions = dop5.NewManagerOptions()

	var prevHash util.Hash

	switch h, err := cmd.processHeight(cmd.ctx, 33, nil, nil); {
	case err != nil:
		return err
	default:
		prevHash = h
	}

	if _, err := cmd.processHeight(cmd.ctx, 34, prevHash, nil); err != nil {
		return err
	}

	return nil
}

func (cmd *managerCommand) processHeight(
	ctx context.Context,
	height base.Height,
	prevManiest util.Hash,
	ophs [][2]util.Hash,
) (util.Hash, error) {
	workID := fmt.Sprintf("%s-%s", height, util.UUID())

	var manifestHash util.Hash

	switch i, err := cmd.startManager(
		ctx,
		height,
		prevManiest,
		workID,
		ophs,
	); {
	case err != nil:
		return err
	default:
		manifestHash = i
	}

	// FIXME collect finished from worker; acceptHandlerWorkerDone

	return manifestHash, nil
}

func (cmd *managerCommand) startManager(
	ctx context.Context,
	height base.Height,
	prevManiest util.Hash,
	workID string,
	ophs [][2]util.Hash,
) (util.Hash, error) {
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
		return nil, err
	default:
		start, workerOwners, workerResult, workerErr, wait = s, o, r, e, w
	}

	addHandler, _, handler, closef := quicstream.NewFuncPrefixStreamHandler()
	defer func() {
		cmd.removeHandler([]byte(workID))
		closef()
	}()
	cmd.mustAddHandler(workID, handler)

	mustaddHandler := func(prefix string, h quicstream.StreamHandler) {
		if !addHandler([]byte(prefix), h) {
			panic(fmt.Sprintf("failed to add handler, %q", prefix))
		}
	}

	mustaddHandler("manager_worker_owners", acceptHandlerWorkerOwners(func(worker string, owners []string) error {
		return workerOwners(worker, owners)
	}))

	mustaddHandler("manager_worker_result", acceptHandlerWorkerResult(workerResult).
		StreamHandler(quicstream.NewFuncAcceptSubStreams(1<<9)))

	mustaddHandler("manager_worker_error", acceptHandlerWorkerErr(workerErr))

	if err := start(
		cmd.newFuncStartWorkers(workID),
		cmd.newFuncSendOp(workID),
	); err != nil {
		return nil, err
	}

	requestWorkerInsertDone := cmd.requestWorkWorker(workID, "manager_worker_insert_done")
	requestWorkerClose := cmd.requestWorkWorker(workID, "manager_worker_close")

	var manifestHash util.Hash

	return manifestHash, wait(
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
			_, err := requestWorkerClose(ctx, worker, nil, nil, requestHandlerWorkerClose())

			return err
		},
	)
}

func (cmd *managerCommand) newQuicStream(
	ctx context.Context,
) error {
	cmd.workers = map[string]*net.UDPAddr{}

	for s := range slices.Values(cmd.Workers) {
		switch addr, err := net.ResolveUDPAddr("udp", s); {
		case err != nil:
			return err
		default:
			cmd.workers[s] = addr
		}
	}

	var pconn net.PacketConn

	switch bind, err := net.ResolveUDPAddr("udp", cmd.Bind); {
	case err != nil:
		return err
	default:
		switch c, err := net.ListenUDP("udp", bind); {
		case err != nil:
			return err
		default:
			pconn = c
		}
	}

	var statelessResetKey *quic.StatelessResetKey

	switch kp, c, err := newTLSConfig("manager"); {
	case err != nil:
		return err
	default:
		cmd.tlsConfig = c
		statelessResetKey = quicstream.NewStatelessResetKey(kp.PrivateKey())
	}

	cmd.quicConfig = quicstream.DefaultQuicConfig()

	listen, closef, err := quicstream.Serve(
		ctx,
		pconn,
		cmd.tlsConfig,
		cmd.quicConfig,
		statelessResetKey,
	)
	if err != nil {
		return err
	}

	cmd.log.Debug().
		Str("bind", cmd.Bind).
		Bool("tls_insecure", cmd.tlsConfig.InsecureSkipVerify).
		Dict("quic_config", zerolog.Dict().
			Dur("HandshakeIdleTimeout", cmd.quicConfig.HandshakeIdleTimeout).
			Dur("MaxIdleTimeout", cmd.quicConfig.MaxIdleTimeout).
			Int64("MaxIncomingStreams", cmd.quicConfig.MaxIncomingStreams).
			Dur("KeepAlivePeriod", cmd.quicConfig.KeepAlivePeriod).
			Bool("Allow0RTT", cmd.quicConfig.Allow0RTT),
		).
		Msg("flags")

	addHandler, removeHandler, handler, handlerClosef := quicstream.NewFuncPrefixStreamHandler()
	cmd.mustAddHandler = func(prefix string, h quicstream.StreamHandler) {
		if !addHandler([]byte(prefix), h) {
			panic(fmt.Sprintf("failed to add handler, %q", prefix))
		}
	}

	cmd.removeHandler = removeHandler

	listench := make(chan error)
	go func() {
		listench <- listen(handler)
	}()

	cmd.closefs = append(cmd.closefs, func() error {
		closef()
		handlerClosef()

		cmd.log.Debug().Err(<-listench).Msg("quicstream listen stopped")

		return nil
	})

	if err := cmd.newDialFunc(); err != nil {
		return err
	}

	return nil
}

func (cmd *managerCommand) newDialFunc() error {
	pool, err := dutil.NewLockedMap[string, quic.Connection](nil, 1<<9, 1<<9)
	if err != nil {
		return err
	}

	dial, closef, err := quicstream.NewDialPool(pool, nil, time.Minute)
	if err != nil {
		return err
	}

	cmd.dialf = dial
	cmd.closefs = append(cmd.closefs, closef)

	openConnSubStreams, openConnSubStreamsClose, _ := quicstream.NewConnsAcceptSubStreams(cmd.ctx, time.Minute)
	cmd.openSubStreams = quicstream.NewFuncOpenSubStreams(openConnSubStreams)

	cmd.closefs = append(cmd.closefs, func() error { openConnSubStreamsClose(); return nil })

	var request quicstream.RequestFunc

	switch r, err := quicstream.NewFuncRequest(
		quicstream.NewDialWithCloseFunc(cmd.dialf, quicstream.NewFuncConnClosingStreamError),
		cmd.tlsConfig, cmd.quicConfig,
	); {
	case err != nil:
		return err
	default:
		request = r
	}

	cmd.request = func(prefix string) quicstream.RequestFunc {
		if _, found := handlerPrefixesString[prefix]; !found {
			panic(fmt.Sprintf("unknown prefix, %q", prefix))
		}

		return func(ctx context.Context, addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config, h quicstream.StreamHandler) (context.Context, error) {
			return request(ctx, addr, tlsConfig, quicConfig, func(ctx context.Context, stream quic.Stream) (context.Context, error) {
				if err := quicstream.WriteStreamHandlerPrefix(stream, []byte(prefix)); err != nil {
					return ctx, err
				}

				return h(ctx, stream)
			})
		}
	}

	cmd.requestWork = func(workID, prefix string) quicstream.RequestFunc {
		if workID == "" {
			panic("empty work id")
		}

		if _, found := workHandlerPrefixesString[prefix]; !found {
			panic(fmt.Sprintf("unknown prefix, %q", prefix))
		}

		return func(ctx context.Context, addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config, h quicstream.StreamHandler) (context.Context, error) {
			return request(ctx, addr, tlsConfig, quicConfig, func(ctx context.Context, stream quic.Stream) (context.Context, error) {
				if err := quicstream.WriteStreamHandlerPrefix(stream, []byte(workID)); err != nil {
					return ctx, err
				}

				if err := quicstream.WriteStreamHandlerPrefix(stream, []byte(prefix)); err != nil {
					return ctx, err
				}

				return h(ctx, stream)
			})
		}
	}

	cmd.requestWorkWorker = func(workID, prefix string) requestWorkerFunc {
		request := cmd.requestWork(workID, prefix)

		return func(ctx context.Context, worker string, tlsConfig *tls.Config, quicConfig *quic.Config, h quicstream.StreamHandler) (context.Context, error) {
			addr, err := cmd.workerAddr(worker)
			if err != nil {
				return ctx, err
			}

			return request(ctx, addr, tlsConfig, quicConfig, h)
		}
	}

	return nil
}

func (cmd *managerCommand) dial(ctx context.Context, addr net.Addr) (quic.Connection, error) {
	c, _, err := cmd.dialf(ctx, addr, cmd.tlsConfig, cmd.quicConfig)

	return c, err
}

func (cmd *managerCommand) newFuncCancelWorker(workID string) dop5.ManagerCancelWorkerFunc {
	request := cmd.requestWorkWorker(workID, "manager_cancel_worker")

	return func(ctx context.Context, worker string, merr error) error {
		_, err := request(ctx, worker, nil, nil, requestHandlerCancelWorker(merr))

		return err
	}
}

func (cmd *managerCommand) workerAddr(worker string) (*net.UDPAddr, error) {
	addr, found := cmd.workers[worker]
	if !found {
		return nil, util.ErrNotFound.Errorf("unknown worker, %q", worker)
	}

	return addr, nil
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
				return jobWait(err)
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
