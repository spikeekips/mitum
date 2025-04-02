package main

import (
	"context"
	"crypto/tls"
	stderrors "errors"
	"fmt"
	"io"
	"maps"
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
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	"github.com/spikeekips/mitum/example/test364_5/quicstream"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	defaultSubStreamItems     uint64 = 1 << 9
	defaultMaxSubStreamsSize  uint16 = 1 << 8
	startHeight                      = base.Height(33)
	nextHeight                       = base.Height(34)
	manifestsCacheFilenameFmt        = "manifests-%d.b"
	statesCacheFilenameFmt           = "states-%d-%d.b"
	opsTreeCacheFilenameFmt          = "ops_trees-%d-%d.b"
)

type requestWorkerFunc = func(context.Context,
	string, /* worker */
	*tls.Config, *quic.Config, quicstream.StreamHandler) (context.Context, error)

type managerCommand struct {
	baseCommand `cmd:""`
	OpsShare    uint64   `arg:"" help:"ops share"`
	StatesShare uint64   `arg:"" help:"states share"`
	Ops         uint64   `arg:"" help:"number of operations"`
	Workers     []string `arg:"" default:""`
	PreProcess  bool     `name:"pre-process"`

	managerOptions    *dop5.ManagerOptions
	privs             []base.Privatekey
	pubs              []base.Publickey
	createFacts       []dop5.OperationFact
	createOphs        [][2]util.Hash
	transferFacts     []dop5.OperationFact
	transferOphs      [][2]util.Hash
	expectedManifests [2]base.Manifest
	expectedStates    [2][]base.State
	expectedOpsTrees  [2]*fixedtree.Tree
}

func (cmd *managerCommand) Run(log *zerolog.Logger) error {
	if err := cmd.baseCommand.prepare("manager", log); err != nil {
		return err
	}
	defer cmd.baseCommand.closef()

	if err := cmd.prepare(); err != nil {
		return err
	}

	cmd.log.Debug().Msg("cmd/prepared")

	if cmd.PreProcess {
		return nil
	}

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
	case cmd.OpsShare > uint64(len(cmd.Workers)):
		cmd.OpsShare = uint64(len(cmd.Workers))
	}

	switch {
	case cmd.StatesShare < 1:
		return errors.Errorf("zero <states share>")
	case cmd.StatesShare > uint64(len(cmd.Workers)):
		cmd.StatesShare = uint64(len(cmd.Workers))
	}

	if err := cmd.parseWorkersAddr(cmd.Workers); err != nil {
		return err
	}

	cmd.log.Debug().
		Interface("addrs", slices.Collect(maps.Keys(cmd.workerAddrs.Map()))).
		Msg("cmd/workers")

	cmd.managerOptions = dop5.NewManagerOptions()

	if err := cmd.db.flushAll(cmd.ctx); err != nil {
		return err
	}

	if err := cmd.preparePKs(); err != nil {
		return err
	}

	cfacts, cophs, err := cmd.prepareCreateOps()
	if err != nil {
		return err
	}
	cmd.createFacts, cmd.createOphs = cfacts, cophs

	if err := cmd.saveOps(cmd.createFacts); err != nil {
		return err
	}

	tfacts, tophs, err := cmd.prepareTransferOps()
	if err != nil {
		return err
	}
	cmd.transferFacts, cmd.transferOphs = tfacts, tophs
	if err := cmd.saveOps(cmd.transferFacts); err != nil {
		return err
	}

	if err := cmd.preparePreProcessed(); err != nil {
		return err
	}

	return nil
}

func (cmd *managerCommand) preparePKs() error {
	size := cmd.Ops + 1

	l := dutil.ElapsedLogEvent(cmd.log.Debug)

	switch f, err := cmd.pprof("pks"); {
	case err != nil:
		return err
	default:
		defer f()
	}

	created, pubs, err := prepareItems(
		cmd.enc,
		"pubs.b",
		size,
		func(i uint64) (base.Publickey, []byte, error) {
			_, k := btcec.PrivKeyFromBytes([]byte(strconv.FormatInt(int64(i), 10)))

			return base.NewMPublickey(k), k.SerializeCompressed(), nil
		},
		func(b []byte, _ uint64) (base.Publickey, error) {
			k, err := btcec.ParsePubKey(b)
			if err != nil {
				return nil, err
			}

			return base.NewMPublickey(k), nil
		},
	)
	if err != nil {
		return err
	}

	l().Uint64("n", size).Uint64("created", created).Msg("cmd/pks prepared")

	cmd.pubs = pubs

	return nil
}

func (cmd *managerCommand) prepareCreateOps() ([]dop5.OperationFact, [][2]util.Hash, error) {
	l := dutil.ElapsedLogEvent(cmd.log.Debug)

	ophs := make([][2]util.Hash, cmd.Ops)

	switch f, err := cmd.pprof("create-ops"); {
	case err != nil:
		return nil, nil, err
	default:
		defer f()
	}

	created, facts, err := prepareItems(
		cmd.enc,
		"create-ops.b",
		cmd.Ops,
		func(i uint64) (dop5.OperationFact, []byte, error) {
			fact := dop5.NewAccountOpFact(
				[]byte("c"),
				1,
				nil,
				cmd.pubs[i],
			)
			ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}

			b, err := cmd.enc.Marshal(fact)

			return fact, b, err
		},
		func(b []byte, i uint64) (fact dop5.OperationFact, _ error) {
			if err := encoder.Unmarshal(cmd.enc, b, &fact); err != nil {
				return nil, err
			}

			ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}

			return fact, nil
		},
	)
	if err != nil {
		return nil, nil, err
	}

	l().
		Uint64("n", cmd.Ops).
		Uint64("created", created).
		Msg("cmd/create ops prepared")

	return facts, ophs, nil
}

func (cmd *managerCommand) prepareTransferOps() ([]dop5.OperationFact, [][2]util.Hash, error) {
	l := dutil.ElapsedLogEvent(cmd.log.Debug)

	ophs := make([][2]util.Hash, cmd.Ops)

	switch f, err := cmd.pprof("transfer-ops"); {
	case err != nil:
		return nil, nil, err
	default:
		defer f()
	}

	created, facts, err := prepareItems(
		cmd.enc,
		"transfer-ops.b",
		cmd.Ops,
		func(i uint64) (dop5.OperationFact, []byte, error) {
			fact := dop5.NewAccountOpFact(
				[]byte("d"),
				1,
				cmd.pubs[i],
				cmd.pubs[i+1],
			)

			ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}

			b, err := cmd.enc.Marshal(fact)

			return fact, b, err
		},
		func(b []byte, i uint64) (fact dop5.OperationFact, _ error) {
			if err := encoder.Unmarshal(cmd.enc, b, &fact); err != nil {
				return nil, err
			}

			ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}

			return fact, nil
		},
	)
	if err != nil {
		return nil, nil, err
	}

	l().
		Uint64("n", cmd.Ops).
		Uint64("created", created).
		Msg("cmd/transfer ops prepared")

	return facts, ophs, nil
}

func (cmd *managerCommand) process() error {
	var prevHash util.Hash

	switch m, err := cmd.process1st(); {
	case err != nil:
		return err
	default:
		prevHash = m.Hash()
	}

	if _, err := cmd.process2nd(prevHash); err != nil {
		return err
	}

	cmd.log.Debug().Msg("cmd/all processed done")

	return cmd.stopWorkers()
}

func (cmd *managerCommand) process1st() (base.Manifest, error) {
	switch m, w, err := cmd.processHeight(cmd.ctx, startHeight, nil, cmd.createOphs); {
	case err != nil:
		return nil, err
	default:
		if err := w(); err != nil {
			return nil, err
		}

		return m, nil
	}
}

func (cmd *managerCommand) process2nd(prevHash util.Hash) (base.Manifest, error) {
	switch m, w, err := cmd.processHeight(cmd.ctx, nextHeight, prevHash, cmd.transferOphs); {
	case err != nil:
		return nil, err
	default:
		return m, w()
	}
}

func (cmd *managerCommand) saveOps(facts []dop5.OperationFact) error {
	defer dutil.ElapsedLogMsg(func() *zerolog.Event {
		return cmd.log.Debug().Int("n", len(facts))
	}, "cmd/save facts")()

	switch f, err := cmd.pprof("save-ops"); {
	case err != nil:
		return err
	default:
		defer f()
	}

	return cmd.db.saveOperationFacts(cmd.ctx, 1<<9, 1<<9, facts)
}

func (cmd *managerCommand) processHeight(
	ctx context.Context,
	height base.Height,
	prevManiest util.Hash,
	ophs [][2]util.Hash,
) (base.Manifest, dutil.ErrorFunc, error) {
	workID := fmt.Sprintf("%s", height)

	defer dutil.ElapsedLogMsg(func() *zerolog.Event {
		return cmd.log.Debug().
			Str("work_id", workID).
			Interface("height", height)
	}, "cmd/processed height")()

	m, w, err := cmd.startManager(
		dutil.SetContextValueLogger(ctx, cmd.log),
		height,
		prevManiest,
		workID,
		ophs,
	)

	return m, w, err
}

func (cmd *managerCommand) startManager(
	ctx context.Context,
	height base.Height,
	prevManiest util.Hash,
	workID string,
	ophs [][2]util.Hash,
) (base.Manifest, dutil.ErrorFunc, error) {
	l := cmd.log.With().
		Str("work_id", workID).
		Interface("height", height).
		Logger()

	defer dutil.ElapsedLogMsg(func() *zerolog.Event {
		return l.Debug().
			Stringer("prev_hash", prevManiest).
			Int("ophs", len(ophs))
	}, "cmd/manager finished")()

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

	mustAddHandler := cmd.newFuncMustAddHandler(addHandler)

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

	manifestElapsedLog := dutil.ElapsedLogEvent(l.Info)

	var pproff func()

	switch f, err := cmd.pprof(fmt.Sprintf("process-height-%d", height)); {
	case err != nil:
		return nil, nil, err
	default:
		pproff = f
	}

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
	var opsTree *fixedtree.Tree

	if err := wait(
		func(ctx context.Context, worker string) error {
			_, err := requestWorkerInsertDone(ctx, worker, nil, nil, requestHandlerWorkerInsertDone())

			return err
		},
		func(wopsTree, statesTree *fixedtree.Tree) (base.Manifest, error) {
			manifest = newManifest(height, prevManiest, wopsTree, statesTree)
			manifestElapsedLog().Msg("cmd/manifest")

			pproff()

			opsTree = wopsTree

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

	return manifest,
		func() error {
			// NOTE check all worker closes work.
			_ = stderrors.Join(
				cmd.requestWorkers(ctx, func(ctx context.Context, worker string) error {
					_, _ = requestWaitWorkerClosed(ctx, worker, nil, nil, requestHandlerWaitWorkerClosed())

					return nil
				}),
				closeManager(),
			)

			return cmd.checkProcessed(manifest, opsTree)
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

		l := dutil.ElapsedLogMsg(func() *zerolog.Event {
			return cmd.log.Debug().
				Str("worker", worker).
				Uint64("max_items", maxItems)
		}, "cmd/send ops")

		go func() {
			_, err := cmd.requestWork(workID, "manager_send_ops")(ctx, addr, nil, nil, handler.StreamHandler(cmd.openSubStreams))
			_ = requestDone(err)
		}()

		return sendOp, func(err error) error {
			defer l()

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
			return jobWait(err)
		}
	}

	jobDone()

	return jobWait(nil)
}

func (cmd *managerCommand) waitWorkers() error {
	cmd.log.Debug().Msg("cmd/waits workers")

	done, wait := dutil.NewContextDone(cmd.ctx)

	pinged := dutil.NewSingleLockedMap[string, struct{}]()
	cmd.mustAddHandler("_worker_ping_manager", acceptHandlerWorkerPingManager(func(worker string) error {
		if slices.Index(cmd.Workers, worker) < 0 {
			return errors.Errorf("unknown worker, %q", worker)
		}

		if created, _ := pinged.Set(worker, func(_ struct{}, found bool) (struct{}, error) {
			if found {
				return struct{}{}, dutil.ErrLockedSetIgnore
			}

			return struct{}{}, nil
		}); created {
			cmd.log.Debug().Str("worker", worker).Msg("cmd/worker ping")
		}

		if pinged.Len() == len(cmd.Workers) {
			done(nil)
			cmd.log.Debug().Msg("cmd/all worker ping")
		}

		return nil
	}))

	return wait()
}

func (cmd *managerCommand) stopWorkers() error {
	requestStopWorker := cmd.requestWorker("_manager_stop_worker")

	return cmd.requestWorkers(cmd.ctx, func(ctx context.Context, worker string) error {
		_, _ = requestStopWorker(ctx, worker, nil, nil, requestHandlerStopWorker())

		return nil
	})
}

func (cmd *managerCommand) preparePreProcessed() error {
	defer dutil.ElapsedLogMsg(cmd.log.Debug, "cmd/load pre-processed")()

	switch ok, err := cmd.loadPreProcessed(); {
	case err != nil:
		return err
	case ok && !cmd.PreProcess:
		return nil
	}

	var pproff func()
	switch f, err := cmd.pprof("pre-process"); {
	case err != nil:
		return err
	default:
		pproff = f
	}

	switch ms, states, opsTrees, err := cmd.preProcess(); {
	case err != nil:
		return err
	default:
		pproff()

		cmd.expectedManifests = ms
		cmd.expectedStates = states
		cmd.expectedOpsTrees = opsTrees

		if err := saveManifests(cmd.enc, fmt.Sprintf(manifestsCacheFilenameFmt, cmd.Ops), ms); err != nil {
			return err
		}

		for i, height := range slices.All([]base.Height{startHeight, nextHeight}) {
			if err := saveStatesToFile(
				cmd.ctx,
				cmd.enc,
				fmt.Sprintf(statesCacheFilenameFmt, cmd.Ops, height),
				states[i],
			); err != nil {
				return err
			}

			if err := saveOpsTreeToFile(
				cmd.ctx,
				cmd.enc,
				fmt.Sprintf(opsTreeCacheFilenameFmt, cmd.Ops, height),
				opsTrees[i],
			); err != nil {
				return err
			}
		}

		return nil
	}
}

func (cmd *managerCommand) loadPreProcessed() (bool, error) {
	defer dutil.ElapsedLogMsg(cmd.log.Debug, "cmd/prepare manifests")()

	switch f, err := cmd.pprof("load-pre-process"); {
	case err != nil:
		return false, err
	default:
		defer f()
	}

	mfilename := fmt.Sprintf(manifestsCacheFilenameFmt, cmd.Ops)

	switch m, err := loadManifests(cmd.enc, mfilename); {
	case err != nil:
		return false, err
	case m[0] == nil || m[1] == nil:
		return false, nil
	default:
		cmd.expectedManifests = m
	}

	for i, height := range slices.All([]base.Height{startHeight, nextHeight}) {
		switch states, err := loadStatesFromFile(cmd.ctx, cmd.enc, fmt.Sprintf(statesCacheFilenameFmt, cmd.Ops, height)); {
		case err != nil:
			return false, err
		case len(states) < 1:
			return false, nil
		default:
			cmd.expectedStates[i] = states
		}

		switch tr, err := loadOpsTreeFromFile(cmd.ctx, cmd.enc, fmt.Sprintf(opsTreeCacheFilenameFmt, cmd.Ops, height)); {
		case err != nil:
			return false, err
		case tr == nil || tr.Len() < 1:
			return false, nil
		default:
			cmd.expectedOpsTrees[i] = tr
		}
	}

	return true, nil
}

func (cmd *managerCommand) preProcess() (manifests [2]base.Manifest, states [2][]base.State, opsTrees [2]*fixedtree.Tree, _ error) {
	defer dutil.ElapsedLogMsg(cmd.log.Debug, "cmd/make manifests")()

	ctx := dutil.SetContextValueLogger(cmd.ctx, cmd.log)

	manifest33, states33, opsTree33, err := managerRunOneWorker(ctx, cmd.enc, startHeight, nil, nil, cmd.createFacts)
	if err != nil {
		return manifests, states, opsTrees, err
	}

	manifest34, states34, opsTree34, err := managerRunOneWorker(ctx, cmd.enc, nextHeight, manifest33.Hash(), states33, cmd.transferFacts)
	if err != nil {
		return manifests, states, opsTrees, err
	}

	return [2]base.Manifest{manifest33, manifest34},
		[2][]base.State{
			slices.Collect(maps.Values(states33)),
			slices.Collect(maps.Values(states34)),
		},
		[2]*fixedtree.Tree{opsTree33, opsTree34},
		nil
}

func (cmd *managerCommand) checkProcessed(
	m base.Manifest,
	opsTree *fixedtree.Tree,
) error {
	var expectedManifest base.Manifest
	var expectedStates []base.State
	var expectedOpsTree *fixedtree.Tree

	switch m.Height() {
	case startHeight:
		expectedManifest = cmd.expectedManifests[0]
		expectedStates = cmd.expectedStates[0]
		expectedOpsTree = cmd.expectedOpsTrees[0]
	default:
		expectedManifest = cmd.expectedManifests[1]
		expectedStates = cmd.expectedStates[1]
		expectedOpsTree = cmd.expectedOpsTrees[1]
	}

	if !m.Hash().Equal(expectedManifest.Hash()) {
		cmd.log.Error().
			Interface("expected", cmd.expectedManifests[0]).
			Interface("result", m).
			Msgf("cmd/manifest hash mismatch, %d", m.Height())

		return errors.Errorf("manifest hash mismatch, %d", m.Height())
	}

	workerSaved := map[string]base.State{}
	for st, err := range cmd.db.getStates(cmd.ctx) {
		if err != nil {
			return err
		}

		workerSaved[st.Key()] = st
	}

	for a := range slices.Values(expectedStates) {
		b, found := workerSaved[a.Key()]
		if !found {
			return errors.Errorf("state not found, %q", a.Key())
		}

		if !base.IsEqualState(a, b) {
			cmd.log.Error().
				Str("key", a.Key()).
				Interface("expected", a).
				Interface("result", b).
				Msg("cmd/state not equal")

			return errors.Errorf("state not equal, %q", a.Key())
		}
	}

	{
		if a, b := expectedOpsTree.Len(), opsTree.Len(); a != b {
			return errors.Errorf("a.Len(), %d != b.Len(), %d", a, b)
		}

		for index, a := range expectedOpsTree.Traverse() {
			b := opsTree.Node(index)

			if !a.Equal(b) {
				cmd.log.Error().
					Uint64("index", index).
					Interface("expected", a).
					Interface("result", b).
					Msg("cmd/ops tree node")

				return errors.Errorf("node not equal")
			}
		}
	}

	return nil
}

func prepareItems[T any](
	enc encoder.Encoder,
	filename string,
	size uint64,
	create func(uint64) (T, []byte, error),
	load func([]byte, uint64) (T, error),
) (uint64, []T, error) {
	items := make([]T, size)
	rem := size

	var prev *os.File

	switch f, err := os.Open(filename); {
	case err != nil && !os.IsNotExist(err):
		return 0, nil, err
	case os.IsNotExist(err):
	default:
		defer f.Close()
		prev = f

		p, err := loadFromFile(f, size, load)

		switch {
		case err != nil:
			return 0, nil, err
		case len(p) > 0:
			copy(items, p)

			rem = size - uint64(len(p))
		}
	}

	if rem < 1 {
		return 0, items, nil
	}

	var w *os.File

	switch f, err := os.Create(filename + ".tmp"); {
	case err != nil:
		return 0, nil, err
	default:
		if err := writeUint64(f, size); err != nil {
			return 0, nil, err
		}

		if prev != nil {
			if _, err := prev.Seek(0, 0); err != nil {
				return 0, nil, err
			}

			var nsize uint64
			if _, err := readUint64(prev, &nsize); err != nil {
				return 0, nil, err
			}

			if _, err := io.Copy(f, prev); err != nil {
				return 0, nil, err
			}

			_ = prev.Close()
		}

		w = f
	}

	newJob, jobDone, jobWait, _ := dutil.NewFuncJobWorker(context.Background(), rem)

	for i := range rem {
		if err := newJob(func(context.Context) error {
			index := size - rem + i

			var b []byte
			switch v, j, err := create(index); {
			case err != nil:
				return err
			default:
				items[index] = v
				b = j
			}

			ib, err := encodeUint64(index)
			if err != nil {
				return err
			}

			if err := frame.WriteFixeds(w, ib, b); err != nil {
				return err
			}

			return nil
		}); err != nil {
			return 0, nil, jobWait(err)
		}
	}

	jobDone()

	if err := jobWait(nil); err != nil {
		return 0, nil, err
	}

	_ = w.Close()

	if err := os.Rename(filename+".tmp", filename); err != nil {
		return 0, nil, err
	}

	return rem, items, nil
}

func loadFromFile[T any](
	r io.Reader,
	limit uint64,
	load func([]byte, uint64) (T, error),
) ([]T, error) {
	var size uint64
	if _, err := readUint64(r, &size); err != nil {
		return nil, err
	}

	nsize := size
	if size > limit {
		nsize = limit
	}

	items := make([]T, nsize)

	newJob, jobDone, jobWait, _ := dutil.NewFuncJobWorker(context.Background(), nsize)

	var count uint64
	for count < nsize {
		index, b, err := readFixedWithIndex(r)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}

		if index >= nsize {
			continue
		}

		count++

		if err := newJob(func(context.Context) error {
			switch v, err := load(b, index); {
			case err != nil:
				return err
			default:
				items[index] = v

				return nil
			}
		}); err != nil {
			return nil, jobWait(err)
		}
	}

	jobDone()

	if err := jobWait(nil); err != nil {
		return nil, err
	}

	return items, nil
}

func newManifest(
	height base.Height,
	prevManiest util.Hash,
	opsTree, statesTree *fixedtree.Tree,
) base.Manifest {
	var opsr, statesr util.Hash
	if opsTree != nil {
		opsr = opsTree.Root()
	}

	if statesTree != nil {
		statesr = statesTree.Root()
	}

	return isaac.NewManifest(
		height,
		prevManiest,
		valuehash.NewBytes([]byte("proposal")),
		opsr,
		statesr,
		valuehash.NewBytes([]byte("suffrage")),
		time.Unix(1, 1),
	)
}
