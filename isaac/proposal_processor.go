package isaac

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	ErrOperationInProcessorNotFound         = util.NewIDError("operation processor not found")
	ErrInvalidOperationInProcessor          = util.NewIDError("invalid operation")
	ErrOperationNotFoundInProcessor         = util.NewIDError("operation not found")
	ErrOperationAlreadyProcessedInProcessor = util.NewIDError("operation already processed")
	ErrSuspendOperation                     = util.NewIDError("suspend operation")
	ErrProcessorAlreadySaved                = util.NewIDError("processor already saved")
	ErrProposalProcessorEmptyOperations     = util.NewIDError("empty operations in proposal")
)

type (
	NewOperationProcessorFunc         func(base.Height, hint.Hint, base.GetStateFunc) (base.OperationProcessor, error)
	NewOperationProcessorInternalFunc func(base.Height, base.GetStateFunc) (base.OperationProcessor, error)

	// OperationProcessorGetOperationFunction works,
	// - if operation is invalid, getOperation should return nil,
	// ErrInvalidOperationInProcessor; it will be not processed and it's fact
	// hash will be stored.
	// - if operation not found in remote, getOperation should return nil,
	// ErrOperationNotFoundInProcessor; it will be ignored.
	// - if operation is known, return nil,
	// ErrOperationAlreadyProcessedInProcessor; it will be ignored.
	// - if operation fact not match with fact in proposal,
	// return ErrNotProposalProcessorProcessed; it will stop processing proposal
	// and makes wrong ACCEPT ballot for next round.
	OperationProcessorGetOperationFunction func(_ context.Context, operationhash, fact util.Hash) (
		base.Operation, error)
	NewBlockWriterFunc func(base.ProposalSignFact, base.GetStateFunc) (BlockWriter, error)
)

type ProposalProcessor interface {
	Proposal() base.ProposalSignFact
	Process(context.Context, base.INITVoteproof) (base.Manifest, error)
	Save(context.Context, base.ACCEPTVoteproof) (base.BlockMap, error)
	Cancel() error
}

type DefaultProposalProcessorArgs struct {
	NewWriterFunc             NewBlockWriterFunc
	GetStateFunc              base.GetStateFunc
	GetOperationFunc          OperationProcessorGetOperationFunction
	NewOperationProcessorFunc NewOperationProcessorFunc
	EmptyProposalNoBlockFunc  func() bool
	MaxWorkerSize             int64
}

func NewDefaultProposalProcessorArgs() *DefaultProposalProcessorArgs {
	return &DefaultProposalProcessorArgs{
		NewOperationProcessorFunc: func(base.Height, hint.Hint, base.GetStateFunc) (base.OperationProcessor, error) {
			return nil, nil
		},
		MaxWorkerSize:            1 << 13, //nolint:mnd // big enough
		EmptyProposalNoBlockFunc: func() bool { return false },
	}
}

type DefaultProposalProcessor struct {
	*logging.Logging
	proposal    base.ProposalSignFact
	getctx      func() context.Context
	cancel      func()
	previous    base.Manifest
	manifest    base.Manifest
	args        *DefaultProposalProcessorArgs
	writer      BlockWriter
	ivp         base.INITVoteproof
	oprs        *util.ShardedMap[string, base.OperationProcessor]
	stcache     *util.ShardedMap[string, [2]interface{}]
	processlock sync.Mutex
	isprocessed bool
	issaved     bool
}

func NewDefaultProposalProcessor(
	proposal base.ProposalSignFact,
	previous base.Manifest,
	args *DefaultProposalProcessorArgs,
) (*DefaultProposalProcessor, error) {
	oprs, _ := util.NewShardedMap[string, base.OperationProcessor](1<<5, nil) //nolint:mnd //...
	stcache, _ := util.NewShardedMap[string, [2]interface{}](uint64(math.MaxUint16), nil)

	ctx, ctxcancel := context.WithCancel(context.Background())

	p := &DefaultProposalProcessor{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "default-proposal-processor")
		}),
		proposal: proposal,
		getctx:   func() context.Context { return ctx },
		previous: previous,
		args:     args,
		oprs:     oprs,
		stcache:  stcache,
	}

	var cancelonce sync.Once

	p.cancel = func() {
		cancelonce.Do(func() {
			ctxcancel()

			p.Log().Debug().Msg("proposal processor canceled")
		})
	}

	return p, nil
}

func (p *DefaultProposalProcessor) Proposal() base.ProposalSignFact {
	return p.proposal
}

func (p *DefaultProposalProcessor) Process(ctx context.Context, ivp base.INITVoteproof) (base.Manifest, error) {
	p.processlock.Lock()
	defer p.processlock.Unlock()

	defer func() {
		p.oprs.Close()
	}()

	switch {
	case p.isprocessed:
		return nil, errors.Errorf("already processed")
	case p.isCanceled():
		return nil, errors.Errorf("already canceled")
	}

	e := util.StringError("process operations")

	pctx, cancel := context.WithCancel(p.getctx())
	defer cancel()

	deferf, f := p.deferctx(ctx, cancel)
	defer deferf()
	f()

	p.ivp = ivp

	switch manifest, err := p.process(pctx); {
	case err != nil:
		return nil, e.Wrap(err)
	default:
		p.isprocessed = true
		p.manifest = manifest

		return manifest, nil
	}
}

func (p *DefaultProposalProcessor) Save(ctx context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
	defer logging.TimeElapsed()(p.Log().Debug(), "saved")

	p.processlock.Lock()
	defer p.processlock.Unlock()

	switch {
	case p.issaved:
		return nil, ErrProcessorAlreadySaved.WithStack()
	case p.isCanceled():
		return nil, errors.Errorf("already canceled")
	}

	sctx, cancel := context.WithCancel(p.getctx())
	defer cancel()

	deferf, f := p.deferctx(ctx, cancel)
	defer deferf()
	f()

	defer p.close()

	p.issaved = true

	switch bm, err := p.save(sctx, avp); {
	case err != nil:
		p.Log().Error().Err(err).Msg("save")

		return nil, err
	default:
		return bm, nil
	}
}

func (p *DefaultProposalProcessor) Cancel() error {
	p.cancel()

	return nil
}

func (p *DefaultProposalProcessor) close() {
	p.cancel()
}

func (p *DefaultProposalProcessor) isCanceled() bool {
	return p.getctx().Err() != nil
}

func (p *DefaultProposalProcessor) process(ctx context.Context) (base.Manifest, error) {
	defer logging.TimeElapsed()(p.Log().Debug(), "processed")

	var cops, reserved []base.Operation

	switch i, j, err := p.collectOperations(ctx); {
	case err != nil:
		return nil, errors.WithMessage(err, "collect operations")
	case len(i) < 1 && len(j) < 1:
		if p.args.EmptyProposalNoBlockFunc() {
			return nil, ErrProposalProcessorEmptyOperations.Errorf("collect operations")
		}
	default:
		cops = i
		reserved = j
	}

	switch writer, err := p.args.NewWriterFunc(p.proposal, p.getStateFunc); {
	case err != nil:
		return nil, errors.Wrap(err, "make new ProposalProcessor")
	default:
		p.writer = writer

		if i, ok := writer.(logging.SetLogging); ok {
			_ = i.SetLogging(p.Logging)
		}
	}

	if len(cops) > 0 || len(reserved) > 0 {
		if err := p.processOperations(ctx, cops, reserved); err != nil {
			return nil, errors.WithMessage(err, "process operations")
		}
	}

	var manifest base.Manifest

	switch i, err := p.createManifest(ctx); {
	case err != nil:
		return nil, err
	default:
		manifest = i
	}

	p.Log().Info().Interface("manifest", manifest).Msg("new manifest prepared")

	return manifest, nil
}

func (p *DefaultProposalProcessor) collectOperations(ctx context.Context) (cops, reserved []base.Operation, _ error) {
	defer logging.TimeElapsed()(p.Log().Debug(), "operations collected")

	e := util.StringError("collect operations")

	if w, ok := p.ivp.(base.ExpelVoteproof); ok {
		expels := w.Expels()
		reserved = make([]base.Operation, len(expels))

		for i := range expels {
			reserved[i] = expels[i]
		}

		p.Log().Debug().
			Int("operations", len(reserved)).
			Msg("collecting reserved operations")
	}

	ophs := p.proposal.ProposalFact().Operations()

	cops = make([]base.Operation, len(ophs))

	p.Log().Debug().
		Int("operations", len(cops)).
		Msg("collecting operations")

	if len(ophs) < 1 {
		return cops, reserved, nil
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workersize := int64(len(ophs))
	if workersize > p.args.MaxWorkerSize {
		workersize = p.args.MaxWorkerSize
	}

	if err := util.RunJobWorker(cctx, workersize, int64(len(ophs)), func(ctx context.Context, i, _ uint64) error {
		oph := ophs[i][0]
		fact := ophs[i][1]

		l := p.Log().With().
			Stringer("operation", oph).
			Stringer("fact", fact).
			Logger()

		switch op, err := p.getOperation(ctx, oph, fact); {
		case err != nil:
			l.Debug().Err(err).Msg("failed to collect operation")

			return err
		case op == nil:
			l.Debug().Msg("operation ignored")
		default:
			l.Trace().Msg("operation collected")

			cops[i] = op
		}

		return nil
	}); err != nil {
		cancel()

		return cops, reserved, e.Wrap(err)
	}

	return cops, reserved, nil
}

func (p *DefaultProposalProcessor) processOperations(ctx context.Context, cops, reserved []base.Operation) error {
	defer logging.TimeElapsed()(
		p.Log().Debug().
			Dict("operations", zerolog.Dict().
				Int("proposal", len(cops)).
				Int("reserved", len(reserved)),
			),
		"operations processed")

	e := util.StringError("process operations")

	nops := len(cops) + len(reserved)

	p.writer.SetOperationsSize(uint64(nops))

	var worker *util.BaseJobWorker

	{
		workersize := int64(nops)
		if workersize > p.args.MaxWorkerSize {
			workersize = p.args.MaxWorkerSize
		}

		switch i, err := util.NewBaseJobWorker(ctx, workersize); {
		case err != nil:
			return e.Wrap(err)
		default:
			worker = i

			defer worker.Close()
		}
	}

	var hasresultcount int64

	pctx := ctx

	for i := 0; i < len(cops)+len(reserved); i++ {
		var op base.Operation

		switch {
		case i < len(cops):
			op = cops[i]
		default:
			op = reserved[i-len(cops)]
		}

		if op == nil {
			continue
		}

		index := i

		var hasresult bool
		var err error

		switch pctx, hasresult, err = p.processOperation(pctx, worker, op, index); {
		case err != nil:
			return e.Wrap(err)
		case hasresult:
			atomic.AddInt64(&hasresultcount, 1)
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e.Wrap(err)
	}

	if atomic.LoadInt64(&hasresultcount) < 1 && p.args.EmptyProposalNoBlockFunc() {
		return ErrProposalProcessorEmptyOperations.Errorf("process")
	}

	return nil
}

func (p *DefaultProposalProcessor) processOperation(
	ctx context.Context,
	worker *util.BaseJobWorker,
	op base.Operation,
	opsindex int,
) (_ context.Context, hasresult bool, _ error) {
	writer := p.writer

	if rop, ok := op.(ReasonProcessedOperation); ok {
		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			return writer.SetProcessResult( //nolint:wrapcheck //...
				ctx, uint64(opsindex), rop.OperationHash(), rop.FactHash(), false, rop.Reason())
		}); err != nil {
			return ctx, false, err
		}

		return ctx, true, nil
	}

	var nctx context.Context

	switch pctx, reasonerr, passed, err := p.doPreProcessOperation(ctx, op); {
	case err != nil:
		return pctx, true, errors.WithMessage(err, "pre process operation")
	case reasonerr != nil:
		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			return writer.SetProcessResult(
				ctx, uint64(opsindex), op.Hash(), op.Fact().Hash(), false, reasonerr,
			)
		}); err != nil {
			return pctx, false, err
		}

		return pctx, true, nil
	case !passed:
		return pctx, false, nil
	default:
		nctx = pctx
	}

	newOperationProcessor := p.args.NewOperationProcessorFunc

	if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
		return p.doProcessOperation(ctx, writer, newOperationProcessor, uint64(opsindex), op)
	}); err != nil {
		return nctx, false, err
	}

	return nctx, true, nil
}

func (p *DefaultProposalProcessor) doPreProcessOperation(
	ctx context.Context,
	op base.Operation,
) (context.Context, base.OperationProcessReasonError, bool, error) {
	var f func(context.Context) (context.Context, base.OperationProcessReasonError, error)

	switch i, err := p.getPreProcessor(p.args.NewOperationProcessorFunc, op); {
	case err != nil:
		return ctx, nil, false, err
	case i == nil:
		return ctx, nil, false, nil // NOTE ignore
	default:
		f = i
	}

	switch pctx, errorreason, err := f(ctx); {
	case err == nil:
		return pctx, errorreason, true, nil
	case errors.Is(err, ErrSuspendOperation):
		return pctx, nil, false, nil
	default:
		return pctx, nil, false, err
	}
}

func (p *DefaultProposalProcessor) doProcessOperation(
	ctx context.Context,
	writer BlockWriter,
	newOperationProcessor NewOperationProcessorFunc,
	opsindex uint64,
	op base.Operation,
) error {
	e := util.StringError("process operation, %q", op.Fact().Hash())

	var f func(context.Context) ([]base.StateMergeValue, base.OperationProcessReasonError, error)

	switch i, err := p.getProcessor(newOperationProcessor, op); {
	case err != nil:
		return err
	case i == nil:
		return nil // NOTE ignore
	default:
		f = i
	}

	switch stvs, errorreason, err := f(ctx); {
	case err != nil:
		return err
	case len(stvs) < 1:
		if errorreason == nil {
			return e.Errorf("empty state must have reason")
		}
	case errorreason != nil:
		return e.Errorf("not empty state must have empty reason")
	default:
		instate := len(stvs) > 0
		if instate {
			if err := writer.SetStates(ctx, opsindex, stvs, op); err != nil {
				return e.Wrap(err)
			}
		}

		if err := writer.SetProcessResult(
			ctx, opsindex, op.Hash(), op.Fact().Hash(), instate, errorreason,
		); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func (p *DefaultProposalProcessor) getPreProcessor(newOperationProcessor NewOperationProcessorFunc, op base.Operation) (
	func(context.Context) (context.Context, base.OperationProcessReasonError, error),
	error,
) {
	switch opp, found, err := p.getOperationProcessor(newOperationProcessor, op.Hint()); {
	case err != nil:
		return nil, errors.Wrap(err, "get OperationProcessor for PreProcess")
	case found:
		return func(ctx context.Context) (context.Context, base.OperationProcessReasonError, error) {
			return opp.PreProcess(ctx, op, p.getStateFunc) //nolint:wrapcheck //...
		}, nil
	}

	return func(ctx context.Context) (context.Context, base.OperationProcessReasonError, error) {
		return op.PreProcess(ctx, p.getStateFunc) //nolint:wrapcheck //...
	}, nil
}

func (p *DefaultProposalProcessor) getProcessor(newOperationProcessor NewOperationProcessorFunc, op base.Operation) (
	func(context.Context) ([]base.StateMergeValue, base.OperationProcessReasonError, error),
	error,
) {
	switch opp, found, err := p.getOperationProcessor(newOperationProcessor, op.Hint()); {
	case err != nil:
		return nil, errors.Wrap(err, "get OperationProcessor for Process")
	case found:
		return func(ctx context.Context) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
			return opp.Process(ctx, op, p.getStateFunc) //nolint:wrapcheck //...
		}, nil
	}

	return func(ctx context.Context) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
		return op.Process(ctx, p.getStateFunc) //nolint:wrapcheck //...
	}, nil
}

func (p *DefaultProposalProcessor) getOperationProcessor(
	newOperationProcessor NewOperationProcessorFunc,
	ht hint.Hint,
) (base.OperationProcessor, bool, error) {
	var j base.OperationProcessor

	switch err := p.oprs.GetOrCreate(
		ht.String(),
		func(i base.OperationProcessor, _ bool) error {
			j = i

			return nil
		},
		func() (base.OperationProcessor, error) {
			switch i, err := newOperationProcessor(p.proposal.Point().Height(), ht, p.getStateFunc); {
			case err != nil:
				return nil, err
			case i == nil:
				return nil, ErrOperationInProcessorNotFound.WithStack()
			default:
				return i, nil
			}
		},
	); {
	case err == nil:
		return j, true, nil
	case errors.Is(err, ErrOperationInProcessorNotFound):
		return nil, false, nil
	default:
		return nil, false, errors.Wrap(err, "get OperationProcessor")
	}
}

func (p *DefaultProposalProcessor) save(ctx context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
	e := util.StringError("save")

	switch {
	case p.manifest == nil,
		!p.manifest.Hash().Equal(avp.BallotMajority().NewBlock()):
		return nil, ErrNotProposalProcessorProcessed.Errorf("different manifest hash with majority")
	}

	if err := p.writer.SetINITVoteproof(ctx, p.ivp); err != nil {
		return nil, e.WithMessage(err, "set init voteproof")
	}

	if err := p.writer.SetACCEPTVoteproof(ctx, avp); err != nil {
		return nil, e.WithMessage(err, "set accept voteproof")
	}

	m, err := p.writer.Save(ctx)
	if err != nil {
		return nil, e.Wrap(err)
	}

	p.Log().Info().Interface("blockmap", m).Msg("new block saved in proposal processor")

	return m, nil
}

func (*DefaultProposalProcessor) deferctx(ctx context.Context, cancel func()) (func(), func()) {
	donech := make(chan struct{}, 1)

	return func() {
			donech <- struct{}{}
		},
		func() {
			go func() {
				select {
				case <-donech:
					return
				case <-ctx.Done():
					if ctx.Err() != nil {
						cancel()
					}
				}
			}()
		}
}

func (p *DefaultProposalProcessor) getOperation(ctx context.Context, oph, fact util.Hash) (base.Operation, error) {
	switch op, err := p.args.GetOperationFunc(ctx, oph, fact); {
	case err == nil:
		if op == nil {
			return nil, nil
		}

		// NOTE suffrage expel operation should be in voteproof.
		if _, ok := op.(base.SuffrageExpelOperation); ok {
			return nil, nil
		}

		// NOTE fetched operation fact hash != fact hash, stop processing
		if !op.Fact().Hash().Equal(fact) {
			return nil, ErrNotProposalProcessorProcessed
		}

		return op, nil
	case errors.Is(err, util.ErrInvalid),
		errors.Is(err, ErrOperationNotFoundInProcessor),
		errors.Is(err, ErrOperationAlreadyProcessedInProcessor):
		return nil, nil
	case errors.Is(err, ErrInvalidOperationInProcessor):
		return NewReasonProcessedOperation(
			oph,
			fact,
			base.NewBaseOperationProcessReason(err.Error()),
		), nil
	default:
		return nil, err
	}
}

func (p *DefaultProposalProcessor) createManifest(ctx context.Context) (base.Manifest, error) {
	defer logging.TimeElapsed()(p.Log().Debug(), "manifest processed")

	return p.writer.Manifest(ctx, p.previous)
}

func (p *DefaultProposalProcessor) getStateFunc(key string) (st base.State, found bool, _ error) {
	err := p.stcache.GetOrCreate(
		key,
		func(i [2]interface{}, _ bool) error {
			if i[0] != nil {
				st = i[0].(base.State) //nolint:forcetypeassert //...
			}

			found = i[1].(bool) //nolint:forcetypeassert //...

			return nil
		},
		func() (v [2]interface{}, _ error) {
			switch i, j, err := p.args.GetStateFunc(key); {
			case err != nil:
				return v, err
			default:
				return [2]interface{}{i, j}, nil
			}
		},
	)

	return st, found, err
}

type ReasonProcessedOperation struct {
	base.Operation
	op       util.Hash
	facthash util.Hash
	reason   base.OperationProcessReasonError
}

func NewReasonProcessedOperation(
	op, facthash util.Hash, reason base.OperationProcessReasonError,
) ReasonProcessedOperation {
	return ReasonProcessedOperation{op: op, facthash: facthash, reason: reason}
}

func (op ReasonProcessedOperation) OperationHash() util.Hash {
	return op.op
}

func (op ReasonProcessedOperation) FactHash() util.Hash {
	return op.facthash
}

func (op ReasonProcessedOperation) Reason() base.OperationProcessReasonError {
	return op.reason
}
