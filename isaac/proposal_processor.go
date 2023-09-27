package isaac

import (
	"context"
	"sync"
	"time"

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
	ErrStopProcessingRetry                  = util.NewIDError("stop processing retrying")
	ErrIgnoreStateValue                     = util.NewIDError("ignore state value")
	ErrSuspendOperation                     = util.NewIDError("suspend operation")
	ErrProcessorAlreadySaved                = util.NewIDError("processor already saved")
)

type (
	NewOperationProcessorFunc         func(base.Height, hint.Hint) (base.OperationProcessor, error)
	NewOperationProcessorInternalFunc func(base.Height) (base.OperationProcessor, error)

	// OperationProcessorGetOperationFunction works,
	// - if operation is invalid, getOperation should return nil,
	// InvalidOperationInProcessorError; it will be not processed and it's fact
	// hash will be stored.
	// - if operation not found in remote, getOperation should return nil,
	// OperationNotFoundInProcessorError; it will be ignored.
	// - if operation is known, return nil,
	// OperationAlreadyProcessedInProcessorError; it will be ignored.
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
	Retrylimit                int
	Retryinterval             time.Duration
	MaxWorkerSize             int64
}

func NewDefaultProposalProcessorArgs() *DefaultProposalProcessorArgs {
	return &DefaultProposalProcessorArgs{
		NewOperationProcessorFunc: func(base.Height, hint.Hint) (base.OperationProcessor, error) {
			return nil, nil
		},
		Retrylimit:    15,                     //nolint:gomnd //...
		Retryinterval: time.Millisecond * 600, //nolint:gomnd //...
		MaxWorkerSize: 1 << 13,                //nolint:gomnd // big enough
	}
}

type DefaultProposalProcessor struct {
	*logging.Logging
	proposal     base.ProposalSignFact
	getctx       func() context.Context
	cancel       func()
	previous     base.Manifest
	args         *DefaultProposalProcessorArgs
	writer       BlockWriter
	ivp          base.INITVoteproof
	oprs         *util.ShardedMap[string, base.OperationProcessor]
	processstate *util.Locked[int]
	processlock  sync.Mutex
}

func NewDefaultProposalProcessor(
	proposal base.ProposalSignFact,
	previous base.Manifest,
	args *DefaultProposalProcessorArgs,
) (*DefaultProposalProcessor, error) {
	oprs, _ := util.NewShardedMap[string, base.OperationProcessor](1<<5, nil) //nolint:gomnd //...

	ctx, cancel := context.WithCancel(context.Background())

	return &DefaultProposalProcessor{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "default-proposal-processor")
		}),
		proposal:     proposal,
		getctx:       func() context.Context { return ctx },
		cancel:       cancel,
		previous:     previous,
		args:         args,
		oprs:         oprs,
		processstate: util.NewLocked(0),
	}, nil
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
	case p.isCanceled():
		return nil, errors.Errorf("already canceled")
	case p.isProcessed():
		return nil, errors.Errorf("already processed")
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
		return manifest, nil
	}
}

func (p *DefaultProposalProcessor) Save(ctx context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
	p.processlock.Lock()
	defer p.processlock.Unlock()

	switch {
	case p.isCanceled():
		return nil, errors.Errorf("already canceled")
	case p.isSaved():
		return nil, ErrProcessorAlreadySaved.WithStack()
	}

	sctx, cancel := context.WithCancel(p.getctx())
	defer cancel()

	deferf, f := p.deferctx(ctx, cancel)
	defer deferf()
	f()

	defer p.close()

	var bm base.BlockMap

	if err := util.Retry(sctx, func() (bool, error) {
		switch i, err := p.save(sctx, avp); {
		case err == nil:
			bm = i

			return false, nil
		case errors.Is(err, ErrProcessorAlreadySaved),
			errors.Is(err, ErrStopProcessingRetry):
			return false, err
		default:
			p.Log().Error().Err(err).Msg("failed to save; will retry")

			return true, err
		}
	}, p.args.Retrylimit, p.args.Retryinterval); err != nil {
		return nil, errors.Wrap(err, "save proposal")
	}

	_, err := p.processstate.Set(func(i int, _ bool) (int, error) {
		if i < 0 {
			return i, errors.Errorf("save proposal; already canceled")
		}

		return 2, nil
	})

	return bm, err
}

func (p *DefaultProposalProcessor) Cancel() error {
	_, _ = p.processstate.Set(func(i int, _ bool) (int, error) {
		if i == -1 {
			return i, util.ErrLockedSetIgnore.WithStack()
		}

		p.cancel()

		p.Log().Debug().Msg("proposal processor canceled")

		return -1, nil
	})

	return nil
}

func (p *DefaultProposalProcessor) close() {
	_, _ = p.processstate.Set(func(i int, _ bool) (int, error) {
		p.cancel()

		return i, util.ErrLockedSetIgnore.WithStack()
	})
}

func (p *DefaultProposalProcessor) isCanceled() bool {
	i, _ := p.processstate.Value()

	return i == -1 //nolint:gomnd //...
}

func (p *DefaultProposalProcessor) isProcessed() bool {
	i, _ := p.processstate.Value()

	return i == 1
}

func (p *DefaultProposalProcessor) isSaved() bool {
	i, _ := p.processstate.Value()

	return i == 2
}

func (p *DefaultProposalProcessor) process(ctx context.Context) (base.Manifest, error) {
	var cops []base.Operation

	switch i, err := p.collectOperations(ctx); {
	case err != nil:
		return nil, errors.WithMessage(err, "collect operations")
	case len(i) < 1:
	default:
		cops = i
	}

	switch writer, err := p.args.NewWriterFunc(p.proposal, p.args.GetStateFunc); {
	case err != nil:
		return nil, errors.Wrap(err, "make new ProposalProcessor")
	default:
		p.writer = writer
	}

	if len(cops) > 0 {
		if err := p.processOperations(ctx, cops); err != nil {
			return nil, errors.WithMessage(err, "process operations")
		}
	}

	manifest, err := p.writer.Manifest(ctx, p.previous)
	if err != nil {
		return nil, err
	}

	p.Log().Info().Interface("manifest", manifest).Msg("new manifest prepared")

	if _, err := p.processstate.Set(func(i int, _ bool) (int, error) {
		if i < 0 {
			return i, errors.Errorf("process proposal; already canceled")
		}

		return 1, nil
	}); err != nil {
		return nil, err
	}

	return manifest, nil
}

func (p *DefaultProposalProcessor) collectOperations(ctx context.Context) ([]base.Operation, error) {
	e := util.StringError("collect operations")

	var cops []base.Operation
	var index int

	switch w, ok := p.ivp.(base.ExpelVoteproof); {
	case ok:
		expels := w.Expels()
		cops = make([]base.Operation, len(p.proposal.ProposalFact().Operations())+len(expels))

		for i := range expels {
			cops[i] = expels[i]
		}

		index = len(expels)

		p.Log().Debug().
			Int("operations", len(cops)).
			Int("expels", len(expels)).
			Msg("collecting operations")
	default:
		cops = make([]base.Operation, len(p.proposal.ProposalFact().Operations()))

		p.Log().Debug().
			Int("operations", len(cops)).
			Msg("collecting operations")
	}

	if len(p.proposal.ProposalFact().Operations()) < 1 {
		return cops, nil
	}

	ophs := p.proposal.ProposalFact().Operations()

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workersize := int64(len(ophs))
	if workersize > p.args.MaxWorkerSize {
		workersize = p.args.MaxWorkerSize
	}

	if err := util.RunErrgroupWorker(cctx, workersize, func(ctx context.Context, i, _ uint64) error {
		oph := ophs[i][0]
		fact := ophs[i][1]

		l := p.Log().With().
			Stringer("operation", oph).
			Stringer("fact", fact).
			Logger()

		op, err := p.collectOperation(ctx, oph, fact)

		switch {
		case err != nil:
			l.Error().Err(err).Msg("failed to collect operation")

			return err
		case op == nil:
			l.Debug().Msg("operation ignored")
		default:
			l.Trace().Msg("operation collected")

			cops[index+int(i)] = op
		}

		return nil
	}); err != nil {
		cancel()

		return nil, e.Wrap(err)
	}

	return cops, nil
}

func (p *DefaultProposalProcessor) processOperations(ctx context.Context, cops []base.Operation) error {
	e := util.StringError("process operations")

	p.Log().Debug().Int("operations", len(cops)).Msg("trying to process operations")

	p.writer.SetOperationsSize(uint64(len(cops)))

	workersize := int64(len(cops))
	if workersize > p.args.MaxWorkerSize {
		workersize = p.args.MaxWorkerSize
	}

	worker, err := util.NewErrgroupWorker(ctx, workersize)
	if err != nil {
		return e.Wrap(err)
	}

	defer worker.Close()

	pctx := ctx

	var opsindex, validindex int

	writer := p.writer
	getStatef := p.args.GetStateFunc
	newOperationProcessor := p.args.NewOperationProcessorFunc

	for i := range cops {
		op := cops[i]
		if op == nil {
			continue
		}

		var err error

		pctx, opsindex, validindex, err = p.processOperation(
			pctx, writer, getStatef, newOperationProcessor, worker, op, opsindex, validindex)
		if err != nil {
			return e.Wrap(err)
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (p *DefaultProposalProcessor) processOperation(
	ctx context.Context,
	writer BlockWriter,
	getStatef base.GetStateFunc,
	newOperationProcessor NewOperationProcessorFunc,
	worker *util.ErrgroupWorker,
	op base.Operation,
	opsindex, validindex int,
) (_ context.Context, newopsindex int, newvalidindex int, _ error) {
	if rop, ok := op.(ReasonProcessedOperation); ok {
		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			return writer.SetProcessResult( //nolint:wrapcheck //...
				ctx, uint64(opsindex), rop.OperationHash(), rop.FactHash(), false, rop.Reason())
		}); err != nil {
			return ctx, 0, 0, err
		}
	}

	var nctx context.Context

	switch pctx, reasonerr, passed, err := p.doPreProcessOperation(ctx, newOperationProcessor, op); {
	case err != nil:
		return pctx, 0, 0, errors.WithMessage(err, "pre process operation")
	case reasonerr != nil:
		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			return writer.SetProcessResult(
				ctx, uint64(opsindex), op.Hash(), op.Fact().Hash(), false, reasonerr,
			)
		}); err != nil {
			return pctx, 0, 0, err
		}

		return pctx, opsindex + 1, validindex, nil
	case !passed:
		return pctx, opsindex, validindex, nil
	default:
		nctx = pctx
	}

	if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
		return p.doProcessOperation(
			ctx, writer, getStatef, newOperationProcessor, uint64(opsindex), uint64(validindex), op)
	}); err != nil {
		return nctx, opsindex + 1, validindex + 1, err
	}

	return nctx, opsindex + 1, validindex + 1, nil
}

func (p *DefaultProposalProcessor) doPreProcessOperation(
	ctx context.Context,
	newOperationProcessor NewOperationProcessorFunc,
	op base.Operation,
) (context.Context, base.OperationProcessReasonError, bool, error) {
	var errorreason base.OperationProcessReasonError

	pctx := ctx

	err := p.retry(ctx, func() (bool, error) {
		f, err := p.getPreProcessor(ctx, newOperationProcessor, op)

		switch {
		case err != nil:
			return false, err
		case f == nil:
			return false, nil
		}

		switch i, j, err := f(ctx); {
		case err == nil:
			errorreason = j

			pctx = i

			return false, nil
		case errors.Is(err, ErrSuspendOperation):
			pctx = i

			return false, err
		default:
			pctx = i

			return true, err
		}
	})

	if errors.Is(err, ErrSuspendOperation) {
		return pctx, nil, false, nil
	}

	return pctx, errorreason, true, err
}

func (p *DefaultProposalProcessor) doProcessOperation(
	ctx context.Context,
	writer BlockWriter,
	getStatef base.GetStateFunc,
	newOperationProcessor NewOperationProcessorFunc,
	opsindex, validindex uint64,
	op base.Operation,
) error {
	e := util.StringError("process operation, %q", op.Fact().Hash())

	var errorreason base.OperationProcessReasonError
	var stvs []base.StateMergeValue

	if err := p.retry(ctx, func() (bool, error) {
		if stvs == nil {
			f, err := p.getProcessor(ctx, getStatef, newOperationProcessor, op)

			switch {
			case err != nil:
				return false, err
			case f == nil:
				return false, nil
			}

			i, j, err := f(ctx)

			switch {
			case err == nil:
				stvs = i
				errorreason = j
			default:
				return true, err
			}
		}

		switch ee := util.StringError("invalid processor"); {
		case len(stvs) < 1:
			if errorreason == nil {
				return false, ee.Errorf("empty state must have reason")
			}
		case errorreason != nil:
			return false, ee.Errorf("not empty state must have empty reason")
		}

		instate := len(stvs) > 0
		if instate {
			if err := writer.SetStates(ctx, validindex, stvs, op); err != nil {
				return true, e.Wrap(err)
			}
		}

		if err := writer.SetProcessResult(
			ctx, opsindex, op.Hash(), op.Fact().Hash(), instate, errorreason,
		); err != nil {
			return true, e.Wrap(err)
		}

		return false, nil
	}); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (p *DefaultProposalProcessor) getPreProcessor(
	ctx context.Context,
	newOperationProcessor NewOperationProcessorFunc,
	op base.Operation,
) (
	func(context.Context) (context.Context, base.OperationProcessReasonError, error),
	error,
) {
	switch opp, found, err := p.getOperationProcessor(ctx, newOperationProcessor, op.Hint()); {
	case err != nil:
		return nil, errors.Wrap(err, "get OperationProcessor for PreProcess")
	case found:
		return func(ctx context.Context) (context.Context, base.OperationProcessReasonError, error) {
			return opp.PreProcess(ctx, op, p.args.GetStateFunc) //nolint:wrapcheck //...
		}, nil
	}

	return func(ctx context.Context) (context.Context, base.OperationProcessReasonError, error) {
		return op.PreProcess(ctx, p.args.GetStateFunc) //nolint:wrapcheck //...
	}, nil
}

func (p *DefaultProposalProcessor) getProcessor(
	ctx context.Context,
	getStatef base.GetStateFunc,
	newOperationProcessor NewOperationProcessorFunc,
	op base.Operation,
) (
	func(context.Context) ([]base.StateMergeValue, base.OperationProcessReasonError, error),
	error,
) {
	switch opp, found, err := p.getOperationProcessor(ctx, newOperationProcessor, op.Hint()); {
	case err != nil:
		return nil, errors.Wrap(err, "get OperationProcessor for Process")
	case found:
		return func(ctx context.Context) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
			return opp.Process(ctx, op, getStatef) //nolint:wrapcheck //...
		}, nil
	}

	return func(ctx context.Context) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
		return op.Process(ctx, getStatef) //nolint:wrapcheck //...
	}, nil
}

func (p *DefaultProposalProcessor) getOperationProcessor(
	ctx context.Context,
	newOperationProcessor NewOperationProcessorFunc,
	ht hint.Hint,
) (
	base.OperationProcessor, bool, error,
) {
	var j base.OperationProcessor

	switch err := p.oprs.GetOrCreate(
		ht.String(),
		func(i base.OperationProcessor, _ bool) error {
			j = i

			return nil
		},
		func() (opp base.OperationProcessor, _ error) {
			if err := p.retry(ctx, func() (bool, error) {
				i, err := newOperationProcessor(p.proposal.Point().Height(), ht)
				if err != nil {
					return true, err
				}

				opp = i

				return false, nil
			}); err != nil {
				return nil, err
			}

			if opp == nil {
				return nil, ErrOperationInProcessorNotFound.WithStack()
			}

			return opp, nil
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

func (p *DefaultProposalProcessor) retry(ctx context.Context, f func() (bool, error)) error {
	return util.Retry(ctx, func() (bool, error) {
		keep, err := f()
		if errors.Is(err, ErrStopProcessingRetry) {
			return false, err
		}

		return keep, err
	}, p.args.Retrylimit, p.args.Retryinterval)
}

func (p *DefaultProposalProcessor) save(ctx context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
	e := util.StringError("save")

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

func (p *DefaultProposalProcessor) collectOperation(
	ctx context.Context,
	oph, fact util.Hash,
) (base.Operation, error) {
	e := util.StringError("collect operation, %q %q", oph, fact)

	var op base.Operation

	if err := p.retry(ctx, func() (bool, error) {
		switch j, ok, err := p.getOperation(ctx, oph, fact); {
		case err == nil:
			op = j

			return false, nil
		case ok:
			return false, err
		default:
			return true, err
		}
	}); err != nil {
		return nil, e.Wrap(err)
	}

	return op, nil
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

func (p *DefaultProposalProcessor) getOperation(ctx context.Context, oph, fact util.Hash) (
	_ base.Operation,
	ok bool,
	_ error,
) {
	switch op, err := p.args.GetOperationFunc(ctx, oph, fact); {
	case err == nil:
		if op == nil {
			return nil, true, nil
		}

		// NOTE suffrage expel operation should be in voteproof.
		if _, ok := op.(base.SuffrageExpelOperation); ok {
			return nil, true, nil
		}

		// NOTE fetched operation fact hash != fact hash, stop processing
		if !op.Fact().Hash().Equal(fact) {
			return nil, true, ErrNotProposalProcessorProcessed
		}

		return op, true, nil
	case errors.Is(err, util.ErrInvalid),
		errors.Is(err, ErrInvalidOperationInProcessor),
		errors.Is(err, ErrOperationNotFoundInProcessor),
		errors.Is(err, ErrOperationAlreadyProcessedInProcessor):
		return nil, true, nil
	case errors.Is(err, ErrNotProposalProcessorProcessed):
		return nil, true, err
	default:
		return nil, false, err
	}
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
