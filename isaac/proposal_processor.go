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
	ErrOperationInProcessorNotFound         = util.NewError("operation processor not found")
	ErrInvalidOperationInProcessor          = util.NewError("invalid operation")
	ErrOperationNotFoundInProcessor         = util.NewError("operation not found")
	ErrOperationAlreadyProcessedInProcessor = util.NewError("operation already processed")
	ErrStopProcessingRetry                  = util.NewError("stop processing retrying")
	ErrIgnoreStateValue                     = util.NewError("ignore state value")
	ErrSuspendOperation                     = util.NewError("suspend operation")
	ErrProcessorAlreadySaved                = util.NewError("processor already saved")
)

type (
	NewOperationProcessorFunction func(base.Height, hint.Hint) (base.OperationProcessor, error)

	// OperationProcessorGetOperationFunction works,
	// - if operation is invalid, getOperation should return nil,
	// InvalidOperationInProcessorError; it will be not processed and it's fact
	// hash will be stored.
	// - if operation is not found in remote, getOperation should return nil,
	// OperationNotFoundInProcessorError; it will be ignored.
	// - if operation is known, return nil,
	// OperationAlreadyProcessedInProcessorError; it will be ignored.
	OperationProcessorGetOperationFunction func(_ context.Context, operationhash util.Hash) (base.Operation, error)
	NewBlockWriterFunc                     func(base.ProposalSignFact, base.GetStateFunc) (BlockWriter, error)
)

type ProposalProcessor interface {
	Proposal() base.ProposalSignFact
	Process(context.Context, base.INITVoteproof) (base.Manifest, error)
	Save(context.Context, base.ACCEPTVoteproof) error
	Cancel() error
}

type DefaultProposalProcessor struct {
	writer                BlockWriter
	ivp                   base.INITVoteproof
	proposal              base.ProposalSignFact
	previous              base.Manifest
	newOperationProcessor NewOperationProcessorFunction
	getStateFunc          base.GetStateFunc
	getOperation          OperationProcessorGetOperationFunction
	*logging.Logging
	oprs          *util.ShardedMap
	cancel        func()
	cops          []base.Operation
	retrylimit    int
	retryinterval time.Duration
	opslock       sync.RWMutex
	sync.RWMutex
	cancellock sync.RWMutex
	issaved    bool
}

func NewDefaultProposalProcessor(
	proposal base.ProposalSignFact,
	previous base.Manifest,
	newWriter NewBlockWriterFunc,
	getStateFunc base.GetStateFunc,
	getOperation OperationProcessorGetOperationFunction,
	newOperationProcessor NewOperationProcessorFunction,
) (*DefaultProposalProcessor, error) {
	writer, err := newWriter(proposal, getStateFunc)
	if err != nil {
		return nil, errors.Wrap(err, "failed to make new ProposalProcessor")
	}

	return &DefaultProposalProcessor{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "default-proposal-processor")
		}),
		proposal:              proposal,
		previous:              previous,
		writer:                writer,
		getStateFunc:          getStateFunc,
		getOperation:          getOperation,
		newOperationProcessor: newOperationProcessor,
		cops:                  make([]base.Operation, len(proposal.ProposalFact().Operations())),
		cancel:                func() {},
		oprs:                  util.NewShardedMap(1 << 5), //nolint:gomnd //...
		retrylimit:            15,                         //nolint:gomnd //...
		retryinterval:         time.Millisecond * 600,     //nolint:gomnd //...
	}, nil
}

func (p *DefaultProposalProcessor) Proposal() base.ProposalSignFact {
	return p.proposal
}

func (p *DefaultProposalProcessor) collected() []base.Operation {
	p.opslock.RLock()
	defer p.opslock.RUnlock()

	return p.cops
}

func (p *DefaultProposalProcessor) setOperation(index int, op base.Operation) {
	p.opslock.Lock()
	defer p.opslock.Unlock()

	p.cops[index] = op
}

func (p *DefaultProposalProcessor) Process(ctx context.Context, vp base.INITVoteproof) (base.Manifest, error) {
	p.Lock()
	defer p.Unlock()

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	p.updateCancel(cancel)

	e := util.StringErrorFunc("failed to process proposal")

	p.ivp = vp

	if err := p.process(wctx, vp); err != nil {
		return nil, e(err, "failed to process operations")
	}

	manifest, err := p.writer.Manifest(wctx, p.previous)
	if err != nil {
		return nil, e(err, "")
	}

	p.Log().Info().Interface("manifest", manifest).Msg("new manifest prepared")

	return manifest, nil
}

func (p *DefaultProposalProcessor) Save(ctx context.Context, avp base.ACCEPTVoteproof) error {
	p.Lock()
	defer p.Unlock()

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	p.updateCancel(cancel)

	if err := util.Retry(wctx, func() (bool, error) {
		switch err := p.save(wctx, avp); {
		case err == nil:
			return false, nil
		case errors.Is(err, ErrProcessorAlreadySaved):
			return false, err
		case errors.Is(err, ErrStopProcessingRetry):
			return false, err
		default:
			p.Log().Error().Err(err).Msg("failed to save; will retry")

			return true, err
		}
	}, p.retrylimit, p.retryinterval); err != nil {
		return errors.Wrap(err, "failed to save proposal")
	}

	return nil
}

func (p *DefaultProposalProcessor) save(ctx context.Context, acceptVoteproof base.ACCEPTVoteproof) error {
	if p.issaved {
		return ErrProcessorAlreadySaved.Call()
	}

	e := util.StringErrorFunc("failed to save")
	if p.isCanceled() {
		return e(context.Canceled, "")
	}

	if err := p.writer.SetACCEPTVoteproof(ctx, acceptVoteproof); err != nil {
		return e(err, "failed to set accept voteproof")
	}

	m, err := p.writer.Save(ctx)
	if err != nil {
		return e(err, "")
	}

	p.Log().Info().Interface("blockmap", m).Msg("new block saved in proposal processor")

	p.close()
	p.issaved = true

	return nil
}

func (p *DefaultProposalProcessor) Cancel() error {
	if !func() bool {
		p.cancellock.Lock()
		defer p.cancellock.Unlock()

		if p.cancel == nil {
			return false
		}

		p.cancel()
		p.cancel = nil

		return true
	}() {
		return nil
	}

	p.Lock()
	defer p.Unlock()

	if p.writer != nil {
		if err := p.writer.Cancel(); err != nil {
			return errors.Wrap(err, "failed to cancel DefaultProposalProcessor")
		}
	}

	p.close()

	return nil
}

func (p *DefaultProposalProcessor) close() {
	p.writer = nil
	p.ivp = nil
	p.proposal = nil
	p.previous = nil
	p.newOperationProcessor = nil
	// p.getStateFunc = nil
	p.getOperation = nil

	if p.oprs != nil {
		p.oprs.Traverse(func(key interface{}, i interface{}) bool {
			opp := i.(base.OperationProcessor) //nolint:forcetypeassert //...

			_ = opp.Close()

			return true
		})

		p.oprs.Close()
		p.oprs = nil
	}

	p.cops = nil
}

func (p *DefaultProposalProcessor) updateCancel(f func()) {
	p.cancellock.Lock()
	defer p.cancellock.Unlock()

	if p.cancel == nil {
		return
	}

	cancel := p.cancel

	p.cancel = func() {
		cancel()
		f()
	}
}

func (p *DefaultProposalProcessor) isCanceled() bool {
	p.cancellock.RLock()
	defer p.cancellock.RUnlock()

	return p.cancel == nil
}

func (p *DefaultProposalProcessor) process(ctx context.Context, vp base.INITVoteproof) error {
	e := util.StringErrorFunc("failed to process all operations")

	if p.isCanceled() {
		return e(context.Canceled, "")
	}

	if err := p.writer.SetINITVoteproof(ctx, vp); err != nil {
		return e(err, "failed to set init voteproof")
	}

	switch err := p.collectOperations(ctx); {
	case err != nil:
		return e(err, "failed to collect operations")
	case len(p.collected()) < 1:
		return nil
	default:
		p.writer.SetOperationsSize(uint64(len(p.collected())))
	}

	if err := p.processOperations(ctx); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) collectOperations(ctx context.Context) (err error) {
	e := util.StringErrorFunc("failed to collect operations")

	p.Log().Debug().Int("operations", len(p.proposal.ProposalFact().Operations())).Msg("collecting operations")

	if len(p.proposal.ProposalFact().Operations()) < 1 {
		return nil
	}

	wctx, done, err := p.wait(ctx)
	if err != nil {
		return e(err, "")
	}

	defer done()

	ophs := p.proposal.ProposalFact().Operations()

	if err := util.RunErrgroupWorker(wctx, uint64(len(ophs)), func(ctx context.Context, i, _ uint64) error {
		h := ophs[i]
		op, err := p.collectOperation(ctx, h)

		p.Log().Trace().Stringer("operation", h).Err(err).Msg("operation collected")

		switch {
		case err == nil:
		case errors.Is(err, util.ErrInvalid),
			errors.Is(err, ErrInvalidOperationInProcessor),
			errors.Is(err, ErrOperationNotFoundInProcessor),
			errors.Is(err, ErrOperationAlreadyProcessedInProcessor):
			p.Log().Debug().Err(err).Stringer("operation", h).Msg("operation ignored")

			return nil
		default:
			return errors.Wrapf(err, "failed to collect operation, %q", h)
		}

		p.setOperation(int(i), op)

		return nil
	}); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) collectOperation(ctx context.Context, h util.Hash) (base.Operation, error) {
	e := util.StringErrorFunc("failed to collect operation, %q", h)

	var op base.Operation

	if err := p.retry(ctx, func() (bool, error) {
		switch j, err := p.getOperation(ctx, h); {
		case err == nil:
			op = j

			return false, nil
		case errors.Is(err, util.ErrInvalid),
			errors.Is(err, ErrInvalidOperationInProcessor),
			errors.Is(err, ErrOperationNotFoundInProcessor),
			errors.Is(err, ErrOperationAlreadyProcessedInProcessor):
			return false, err
		default:
			return true, err
		}
	}); err != nil {
		return nil, e(err, "")
	}

	if op == nil {
		return nil, ErrOperationNotFoundInProcessor.Errorf("empty operation")
	}

	return op, nil
}

func (p *DefaultProposalProcessor) processOperations(ctx context.Context) error {
	e := util.StringErrorFunc("failed to process operations")

	wctx, done, err := p.wait(ctx)
	if err != nil {
		return e(err, "")
	}

	defer done()

	cops := p.collected()

	p.Log().Debug().Int("operations", len(cops)).Msg("trying to process operations")

	worker := util.NewErrgroupWorker(wctx, int64(len(cops)))
	defer worker.Close()

	gopsindex := -1
	gvalidindex := -1

	for i := range cops {
		op := cops[i]
		if op == nil {
			continue
		}

		gopsindex++
		opsindex := gopsindex

		if rop, ok := op.(ReasonProcessedOperation); ok {
			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				return p.writer.SetProcessResult( //nolint:wrapcheck //...
					ctx, uint64(opsindex), rop.OperationHash(), rop.FactHash(), false, rop.Reason())
			}); err != nil {
				return e(err, "")
			}
		}

		switch reasonerr, passed, err := p.doPreProcessOperation(ctx, op); {
		case err != nil:
			return e(err, "failed to pre process operation")
		case !passed:
			gopsindex--

			continue
		case reasonerr != nil:
			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				return p.writer.SetProcessResult(
					ctx, uint64(opsindex), op.Hash(), op.Fact().Hash(), false, reasonerr,
				)
			}); err != nil {
				return e(err, "")
			}

			continue
		}

		gvalidindex++
		validindex := gvalidindex

		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			return p.doProcessOperation(ctx, uint64(opsindex), uint64(validindex), op)
		}); err != nil {
			return e(err, "")
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) doPreProcessOperation(
	ctx context.Context, op base.Operation,
) (base.OperationProcessReasonError, bool, error) {
	var errorreason base.OperationProcessReasonError

	err := p.retry(ctx, func() (bool, error) {
		f, err := p.getPreProcessor(ctx, op)

		switch {
		case err != nil:
			return false, err
		case f == nil:
			return false, nil
		}

		switch i, err := f(ctx); {
		case err == nil:
			errorreason = i

			return false, nil
		case errors.Is(err, ErrSuspendOperation):
			return false, err
		default:
			return true, err
		}
	})

	if errors.Is(err, ErrSuspendOperation) {
		return nil, false, nil
	}

	return errorreason, true, err
}

func (p *DefaultProposalProcessor) doProcessOperation(
	ctx context.Context, opsindex, validindex uint64, op base.Operation,
) error {
	e := util.StringErrorFunc("failed to process operation, %q", op.Fact().Hash())

	var errorreason base.OperationProcessReasonError
	var stvs []base.StateMergeValue

	if err := p.retry(ctx, func() (bool, error) {
		if stvs == nil {
			f, err := p.getProcessor(ctx, op)

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

		switch ee := util.StringErrorFunc("invalid processor"); {
		case len(stvs) < 1:
			if errorreason == nil {
				return false, ee(nil, "empty state must have reason")
			}
		case errorreason != nil:
			return false, ee(nil, "not empty state must have empty reason")
		}

		instate := len(stvs) > 0
		if instate {
			if err := p.writer.SetStates(ctx, validindex, stvs, op); err != nil {
				return true, e(err, "")
			}
		}

		if err := p.writer.SetProcessResult(
			ctx, opsindex, op.Hash(), op.Fact().Hash(), instate, errorreason,
		); err != nil {
			return true, e(err, "")
		}

		return false, nil
	}); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) getPreProcessor(ctx context.Context, op base.Operation) (
	func(context.Context) (base.OperationProcessReasonError, error),
	error,
) {
	switch opp, found, err := p.getOperationProcessor(ctx, op.Hint()); {
	case err != nil:
		return nil, errors.Wrap(err, "failed to get OperationProcessor for PreProcess")
	case found:
		return func(ctx context.Context) (base.OperationProcessReasonError, error) {
			return opp.PreProcess(ctx, op, p.getStateFunc) //nolint:wrapcheck //...
		}, nil
	}

	return func(ctx context.Context) (base.OperationProcessReasonError, error) {
		return op.PreProcess(ctx, p.getStateFunc) //nolint:wrapcheck //...
	}, nil
}

func (p *DefaultProposalProcessor) getProcessor(ctx context.Context, op base.Operation) (
	func(context.Context) ([]base.StateMergeValue, base.OperationProcessReasonError, error),
	error,
) {
	switch opp, found, err := p.getOperationProcessor(ctx, op.Hint()); {
	case err != nil:
		return nil, errors.Wrap(err, "failed to get OperationProcessor for Process")
	case found:
		return func(ctx context.Context) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
			return opp.Process(ctx, op, p.getStateFunc) //nolint:wrapcheck //...
		}, nil
	}

	getStateFunc := p.getStateFunc

	return func(ctx context.Context) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
		return op.Process(ctx, getStateFunc) //nolint:wrapcheck //...
	}, nil
}

func (p *DefaultProposalProcessor) getOperationProcessor(ctx context.Context, ht hint.Hint) (
	base.OperationProcessor, bool, error,
) {
	i, _, err := p.oprs.Get(ht.String(), func() (interface{}, error) {
		var opp base.OperationProcessor

		if err := p.retry(ctx, func() (bool, error) {
			i, err := p.newOperationProcessor(p.proposal.Point().Height(), ht)
			if err != nil {
				return true, err
			}

			opp = i

			return false, nil
		}); err != nil {
			return nil, err
		}

		if opp == nil {
			return nil, ErrOperationInProcessorNotFound.Call()
		}

		return opp, nil
	})

	switch {
	case err == nil:
		return i.(base.OperationProcessor), true, nil //nolint:forcetypeassert //...
	case errors.Is(err, ErrOperationInProcessorNotFound):
		return nil, false, nil
	default:
		return nil, false, errors.Wrap(err, "failed to get OperationProcessor")
	}
}

func (p *DefaultProposalProcessor) wait(ctx context.Context) (
	context.Context,
	func(),
	error,
) {
	if p.isCanceled() {
		return context.TODO(), nil, context.Canceled
	}

	wctx, cancel := context.WithCancel(ctx)

	donech := make(chan struct{}, 1)

	var cancelonce sync.Once

	p.updateCancel(func() {
		cancelonce.Do(func() {
			cancel()

			<-donech
		})
	})

	return wctx, func() {
		donech <- struct{}{}
	}, nil
}

func (p *DefaultProposalProcessor) retry(ctx context.Context, f func() (bool, error)) error {
	return util.Retry(ctx, func() (bool, error) {
		keep, err := f()
		if errors.Is(err, ErrStopProcessingRetry) {
			return false, err
		}

		return keep, err
	}, p.retrylimit, p.retryinterval)
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
