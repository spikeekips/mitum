package isaac

import (
	"context"
	"math"
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
	InvalidOperationInProcessorError          = util.NewError("invalid operation")
	OperationNotFoundInProcessorError         = util.NewError("operation not found")
	OperationAlreadyProcessedInProcessorError = util.NewError("operation already processed")
	StopProcessingRetryError                  = util.NewError("stop processing retrying")
)

type (
	NewOperationProcessorFunction func(base.Height, hint.Hint) (base.OperationProcessor, bool, error)

	// OperationProcessorGetOperationFunction works,
	// - if operation is invalid, getOperation should return nil,
	// InvalidOperationInProcessorError; it will be not processed and it's fact
	// hash will be stored.
	// - if operation is not found in remote, getOperation should return nil,
	// OperationNotFoundInProcessorError; it will be ignored.
	// - if operation is known, return nil,
	// OperationAlreadyProcessedInProcessorError; it will be ignored.
	OperationProcessorGetOperationFunction func(context.Context, util.Hash) (base.Operation, error)
	NewBlockWriterFunc                     func(base.ProposalSignedFact, base.GetStateFunc) (BlockWriter, error)
)

type ProposalProcessor interface {
	Proposal() base.ProposalSignedFact
	Process(context.Context, base.INITVoteproof) (base.Manifest, error)
	Save(context.Context, base.ACCEPTVoteproof) error
	Cancel() error
}

type DefaultProposalProcessor struct {
	writer                BlockWriter
	ivp                   base.INITVoteproof
	proposal              base.ProposalSignedFact
	previous              base.Manifest
	newOperationProcessor NewOperationProcessorFunction
	getStateFunc          base.GetStateFunc
	getOperation          OperationProcessorGetOperationFunction
	*logging.Logging
	setLastVoteproofsFunc func(base.INITVoteproof, base.ACCEPTVoteproof) error
	oprs                  *util.LockedMap
	cancel                func()
	ops                   []base.Operation
	retrylimit            int
	retryinterval         time.Duration
	opslock               sync.RWMutex
	sync.RWMutex
}

func NewDefaultProposalProcessor(
	proposal base.ProposalSignedFact,
	previous base.Manifest,
	newWriter NewBlockWriterFunc,
	getStateFunc base.GetStateFunc,
	getOperation OperationProcessorGetOperationFunction,
	newOperationProcessor NewOperationProcessorFunction,
	setLastVoteproofsFunc func(base.INITVoteproof, base.ACCEPTVoteproof) error,
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
		setLastVoteproofsFunc: setLastVoteproofsFunc,
		ops:                   make([]base.Operation, len(proposal.ProposalFact().Operations())),
		cancel:                func() {},
		oprs:                  util.NewLockedMap(),
		retrylimit:            15,                     //nolint:gomnd //...
		retryinterval:         time.Millisecond * 600, //nolint:gomnd //...
	}, nil
}

func (p *DefaultProposalProcessor) Proposal() base.ProposalSignedFact {
	return p.proposal
}

func (p *DefaultProposalProcessor) operations() []base.Operation {
	p.opslock.RLock()
	defer p.opslock.RUnlock()

	return p.ops
}

func (p *DefaultProposalProcessor) setOperation(index int, op base.Operation) {
	p.opslock.Lock()
	defer p.opslock.Unlock()

	p.ops[index] = op
}

func (p *DefaultProposalProcessor) Process(ctx context.Context, vp base.INITVoteproof) (base.Manifest, error) {
	e := util.StringErrorFunc("failed to process proposal")

	p.ivp = vp

	if err := p.process(ctx, vp); err != nil {
		return nil, e(err, "failed to process operations")
	}

	manifest, err := p.writer.Manifest(ctx, p.previous)
	if err != nil {
		return nil, e(err, "")
	}

	p.Log().Info().Interface("manifest", manifest).Msg("new manifest prepared")

	return manifest, nil
}

func (p *DefaultProposalProcessor) Save(ctx context.Context, avp base.ACCEPTVoteproof) error {
	if err := util.Retry(ctx, func() (bool, error) {
		switch err := p.save(ctx, avp); {
		case err == nil:
			return false, nil
		case errors.Is(err, StopProcessingRetryError):
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
	p.Lock()
	defer p.Unlock()

	e := util.StringErrorFunc("failed to save")
	if p.proposal == nil {
		return e(context.Canceled, "")
	}

	if err := p.writer.SetACCEPTVoteproof(ctx, acceptVoteproof); err != nil {
		return e(err, "failed to set accept voteproof")
	}

	m, err := p.writer.Save(ctx)
	if err != nil {
		return e(err, "")
	}

	if p.setLastVoteproofsFunc != nil {
		if err := p.setLastVoteproofsFunc(p.ivp, acceptVoteproof); err != nil {
			return e(err, "failed to save last voteproofs")
		}
	}

	p.Log().Info().Interface("blockmap", m).Msg("new block saved")

	p.close()

	return nil
}

func (p *DefaultProposalProcessor) Cancel() error {
	p.Lock()
	defer p.Unlock()

	p.close()

	if err := p.writer.Cancel(); err != nil {
		return errors.Wrap(err, "failed to cancel DefaultProposalProcessor")
	}

	return nil
}

func (p *DefaultProposalProcessor) close() {
	if p.proposal == nil {
		return
	}

	p.cancel()

	p.ivp = nil
	p.proposal = nil
	p.ops = nil
	p.oprs = nil
}

func (p *DefaultProposalProcessor) isCanceled() bool {
	p.RLock()
	defer p.RUnlock()

	return p.proposal == nil
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
	case len(p.operations()) < 1:
		return nil
	default:
		p.writer.SetOperationsSize(uint64(len(p.operations())))
	}

	if err := p.processOperations(ctx); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) collectOperations(ctx context.Context) (err error) {
	e := util.StringErrorFunc("failed to collect operations")

	if len(p.proposal.ProposalFact().Operations()) < 1 {
		return nil
	}

	wctx, done, err := p.wait(ctx)
	if err != nil {
		return e(err, "")
	}

	defer done()

	worker := util.NewErrgroupWorker(wctx, math.MaxInt32)
	defer worker.Close()

	go func() {
		defer worker.Done()

		ophs := p.proposal.ProposalFact().Operations()

		for i := range ophs {
			i := i
			h := ophs[i]

			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				op, err := p.collectOperation(ctx, h)

				switch {
				case err == nil:
				case errors.Is(err, util.ErrInvalid),
					errors.Is(err, InvalidOperationInProcessorError),
					errors.Is(err, OperationNotFoundInProcessorError),
					errors.Is(err, OperationAlreadyProcessedInProcessorError):
					p.Log().Debug().Err(err).Stringer("facthash", h).Msg("operation ignored")

					return nil
				default:
					return errors.Wrapf(err, "failed to collect operation, %q", h)
				}

				p.setOperation(i, op)

				return nil
			}); err != nil {
				return
			}
		}
	}()

	if err := worker.Wait(); err != nil {
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
			errors.Is(err, InvalidOperationInProcessorError),
			errors.Is(err, OperationNotFoundInProcessorError),
			errors.Is(err, OperationAlreadyProcessedInProcessorError):
			return false, err
		default:
			return true, err
		}
	}); err != nil {
		return nil, e(err, "")
	}

	if op == nil {
		return nil, OperationNotFoundInProcessorError.Errorf("empty operation")
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

	worker := util.NewErrgroupWorker(wctx, math.MaxInt32)
	defer worker.Close()

	ops := p.operations()

	gopsindex := -1
	gvalidindex := -1

	for i := range ops {
		op := ops[i]
		if op == nil {
			continue
		}

		gopsindex++
		opsindex := gopsindex

		if i, ok := op.(ReasonProcessedOperation); ok {
			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				return p.writer.SetProcessResult( //nolint:wrapcheck //...
					ctx, uint64(opsindex), i.OperationHash(), i.FactHash(), false, i.Reason())
			}); err != nil {
				return e(err, "")
			}

			continue
		}

		gvalidindex++
		validindex := gvalidindex

		if err := p.workOperation(wctx, worker, uint64(opsindex), uint64(validindex), op); err != nil {
			if !errors.Is(err, util.ErrWorkerCanceled) {
				return e(err, "")
			}

			break
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) workOperation(
	ctx context.Context,
	worker *util.ErrgroupWorker,
	opsindex, validindex uint64,
	op base.Operation,
) error {
	e := util.StringErrorFunc("failed to process operation, %q", op.Fact().Hash())

	switch passed, err := p.doPreProcessOperation(ctx, opsindex, op); {
	case err != nil:
		return e(err, "failed to pre process operation")
	case !passed:
		return nil
	}

	if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
		return p.doProcessOperation(ctx, opsindex, validindex, op)
	}); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) doPreProcessOperation(
	ctx context.Context, opsindex uint64, op base.Operation,
) (bool, error) {
	e := util.StringErrorFunc("failed to pre process operation, %q", op.Fact().Hash())

	var errorreason base.OperationProcessReasonError

	if err := p.retry(ctx, func() (bool, error) {
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
		default:
			return true, err
		}
	}); err != nil {
		return false, e(err, "")
	}

	if errorreason != nil {
		if err := p.writer.SetProcessResult(
			ctx, opsindex, op.Hash(), op.Fact().Hash(), false, errorreason,
		); err != nil {
			return false, e(err, "")
		}
	}

	return errorreason == nil, nil
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
	p.RLock()
	defer p.RUnlock()

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
	p.RLock()
	defer p.RUnlock()

	switch opp, found, err := p.getOperationProcessor(ctx, op.Hint()); {
	case err != nil:
		return nil, errors.Wrap(err, "failed to get OperationProcessor for Process")
	case found:
		return func(ctx context.Context) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
			return opp.Process(ctx, op, p.getStateFunc) //nolint:wrapcheck //...
		}, nil
	}

	return func(ctx context.Context) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
		return op.Process(ctx, p.getStateFunc) //nolint:wrapcheck //...
	}, nil
}

func (p *DefaultProposalProcessor) getOperationProcessor(ctx context.Context, ht hint.Hint) (
	base.OperationProcessor, bool, error,
) {
	j, _, err := p.oprs.Get(ht.String(), func() (interface{}, error) {
		var opp base.OperationProcessor
		var found bool
		if err := p.retry(ctx, func() (bool, error) {
			switch i, ok, err := p.newOperationProcessor(p.proposal.Point().Height(), ht); {
			case err != nil:
				return true, err
			case !ok:
				opp = nil
				found = false

				return false, nil
			default:
				opp = i
				found = true

				return false, nil
			}
		}); err != nil {
			return util.NilLockedValue{}, err
		}

		if !found {
			return util.NilLockedValue{}, nil
		}

		return opp, nil
	})
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to get OperationProcessor")
	}

	if util.IsNilLockedValue(j) {
		return nil, false, nil
	}

	return j.(base.OperationProcessor), true, nil //nolint:forcetypeassert //...
}

func (p *DefaultProposalProcessor) wait(ctx context.Context) (
	context.Context,
	func(),
	error,
) {
	p.Lock()
	defer p.Unlock()

	if p.proposal == nil {
		return context.TODO(), nil, context.Canceled
	}

	wctx, cancel := context.WithCancel(ctx)

	donech := make(chan struct{}, 1)

	var cancelonce sync.Once
	p.cancel = func() {
		cancelonce.Do(func() {
			cancel()

			<-donech
		})
	}

	return wctx, func() {
		donech <- struct{}{}
	}, nil
}

func (p *DefaultProposalProcessor) retry(ctx context.Context, f func() (bool, error)) error {
	if err := util.Retry(ctx, func() (bool, error) {
		keep, err := f()
		if errors.Is(err, StopProcessingRetryError) {
			return false, err
		}

		return keep, err
	}, p.retrylimit, p.retryinterval); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
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
