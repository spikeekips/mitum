package isaac

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
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
)

type (
	NewOperationProcessorFunction func(hint.Hint) (base.OperationProcessor, bool)

	// OperationProcessorGetOperationFunction works,
	// - if operation is invalid, getOperation should return nil,
	// InvalidOperationInProcessorError; it will be not processed and it's fact
	// hash will be stored.
	// - if operation is not found in remote, getOperation should return nil,
	// OperationNotFoundInProcessorError; it will be ignored.
	// - if operation is known, return nil,
	// OperationAlreadyProcessedInProcessorError; it will be ignored.
	OperationProcessorGetOperationFunction func(context.Context, util.Hash) (base.Operation, error)
)

type proposalProcessor interface {
	Proposal() base.ProposalSignedFact
	Process(context.Context, base.INITVoteproof) (base.Manifest, error)
	Save(context.Context, base.ACCEPTVoteproof) error
	Cancel() error
}

type DefaultProposalProcessor struct {
	sync.RWMutex
	*logging.Logging
	proposal              base.ProposalSignedFact
	previous              base.Manifest
	writer                BlockDataWriter
	sp                    base.StatePool
	getOperation          OperationProcessorGetOperationFunction
	newOperationProcessor NewOperationProcessorFunction
	opslock               sync.RWMutex
	ops                   []base.Operation
	cancel                func()
	oprs                  *util.LockedMap
	retrylimit            int
	retryinterval         time.Duration
}

func NewDefaultProposalProcessor(
	proposal base.ProposalSignedFact,
	previous base.Manifest,
	writer BlockDataWriter,
	sp base.StatePool,
	getOperation OperationProcessorGetOperationFunction,
	newOperationProcessor NewOperationProcessorFunction,
) *DefaultProposalProcessor {
	return &DefaultProposalProcessor{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "default-proposal-processor")
		}),
		proposal:              proposal,
		previous:              previous,
		writer:                writer,
		sp:                    sp,
		getOperation:          getOperation,
		newOperationProcessor: newOperationProcessor,
		ops:                   make([]base.Operation, len(proposal.ProposalFact().Operations())),
		cancel:                func() {},
		oprs:                  util.NewLockedMap(),
		retrylimit:            15,
		retryinterval:         time.Millisecond * 600,
	}
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

	if err := p.process(ctx, vp); err != nil {
		return nil, e(err, "failed to process operations")
	}

	m, err := p.writer.Manifest(ctx, p.previous)
	if err != nil {
		return nil, e(err, "")
	}

	return m, nil
}

func (p *DefaultProposalProcessor) Save(ctx context.Context, acceptVoteproof base.ACCEPTVoteproof) error {
	p.Lock()
	defer p.Unlock()

	e := util.StringErrorFunc("failed to save")
	if p.proposal == nil {
		return e(context.Canceled, "")
	}

	if err := p.writer.SetACCEPTVoteproof(ctx, acceptVoteproof); err != nil {
		return e(err, "failed to set accept voteproof")
	}

	if err := p.writer.Save(ctx); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) Cancel() error {
	p.Lock()
	defer p.Unlock()

	if p.proposal == nil {
		return nil
	}

	p.cancel()

	p.proposal = nil
	p.ops = nil
	p.oprs = nil

	if err := p.writer.Cancel(); err != nil {
		return errors.Wrap(err, "failed to cancel DefaultProposalProcessor")
	}

	return nil
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

	if err := p.setProposal(ctx); err != nil {
		return e(err, "failed to set proposal")
	}

	if err := p.writer.SetINITVoteproof(ctx, vp); err != nil {
		return e(err, "failed to set init voteproof")
	}

	switch collected, valids, err := p.collectOperations(ctx); {
	case err != nil:
		return e(err, "failed to collect operations")
	case collected < 1:
		return nil
	default:
		p.writer.SetOperationsSize(collected, valids)
	}

	if err := p.processOperations(ctx); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) setProposal(ctx context.Context) error {
	e := util.StringErrorFunc("failed to set proposal")
	if err := util.Retry(ctx, func() (bool, error) {
		if err := p.writer.SetProposal(ctx, p.proposal); err != nil {
			return true, e(err, "")
		}

		return false, nil
	}, p.retrylimit, p.retryinterval); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) collectOperations(ctx context.Context) (
	countcollected, countvalids uint64, err error,
) {
	e := util.StringErrorFunc("failed to collect operations")

	if len(p.proposal.ProposalFact().Operations()) < 1 {
		return 0, 0, nil
	}

	wctx, done, err := p.wait(ctx)
	if err != nil {
		return 0, 0, e(err, "")
	}
	defer done()

	worker := util.NewErrgroupWorker(wctx, math.MaxInt32)
	defer worker.Close()

	if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
		return p.writer.SetProposal(ctx, p.proposal)
	}); err != nil {
		return 0, 0, e(err, "")
	}

	var collected, valids uint64
	go func() {
		ophs := p.proposal.ProposalFact().Operations()
		for i := range ophs {
			i := i
			h := ophs[i]
			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				op, err := p.collectOperation(ctx, h)
				switch {
				case err == nil:
					atomic.AddUint64(&valids, 1)
				case errors.Is(err, InvalidOperationInProcessorError):
					op = NewReasonProcessedOperation(h, base.NewBaseOperationProcessReasonError("invalid operation"))
				case errors.Is(err, OperationNotFoundInProcessorError),
					errors.Is(err, OperationAlreadyProcessedInProcessorError):
					return nil
				default:
					return errors.Wrapf(err, "failed to collect operation, %q", h)
				}

				p.setOperation(i, op)

				atomic.AddUint64(&collected, 1)

				return nil
			}); err != nil {
				break
			}
		}

		worker.Done()
	}()

	if err := worker.Wait(); err != nil {
		return 0, 0, e(err, "")
	}

	return atomic.LoadUint64(&collected), atomic.LoadUint64(&valids), nil
}

func (p *DefaultProposalProcessor) collectOperation(ctx context.Context, h util.Hash) (base.Operation, error) {
	e := util.StringErrorFunc("failed to collect operation, %q", h)

	var op base.Operation
	if err := util.Retry(ctx, func() (bool, error) {
		switch j, err := p.getOperation(ctx, h); {
		case err == nil:
			op = j

			return false, nil
		case errors.Is(err, InvalidOperationInProcessorError),
			errors.Is(err, OperationNotFoundInProcessorError),
			errors.Is(err, OperationAlreadyProcessedInProcessorError):
			return false, err
		default:
			return true, err
		}
	}, p.retrylimit, p.retryinterval); err != nil {
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

	errch := make(chan error, 1)
	go func() {
		defer worker.Done()

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
					return p.writer.SetOperation(ctx, opsindex, i.FactHash(), false, i.Reason())
				}); err != nil {
					errch <- errors.Wrapf(err, "failed to process operation, %d", opsindex)
				}

				continue
			}

			gvalidindex++
			validindex := gvalidindex
			if err := p.workOperation(wctx, worker, opsindex, validindex, op); err != nil {
				errch <- err

				return
			}
		}

		errch <- nil
	}()

	gerr := <-errch
	if err := worker.Wait(); err != nil {
		if gerr == nil {
			gerr = e(err, "")
		}
	}

	return gerr
}

func (p *DefaultProposalProcessor) workOperation(
	ctx context.Context,
	worker *util.ErrgroupWorker,
	opsindex, validindex int,
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
	ctx context.Context, opsindex int, op base.Operation,
) (bool, error) {
	e := util.StringErrorFunc("failed to pre process operation, %q", op.Fact().Hash())

	var errorreason base.OperationProcessReasonError
	if err := util.Retry(ctx, func() (bool, error) {
		f := p.getPreProcessor(op)
		if f == nil {
			return false, nil
		}

		switch i, err := f(ctx); {
		case err == nil:
			errorreason = i

			return false, nil
		default:
			return true, err
		}
	}, p.retrylimit, p.retryinterval); err != nil {
		return false, e(err, "")
	}

	if errorreason != nil {
		if err := p.writer.SetOperation(ctx, opsindex, op.Fact().Hash(), false, errorreason); err != nil {
			return false, e(err, "")
		}
	}

	return errorreason == nil, nil
}

func (p *DefaultProposalProcessor) doProcessOperation(
	ctx context.Context, opsindex, validindex int, op base.Operation,
) error {
	e := util.StringErrorFunc("failed to process operation, %q", op.Fact().Hash())

	var errorreason base.OperationProcessReasonError
	var sts []base.State
	if err := util.Retry(ctx, func() (bool, error) {
		if sts == nil {
			f := p.getProcessor(op)
			if f == nil {
				return false, nil
			}

			i, j, err := f(ctx)
			switch {
			case err == nil:
				sts = i
				errorreason = j
			default:
				return true, err
			}
		}

		switch ee := util.StringErrorFunc("invalid processor"); {
		case len(sts) < 1:
			if errorreason == nil {
				return false, ee(nil, "empty state must have reason")
			}
		case errorreason != nil:
			return false, ee(nil, "not empty state must have empty reason")
		}

		return false, nil
	}, p.retrylimit, p.retryinterval); err != nil {
		return e(err, "")
	}

	instate := len(sts) > 0
	if instate {
		if err := p.writer.SetStates(ctx, validindex, sts, op); err != nil {
			return e(err, "")
		}
	}

	if err := p.writer.SetOperation(ctx, opsindex, op.Fact().Hash(), instate, errorreason); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) getPreProcessor(op base.Operation) func(context.Context) (
	base.OperationProcessReasonError, error) {
	p.RLock()
	defer p.RUnlock()

	if opp, found := p.getOperationProcessor(op.Hint()); found {
		return func(ctx context.Context) (base.OperationProcessReasonError, error) {
			return opp.PreProcess(ctx, op, p.sp)
		}
	}

	opp, ok := op.(base.ProcessableOperation)
	if !ok {
		return nil
	}

	return func(ctx context.Context) (base.OperationProcessReasonError, error) {
		return opp.PreProcess(ctx, p.sp)
	}
}

func (p *DefaultProposalProcessor) getProcessor(op base.Operation) func(context.Context) (
	[]base.State, base.OperationProcessReasonError, error) {
	p.RLock()
	defer p.RUnlock()

	if opp, found := p.getOperationProcessor(op.Hint()); found {
		return func(ctx context.Context) ([]base.State, base.OperationProcessReasonError, error) {
			return opp.Process(ctx, op, p.sp)
		}
	}

	opp, ok := op.(base.ProcessableOperation)
	if !ok {
		return nil
	}

	return func(ctx context.Context) ([]base.State, base.OperationProcessReasonError, error) {
		return opp.Process(ctx, p.sp)
	}
}

func (p *DefaultProposalProcessor) getOperationProcessor(ht hint.Hint) (base.OperationProcessor, bool) {
	j, _, _ := p.oprs.Get(ht.String(), func() (interface{}, error) {
		if i, ok := p.newOperationProcessor(ht); ok {
			return i, nil
		}

		return util.NilLockedValue{}, nil
	})

	if util.IsNilLockedValue(j) {
		return nil, false
	}

	return j.(base.OperationProcessor), true
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

type ReasonProcessedOperation struct {
	base.Operation
	facthash util.Hash
	reason   base.OperationProcessReasonError
}

func NewReasonProcessedOperation(facthash util.Hash, reason base.OperationProcessReasonError) ReasonProcessedOperation {
	return ReasonProcessedOperation{facthash: facthash, reason: reason}
}

func (op ReasonProcessedOperation) FactHash() util.Hash {
	return op.facthash
}

func (op ReasonProcessedOperation) Reason() base.OperationProcessReasonError {
	return op.reason
}
