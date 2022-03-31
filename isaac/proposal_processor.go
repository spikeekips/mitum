package isaac

import (
	"context"
	"math"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var IgnoreOperationInProcessorError = util.NewError("ignore operation in processor")

type (
	NewOperationProcessorFunction func(hint.Hint) (base.OperationProcessor, bool)

	// BlOCK if bad operation, return IgnoreOperationInProcessorError
	// BLOCK if error to fetch operations, return RetryProposalProcessorError

	OperationProcessorGetOperationFunction func(context.Context, util.Hash) (base.Operation, error)
)

type proposalProcessor interface {
	Proposal() base.ProposalFact
	Process(context.Context) (base.Manifest, error)
	Save(context.Context, base.ACCEPTVoteproof) error
	Cancel() error
}

type BlockDataWriter interface {
	SetProposalOperations([]util.Hash) error
	SetOperation(index int, operation base.Operation) error
	SetStates(states []base.State, operation util.Hash) error
	SetManifest(base.Manifest) error
	Manifest(context.Context) (base.Manifest, error)
	Save(context.Context, base.ACCEPTVoteproof) error
	Cancel() error
}

type DefaultProposalProcessor struct {
	sync.RWMutex
	proposal              base.ProposalFact
	writer                BlockDataWriter
	sp                    base.StatePool
	getOperation          OperationProcessorGetOperationFunction
	newOperationProcessor NewOperationProcessorFunction
	ops                   []base.Operation
	ophs                  []util.Hash
	cancel                func()
	oprs                  *util.LockedMap
}

func NewDefaultProposalProcessor(
	proposal base.ProposalFact,
	writer BlockDataWriter,
	sp base.StatePool,
	getOperation OperationProcessorGetOperationFunction,
	newOperationProcessor NewOperationProcessorFunction,
) *DefaultProposalProcessor {
	return &DefaultProposalProcessor{
		proposal:              proposal,
		writer:                writer,
		sp:                    sp,
		getOperation:          getOperation,
		newOperationProcessor: newOperationProcessor,
		ophs:                  proposal.Operations(),
		cancel:                func() {},
		oprs:                  util.NewLockedMap(),
	}
}

func (p *DefaultProposalProcessor) Proposal() base.ProposalFact {
	p.RLock()
	defer p.RUnlock()

	return p.proposal
}

func (p *DefaultProposalProcessor) proposalOperations() []util.Hash {
	p.RLock()
	defer p.RUnlock()

	return p.ophs
}

func (p *DefaultProposalProcessor) operations() []base.Operation {
	p.RLock()
	defer p.RUnlock()

	return p.ops
}

func (p *DefaultProposalProcessor) Process(ctx context.Context) (base.Manifest, error) {
	e := util.StringErrorFunc("failed to process proposal")

	if len(p.proposalOperations()) > 0 {
		if err := p.process(ctx); err != nil {
			return nil, e(err, "failed to process operations")
		}
	}

	m, err := p.writer.Manifest(ctx)
	if err != nil {
		return nil, e(err, "")
	}

	return m, nil
}

func (p *DefaultProposalProcessor) Save(ctx context.Context, acceptVoteproof base.ACCEPTVoteproof) error {
	p.Lock()
	defer p.Unlock()

	if p.proposal == nil {
		return errors.Wrap(context.Canceled, "failed to save")
	}

	return p.writer.Save(ctx, acceptVoteproof)
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
	p.ophs = nil
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

func (p *DefaultProposalProcessor) process(ctx context.Context) error {
	e := util.StringErrorFunc("failed to process all operations")

	if p.isCanceled() {
		return e(context.Canceled, "")
	}

	if err := p.writer.SetProposalOperations(p.proposalOperations()); err != nil {
		return e(err, "failed to set operations")
	}

	if err := p.collectOperations(ctx); err != nil {
		return e(err, "failed to collect operations")
	}

	if len(p.ops) < 1 {
		return nil
	}

	if err := p.processOperations(ctx); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) collectOperations(ctx context.Context) error {
	e := util.StringErrorFunc("failed to collect operations")

	if len(p.proposalOperations()) < 1 {
		return nil
	}

	wctx, done, err := p.wait(ctx)
	if err != nil {
		return e(err, "")
	}
	defer done()

	worker := util.NewErrgroupWorker(wctx, math.MaxInt32)
	defer worker.Close()

	ops := make([]base.Operation, len(p.proposalOperations()))

	go func() {
		ophs := p.proposalOperations()
		for i := range ophs {
			i := i
			h := ophs[i]
			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				switch op, err := p.getOperation(ctx, h); {
				case err == nil:
					if err = p.writer.SetOperation(i, op); err != nil {
						return errors.Wrapf(err, "failed to write operations, %q", h)
					}

					ops[i] = op

					return nil
				case errors.Is(err, IgnoreOperationInProcessorError):
					return nil
				default:
					return errors.Wrapf(err, "bad operation found, %q", h)
				}
			}); err != nil {
				break
			}
		}

		worker.Done()
	}()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	p.ops = ops

	return nil
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
		ops := p.operations()

	end:
		for i := range ops {
			op := ops[i]
			if op == nil {
				continue
			}

			i := i
			switch passed, err := p.preProcessOperation(ctx, op); {
			case err != nil:
				errch <- errors.Wrapf(err, "failed to pre process operation, %q", op.Fact().Hash())

				break end
			case !passed:
				// BLOCK save operations tree
				continue end
			}

			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				if err := p.processOperation(ctx, i, op); err != nil {
					return errors.Wrapf(err, "failed to process operation, %q", op.Fact().Hash())
				}

				return nil
			}); err != nil {
				break
			}
		}

		worker.Done()

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

func (p *DefaultProposalProcessor) preProcessOperation(ctx context.Context, op base.Operation) (bool, error) {
	switch f := p.getPreProcessor(op); {
	case f == nil:
		return false, nil
	default:
		return f(ctx)
	}
}

func (p *DefaultProposalProcessor) processOperation(ctx context.Context, index int, op base.Operation) error {
	e := util.StringErrorFunc("failed to process operation")

	f := p.getProcessor(op)
	if f == nil {
		return nil
	}

	sts, err := f(ctx)
	switch {
	case err == nil:
		if len(sts) < 1 {
			return nil
		}
	case errors.Is(err, IgnoreOperationInProcessorError):
		return nil
	default:
		return e(err, "")
	}

	if err := p.writer.SetStates(sts, op.Fact().Hash()); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) getPreProcessor(op base.Operation) func(context.Context) (bool, error) {
	p.RLock()
	defer p.RUnlock()

	if opp, found := p.getOperationProcessor(op.Hint()); found {
		return func(ctx context.Context) (bool, error) {
			return opp.PreProcess(ctx, op, p.sp)
		}
	}

	opp, ok := op.(base.ProcessableOperation)
	if !ok {
		return nil
	}

	return func(ctx context.Context) (bool, error) {
		return opp.PreProcess(ctx, p.sp)
	}
}

func (p *DefaultProposalProcessor) getProcessor(op base.Operation) func(context.Context) ([]base.State, error) {
	p.RLock()
	defer p.RUnlock()

	if opp, found := p.getOperationProcessor(op.Hint()); found {
		return func(ctx context.Context) ([]base.State, error) {
			return opp.Process(ctx, op, p.sp)
		}
	}

	opp, ok := op.(base.ProcessableOperation)
	if !ok {
		return nil
	}

	return func(ctx context.Context) ([]base.State, error) {
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
