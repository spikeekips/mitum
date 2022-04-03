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

var IgnoreOperationInProcessorError = util.NewError("ignore operation in processor")

type (
	NewOperationProcessorFunction func(hint.Hint) (base.OperationProcessor, bool)

	// BlOCK if bad operation, return IgnoreOperationInProcessorError
	// BLOCK if error to fetch operations, return RetryProposalProcessorError

	OperationProcessorGetOperationFunction func(context.Context, util.Hash) (base.Operation, error)
)

type proposalProcessor interface {
	Proposal() base.ProposalSignedFact
	Process(context.Context) (base.Manifest, error)
	Save(context.Context, base.ACCEPTVoteproof) error
	Cancel() error
}

type DefaultProposalProcessor struct {
	sync.RWMutex
	*logging.Logging
	proposal              base.ProposalSignedFact
	proposalfact          base.ProposalFact
	writer                BlockDataWriter
	sp                    base.StatePool
	getOperation          OperationProcessorGetOperationFunction
	newOperationProcessor NewOperationProcessorFunction
	ops                   []base.Operation
	cancel                func()
	oprs                  *util.LockedMap
	retrylimit            int
	retryinterval         time.Duration
}

func NewDefaultProposalProcessor(
	proposal base.ProposalSignedFact,
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
		proposalfact:          proposal.ProposalFact(),
		writer:                writer,
		sp:                    sp,
		getOperation:          getOperation,
		newOperationProcessor: newOperationProcessor,
		cancel:                func() {},
		oprs:                  util.NewLockedMap(),
		retrylimit:            15,
		retryinterval:         time.Millisecond * 600,
	}
}

func (p *DefaultProposalProcessor) Proposal() base.ProposalSignedFact {
	p.RLock()
	defer p.RUnlock()

	return p.proposal
}

func (p *DefaultProposalProcessor) proposalOperations() []util.Hash {
	p.RLock()
	defer p.RUnlock()

	return p.proposalfact.Operations()
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

	e := util.StringErrorFunc("failed to save")
	if p.proposalfact == nil {
		return e(context.Canceled, "")
	}

	if err := p.writer.Save(ctx, acceptVoteproof); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) Cancel() error {
	p.Lock()
	defer p.Unlock()

	if p.proposalfact == nil {
		return nil
	}

	p.cancel()

	p.proposalfact = nil
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

	return p.proposalfact == nil
}

func (p *DefaultProposalProcessor) process(ctx context.Context) error {
	e := util.StringErrorFunc("failed to process all operations")

	if p.isCanceled() {
		return e(context.Canceled, "")
	}

	if err := p.writer.SetProposal(p.proposal); err != nil {
		return e(err, "failed to set proposal")
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
				op, err := p.collectOperation(ctx, i, h)
				if err != nil {
					return errors.Wrapf(err, "bad operation found, %q", h)
				}
				ops[i] = op

				return nil
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

func (p *DefaultProposalProcessor) collectOperation(ctx context.Context, index int, h util.Hash) (base.Operation, error) {
	e := util.StringErrorFunc("failed to collect operation, %q", h)

	var op base.Operation

	if err := util.Retry(ctx, func() (bool, error) {
		if op == nil {
			switch j, err := p.getOperation(ctx, h); {
			case err == nil:
				op = j
			case errors.Is(err, IgnoreOperationInProcessorError):
				return false, nil
			default:
				return true, err
			}
		}

		if op == nil {
			return false, nil
		}

		if err := p.writer.SetOperation(index, op); err != nil {
			return true, e(err, "failed to write operations")
		}

		return false, nil
	}, p.retrylimit, p.retryinterval); err != nil {
		return nil, e(err, "")
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
	var passed bool
	if err := util.Retry(ctx, func() (bool, error) {
		f := p.getPreProcessor(op)
		if f == nil {
			return false, nil
		}

		switch i, err := f(ctx); {
		case err == nil:
			passed = i

			return false, nil
		case errors.Is(err, IgnoreOperationInProcessorError):
			return false, nil
		default:
			return true, err
		}
	}, p.retrylimit, p.retryinterval); err != nil {
		return false, errors.Wrapf(err, "failed to pre process operation, %q", op.Fact().Hash())
	}

	return passed, nil
}

func (p *DefaultProposalProcessor) processOperation(ctx context.Context, index int, op base.Operation) error {
	var sts []base.State
	if err := util.Retry(ctx, func() (bool, error) {
		if sts == nil {
			f := p.getProcessor(op)
			if f == nil {
				return false, nil
			}

			i, err := f(ctx)
			switch {
			case err == nil:
				sts = i
				if len(sts) < 1 {
					return false, nil
				}
			case errors.Is(err, IgnoreOperationInProcessorError):
				return false, nil
			default:
				return true, err
			}
		}

		if sts == nil {
			return false, nil
		}

		if err := p.writer.SetStates(sts, index, op.Fact().Hash()); err != nil {
			return true, err
		}

		return false, nil
	}, p.retrylimit, p.retryinterval); err != nil {
		return errors.Wrapf(err, "failed to process operation, %q", op.Fact().Hash())
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

	if p.proposalfact == nil {
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
