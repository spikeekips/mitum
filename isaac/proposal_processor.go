package isaac

import (
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

var IgnoreOperationInProcessorError = util.NewError("ignore operation in processor")

type proposalProcessor interface {
	Proposal() base.ProposalFact
	Process(context.Context) (base.Manifest, error)
	Save(context.Context, base.ACCEPTVoteproof) error
	Cancel() error
}

type DefaultProposalProcessor struct {
	proposal           base.ProposalFact
	sp                 base.StatePool
	wdb                BlockWriteDatabase
	mergeWriteDatabase func(BlockWriteDatabase) error
	ops                []base.Operation
}

func NewDefaultProposalProcessor(
	proposal base.ProposalFact,
	sp base.StatePool,
	wdb BlockWriteDatabase,
	mergeWriteDatabase func(BlockWriteDatabase) error,
) *DefaultProposalProcessor {
	return &DefaultProposalProcessor{
		proposal:           proposal,
		sp:                 sp,
		wdb:                wdb,
		mergeWriteDatabase: mergeWriteDatabase,
	}
}

func (p *DefaultProposalProcessor) Process(ctx context.Context) (base.Manifest, error) {
	e := util.StringErrorFunc("failed to process proposal")

	if len(p.proposal.Operations()) > 0 {
		if err := p.processOperations(ctx); err != nil {
			return nil, e(err, "failed to process operations")
		}
	}

	// NOTE check PreProcess()
	// NOTE Process()
	// NOTE collect 'State's; wdb.SetStates()
	// NOTE create manifest; wdb.SetManifest()
	return nil, nil
}

func (p *DefaultProposalProcessor) processOperations(ctx context.Context) error {
	if err := p.wdb.SetOperations(p.proposal.Operations()); err != nil {
		return errors.Wrap(err, "failed to set operations")
	}

	if err := p.collectOperations(ctx); err != nil {
		return errors.Wrap(err, "failed to collect operations")
	}

	return nil
}

func (p *DefaultProposalProcessor) collectOperations(ctx context.Context) error {
	// BLOCK if error to fetch operations, return RetryProposalProcessorError
	return nil
}

func (p *DefaultProposalProcessor) checkPreProcess(ctx context.Context, ops []base.Operation) ([]base.Operation, error) {
	e := util.StringErrorFunc("failed to check PreProcess")

	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

	done := make(chan []base.Operation)
	defer close(done)

	opsch := make(chan base.Operation, 1)
	go func() {
		var processed []base.Operation
		for op := range opsch {
			processed = append(processed, op)
		}

		done <- processed
	}()

	for i := range ops {
		op := ops[i]
		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			switch passed, err := p.checkPreProcessor(ctx, op); {
			case err != nil:
				return errors.Wrapf(err, "failed to process operation, %q", op.Fact().Hash())
			case !passed:
				return errors.Wrapf(err, "operation ignored, %q", op.Fact().Hash())
			default:
				opsch <- op

				return nil
			}
		}); err != nil {
			return nil, e(err, "")
		}
	}

	worker.Done()

	if err := func() error {
		defer close(opsch)

		return worker.Wait()
	}(); err != nil {
		return nil, e(err, "")
	}

	return <-done, nil
}

func (p *DefaultProposalProcessor) checkPreProcessor(ctx context.Context, op base.Operation) (bool, error) {
	switch t := op.(type) {
	case base.OperationProcessor:
		return t.PreProcess(p.sp)
	default:
		return false, nil
	}
}
