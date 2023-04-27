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
	ErrOperationInProcessorNotFound         = util.NewMError("operation processor not found")
	ErrInvalidOperationInProcessor          = util.NewMError("invalid operation")
	ErrOperationNotFoundInProcessor         = util.NewMError("operation not found")
	ErrOperationAlreadyProcessedInProcessor = util.NewMError("operation already processed")
	ErrStopProcessingRetry                  = util.NewMError("stop processing retrying")
	ErrIgnoreStateValue                     = util.NewMError("ignore state value")
	ErrSuspendOperation                     = util.NewMError("suspend operation")
	ErrProcessorAlreadySaved                = util.NewMError("processor already saved")
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
	Save(context.Context, base.ACCEPTVoteproof) (base.BlockMap, error)
	Cancel() error
}

type DefaultProposalProcessor struct {
	*logging.Logging
	proposal              base.ProposalSignFact
	ctx                   context.Context //nolint:containedctx //...
	cancel                func()
	previous              base.Manifest
	writer                BlockWriter
	getStatef             base.GetStateFunc
	getOperationf         OperationProcessorGetOperationFunction
	newOperationProcessor NewOperationProcessorFunction
	ivp                   base.INITVoteproof
	oprs                  *util.ShardedMap[string, base.OperationProcessor]
	processstate          *util.Locked[int]
	processlock           sync.Mutex
	retrylimit            int
	retryinterval         time.Duration
}

func NewDefaultProposalProcessor(
	proposal base.ProposalSignFact,
	previous base.Manifest,
	newWriter NewBlockWriterFunc,
	getStatef base.GetStateFunc,
	getOperationf OperationProcessorGetOperationFunction,
	newOperationProcessor NewOperationProcessorFunction,
) (*DefaultProposalProcessor, error) {
	writer, err := newWriter(proposal, getStatef)
	if err != nil {
		return nil, errors.Wrap(err, "make new ProposalProcessor")
	}

	oprs, _ := util.NewShardedMap[string, base.OperationProcessor](1 << 5) //nolint:gomnd //...

	ctx, cancel := context.WithCancel(context.Background())

	return &DefaultProposalProcessor{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "default-proposal-processor")
		}),
		proposal:              proposal,
		ctx:                   ctx,
		cancel:                cancel,
		previous:              previous,
		writer:                writer,
		getStatef:             getStatef,
		getOperationf:         getOperationf,
		newOperationProcessor: newOperationProcessor,
		oprs:                  oprs,
		processstate:          util.NewLocked(0),
		retrylimit:            15,                     //nolint:gomnd //...
		retryinterval:         time.Millisecond * 600, //nolint:gomnd //...
	}, nil
}

func (p *DefaultProposalProcessor) Proposal() base.ProposalSignFact {
	return p.proposal
}

func (p *DefaultProposalProcessor) Process(ctx context.Context, ivp base.INITVoteproof) (base.Manifest, error) {
	p.processlock.Lock()
	defer p.processlock.Unlock()

	switch {
	case p.isCanceled():
		return nil, errors.Errorf("already canceled")
	case p.isProcessed():
		return nil, errors.Errorf("already processed")
	}

	e := util.StringErrorFunc("process operations")

	pctx, cancel := context.WithCancel(p.ctx)
	defer cancel()

	deferf, f := p.deferctx(ctx, cancel)
	defer deferf()
	f()

	p.ivp = ivp

	if err := p.process(pctx); err != nil {
		p.clean()

		return nil, e(err, "")
	}

	manifest, err := p.writer.Manifest(pctx, p.previous)
	if err != nil {
		p.clean()

		return nil, e(err, "")
	}

	p.Log().Info().Interface("manifest", manifest).Msg("new manifest prepared")

	if _, err := p.processstate.Set(func(i int, _ bool) (int, error) {
		if i < 0 {
			return i, errors.Errorf("process proposal; already canceled")
		}

		return 1, nil
	}); err != nil {
		p.clean()

		return nil, e(err, "")
	}

	return manifest, nil
}

func (p *DefaultProposalProcessor) Save(ctx context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
	p.processlock.Lock()
	defer p.processlock.Unlock()

	switch {
	case p.isCanceled():
		return nil, errors.Errorf("already canceled")
	case p.isSaved():
		return nil, ErrProcessorAlreadySaved.Call()
	}

	sctx, cancel := context.WithCancel(p.ctx)
	defer cancel()

	deferf, f := p.deferctx(ctx, cancel)
	defer deferf()
	f()

	defer p.clean()

	var bm base.BlockMap

	if err := util.Retry(sctx, func() (bool, error) {
		switch i, err := p.save(sctx, avp); {
		case err == nil:
			return false, nil
		case errors.Is(err, ErrProcessorAlreadySaved):
			return false, err
		case errors.Is(err, ErrStopProcessingRetry):
			return false, err
		default:
			p.Log().Error().Err(err).Msg("failed to save; will retry")

			bm = i

			return true, err
		}
	}, p.retrylimit, p.retryinterval); err != nil {
		return nil, errors.Wrap(err, "save proposal")
	}

	_, err := p.processstate.Set(func(i int, _ bool) (int, error) {
		if i < 0 {
			return i, errors.Errorf("save proposal; already canceled")
		}

		return 2, nil //nolint:gomnd //...
	})

	return bm, err
}

func (p *DefaultProposalProcessor) Cancel() error {
	_, _ = p.processstate.Set(func(i int, _ bool) (int, error) {
		if i == -1 {
			return i, util.ErrLockedSetIgnore.Call()
		}

		p.cancel()

		p.Log().Debug().Msg("proposal processor canceled")

		return -1, nil
	})

	return nil
}

func (p *DefaultProposalProcessor) clean() {
	p.ctx = nil
	p.previous = nil
	p.writer = nil
	p.getStatef = nil
	p.getOperationf = nil
	p.newOperationProcessor = nil
	p.ivp = nil
	p.oprs.Close()
}

func (p *DefaultProposalProcessor) isCanceled() bool {
	i, _ := p.processstate.Value()

	return i == -1 //nolint:gomnd //...
}

func (p *DefaultProposalProcessor) isProcessed() bool {
	i, _ := p.processstate.Value()

	return i == 1 //nolint:gomnd //...
}

func (p *DefaultProposalProcessor) isSaved() bool {
	i, _ := p.processstate.Value()

	return i == 2 //nolint:gomnd //...
}

func (p *DefaultProposalProcessor) process(ctx context.Context) error {
	if err := p.writer.SetINITVoteproof(ctx, p.ivp); err != nil {
		return errors.WithMessage(err, "set init voteproof")
	}

	var cops []base.Operation

	switch i, err := p.collectOperations(ctx); {
	case err != nil:
		return errors.WithMessage(err, "collect operations")
	case len(i) < 1:
		return nil
	default:
		p.writer.SetOperationsSize(uint64(len(i)))

		cops = i
	}

	if err := p.processOperations(ctx, cops); err != nil {
		return errors.WithMessage(err, "process operations")
	}

	return nil
}

func (p *DefaultProposalProcessor) collectOperations(ctx context.Context) ([]base.Operation, error) {
	e := util.StringErrorFunc("collect operations")

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

	getOperationf := p.getOperationf

	if err := util.RunErrgroupWorker(cctx, uint64(len(ophs)), func(ctx context.Context, i, _ uint64) error {
		h := ophs[i]
		op, err := p.collectOperation(ctx, h, getOperationf)

		switch {
		case err != nil:
			return err
		case op == nil:
			p.Log().Debug().Err(err).Stringer("operation", h).Msg("operation ignored")
		default:
			p.Log().Trace().Stringer("operation", h).Err(err).Msg("operation collected")

			cops[index+int(i)] = op
		}

		return nil
	}); err != nil {
		cancel()

		return nil, e(err, "")
	}

	return cops, nil
}

func (p *DefaultProposalProcessor) processOperations(ctx context.Context, cops []base.Operation) error {
	e := util.StringErrorFunc("process operations")

	p.Log().Debug().Int("operations", len(cops)).Msg("trying to process operations")

	worker := util.NewErrgroupWorker(ctx, int64(len(cops)))
	defer worker.Close()

	pctx := ctx

	var opsindex, validindex int

	writer := p.writer
	getStatef := p.getStatef
	newOperationProcessor := p.newOperationProcessor

	for i := range cops {
		op := cops[i]
		if op == nil {
			continue
		}

		var err error

		pctx, opsindex, validindex, err = p.processOperation(
			pctx, writer, getStatef, newOperationProcessor, worker, op, opsindex, validindex)
		if err != nil {
			return e(err, "")
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	return nil
}

func (p *DefaultProposalProcessor) processOperation(
	ctx context.Context,
	writer BlockWriter,
	getStatef base.GetStateFunc,
	newOperationProcessor NewOperationProcessorFunction,
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
		ctx = pctx //revive:disable-line:modifies-parameter
	}

	if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
		return p.doProcessOperation(
			ctx, writer, getStatef, newOperationProcessor, uint64(opsindex), uint64(validindex), op)
	}); err != nil {
		return ctx, opsindex + 1, validindex + 1, err
	}

	return ctx, opsindex + 1, validindex + 1, nil
}

func (p *DefaultProposalProcessor) doPreProcessOperation(
	ctx context.Context,
	newOperationProcessor NewOperationProcessorFunction,
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
	newOperationProcessor NewOperationProcessorFunction,
	opsindex, validindex uint64,
	op base.Operation,
) error {
	e := util.StringErrorFunc("process operation, %q", op.Fact().Hash())

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
			if err := writer.SetStates(ctx, validindex, stvs, op); err != nil {
				return true, e(err, "")
			}
		}

		if err := writer.SetProcessResult(
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

func (p *DefaultProposalProcessor) getPreProcessor(
	ctx context.Context,
	newOperationProcessor NewOperationProcessorFunction,
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
			return opp.PreProcess(ctx, op, p.getStatef) //nolint:wrapcheck //...
		}, nil
	}

	return func(ctx context.Context) (context.Context, base.OperationProcessReasonError, error) {
		return op.PreProcess(ctx, p.getStatef) //nolint:wrapcheck //...
	}, nil
}

func (p *DefaultProposalProcessor) getProcessor(
	ctx context.Context,
	getStatef base.GetStateFunc,
	newOperationProcessor NewOperationProcessorFunction,
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
	newOperationProcessor NewOperationProcessorFunction,
	ht hint.Hint,
) (
	base.OperationProcessor, bool, error,
) {
	i, _, err := p.oprs.GetOrCreate(ht.String(), func() (opp base.OperationProcessor, _ error) {
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
			return nil, ErrOperationInProcessorNotFound.Call()
		}

		return opp, nil
	})

	switch {
	case err == nil:
		return i, true, nil
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
	}, p.retrylimit, p.retryinterval)
}

func (p *DefaultProposalProcessor) save(ctx context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
	e := util.StringErrorFunc("save")

	if err := p.writer.SetACCEPTVoteproof(ctx, avp); err != nil {
		return nil, e(err, "set accept voteproof")
	}

	m, err := p.writer.Save(ctx)
	if err != nil {
		return nil, e(err, "")
	}

	p.Log().Info().Interface("blockmap", m).Msg("new block saved in proposal processor")

	return m, nil
}

func (p *DefaultProposalProcessor) collectOperation(
	ctx context.Context,
	h util.Hash,
	getOperationf OperationProcessorGetOperationFunction,
) (base.Operation, error) {
	e := util.StringErrorFunc("collect operation, %q", h)

	var op base.Operation

	if err := p.retry(ctx, func() (bool, error) {
		switch j, err := getOperationf(ctx, h); {
		case err == nil:
			op = j

			return false, nil
		case errors.Is(err, util.ErrInvalid),
			errors.Is(err, ErrInvalidOperationInProcessor),
			errors.Is(err, ErrOperationNotFoundInProcessor),
			errors.Is(err, ErrOperationAlreadyProcessedInProcessor):
			return false, nil
		default:
			// NOTE suffrage expel operation should be in ballot.
			if _, ok := j.(base.SuffrageExpelOperation); ok {
				return false, nil
			}

			return true, err
		}
	}); err != nil {
		return nil, e(err, "")
	}

	if op == nil {
		p.Log().Debug().Stringer("operation", h).Msg("operation ignored")

		return nil, nil
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
