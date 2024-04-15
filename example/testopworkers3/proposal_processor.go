package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type ProposalProcessorArgs struct {
	NewJobWorker       func(context.Context) (util.JobWorker, error)
	SignBlockMapItem   func(isaacblock.BlockMap) error
	SaveBlockItemFuncs writerSaveBlockItemFuncs
}

func NewProposalProcessorArgs() *ProposalProcessorArgs {
	return &ProposalProcessorArgs{
		NewJobWorker: func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, 1<<6)
		},
		SignBlockMapItem: func(isaacblock.BlockMap) error {
			return util.ErrNotImplemented.Errorf("SignBlockMapItem")
		},
		SaveBlockItemFuncs: func(context.Context, base.BlockItemType) (writerSaveBlockItemFunc, writerCloseSaveFunc, error) {
			return nil, nil, util.ErrNotImplemented.Errorf("SaveBlockItemFuncs")
		},
	}
}

var _ isaac.ProposalProcessor = &ProposalProcessor{}

type ProposalProcessor struct {
	*logging.Logging
	args          *ProposalProcessorArgs
	pr            base.ProposalSignFact
	manager       *ProcessManager
	ctx           func() context.Context
	ctxCancel     func(error)
	bm            isaacblock.BlockMap
	saveJobWorker util.JobWorker
	vpWriter      *VoteproofWriter
	bf            map[base.BlockItemType]base.BlockItemFile
}

func NewProposalProcessor(
	ctx context.Context,
	args *ProposalProcessorArgs,
	pr base.ProposalSignFact,
	manager *ProcessManager,
) (*ProposalProcessor, error) {
	pctx, pctxCancel := context.WithCancelCause(ctx)

	pp := &ProposalProcessor{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "proposal-processor")
		}),
		args:      args,
		pr:        pr,
		manager:   manager,
		ctx:       func() context.Context { return pctx },
		ctxCancel: pctxCancel,
	}

	return pp, nil
}

func (pp *ProposalProcessor) Proposal() base.ProposalSignFact {
	return pp.pr
}

func (pp *ProposalProcessor) Process(_ context.Context, ivp base.INITVoteproof) (manifest base.Manifest, _ error) {
	switch i, err := pp.process(pp.ctx(), ivp); {
	case err != nil:
		pp.ctxCancel(err)

		return nil, err
	default:
		return i, nil
	}
}

func (pp *ProposalProcessor) Save(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
	switch i, err := pp.save(pp.ctx(), avp); {
	case err != nil:
		pp.ctxCancel(err)

		return nil, err
	default:
		return i, nil
	}
}

func (pp *ProposalProcessor) process(ctx context.Context, ivp base.INITVoteproof) (manifest base.Manifest, _ error) {
	switch i, err := pp.args.NewJobWorker(ctx); {
	case err != nil:
		return nil, err
	default:
		pp.saveJobWorker = i
	}

	pp.bf = map[base.BlockItemType]base.BlockItemFile{}
	pp.bm = isaacblock.NewBlockMap()

	if err := pp.saveJobWorker.NewJob(func(ctx context.Context, _ uint64) error {
		switch i, err := NewSingleBlockItemWriter(ctx, base.BlockItemProposal, pp.pr, pp.args.SaveBlockItemFuncs); {
		case err != nil:
			return err
		default:
			if err := pp.setBlockItem(base.BlockItemProposal, ""); err != nil {
				return err
			}

			pp.bf[base.BlockItemProposal] = NewBlockItemFile([]string{i})
		}

		return nil
	}); err != nil {
		return nil, err
	}

	switch i, err := NewVoteproofWriter(ctx, ivp, pp.args.SaveBlockItemFuncs); {
	case err != nil:
		return nil, err
	default:
		pp.vpWriter = i
	}

	go func() {
		if err := pp.manager.Process(); err != nil {
			pp.Log().Error().Err(err).Msg("process failed")
		}
	}()

	switch i, err := pp.manager.ManifestDone(); {
	case err != nil:
		return nil, err
	default:
		manifest = i

		pp.bm.SetManifest(manifest)
	}

	return manifest, nil
}

func (pp *ProposalProcessor) save(ctx context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
	defer clear(pp.bf)

	if err := pp.saveJobWorker.NewJob(func(ctx context.Context, _ uint64) error {
		switch i, err := pp.vpWriter.SetACCEPTVoteproof(avp); {
		case err != nil:
			return err
		default:
			if err := pp.setBlockItem(base.BlockItemVoteproofs, ""); err != nil {
				return err
			}

			pp.bf[base.BlockItemVoteproofs] = NewBlockItemFile([]string{i})
		}

		return nil
	}); err != nil {
		return nil, err
	}

	switch i, err := pp.manager.Done(); {
	case err != nil:
		return nil, err
	default:
		for t := range i {
			if err := pp.setBlockItem(t, util.UUID().String()); err != nil {
				return nil, err
			}

			pp.bf[t] = i[t]
		}
	}

	pp.saveJobWorker.Done()

	if err := pp.saveJobWorker.Wait(); err != nil {
		return nil, err
	}

	switch i, err := NewSingleBlockItemWriter(ctx, base.BlockItemMap, pp.bm, pp.args.SaveBlockItemFuncs); {
	case err != nil:
		return nil, err
	default:
		pp.bf[base.BlockItemMap] = NewBlockItemFile([]string{i})
	}

	bfs := isaac.NewBlockItemFiles(pp.bf)
	if err := bfs.IsValid(nil); err != nil {
		return nil, err
	}

	return pp.bm, nil
}

func (pp *ProposalProcessor) Cancel() error {
	pp.ctxCancel(nil)

	pp.manager.Cancel()
	if pp.vpWriter != nil {
		pp.vpWriter.Cancel()
	}

	return nil
}

func (pp *ProposalProcessor) setBlockItem(t base.BlockItemType, _ string) error {
	return pp.bm.SetItem(
		isaacblock.NewBlockMapItem(t, util.UUID().String()), // FIXME use real checksum
	)
}
