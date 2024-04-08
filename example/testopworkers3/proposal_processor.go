package testopsworkers

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/util"
)

type ProposalProcessor struct {
	pr               base.ProposalSignFact
	manager          *ProcessManager
	bm               isaacblock.BlockMap
	signBlockMapItem func(isaacblock.BlockMap) error
}

func (pp *ProposalProcessor) Proposal() base.ProposalSignFact {
	return pp.pr
}

func (pp *ProposalProcessor) Process(ctx context.Context, ivp base.INITVoteproof) (manifest base.Manifest, _ error) {
	if err := pp.manager.Process(); err != nil {
		return nil, err
	}

	switch i, err := pp.manager.ManifestDone(); {
	case err != nil:
		return nil, err
	default:
		manifest = i

		pp.bm.SetManifest(manifest)

		// FIXME save ivp
	}

	return manifest, nil
}

func (pp *ProposalProcessor) Save(ctx context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
	bf := map[base.BlockItemType]base.BlockItemFile{}

	switch i, err := pp.manager.Done(); {
	case err != nil:
		return nil, err
	default:
		for t := range i {
			if err := pp.bm.SetItem(
				isaacblock.NewBlockMapItem(t, util.UUID().String()), // FIXME use real checksum
			); err != nil {
				return nil, err
			}

			bf[t] = i[t]
		}
	}

	// FIXME save avp

	bfs := isaac.NewBlockItemFiles(bf)
	if err := bfs.IsValid(nil); err != nil {
		return nil, err
	}

	if err := pp.signBlockMapItem(pp.bm); err != nil {
		return nil, err
	}

	return pp.bm, nil
}

func (pp *ProposalProcessor) Cancel() error {
	pp.manager.Cancel()

	return nil
}
