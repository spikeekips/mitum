// +build test

package block

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

func NewTestBlockV0(height base.Height, round base.Round, proposal, previousBlock valuehash.Hash) (BlockV0, error) {
	nodes := []base.Node{base.RandomNode(util.UUID().String())}

	return NewBlockV0(
		NewSuffrageInfoV0(nodes[0].Address(), nodes),
		height,
		round,
		proposal,
		previousBlock,
		valuehash.RandomSHA256(),
		valuehash.RandomSHA256(),
		localtime.Now(),
	)
}
