package base

import (
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/tree"
)

type Manifest interface {
	hint.Hinter
	util.Hasher
	util.IsValider
	Height() Height
	Previous() util.Hash
	Proposal() util.Hash       // NOTE proposal fact hash
	OperationsTree() util.Hash // NOTE operations tree root hash
	StatesTree() util.Hash     // NOTE states tree root hash
	Suffrage() util.Hash       // NOTE state hash of SuffrageStateValue
	CreatedAt() time.Time      // NOTE Proposal proposed time
	NodeCreatedAt() time.Time  // NOTE created time in local node
}

type Block interface {
	hint.Hinter
	util.Hash
	util.IsValider
	Height() Height
	Manifest() Manifest
	Proposal() ProposalSignedFact
	Operations() []Operation
	OperationsTree() tree.FixedTree
	States() []State
	StatesTree() tree.FixedTree
	INITVoteproof() INITVoteproof
	ACCEPTVoteproof() ACCEPTVoteproof
}
