package mitum

import (
	"github.com/spikeekips/mitum/hint"
	"github.com/spikeekips/mitum/seal"
	"github.com/spikeekips/mitum/valuehash"
)

type Ballot interface {
	seal.Seal
	Stage() Stage
	Height() Height
	Round() Round
	Node() Address
}

var (
	INITBallotType     hint.Type = hint.Type([2]byte{0x03, 0x00})
	ProposalBallotType hint.Type = hint.Type([2]byte{0x03, 0x01})
	SIGNBallotType     hint.Type = hint.Type([2]byte{0x03, 0x02})
	ACCEPTBallotType   hint.Type = hint.Type([2]byte{0x03, 0x03})
)

type INITBallot interface {
	Ballot
	PreviousBlock() valuehash.Hash
	PreviousRound() Round
	VoteResult() VoteResult
}

type Proposal interface {
	Ballot
	Seals() []valuehash.Hash // collection of received Seals
}

type SIGNBallot interface {
	Ballot
	Proposal() valuehash.Hash
	NewBlock() valuehash.Hash
}

type ACCEPTBallot interface {
	Ballot
	Proposal() valuehash.Hash
	NewBlock() valuehash.Hash
	VoteResult() VoteResult
}