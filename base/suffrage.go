package base

type Suffrage interface {
	Exists(Address) bool
	Nodes() []Address
	Len() int
}

// ProposalSelector fetchs proposal from selected proposer
type ProposalSelector interface {
	Select(Point) Proposal
}
