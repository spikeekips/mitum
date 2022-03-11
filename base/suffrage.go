package base

import "github.com/spikeekips/mitum/util"

var SuffrageStateKey = "suffrage"

type Suffrage interface {
	Exists(Address) bool
	ExistsPublickey(Address, Publickey) bool
	Nodes() []Node
	Len() int
}

type SuffrageStateValue interface {
	StateValue
	Height() Height      // NOTE not manifest height
	Previous() util.Hash // NOTE previous state hash of SuffrageStateValue
	Nodes() []Node
	Suffrage() (Suffrage, error)
}
