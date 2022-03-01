package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type Database interface {
	Close() error
	Manifest(height base.Height) (base.Manifest, bool, error)
	LastManifest() (base.Manifest, bool, error)
	Suffrage(suffrageHeight base.Height) (base.State, bool, error)
	LastSuffrage() (base.State, bool, error)
	State(stateHash util.Hash) (base.State, bool, error)
	ExistsOperation(operationFactHash util.Hash) (bool, error)
}
