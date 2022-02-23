package isaac

import (
	"github.com/spikeekips/mitum/base"
)

type Database interface {
	Manifest(base.Height) (base.Manifest, bool, error)
	LastManifest() (base.Manifest, bool, error)
	Suffrage(base.Height /* not manifest Height */) (base.SuffrageStateValue, bool, error)
	LastSuffrage() (base.SuffrageStateValue, bool, error)
}
