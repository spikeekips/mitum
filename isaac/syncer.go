package isaac

import (
	"github.com/spikeekips/mitum/base"
)

type Syncer interface {
	Add(base.Height) bool
	Finished() <-chan base.Height
	Done() <-chan struct{} // revive:disable-line:nested-structs
	Err() error
	IsFinished() (base.Height, bool)
	Cancel() error
}
