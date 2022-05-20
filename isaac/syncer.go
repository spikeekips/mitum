package isaac

import (
	"github.com/spikeekips/mitum/base"
)

type Syncer interface {
	Top() base.Height
	Add(base.Height) bool
	Finished() <-chan base.Height
	Done() <-chan struct{} // revive:disable-line:nested-structs
	Err() error
	IsFinished() bool
	Cancel() error
}
