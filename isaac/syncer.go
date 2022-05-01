package isaac

import (
	"github.com/spikeekips/mitum/base"
)

type Syncer interface {
	Top() base.Height
	Add(base.Height) bool
	Finished() <-chan base.Height
	IsFinished() bool
	Cancel() error
}
