package isaacstates

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

var SyncerCanNotCancelError = util.NewError("can not cancel syncer")

type Syncer interface {
	Top() base.Height
	Add(base.Height) bool
	Finished() <-chan base.Height
	IsFinished() bool
	Cancel() error
}
