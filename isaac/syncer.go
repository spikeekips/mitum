package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

var syncerCanNotCancelError = util.NewError("can not cancel syncer")

type syncer interface {
	top() base.Height
	add(base.Height) bool
	finished() <-chan base.Height
	isFinished() bool
	cancel() error
}
