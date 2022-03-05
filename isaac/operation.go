package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/hint"
)

type OperationProcessHandler interface {
	Hints() []hint.Hint
	PreProcess(operation base.Operation) (bool, error)
	Process(operation base.Operation) ([]base.State, error)
}
