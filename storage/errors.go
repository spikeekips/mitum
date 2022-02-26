package storage

import "github.com/spikeekips/mitum/util"

var (
	ConnectionError = util.NewError("failed to connect storage")
	ExecError       = util.NewError("failed to execute storage")
	NotFoundError   = util.NewError("not found")
	FoundError      = util.NewError("found")
)
