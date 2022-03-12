package storage

import (
	"errors"

	"github.com/spikeekips/mitum/util"
)

var (
	ConnectionError = util.NewError("storage connection error")
	InternalError   = util.NewError("storage internal error")
	ExecError       = util.NewError("failed to execute storage")
	NotFoundError   = util.NewError("not found")
	FoundError      = util.NewError("found")
)

func IsStorageError(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, InternalError):
		return true
	case errors.Is(err, ConnectionError):
		return true
	case errors.Is(err, ExecError):
		return true
	case errors.Is(err, NotFoundError):
		return true
	case errors.Is(err, FoundError):
		return true
	default:
		return false
	}
}
