package storage

import (
	"errors"

	"github.com/spikeekips/mitum/util"
)

var (
	ErrConnection = util.NewError("storage connection error")
	ErrInternal   = util.NewError("storage internal error")
	ErrExec       = util.NewError("failed to execute storage")
	ErrNotFound   = util.NewError("not found")
	ErrFound      = util.NewError("found")
)

func IsStorageError(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrInternal),
		errors.Is(err, ErrConnection),
		errors.Is(err, ErrExec),
		errors.Is(err, ErrNotFound),
		errors.Is(err, ErrFound):
		return true
	default:
		return false
	}
}
