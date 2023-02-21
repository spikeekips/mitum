package storage

import (
	"errors"

	"github.com/spikeekips/mitum/util"
)

var (
	ErrConnection = util.NewMError("storage connection error")
	ErrInternal   = util.NewMError("storage internal error")
	ErrExec       = util.NewMError("failed to execute storage")
	ErrNotFound   = util.NewMError("not found")
	ErrFound      = util.NewMError("found")
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
