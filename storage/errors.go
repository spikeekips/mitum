package storage

import (
	"errors"

	"github.com/spikeekips/mitum/util"
)

var (
	ErrConnection = util.NewIDError("storage connection error")
	ErrInternal   = util.NewIDError("storage internal error")
	ErrExec       = util.NewIDError("storage execute")
	ErrNotFound   = util.NewIDError("not found in storage")
	ErrFound      = util.NewIDError("found in storage")
	ErrClosed     = util.NewIDError("storage closed")
)

func IsStorageError(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrInternal),
		errors.Is(err, ErrConnection),
		errors.Is(err, ErrExec),
		errors.Is(err, ErrNotFound),
		errors.Is(err, ErrFound),
		errors.Is(err, ErrClosed):
		return true
	default:
		return false
	}
}
