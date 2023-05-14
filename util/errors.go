package util

var (
	ErrFound          = NewIDError("found")
	ErrNotFound       = NewIDError("not found")
	ErrDuplicated     = NewIDError("duplicated")
	ErrNotImplemented = NewIDError("not implemented")
	ErrInternal       = NewIDError("internal error")
)
