package util

var (
	ErrFound          = NewMError("found")
	ErrNotFound       = NewMError("not found")
	ErrDuplicated     = NewMError("duplicated")
	ErrWrongType      = NewMError("wrong type")
	ErrNotImplemented = NewMError("not implemented")
	ErrInternal       = NewMError("internal error")
)
