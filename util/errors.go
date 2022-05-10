package util

var (
	ErrFound          = NewError("found")
	ErrNotFound       = NewError("not found")
	ErrDuplicated     = NewError("duplicated")
	ErrWrongType      = NewError("wrong type")
	ErrNotImplemented = NewError("not implemented")
)
