package util

var (
	FoundError      = NewError("found")
	NotFoundError   = NewError("not found")
	DuplicatedError = NewError("duplicated")
	WrongTypeError  = NewError("wrong type")
)
