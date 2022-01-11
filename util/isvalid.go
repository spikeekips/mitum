package util

var InvalidError = NewError("invalid")

type IsValider interface {
	IsValid([]byte) error
}
