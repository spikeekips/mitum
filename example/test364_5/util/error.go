package util

import "github.com/pkg/errors"

type SError string

func NewSError(s string) *SError {
	e := SError(s)

	return &e
}

func (s *SError) Error() string {
	return string(*s)
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func ErrorWithStack(err error) error {
	if err == nil {
		return nil
	}

	if _, ok := err.(stackTracer); ok {
		return err
	}

	return errors.WithStack(err)
}
