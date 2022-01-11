package util

import (
	"fmt"
	"io"
	"runtime"
	"strings"

	"github.com/pkg/errors"
)

type Error struct {
	stack
	id      string
	msg     string
	extra   string
	wrapped error
}

func NewError(s string, a ...interface{}) Error {
	var pcs [1]uintptr
	_ = runtime.Callers(2, pcs[:])
	f := errors.Frame(pcs[0])

	return Error{
		id:  fmt.Sprintf("%n:%d", f, f),
		msg: strings.TrimSpace(fmt.Sprintf(s, a...)),
	}
}

func (er Error) Unwrap() error {
	return er.wrapped
}

func (er Error) Is(err error) bool {
	er.checkStack()

	e, ok := err.(Error) // nolint:errorlint
	if !ok {
		if er.wrapped == nil {
			return false
		}

		return errors.Is(er.wrapped, err)
	}

	return e.id == er.id
}

func (er Error) Wrap(err error) Error {
	er.stack = er.setStack()
	er.wrapped = err

	return er
}

// Wrapf formats strings with error.
func (er Error) Wrapf(err error, s string, a ...interface{}) Error {
	er.stack = er.setStack()
	er.extra = fmt.Sprintf(s, a...)
	er.wrapped = err

	return er
}

// Errorf formats strings. It does not support `%w` error formatting.
func (er Error) Errorf(s string, a ...interface{}) Error {
	er.stack = er.setStack()
	er.extra = fmt.Sprintf(s, a...)

	return er
}

func (er Error) Format(st fmt.State, verb rune) {
	er.checkStack()

	switch verb {
	case 'v':
		if st.Flag('+') {
			ws := er.wrapped != nil || er.stack != nil

			if ws {
				_, _ = fmt.Fprintf(st, "> %s", er.message())
			}

			if er.stack != nil {
				er.stack.Format(st, verb)
			}

			if er.wrapped != nil {
				if fm, ok := er.wrapped.(fmt.Formatter); ok { // nolint:errorlint
					_, _ = fmt.Fprintln(st)
					fm.Format(st, verb)
				} else {
					var d string
					if len(er.msg) > 0 {
						d = "; "
					}
					_, _ = fmt.Fprintf(st, "%s\n%+v", d, er.wrapped)
				}
			}

			if ws {
				return
			}
		}

		fallthrough
	case 's':
		_, _ = io.WriteString(st, er.Error())
	case 'q':
		_, _ = fmt.Fprintf(st, "%q", er.Error())
	}
}

func (er Error) StackTrace() errors.StackTrace {
	if er.stack != nil {
		return er.stack.StackTrace()
	}

	if er.wrapped == nil {
		return nil
	}

	i, ok := er.wrapped.(stackTracer) // nolint:errorlint
	if !ok {
		return nil
	}

	return i.StackTrace()
}

func (er Error) Call() Error {
	er.stack = er.setStack()

	return er
}

func (er Error) checkStack() {
	if er.stack == nil {
		panic(fmt.Errorf("Error, %q should not be used as error directly without Call()", er.msg))
	}
}

func (Error) setStack() stack {
	return callers(3)
}

func (er Error) message() string {
	s := er.msg
	if len(er.extra) > 0 {
		s += " - " + er.extra
	}

	return s
}

func (er Error) Error() string {
	er.checkStack()

	s := er.message()
	if er.wrapped != nil {
		if e := er.wrapped.Error(); len(e) > 0 {
			s += "; " + e
		}
	}

	return s
}

// callers is from
// https://github.com/pkg/errors/blob/856c240a51a2bf8fb8269ea7f3f9b046aadde36e/stack.go#L163
func callers(skip int) stack {
	const depth = 32
	var pcs [depth]uintptr
	n := runtime.Callers(skip, pcs[:])

	return stack(pcs[0:n])
}

type stack []uintptr

func (s stack) Format(st fmt.State, verb rune) {
	if verb == 'v' && st.Flag('+') {
		for _, pc := range s {
			_, _ = fmt.Fprintf(st, "\n%+v", errors.Frame(pc))
		}
	}
}

func (s stack) StackTrace() errors.StackTrace {
	f := make([]errors.Frame, len(s))
	for i := 0; i < len(f); i++ {
		f[i] = errors.Frame((s)[i])
	}
	return f
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}
