package util

import (
	goerrors "errors"
	"fmt"
	"io"
	"runtime"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/pkgerrors"
)

type baseError struct {
	wrapped error
	msg     string
	extra   string
	stack
}

func newBaseError(format string, args ...interface{}) *baseError {
	er := &baseError{
		msg: fmt.Sprintf(format, args...),
	}

	er.stack = er.setStack()

	return er
}

func (er *baseError) Unwrap() error {
	return er.wrapped
}

func (er *baseError) Wrap(err error) error {
	if err == nil {
		return nil
	}

	var stk stack
	if _, ok := err.(stackTracer); !ok {
		stk = er.setStack()
	}

	return &baseError{
		wrapped: err,
		msg:     er.msg,
		extra:   er.extra,
		stack:   stk,
	}
}

// Wrapf formats strings with error.
func (er *baseError) WithMessage(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}

	var stk stack
	if _, ok := err.(stackTracer); !ok {
		stk = er.setStack()
	}

	extra := fmt.Sprintf(format, args...)

	if len(er.extra) > 0 {
		extra = er.extra + "; " + extra
	}

	return &baseError{
		wrapped: err,
		msg:     er.msg,
		extra:   extra,
		stack:   stk,
	}
}

// Errorf formats strings. It does not support `%w` error formatting.
func (er *baseError) Errorf(format string, args ...interface{}) *baseError {
	extra := fmt.Sprintf(format, args...)

	if len(er.extra) > 0 {
		extra = er.extra + "; " + extra
	}

	return &baseError{
		wrapped: er.wrapped,
		msg:     er.msg,
		extra:   extra,
		stack:   er.setStack(),
	}
}

func (er *baseError) StackTrace() errors.StackTrace {
	if er.stack != nil {
		return er.stack.StackTrace()
	}

	if er.wrapped == nil {
		return nil
	}

	i, ok := er.wrapped.(stackTracer) //nolint:errorlint //...
	if !ok {
		return nil
	}

	return i.StackTrace()
}

func (er *baseError) WithStack() *baseError {
	return &baseError{
		wrapped: er.wrapped,
		msg:     er.msg,
		extra:   er.extra,
		stack:   er.setStack(),
	}
}

func (er *baseError) Error() string {
	s := er.message()

	if er.wrapped != nil {
		if e := er.wrapped.Error(); len(e) > 0 {
			s += "; " + e
		}
	}

	return s
}

func (er *baseError) Format(st fmt.State, verb rune) {
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
				if fm, ok := er.wrapped.(fmt.Formatter); ok { //nolint:errorlint //...
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

func (er *baseError) message() string {
	s := er.msg
	if len(er.extra) > 0 {
		s += " - " + er.extra
	}

	return s
}

func (*baseError) setStack() stack {
	return callers(4)
}

type IDError struct {
	*baseError
	id string
}

func NewIDError(format string, args ...interface{}) *IDError {
	return NewIDErrorWithID(fmt.Sprintf("%+v", FuncCaller(3)), format, args...)
}

func NewIDErrorWithID(id, format string, args ...interface{}) *IDError {
	return &IDError{
		baseError: newBaseError(format, args...),
		id:        id,
	}
}

func (er *IDError) Is(err error) bool {
	if err == nil {
		return false
	}

	var me *IDError

	switch {
	case errors.As(err, &me):
		return me.id == er.id
	case er.wrapped == nil:
		return false
	default:
		return errors.Is(er.wrapped, err)
	}
}

func (er *IDError) Wrap(err error) error {
	ne := er.baseError.Wrap(err)

	if ne == nil {
		return nil
	}

	return &IDError{
		id:        er.id,
		baseError: ne.(*baseError), //nolint:forcetypeassert //...
	}
}

// WithMessage formats strings with error.
func (er *IDError) WithMessage(err error, format string, args ...interface{}) error {
	ne := er.baseError.WithMessage(err, format, args...)

	if ne == nil {
		return nil
	}

	return &IDError{
		id:        er.id,
		baseError: ne.(*baseError), //nolint:forcetypeassert //...
	}
}

// Errorf formats strings. It does not support `%w` error formatting.
func (er *IDError) Errorf(format string, args ...interface{}) *IDError {
	return &IDError{
		id:        er.id,
		baseError: er.baseError.Errorf(format, args...),
	}
}

func (er *IDError) WithStack() *IDError {
	return &IDError{
		id:        er.id,
		baseError: er.baseError.WithStack(),
	}
}

func StringError(format string, args ...interface{}) *baseError { //revive:disable-line:unexported-return
	return newBaseError(format, args...)
}

func FuncCaller(skip int) errors.Frame {
	var pcs [1]uintptr
	_ = runtime.Callers(skip, pcs[:])

	return errors.Frame(pcs[0])
}

func ZerologMarshalStack(err error) interface{} {
	var sterr stackTracer

	if !errors.As(err, &sterr) {
		uerr := errors.Unwrap(err)
		if uerr == nil {
			return nil
		}

		return ZerologMarshalStack(uerr)
	}

	switch uerr := errors.Unwrap(err); {
	case uerr == nil:
	case errors.As(err, &sterr):
		return ZerologMarshalStack(uerr)
	}

	st := sterr.StackTrace()
	s := &state{}
	out := make([]map[string]string, len(st))

	for i := range st {
		frame := st[i]
		out[i] = map[string]string{
			pkgerrors.StackSourceFileName:     frameField(frame, s, 's') + ":" + frameField(frame, s, 'd'),
			pkgerrors.StackSourceFunctionName: frameField(frame, s, 'n'),
		}
	}

	return out
}

func JoinErrors(errs ...error) error {
	return errors.WithStack(goerrors.Join(errs...))
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

// -x----------------------------------------------------
// NOTE from github.com/pkg/errors/stack.go

type state struct {
	b []byte
}

func (s *state) Write(b []byte) (n int, err error) {
	s.b = b
	return len(b), nil
}

func (*state) Width() (wid int, ok bool) {
	return 0, false
}

func (*state) Precision() (prec int, ok bool) {
	return 0, false
}

func (*state) Flag(int) bool {
	return true
}

func frameField(f errors.Frame, s *state, c rune) string {
	f.Format(s, c)

	return string(s.b)
}

// ----------------------------------------------------x-
