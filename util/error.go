package util

import (
	"fmt"
	"io"
	"runtime"
	"strings"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/pkgerrors"
)

type Error struct {
	wrapped error
	id      string
	msg     string
	extra   string
	stack
}

func NewError(s string, a ...interface{}) Error {
	f := FuncCaller(3)

	return Error{
		id:  fmt.Sprintf("%+v", f),
		msg: strings.TrimSpace(fmt.Sprintf(s, a...)),
	}
}

func (er Error) Unwrap() error {
	return er.wrapped
}

func (er Error) Is(err error) bool {
	er.checkStack()

	e, ok := err.(Error) //nolint:errorlint //...
	if !ok {
		if er.wrapped == nil {
			return false
		}

		return errors.Is(er.wrapped, err)
	}

	return e.id == er.id
}

func (er Error) Wrap(err error) Error {
	return Error{
		wrapped: err,
		id:      er.id,
		msg:     er.msg,
		extra:   er.extra,
		stack:   er.setStack(),
	}
}

// Wrapf formats strings with error.
func (er Error) Wrapf(err error, s string, a ...interface{}) Error {
	return Error{
		wrapped: err,
		id:      er.id,
		msg:     er.msg,
		extra:   fmt.Sprintf(s, a...),
		stack:   er.setStack(),
	}
}

// Errorf formats strings. It does not support `%w` error formatting.
func (er Error) Errorf(s string, a ...interface{}) Error {
	prev := er.wrapped
	if er.stack != nil {
		prev = er
	}

	return Error{
		wrapped: prev,
		id:      er.id,
		msg:     er.msg,
		extra:   fmt.Sprintf(s, a...),
		stack:   er.setStack(),
	}
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

func (er Error) StackTrace() errors.StackTrace {
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

func (er Error) Call() Error {
	er.stack = er.setStack()

	return er
}

func (er Error) checkStack() {
	if er.stack == nil {
		panic(errors.Errorf("error, %q should not be used as error directly without Call()", er.msg))
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

func StringErrorFunc(m string, a ...interface{}) func(error, string, ...interface{}) error {
	f := fmt.Sprintf(m, a...)

	return func(err error, s string, a ...interface{}) error {
		if len(s) > 0 {
			s = "; " + s
		}

		if err == nil {
			return errors.Errorf(f+s, a...)
		}

		return errors.Wrapf(err, f+s, a...)
	}
}

func FuncCaller(skip int) errors.Frame {
	var pcs [1]uintptr
	_ = runtime.Callers(skip, pcs[:])

	return errors.Frame(pcs[0])
}

func ZerologMarshalStack(err error) interface{} {
	type stackTracer interface {
		StackTrace() errors.StackTrace
	}

	var sterr stackTracer

	if !errors.As(err, &sterr) {
		uerr := errors.Unwrap(err)
		if uerr == nil {
			return nil
		}

		return ZerologMarshalStack(uerr)
	}

	st := sterr.StackTrace()
	s := &state{}
	out := make([]map[string]string, len(st)+1)

	out[0] = map[string]string{"error": err.Error()}

	for i := range st {
		frame := st[i]
		out[i+1] = map[string]string{
			pkgerrors.StackSourceFileName:     frameField(frame, s, 's') + ":" + frameField(frame, s, 'd'),
			pkgerrors.StackSourceFunctionName: frameField(frame, s, 'n'),
		}
	}

	uerr := errors.Unwrap(err)
	if uerr == nil {
		return out
	}

	uout := ZerologMarshalStack(uerr)
	if uout == nil {
		return out
	}

	uoutl := uout.([]map[string]string) //nolint:forcetypeassert //...

	nout := make([]map[string]string, len(out)+len(uoutl))
	copy(nout[:len(uoutl)], uoutl)
	copy(nout[len(uoutl):], out)

	return nout
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
