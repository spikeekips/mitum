//go:build test
// +build test

package util

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

var TestLogger = logging.TestLogging.Log()

type BaseTest struct{}

func (t BaseTest) Print(a string, b ...any) {
	t.PrintCaller(2, a, b...)
}

func (t BaseTest) errorType(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "context.Canceled"
	default:
		return fmt.Sprintf("%T", err)
	}
}

func (t BaseTest) unwrapError(err error) (es []error) {
	if err == nil {
		return nil
	}

	es = append(es, err)

	uerr := err

	for {
		uerr = errors.Unwrap(uerr)
		if uerr == nil {
			break
		}

		es = append(es, uerr)
	}

	return es
}

func (t BaseTest) printUnwrapError(err error) string {
	es := t.unwrapError(err)
	if len(es) < 1 {
		return "nil"
	}

	ses := make([]string, len(es))
	for i, err := range slices.All(es) {
		ses[i] = t.errorType(err)
	}

	return strings.Join(ses, "|")
}

func (t BaseTest) PrintErr(err error, a string, b ...any) {
	switch {
	case err != nil:
		a = a + "; err=(%s)%v"
		b = append(b, t.printUnwrapError(err), err)
	default:
		a = a + "; err=nil"
	}

	t.PrintCaller(2, a, b...)
}

func (t BaseTest) PrintCaller(skip int, a string, b ...any) {
	_, file, no, _ := runtime.Caller(skip)
	f := fmt.Sprintf("    %s:%d: ", filepath.Base(file), no) + a + "\n"

	fmt.Printf(f, b...)
}

func (t BaseTest) PrintErrCaller(err error, skip int, a string, b ...any) {
	switch {
	case err != nil:
		a = a + "; err=(%s)%v"
		b = append(b, t.printUnwrapError(err), err)
	default:
		a = a + "; err=nil"
	}

	_, file, no, _ := runtime.Caller(skip)
	f := fmt.Sprintf("    %s:%d: ", filepath.Base(file), no) + a + "\n"

	fmt.Printf(f, b...)
}

func (t BaseTest) IfErrPanic(err error) {
	if err != nil {
		panic(err)
	}
}

func (t BaseTest) Panic(err error) {
	if err == nil {
		panic("")
	}

	_, file, no, _ := runtime.Caller(2)
	f := fmt.Sprintf("%s:%d: ", filepath.Base(file), no) + "; err=(%s)%v"

	panic(fmt.Sprintf(f, t.printUnwrapError(err), err))
}

func (t BaseTest) Panicf(err error, a string, b ...interface{}) {
	_, file, no, _ := runtime.Caller(2)
	f := fmt.Sprintf("%s:%d: ", filepath.Base(file), no) + a + "; err=(%s)%v"

	b = append(b, t.printUnwrapError(err), err)

	panic(fmt.Sprintf(f, b...))
}

func (t BaseTest) IfErrPanicf(err error, a string, b ...interface{}) {
	if err != nil {
		_, file, no, _ := runtime.Caller(2)
		f := fmt.Sprintf("%s:%d: ", filepath.Base(file), no) + a + "; err=(%s)%v"

		b = append(b, t.printUnwrapError(err), err)

		panic(errors.WithMessagef(err, f, b...))
	}
}

func (t BaseTest) IfTruePanic(ok bool, a string, b ...interface{}) {
	if ok {
		_, file, no, _ := runtime.Caller(2)
		f := fmt.Sprintf("%s:%d: ", filepath.Base(file), no) + a

		panic(fmt.Sprintf(f, b...))
	}
}

func (t BaseTest) PrintIfErrPanic(err error, a string, b ...interface{}) {
	if err != nil {
		_, file, no, _ := runtime.Caller(2)
		f := fmt.Sprintf("%s:%d: ", filepath.Base(file), no) + a + "; err=(%s)%v"

		b = append(b, t.printUnwrapError(err), err)

		panic(errors.WithMessagef(err, f, b...))
	}

	a = a + " err=%s"
	b = append(b, t.printUnwrapError(err))

	t.PrintCaller(2, a, b...)
}

func (t BaseTest) PrintIfTruePanic(ok bool, a string, b ...interface{}) {
	if ok {
		t.IfTruePanic(ok, a, b...)
	}

	t.PrintCaller(2, a, b...)
}

func (t BaseTest) Marshal(i interface{}) string {
	return util.MustMarshalJSONString(i)
}

func (t BaseTest) MarshalIndent(i interface{}) string {
	return util.MustMarshalJSONIndentString(i)
}

func (t BaseTest) RandomError() error {
	return errors.Errorf(util.UUID().String())
}

type BaseSuite struct {
	suite.Suite
	BaseTest
}

func (t *BaseSuite) Ctx() (context.Context, InErrorFunc[error]) {
	ctx, cancel := ContextWithCancelCause(context.Background())

	t.T().Cleanup(func() {
		cancel(nil)
	})

	return ctx, cancel
}

func (t *BaseSuite) RunWithGoLeak(name string, f func()) {
	t.Run(name, func() {
		defer goleak.VerifyNone(t.T())

		f()
	})
}

func AssertWaitChan[T any](t *assert.Assertions, timeout time.Duration, f func() T) func() T {
	ch := make(chan T, 1)

	go func() {
		ch <- f()
	}()

	return func() T {
		select {
		case <-time.After(timeout):
			t.Fail("timeout")

			var i T
			return i
		case i := <-ch:
			return i
		}
	}
}
