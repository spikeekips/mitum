package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
)

type testError struct {
	suite.Suite
}

func (t *testError) TestErrorWithoutCall() {
	e := NewError("showme")
	t.PanicsWithError("Error should not be used as error directly without Call()", func() { _ = e.Error() })

	t.NotPanics(func() { _ = e.Call().Error() })
}

func (t *testError) TestIs() {
	e := NewError("showme")

	e0 := e.Call()
	t.Implements((*(interface{ Error() string }))(nil), e0)

	t.Equal("showme", e0.Error())

	t.True(errors.Is(e0, e0))
	t.False(errors.Is(e0, NewError("showme").Call()))
	t.False(errors.Is(e0, NewError("findme").Call()))
	t.True(errors.Is(e0, e0.Errorf("showme")))
}

func (t *testError) TestAs() {
	e := NewError("showme")
	e0 := e.Call()

	var e1 Error
	t.True(errors.As(e0, &e1))

	t.True(errors.Is(e0, e1))
	t.True(errors.Is(e1, e0))
	t.Equal(e0.Error(), e1.Error())
}

func (t *testError) TestWrap() {
	e := NewError("showme")
	e0 := e.Call()

	pe := &os.PathError{Op: "not found", Path: "/tmp", Err: errors.Errorf("???")}
	e1 := e0.Wrap(pe)

	t.False(errors.Is(e1, NewError("showme").Call()))
	t.True(errors.Is(e1, e1.Errorf("showme")))
	t.True(errors.Is(e1, pe))

	var e2 Error
	t.True(errors.As(e0, &e2))
	t.True(errors.As(e1, &e2))

	t.True(errors.Is(e0, e2))
	t.True(errors.Is(e1, e2))

	var npe *os.PathError
	t.True(errors.As(e1, &npe))

	t.True(errors.Is(pe, npe))
	t.Equal(pe.Error(), npe.Error())
}

func (t *testError) TestWrapAgain() {
	ea := NewError("showme")
	eb := NewError("findme")
	e0 := ea.Call()

	e1 := e0.Wrap(eb.Call())

	t.True(errors.Is(e0, e1))
	t.True(errors.Is(e1, e0))
}

func (t *testError) TestWrapf() {
	e := NewError("showme")
	e0 := e.Call()

	pe := &os.PathError{Op: "not found", Path: "/tmp", Err: errors.Errorf("???")}
	e1 := e0.Wrapf(pe, "find me: %d", 3)

	t.True(errors.Is(e0, e1))
	t.False(errors.Is(e1, NewError("showme").Call()))
	t.True(errors.Is(e1, e1.Errorf("showme")))
	t.True(errors.Is(e1, pe))

	var e2 Error
	t.True(errors.As(e0, &e2))
	t.True(errors.As(e1, &e2))

	t.True(errors.Is(e0, e2))
	t.True(errors.Is(e1, e2))

	var npe *os.PathError
	t.True(errors.As(e1, &npe))

	t.True(errors.Is(pe, npe))
	t.Equal(pe.Error(), npe.Error())
}

func (t *testError) TestErrorf() {
	e := NewError("showme")
	e0 := e.Call()

	e1 := e0.Errorf("error: %d", 33)

	var e2 Error
	t.True(errors.As(e0, &e2))
	t.True(errors.As(e1, &e2))

	t.True(errors.Is(e0, e1))
	t.True(errors.Is(e1, e1))
}

func (t *testError) printStack(err error) (string, bool) {
	i, ok := err.(interface {
		StackTrace() errors.StackTrace
	})
	if !ok {
		return "<no StackTrace()>", false
	}

	buf := bytes.NewBuffer(nil)

	st := i.StackTrace()
	for i, f := range st {
		_, _ = fmt.Fprintf(buf, "%+s:%d", f, f)
		if i < len(st)-1 {
			fmt.Fprintln(buf)
		}
	}

	return buf.String(), true
}

func (t *testError) printStacks(err error) string {
	buf := bytes.NewBuffer(nil)

	_, _ = fmt.Fprintln(buf, "================================================================================")

	var e error = err
	for {
		i, ok := t.printStack(e)
		if ok {
			_, _ = fmt.Fprintln(buf, i)
			_, _ = fmt.Fprintln(buf, "================================================================================")
		}
		e = errors.Unwrap(e)
		if e == nil {
			break
		}
	}

	return buf.String()
}

func (t *testError) TestPrint() {
	e := NewError("showme")
	e0 := e.Call()

	t.T().Logf("e0,  v: %v", e0)
	t.T().Logf("e0, +v: %+v", e0)

	e1 := e0.Wrapf(&os.PathError{Op: "op", Path: "/tmp", Err: errors.Errorf("path error")}, "findme")
	t.T().Logf("e1,  v: %v", e1)
	t.T().Logf("e1, +v: %+v", e1)

	e2 := e0.Wrap(&os.PathError{Op: "e2", Path: "/tmp/e2", Err: errors.Errorf("path error")})
	t.T().Logf("e2,  v: %v", e2)
	t.T().Logf("e2, +v: %+v", e2)
}

func (t *testError) TestPrintStacks() {
	e := NewError("showme")
	e0 := e.Call()

	e1 := errors.New("findme")

	e2 := e0.Wrap(e1)
	t.T().Logf("e2,      v: %v", e2)
	t.T().Logf("e2,     +v: %+v", e2)
	t.T().Logf("e2, stacks:\n%s", t.printStacks(e2))
}

func (t *testError) checkStack(b []byte) bool {
	var m map[string]interface{}
	t.NoError(json.Unmarshal(b, &m))

	i := m["stack"]
	stacks, ok := i.([]interface{})
	t.True(ok)
	t.NotNil(stacks)

	var goexitfound bool
	for i := range stacks {
		s := stacks[i]
		sm := s.(map[string]interface{})
		j := sm[pkgerrors.StackSourceFileName]
		k, ok := j.(string)
		t.True(ok)

		goexitfound = k == "goexit"
		if goexitfound {
			break
		}
	}

	return goexitfound
}

func (t *testError) TestPKGErrorStack() {
	e := errors.Errorf("showme")

	var bf bytes.Buffer
	l := logging.Setup(&bf, zerolog.DebugLevel, "json", false)

	l.Log().Error().Err(e).Msg("find")
	t.T().Log(bf.String())

	t.False(t.checkStack(bf.Bytes()))
}

func (t *testError) TestErrorStack() {
	e := NewError("showme").Call()

	var bf bytes.Buffer
	l := logging.Setup(&bf, zerolog.DebugLevel, "json", false)

	l.Log().Error().Err(e).Msg("find")
	t.T().Log(bf.String())

	t.False(t.checkStack(bf.Bytes()))
}

func TestError(t *testing.T) {
	suite.Run(t, new(testError))
}
