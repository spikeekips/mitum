package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/stretchr/testify/suite"
)

type testError struct {
	suite.Suite
}

func (t *testError) TestFuncCaller() {
	f := FuncCaller(2)
	t.Equal("(*testError).TestFuncCaller:23", fmt.Sprintf("%n:%d", f, f))

	e := NewIDError("showme")
	t.Contains(e.id, "(*testError).TestFuncCaller")
}

func (t *testError) TestIs() {
	e := NewIDError("showme")

	e0 := e.WithStack()
	t.Implements((*(interface{ Error() string }))(nil), e0)

	t.Equal("showme", e0.Error())

	t.ErrorIs(e0, e0)
	t.False(errors.Is(e0, nil))
	t.False(errors.Is(e0, NewIDError("showme").WithStack()))
	t.False(errors.Is(e0, NewIDError("findme").WithStack()))
	t.ErrorIs(e0, e0.Errorf("showme"))
}

func (t *testError) TestAs() {
	e := NewIDError("showme")
	e0 := e.WithStack()

	var e1 *IDError
	t.ErrorAs(e0, &e1)

	t.ErrorIs(e0, e1)
	t.ErrorIs(e1, e0)
	t.Equal(e0.Error(), e1.Error())
}

func (t *testError) TestWrap() {
	e := NewIDError("showme")
	e0 := e.WithStack()

	t.Run("wrap nil", func() {
		t.Nil(e0.Wrap(nil))
	})

	t.Run("wrap external error", func() {
		pe := &os.PathError{Op: "not found", Path: "/tmp", Err: errors.Errorf("???")}
		e1 := e0.Wrap(pe)

		t.False(errors.Is(e1, NewIDError("showme").WithStack()))
		t.ErrorIs(e1, errors.WithMessage(e1, "showme"))
		t.ErrorIs(e1, pe)

		var npe *os.PathError
		t.ErrorAs(e1, &npe)

		t.ErrorIs(pe, npe)
		t.Equal(pe.Error(), npe.Error())

		var e2 *IDError
		t.ErrorAs(e0, &e2)
		t.ErrorAs(e1, &e2)

		t.ErrorIs(e0, e2)
		t.ErrorIs(e1, e2)
	})
}

func (t *testError) TestWrapAgain() {
	ea := NewIDError("showme")
	eb := NewIDError("findme")
	e0 := ea.WithStack()

	e1 := e0.Wrap(eb.WithStack())

	t.ErrorIs(e0, e1)
	t.ErrorIs(e1, e0)
}

func (t *testError) TestWrapf() {
	e := NewIDError("showme")
	e0 := e.WithStack()

	t.Run("wrap nil", func() {
		t.Nil(e0.WithMessage(nil, "findme"))
	})

	t.Run("wrap external error", func() {
		pe := &os.PathError{Op: "not found", Path: "/tmp", Err: errors.Errorf("???")}
		e1 := e0.WithMessage(pe, "find me: %d", 3)

		t.ErrorIs(e0, e1)
		t.False(errors.Is(e1, NewIDError("showme").WithStack()))
		t.ErrorIs(e1, errors.WithMessage(e1, "showme"))
		t.ErrorIs(e1, pe)

		var e2 *IDError
		t.ErrorAs(e0, &e2)
		t.ErrorAs(e1, &e2)

		t.ErrorIs(e0, e2)
		t.ErrorIs(e1, e2)

		var npe *os.PathError
		t.ErrorAs(e1, &npe)

		t.ErrorIs(pe, npe)
		t.Equal(pe.Error(), npe.Error())
	})
}

func (t *testError) TestErrorf() {
	e := NewIDError("showme")
	e0 := e.WithStack()

	e1 := e0.Errorf("error: %d", 33)

	var e2 *IDError
	t.ErrorAs(e0, &e2)
	t.ErrorAs(e1, &e2)

	t.ErrorIs(e0, e1)
	t.ErrorIs(e1, e1)
}

func (t *testError) printStack(err error) (string, bool) {
	i, ok := err.(stackTracer)
	if !ok {
		return "<no StackTrace()>", false
	}

	buf := bytes.NewBuffer(nil)

	st := i.StackTrace()
	for i, f := range st {
		_, _ = fmt.Fprintf(buf, "%+s:%d", f, f)
		if i < len(st)-1 {
			_, _ = fmt.Fprintln(buf)
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
	e := NewIDError("showme")
	e0 := e.WithStack()

	t.T().Logf("e0,  v: %v", e0)
	t.T().Logf("e0, +v: %+v", e0)

	e1 := e0.WithMessage(&os.PathError{Op: "op", Path: "/tmp", Err: errors.Errorf("path error")}, "findme")
	t.T().Logf("e1,  v: %v", e1)
	t.T().Logf("e1, +v: %+v", e1)

	e2 := e0.Wrap(&os.PathError{Op: "e2", Path: "/tmp/e2", Err: errors.Errorf("path error")})
	t.T().Logf("e2,  v: %v", e2)
	t.T().Logf("e2, +v: %+v", e2)
}

func (t *testError) TestPrintStacks() {
	e := NewIDError("showme")
	e0 := e.WithStack()

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

	var end bool
	for i := range stacks {
		s := stacks[i]
		sm := s.(map[string]interface{})
		j := sm[pkgerrors.StackSourceFileName]
		if j == nil {
			continue
		}

		k, ok := j.(string)
		t.True(ok)

		end = strings.Contains(k, "error_test.go")
		if end {
			break
		}
	}

	return end
}

func (t *testError) setupLogging(out io.Writer) zerolog.Logger {
	zerolog.ErrorStackMarshaler = ZerologMarshalStack

	z := zerolog.New(out).With().Timestamp().Caller().Stack()

	return z.Logger().Level(zerolog.DebugLevel)
}

func (t *testError) TestPKGErrorStack() {
	e := errors.Errorf("showme")

	var bf bytes.Buffer
	l := t.setupLogging(&bf)

	l.Error().Err(e).Msg("find")
	t.T().Log(bf.String())

	t.True(t.checkStack(bf.Bytes()))
}

func (t *testError) TestErrorStack() {
	e := NewIDError("showme").WithStack()

	var bf bytes.Buffer
	l := t.setupLogging(&bf)

	l.Error().Err(e).Msg("killme")
	t.T().Log(bf.String())

	t.True(t.checkStack(bf.Bytes()))
}

func (t *testError) TestStringError() {
	e := StringError("showme")

	t.Run("WithMessage; nil error", func() {
		t.Nil(e.WithMessage(nil, "hehehe"))
	})

	t.Run("Wrap; nil error", func() {
		t.Nil(e.Wrap(nil))
	})

	t.Run("msg", func() {
		ee := e.Errorf("hehehe")

		t.T().Logf("nil error:\n%s", t.printStacks(ee))
	})

	t.Run("WithMessage error", func() {
		ee := e.WithMessage(errors.Errorf("hohoho"), "hehehe")

		t.T().Logf("with error:\n%s", t.printStacks(ee))
	})
}

func TestError(t *testing.T) {
	suite.Run(t, new(testError))
}
