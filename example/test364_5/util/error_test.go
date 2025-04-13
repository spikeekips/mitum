package util

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testSError struct {
	suite.Suite
}

func (t *testSError) TestNew() {
	err := NewSError("showme")
	_ = any(err).(error)

	t.Equal("showme", err.Error())
	t.True(err == err) //lint:ignore SA4000 ...
	t.True(errors.Is(err, err))
}

func (t *testSError) TestEqual() {
	t.Run("equal", func() {
		err := NewSError("showme")

		t.True(err == err) //lint:ignore SA4000 ...
		t.True(errors.Is(err, err))
	})

	t.Run("not equal; same string", func() {
		err0 := NewSError("showme")
		err1 := NewSError("showme")

		t.False(err0 == err1)
		t.False(errors.Is(err0, err1))
	})

	t.Run("not equal; WithStack", func() {
		err0 := NewSError("showme")
		err1 := errors.WithStack(err0)

		t.False(err0 == err1)
		t.False(errors.Is(err0, err1))
		t.True(errors.Is(err1, err0))
	})

	t.Run("not equal; WithMessage", func() {
		err0 := NewSError("showme")
		err1 := errors.WithMessage(err0, "findme")

		t.False(err0 == err1)
		t.False(errors.Is(err0, err1))
		t.True(errors.Is(err1, err0))
	})

	t.Run("not equal; Wrap", func() {
		err0 := NewSError("showme")
		err1 := errors.Wrap(err0, "findme")

		t.False(err0 == err1)
		t.False(errors.Is(err0, err1))
		t.True(errors.Is(err1, err0))
	})
}

func (t *testSError) TestStack() {
	t.Run("bare", func() {
		err := NewSError("showme")
		t.T().Logf("%+v", err)
	})

	t.Run("WithStack", func() {
		err := errors.WithStack(NewSError("showme"))
		t.T().Logf("%+v", err)
	})

	t.Run("Wrap", func() {
		err := errors.Wrap(NewSError("showme"), "findme")
		t.T().Logf("%+v", err)
	})

	t.Run("WithMessage", func() {
		err := errors.WithMessage(NewSError("showme"), "findme")
		t.T().Logf("%+v", err)
	})
}

func TestSError(t *testing.T) {
	suite.Run(t, new(testSError))
}

func BenchmarkErrorIsIDError(b *testing.B) {
	err := util.NewIDError("showme")

	for b.Loop() {
		errors.Is(err, err)
	}
}

func BenchmarkErrorIsSError(b *testing.B) {
	err := NewSError("showme")

	for b.Loop() {
		errors.Is(err, err)
	}
}

func BenchmarkEqualSError(b *testing.B) {
	err := NewSError("showme")

	for b.Loop() {
		_ = err == err //lint:ignore SA4000 ...
	}
}
