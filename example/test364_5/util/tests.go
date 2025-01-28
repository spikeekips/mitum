//go:build test
// +build test

package util

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

var TestLogger = logging.TestLogging.Log()

type BaseSuite struct {
	suite.Suite
}

func (t *BaseSuite) Ctx() (context.Context, InErrorFunc[error]) {
	ctx, cancel := ContextWithCancelCause(context.Background())

	t.T().Cleanup(func() {
		cancel(nil)
	})

	return ctx, cancel
}

func (t *BaseSuite) Println(a string, b ...any) {
	fmt.Printf(a+"\n", b...)
}

func (t *BaseSuite) IfErrPanic(err error) {
	if err != nil {
		panic(err)
	}
}

func (t *BaseSuite) IfTruePanic(ok bool, a string, b ...interface{}) {
	if ok {
		panic(fmt.Sprintf(a, b...))
	}
}

func (t *BaseSuite) WorkerIfErrPanic(worker string, err error) error {
	if err != nil {
		err = fmt.Errorf("panic: worker=%q err=%w", worker, err)
	}

	t.IfErrPanic(err)

	return nil
}

func (t *BaseSuite) RunWithGoLeak(name string, f func()) {
	t.Run(name, func() {
		defer goleak.VerifyNone(t.T())

		f()
	})
}

func (t *BaseSuite) Marshal(i interface{}) string {
	return util.MustMarshalJSONString(i)
}

func (t *BaseSuite) MarshalIndent(i interface{}) string {
	return util.MustMarshalJSONIndentString(i)
}

func (t *BaseSuite) RandomError() error {
	return errors.Errorf(util.UUID().String())
}
