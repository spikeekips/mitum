package util

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testContext struct {
	BaseSuite
}

func (t *testContext) TestContextDone() {
	t.Run("cancel; with error", func() {
		cancel, done := NewContextDone(context.Background())

		err := errors.New("findme")
		t.ErrorContains(cancel(err), "findme")

		var wg sync.WaitGroup

		for range 333 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				_ = cancel(t.RandomError())

				err := done()
				if !strings.Contains(err.Error(), "findme") {
					t.Panic(err)
				}
			}()
		}

		wg.Wait()
	})

	t.Run("cancel; with nil error", func() {
		cancel, done := NewContextDone(context.Background())

		t.NoError(cancel(nil))

		var wg sync.WaitGroup

		for range 333 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				_ = cancel(t.RandomError())

				err := done()
				t.IfErrPanic(err)
			}()
		}

		wg.Wait()
	})
}

func TestContext(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testContext))
}
