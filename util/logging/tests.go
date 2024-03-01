//go:build test
// +build test

package logging

import (
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/stretchr/testify/suite"
)

var (
	TestLogging    *Logging
	TestNilLogging *Logging
)

func init() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	TestLogging = Setup(os.Stderr, zerolog.TraceLevel, "", false)
	TestNilLogging = NewLogging(nil).SetLogger(zerolog.Nop())
}

type BaseTestSuite struct {
	suite.Suite
	loglock sync.Mutex
	logf    func(string, ...any)
}

func (t *BaseTestSuite) SetupSuite() {
	t.logf = t.T().Logf
}

func (t *BaseTestSuite) LockedLog(a string, b ...any) {
	t.loglock.Lock()
	defer t.loglock.Unlock()

	t.logf(a, b...)
}
