package util

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

const contextLoggerKey ContextKey = "context-logger"

var NopLogger = zerolog.Nop()

func SetContextValueLogger(ctx context.Context, v *zerolog.Logger) context.Context {
	return context.WithValue(ctx, contextLoggerKey, v)
}

func LoadContextValueLogger(ctx context.Context) *zerolog.Logger {
	switch t := ctx.Value(contextLoggerKey).(type) {
	case *zerolog.Logger:
		return t
	default:
		return &NopLogger
	}
}

func NewModuleLogger(
	logger *zerolog.Logger,
	module string,
	f func(zctx zerolog.Context) zerolog.Context,
) *zerolog.Logger {
	if f == nil {
		f = func(zctx zerolog.Context) zerolog.Context { return zctx }
	}

	zctx := logger.With()

	zctx = zctx.Str("module", module)
	if f != nil {
		zctx = f(zctx)
	}

	l := zctx.Logger()

	return &l
}

func NewModuleLoggerFromContext(
	ctx context.Context,
	module string,
	f func(zerolog.Context) zerolog.Context,
) *zerolog.Logger {
	logger := LoadContextValueLogger(ctx)
	if logger == nil {
		return &NopLogger
	}

	return NewModuleLogger(logger, module, f)
}

func UpdateModuleLoggerFromContext(ctx context.Context, module string, f func(zerolog.Context) zerolog.Context) (
	context.Context, *zerolog.Logger,
) {
	log := NewModuleLoggerFromContext(ctx, module, f)

	return SetContextValueLogger(ctx, log), log
}

func ElapsedLogEvent(f func() *zerolog.Event) func() *zerolog.Event {
	started := time.Now()

	return func() *zerolog.Event {
		return f().
			Stringer("elapsed", time.Since(started))
	}
}

func ElapsedLogMsg(f func() *zerolog.Event, format string, a ...any) func() {
	l := ElapsedLogEvent(f)
	msg := fmt.Sprintf(format, a...)

	return func() {
		l().CallerSkipFrame(1).Msg(msg)
	}
}

func ElapsedLogError(f func() *zerolog.Event) InFunc[error, *zerolog.Event] {
	l := ElapsedLogEvent(f)

	return func(err error) *zerolog.Event {
		return l().Err(err)
	}
}

func AtomicElapsedLogEvent(e *zerolog.Event, v *atomic.Value) *zerolog.Event {
	switch i := v.Load(); i {
	case nil:
		return e
	default:
		t, ok := i.(time.Time)
		if !ok {
			return e
		}

		return e.Stringer("elapsed", time.Since(t))
	}
}
