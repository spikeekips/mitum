package util

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

const contextLoggerKey = "context-logger"

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

func NewModuleLogger(logger *zerolog.Logger, module string, f func(zctx zerolog.Context) zerolog.Context) *zerolog.Logger {
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

func NewModuleLoggerFromContext(ctx context.Context, module string, f func(zerolog.Context) zerolog.Context) *zerolog.Logger {
	logger := LoadContextValueLogger(ctx)
	if logger == nil {
		return &NopLogger
	}

	return NewModuleLogger(logger, module, f)
}

func UpdateModuleLoggerFromContext(ctx context.Context, module string, f func(zerolog.Context) zerolog.Context) (context.Context, *zerolog.Logger) {
	log := NewModuleLoggerFromContext(ctx, module, f)

	return SetContextValueLogger(ctx, log), log
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func ErrorWithStack(err error) error {
	if err == nil {
		return nil
	}

	if _, ok := err.(stackTracer); ok {
		return err
	}

	return errors.WithStack(err)
}

func ElapsedLogger(l zerolog.Logger, format string, a ...any) InErrorFunc[error] {
	started := time.Now()

	msg := fmt.Sprintf(format, a...)
	l.Debug().CallerSkipFrame(1).Msg("> " + msg)

	return func(err error) error {
		ll := l.With().Dur("elapsed", time.Since(started)).Logger()

		e := ll.Debug().CallerSkipFrame(1)

		if err != nil {
			e = ll.Error().Err(err)
		}

		e.Msg(fmt.Sprintf("< %s", msg))

		return err
	}
}

func ElapsedLogEvent(e *zerolog.Event, msg string) func() {
	started := time.Now()

	return func() {
		e.Func(func(e *zerolog.Event) {
			e.CallerSkipFrame(1).Stringer("elapsed", time.Since(started)).Msg(msg)
		})
	}
}
