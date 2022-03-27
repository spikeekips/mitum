package logging

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
)

func Setup(
	output io.Writer,
	level zerolog.Level,
	format string,
	forceColor bool,
) *Logging {
	o := output
	if format == "terminal" {
		var useColor bool
		if forceColor {
			useColor = true
		} else if isatty.IsTerminal(os.Stdout.Fd()) {
			useColor = true
		}

		o = zerolog.ConsoleWriter{
			Out:        o,
			TimeFormat: time.RFC3339Nano,
			NoColor:    !useColor,
		}
	}

	z := zerolog.New(o).With().Timestamp()

	if level <= zerolog.DebugLevel {
		z = z.Caller().Stack()
	}

	return NewLogging(nil).SetLogger(z.Logger().Level(level))
}

func Output(f string) (io.Writer, error) {
	out, err := os.OpenFile(filepath.Clean(f), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644) // nolint:gosec
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file, %q", f)
	}

	return diode.NewWriter(out, 1000, 0, nil), nil
}

func Outputs(files []string) (io.Writer, error) {
	if len(files) < 1 {
		return nil, errors.Errorf("empty log files")
	}

	ws := make([]io.Writer, len(files))
	for i, f := range files {
		out, err := Output(f)
		if err != nil {
			return zerolog.Logger{}, errors.Wrap(err, "failed Outputs")
		}

		ws[i] = out
	}

	return zerolog.MultiLevelWriter(ws...), nil
}

type ZerologSTDLoggingWriter struct {
	f func() *zerolog.Event
}

func NewZerologSTDLoggingWriter(f func() *zerolog.Event) ZerologSTDLoggingWriter {
	return ZerologSTDLoggingWriter{f: f}
}

func (w ZerologSTDLoggingWriter) Write(b []byte) (int, error) {
	if w.f != nil {
		w.f().Msg(string(bytes.TrimRight(b, "\n")))
	}

	return len(b), nil
}

type ZerologObjectMarshaler func(*zerolog.Event)

func (z ZerologObjectMarshaler) MarshalZerologObject(e *zerolog.Event) {
	z(e)
}
