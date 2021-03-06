package cmds

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/alecthomas/kong"
	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/util/logging"
)

func init() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.LevelFieldName = "l"
	zerolog.TimestampFieldName = "t"
	zerolog.MessageFieldName = "m"

	zerolog.DisableSampling(true)
}

var LogVars = kong.Vars{
	"log":        "",
	"log_level":  "info",
	"log_format": "terminal",
	"log_color":  "false",
	"verbose":    "false",
}

type LogFlags struct {
	Verbose   bool      `help:"verbose log output (default: false)" default:"${verbose}"`
	LogColor  bool      `help:"show color log" default:"${log_color}"`
	LogLevel  LogLevel  `help:"log level {debug error warn info crit} (default: ${log_level})" default:"${log_level}"`
	LogFormat LogFormat `help:"log format {json terminal} (default: ${log_format})" default:"${log_format}"`
	LogFile   []string  `name:"log" help:"log file"`
}

type LogLevel zerolog.Level

func (ll LogLevel) Zero() zerolog.Level {
	return zerolog.Level(ll)
}

func (ll LogLevel) MarshalText() ([]byte, error) {
	return []byte(zerolog.Level(ll).String()), nil
}

func (ll *LogLevel) UnmarshalText(b []byte) error {
	lvl, err := zerolog.ParseLevel(string(b))
	if err != nil {
		return err
	}

	*ll = LogLevel(lvl)

	return nil
}

type LogFormat string

func (lf *LogFormat) UnmarshalText(b []byte) error {
	s := string(bytes.TrimSpace(bytes.ToLower(b)))
	switch s {
	case "json":
	case "terminal":
	default:
		return xerrors.Errorf("invalid log_format: %q", s)
	}

	*lf = LogFormat(s)

	return nil
}

func SetupLoggingFromFlags(flags *LogFlags) (logging.Logger, error) {
	var output io.Writer
	if len(flags.LogFile) < 1 {
		output = os.Stdout
	} else {
		outs := make([]io.Writer, len(flags.LogFile))
		for i, f := range flags.LogFile {
			if out, err := LogOutput(f); err != nil {
				return logging.Logger{}, err
			} else {
				outs[i] = out
			}
		}

		output = zerolog.MultiLevelWriter(outs...)
	}

	return SetupLogging(output, zerolog.Level(flags.LogLevel), string(flags.LogFormat), flags.Verbose, flags.LogColor), nil
}

func SetupLogging(out io.Writer, level zerolog.Level, format string, verbose, forceColor bool) logging.Logger {
	if format == "terminal" {
		var useColor bool
		if forceColor {
			useColor = true
		} else if isatty.IsTerminal(os.Stdout.Fd()) {
			useColor = true
		}

		out = zerolog.ConsoleWriter{
			Out:        out,
			TimeFormat: time.RFC3339Nano,
			NoColor:    !useColor,
		}
	}

	z := zerolog.New(out).With().Timestamp()

	if verbose {
		level = zerolog.TraceLevel
	}

	if level <= zerolog.DebugLevel {
		z = z.Caller().Stack()
	}

	l := z.Logger().Level(level)

	return logging.NewLogger(&l, verbose)
}

func LogOutput(f string) (io.Writer, error) {
	if out, err := os.OpenFile(f, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644); err != nil { // nolint
		return nil, err
	} else {
		return diode.NewWriter(
			out,
			1000,
			0,
			func(missed int) {
				fmt.Fprintf(os.Stderr, "zerolog: dropped %d log mesages\n", missed)
			},
		), nil
	}
}
