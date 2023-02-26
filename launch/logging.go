package launch

import (
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

func init() { //nolint:gochecknoinits //...
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = util.ZerologMarshalStack //nolint:reassign //...
}

type LoggingFlags struct {
	//revive:disable:line-length-limit
	//revive:disable:struct-tag
	Format     string       `enum:"json, terminal" default:"${log_format}" help:"log format: {${enum}}" group:"logging"`
	Out        []LogOutFlag `name:"out" default:"${log_out}" help:"log output file: {stdout, stderr, <file>}" group:"logging"`
	Level      LogLevelFlag `name:"level" default:"${log_level}" help:"log level: {trace, debug, info, warn, error}" group:"logging"`
	ForceColor bool         `name:"force-color" default:"${log_force_color}" negatable:"" help:"log force color" group:"logging"`
	//revive:enable:struct-tag
	//revive:enable:line-length-limit
}

type LogLevelFlag struct {
	level zerolog.Level
}

func (f *LogLevelFlag) UnmarshalText(b []byte) error {
	l, err := zerolog.ParseLevel(string(b))
	if err != nil {
		return errors.WithStack(err)
	}

	f.level = l

	return nil
}

func (f LogLevelFlag) Level() zerolog.Level {
	return f.level
}

func (f LogLevelFlag) String() string {
	return f.level.String()
}

type LogOutFlag string

func (f LogOutFlag) File() (io.Writer, error) {
	switch f {
	case "stdout":
		return os.Stdout, nil
	case "stderr":
		return os.Stderr, nil
	default:
		return logging.Output(string(f))
	}
}

func SetupLoggingFromFlags(flag LoggingFlags) (*logging.Logging, error) {
	fs, _ := util.RemoveDuplicatedSlice(flag.Out, func(f LogOutFlag) (string, error) { return string(f), nil })

	var logout io.Writer

	if len(fs) > 0 {
		ws := make([]io.Writer, len(fs))

		for i := range fs {
			w, err := fs[i].File()
			if err != nil {
				return nil, err
			}

			ws[i] = w
		}

		logout = zerolog.MultiLevelWriter(ws...)
	}

	return logging.Setup(
		logout,
		flag.Level.Level(),
		flag.Format,
		flag.ForceColor,
	), nil
}
