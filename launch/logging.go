package launch

import (
	"io"
	"os"
	"path/filepath"
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

// revive:disable:line-length-limit
type LoggingFlags struct {
	//revive:disable:struct-tag
	Out        LogOutFlag   `name:"out" default:"${log_out}" help:"log output file: {stdout, stderr, <file>}" group:"logging"`
	Format     string       `enum:"json, terminal" default:"${log_format}" help:"log format: {${enum}}" group:"logging"`
	Level      LogLevelFlag `name:"level" default:"${log_level}" help:"log level: {trace, debug, info, warn, error}" group:"logging"`
	ForceColor bool         `name:"force-color" default:"${log_force_color}" negatable:"" help:"log force color" group:"logging"`
	//revive:enable:struct-tag
}

// revive:enable:line-length-limit

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
		w, err := os.OpenFile(filepath.Clean(string(f)), os.O_WRONLY|os.O_APPEND|os.O_SYNC|os.O_CREATE, 0o600)
		if err != nil {
			return nil, errors.Wrap(err, "failed to open log file")
		}

		return w, nil
	}
}

func SetupLoggingFromFlags(flag LoggingFlags) (*logging.Logging, error) {
	logout, err := flag.Out.File()
	if err != nil {
		return nil, err
	}

	return logging.Setup(
		logout,
		flag.Level.Level(),
		flag.Format,
		flag.ForceColor,
	), nil
}
