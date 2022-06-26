package launch

import (
	"io"
	"net"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util"
)

// revive:disable:line-length-limit
type Logging struct {
	Out        LogOut   `name:"out" default:"stderr" help:"log output file: {stdout, stderr, <file>}" group:"logging"`
	Format     string   `enum:"json, terminal" default:"terminal" help:"log format: {${enum}}" group:"logging"`
	Level      LogLevel `name:"level" default:"debug" help:"log level: {trace, debug, info, warn, error}" group:"logging"`
	ForceColor bool     `name:"force-color" negatable:"" help:"log force color" group:"logging"`
}

// revive:enable:line-length-limit

type LogLevel struct {
	level zerolog.Level
}

func (f *LogLevel) UnmarshalText(b []byte) error {
	l, err := zerolog.ParseLevel(string(b))
	if err != nil {
		return errors.WithStack(err)
	}

	f.level = l

	return nil
}

func (f LogLevel) Level() zerolog.Level {
	return f.level
}

func (f LogLevel) String() string {
	return f.level.String()
}

type LogOut string

func (f LogOut) File() (io.Writer, error) {
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

type AddressFlag struct {
	address base.Address
}

func (f *AddressFlag) UnmarshalText(b []byte) error {
	e := util.StringErrorFunc("failed to parse address flag")

	address, err := base.ParseStringAddress(string(b))
	if err != nil {
		return e(err, "")
	}

	if err := address.IsValid(nil); err != nil {
		return e(err, "")
	}

	f.address = address

	return nil
}

func (f AddressFlag) Address() base.Address {
	return f.address
}

type ConnInfoFlag struct {
	ci          quictransport.ConnInfo
	addr        string
	tlsinsecure bool
}

func (f *ConnInfoFlag) UnmarshalText(b []byte) error {
	e := util.StringErrorFunc("failed to parse ConnInfo flag")

	s := string(b)

	if _, _, err := net.SplitHostPort(s); err != nil {
		return e(err, "")
	}

	f.addr, f.tlsinsecure = network.ParseInsecure(s)

	return nil
}

func (f ConnInfoFlag) String() string {
	return network.ConnInfoToString(f.addr, f.tlsinsecure)
}

func (f *ConnInfoFlag) ConnInfo() (quictransport.ConnInfo, error) {
	if f.ci == nil {
		ci, err := quictransport.NewBaseConnInfoFromStringAddress(f.addr, f.tlsinsecure)
		if err != nil {
			return nil, errors.Wrap(err, "failed to convert to quic ConnInfo")
		}

		f.ci = ci
	}

	return f.ci, nil
}
