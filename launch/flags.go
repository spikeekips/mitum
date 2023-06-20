package launch

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type BaseFlags struct {
	LoggingFlags `embed:"" prefix:"log."`
}

type AddressFlag struct {
	address base.Address
}

func (f *AddressFlag) UnmarshalText(b []byte) error {
	e := util.StringError("unmarshal address flag")

	address, err := base.ParseStringAddress(string(b))
	if err != nil {
		return e.Wrap(err)
	}

	if err := address.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	f.address = address

	return nil
}

func (f AddressFlag) Address() base.Address {
	return f.address
}

type ConnInfoFlag struct {
	ci          quicstream.ConnInfo
	addr        string
	tlsinsecure bool
}

func (f *ConnInfoFlag) UnmarshalText(b []byte) error {
	e := util.StringError("unmarshal ConnInfo flag")

	s := string(b)

	if err := network.IsValidAddr(s); err != nil {
		return e.Wrap(err)
	}

	f.addr, f.tlsinsecure = network.ParseTLSInsecure(s)

	return nil
}

func (f ConnInfoFlag) String() string {
	return network.ConnInfoToString(f.addr, f.tlsinsecure)
}

func (f ConnInfoFlag) MarshalText() ([]byte, error) {
	return []byte(f.String()), nil
}

func (f *ConnInfoFlag) ConnInfo() (quicstream.ConnInfo, error) {
	if f.ci.Addr() == nil {
		ci, err := quicstream.NewConnInfoFromStringAddr(f.addr, f.tlsinsecure)
		if err != nil {
			return quicstream.ConnInfo{}, errors.Wrap(err, "convert to quic ConnInfo")
		}

		f.ci = ci
	}

	return f.ci, nil
}

type HeightFlag struct {
	height *base.Height
}

func (f *HeightFlag) UnmarshalText(b []byte) error {
	e := util.StringError("unmarshal address flag")

	if len(b) < 1 {
		f.height = &base.NilHeight

		return nil
	}

	height, err := base.ParseHeightString(string(b))
	if err != nil {
		return e.Wrap(err)
	}

	f.height = &height

	return nil
}

func (f *HeightFlag) IsSet() bool {
	if f.height == nil {
		return false
	}

	return *f.height >= base.NilHeight
}

func (f *HeightFlag) Height() base.Height {
	if f.height == nil {
		return base.NilHeight - 1
	}

	return *f.height
}

var DefaultDesignURI = "./config.yml"

// DesignFlag is the flag for loading design from various locations. Only one
// location is allowed. For example,
// - "--design=./no0sas.yml", or
// - "--design=https://a.b.c.d/no0sas.yml", or
// - "--design=consul://a.b.c.d:8500/no0sas/design", or
// - "--design=consul:///no0sas/design": If address not set, the environment variables of
// consul will be used.
type DesignFlag struct {
	//revive:disable:line-length-limit
	URI                 DesignURIFlag `name:"design" help:"design uri; 'file:///config.yml', 'https://a.b.c.d/config.yml'" group:"design" default:"${design_uri}"`
	DesignURIProperties `embed:"" prefix:"design."`
	//revive:enable:line-length-limit
}

func (f DesignFlag) Scheme() string {
	return f.URI.scheme
}

func (f DesignFlag) URL() *url.URL {
	return f.URI.loc
}

func (f DesignFlag) Properties() DesignURIProperties {
	return f.DesignURIProperties
}

type DesignURIFlag struct {
	loc    *url.URL
	scheme string
}

func (f *DesignURIFlag) UnmarshalText(b []byte) error {
	e := util.StringError("unmarshal design flag")

	s := string(b)

	u, err := url.Parse(s)
	if err != nil {
		return e.Wrap(err)
	}

	switch scheme := u.Scheme; strings.ToLower(scheme) {
	case "", "file": // NOTE file
		f.scheme = "file"
		f.loc = u
	case "http", "https":
		f.scheme = scheme
		f.loc = u
	case "consul":
		if len(u.Path) < 1 {
			return e.Errorf("empty key for consul")
		}

		f.scheme = scheme
		f.loc = u
	}

	return nil
}

type DesignURIProperties struct {
	HTTPSTLSInsecure bool `name:"https.tls_insecure" negatable:"" help:"https tls insecure" group:"design"`
}

type DevFlags struct { //nolint:govet //...
	//revive:disable:line-length-limit
	//revive:disable:struct-tag
	AllowRiskyThreshold bool          `name:"allow-risky-threshold" help:"allow risky threshold under threshold, ${safe_threshold}" group:"dev"`
	DelaySyncer         time.Duration `name:"delay-syncer" help:"initial delay when sync one block" group:"dev"`
	AllowConsensus      bool          `name:"allow-consensus" help:"allow to enter consensus" group:"dev"`
	//revive:enable:struct-tag
	//revive:enable:line-length-limit
}

type RangeFlag struct {
	from *uint64
	to   *uint64
}

func (f *RangeFlag) UnmarshalText(b []byte) error {
	e := util.StringError("unmarshal range flag, %q", string(b))

	n := strings.SplitN(strings.TrimSpace(string(b)), "-", 2)

	switch {
	case len(n) < 1:
		return nil
	case len(n) > 2:
		return e.Errorf("wrong format")
	case len(n) < 2 && len(n[0]) < 1:
		return nil
	}

	if len(n) > 0 && len(n[0]) > 0 {
		switch i, err := strconv.ParseUint(n[0], 10, 64); {
		case err != nil:
			return e.Wrap(err)
		default:
			f.from = &i
		}
	}

	if len(n) > 1 && len(n[1]) > 0 {
		switch i, err := strconv.ParseUint(n[1], 10, 64); {
		case err != nil:
			return e.Wrap(err)
		default:
			f.to = &i
		}
	}

	return nil
}

func (f *RangeFlag) From() *uint64 {
	return f.from
}

func (f *RangeFlag) To() *uint64 {
	return f.to
}

func DecodePrivatekey(s string, enc encoder.Encoder) (base.Privatekey, error) {
	switch key, err := base.DecodePrivatekeyFromString(s, enc); {
	case err != nil:
		return nil, err
	default:
		if err := key.IsValid(nil); err != nil {
			return nil, err
		}

		return key, nil
	}
}
