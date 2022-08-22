package launch

import (
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
)

type BaseFlags struct {
	LoggingFlags `embed:"" prefix:"log."`
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
	ci          quicstream.UDPConnInfo
	addr        string
	tlsinsecure bool
}

func (f *ConnInfoFlag) UnmarshalText(b []byte) error {
	e := util.StringErrorFunc("failed to parse ConnInfo flag")

	s := string(b)

	if err := network.IsValidAddr(s); err != nil {
		return e(err, "")
	}

	f.addr, f.tlsinsecure = network.ParseTLSInsecure(s)

	return nil
}

func (f ConnInfoFlag) String() string {
	return network.ConnInfoToString(f.addr, f.tlsinsecure)
}

func (f *ConnInfoFlag) ConnInfo() (quicstream.UDPConnInfo, error) {
	if f.ci.Addr() == nil {
		ci, err := quicstream.NewUDPConnInfoFromStringAddress(f.addr, f.tlsinsecure)
		if err != nil {
			return quicstream.UDPConnInfo{}, errors.Wrap(err, "failed to convert to quic ConnInfo")
		}

		f.ci = ci
	}

	return f.ci, nil
}

type HeightFlag struct {
	height *base.Height
}

func (f *HeightFlag) UnmarshalText(b []byte) error {
	e := util.StringErrorFunc("failed to parse address flag")

	if len(b) < 1 {
		f.height = &base.NilHeight

		return nil
	}

	height, err := base.NewHeightFromString(string(b))
	if err != nil {
		return e(err, "")
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
	e := util.StringErrorFunc("failed to design flag")

	s := string(b)

	u, err := url.Parse(s)
	if err != nil {
		return e(err, "")
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
			return e(nil, "empty key for consul")
		}

		f.scheme = scheme
		f.loc = u
	}

	return nil
}

type DesignURIProperties struct {
	HTTPSTLSInsecure bool `name:"https.tls_insecure" negatable:"" help:"https tls insecure" group:"design"`
}
