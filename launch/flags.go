package launch

import (
	"bytes"
	"io"
	"net/url"
	"os"
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

	switch ci, err := quicstream.NewConnInfoFromStringAddr(f.addr, f.tlsinsecure); {
	case err != nil:
		return err
	default:
		f.ci = ci
	}

	return nil
}

func (f ConnInfoFlag) String() string {
	return network.ConnInfoToString(f.addr, f.tlsinsecure)
}

func (f ConnInfoFlag) MarshalText() ([]byte, error) {
	return []byte(f.String()), nil
}

func (f *ConnInfoFlag) ConnInfo() quicstream.ConnInfo {
	return f.ci
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
	URI           DesignURIFlag `name:"design" help:"design uri; 'file:///config.yml', 'https://a.b.c.d/config.yml'" group:"design" default:"${design_uri}"`
	URLProperties `embed:"" prefix:"design." group:"design"`
	//revive:enable:line-length-limit
}

func (f DesignFlag) Scheme() string {
	return f.URI.scheme
}

func (f DesignFlag) URL() *url.URL {
	return f.URI.loc
}

func (f DesignFlag) Properties() URLProperties {
	return f.URLProperties
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

type URLProperties struct {
	HTTPSTLSInsecure bool `name:"https.tls_insecure" negatable:"" help:"https tls insecure"`
}

type DevFlags struct { //nolint:govet //...
	//revive:disable:line-length-limit
	//revive:disable:struct-tag
	AllowRiskyThreshold bool          `name:"allow-risky-threshold" help:"allow risky threshold under threshold, ${safe_threshold}" group:"dev"`
	DelaySyncer         time.Duration `name:"delay-syncer" help:"initial delay when sync one block" group:"dev"`
	AllowConsensus      bool          `name:"allow-consensus" help:"allow to enter consensus" group:"dev"`
	ExitBroken          bool          `name:"exit-broken" help:"exit broken state" group:"dev"`
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

	if len(n) > 0 && n[0] != "" {
		switch i, err := strconv.ParseUint(n[0], 10, 64); {
		case err != nil:
			return e.Wrap(err)
		default:
			f.from = &i
		}
	}

	if len(n) > 1 && n[1] != "" {
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

type SecretFlag struct {
	s    string
	body []byte
}

func (f SecretFlag) String() string {
	return f.s
}

func (f SecretFlag) Body() []byte {
	return f.body
}

func (f *SecretFlag) UnmarshalText(b []byte) error {
	e := util.StringError("secret flag")

	f.s = strings.TrimSpace(string(b))

	switch {
	case len(f.s) < 1:
		return nil
	case f.s == "-": // NOTE stdin
		switch i, err := io.ReadAll(os.Stdin); {
		case err != nil:
			return e.Wrap(err)
		default:
			j, _ := strings.CutSuffix(string(i), "\n")
			f.body = []byte(j)

			return nil
		}
	}

	var u *url.URL

	switch i, err := url.Parse(f.s); {
	case err != nil:
		return e.Wrap(err)
	case len(i.Scheme) < 1 && len(i.Path) < 1:
		return e.Errorf("empty")
	default:
		u = i
	}

	switch u.Scheme {
	case "file", "":
		switch fi, err := os.Stat(u.Path); {
		case err != nil:
			return e.Wrap(err)
		case fi.Mode()&(1<<2) != 0:
			return e.Errorf("too open, %q", u.Path)
		}

		switch i, err := os.ReadFile(u.Path); {
		case err != nil:
			return e.Wrap(err)
		default:
			f.body = i

			return nil
		}
	case "vault":
		switch i, err := getFromVault(u.String()); {
		case err != nil:
			return e.Wrap(err)
		default:
			f.body = i

			return nil
		}
	default:
		return e.Errorf("unsupported secret url, %q", f.s)
	}
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

type PrivatekeyArgument struct {
	//revive:disable:line-length-limit
	Flag SecretFlag `arg:"" name:"privatekey" help:"privatekey uri; './privatekey.yml', 'file:///privatekey.yml', 'vault://a.b.c.d/privatekey'" placeholder:"URL"`
	//revive:enable:line-length-limit
}

type PrivatekeyFlags struct {
	//revive:disable:line-length-limit
	Flag SecretFlag `name:"privatekey" help:"privatekey uri; './privatekey.yml', 'file:///privatekey.yml', 'vault://a.b.c.d/privatekey'" placeholder:"URL"`
	//revive:enable:line-length-limit
}

type ACLFlags struct {
	//revive:disable:line-length-limit
	Flag SecretFlag `name:"acl" help:"acl uri; './acl.yml', 'file:///acl.yml', 'vault://a.b.c.d/acl'" placeholder:"URL"`
	//revive:enable:line-length-limit
}

func LoadInputFlag(s string, isfile bool) ([]byte, error) { // revive:disable-line:flag-parameter
	if !isfile {
		return []byte(s), nil
	}

	if s == "-" { // NOTE stdin
		buf := bytes.NewBuffer(nil)

		if _, err := io.Copy(buf, os.Stdin); err != nil {
			return nil, errors.WithStack(err)
		}

		return buf.Bytes(), nil
	}

	switch i, err := os.ReadFile(s); {
	case err != nil:
		return nil, errors.WithStack(err)
	default:
		return i, nil
	}
}
