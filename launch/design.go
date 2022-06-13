package launch

import (
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"gopkg.in/yaml.v3"
)

var (
	DefaultNetworkBind     *netip.AddrPort
	DefaultStorageBase     string
	DefaultStorageDatabase *url.URL
)

func init() {
	{
		i := netip.MustParseAddrPort("0.0.0.0:4321")
		DefaultNetworkBind = &i
	}

	a, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	DefaultStorageBase = filepath.Join(a, "tmp", "data")
	DefaultStorageDatabase = &url.URL{
		Scheme: LeveldbURIScheme,
		Path:   filepath.Join(a, "tmp", "db"),
	}
}

type NodeDesign struct {
	Address    base.Address
	Privatekey base.Privatekey
	NetworkID  base.NetworkID
	Network    NodeNetworkDesign
	Storage    NodeStorageDesign
}

func (d *NodeDesign) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid NodeDesign")

	if err := util.CheckIsValid(nil, false, d.Address, d.Privatekey, d.NetworkID); err != nil {
		return e(err, "")
	}

	if err := d.Network.IsValid(nil); err != nil {
		return e(err, "")
	}

	if err := d.Storage.IsValid(nil); err != nil {
		return e(err, "")
	}

	return nil
}

type NodeDesignYAMLUnmarshaler struct {
	Address    string                         `yaml:"address"`
	Privatekey string                         `yaml:"privatekey"`
	NetworkID  string                         `yaml:"network_id"`
	Network    NodeNetworkDesignYAMLMarshaler `yaml:"network"`
	Storage    NodeStorageDesignYAMLMarshal   `yaml:"storage"`
}

type NodeDesignYAMLMarshaler struct {
	Address    base.Address      `yaml:"address"`
	Privatekey base.Privatekey   `yaml:"privatekey"`
	NetworkID  string            `yaml:"network_id"`
	Network    NodeNetworkDesign `yaml:"network"`
	Storage    NodeStorageDesign `yaml:"storage"`
}

func (d NodeDesign) MarshalYAML() (interface{}, error) {
	return NodeDesignYAMLMarshaler{
		Address:    d.Address,
		Privatekey: d.Privatekey,
		NetworkID:  string(d.NetworkID),
		Network:    d.Network,
		Storage:    d.Storage,
	}, nil
}

func (d *NodeDesign) DecodeYAML(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to unmarshal NodeDesign")

	var u NodeDesignYAMLUnmarshaler

	if err := yaml.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch address, err := base.DecodeAddress(u.Address, enc); {
	case err != nil:
		return e(err, "invalid address")
	default:
		d.Address = address
	}

	switch priv, err := base.DecodePrivatekeyFromString(u.Privatekey, enc); {
	case err != nil:
		return e(err, "invalid privatekey")
	default:
		d.Privatekey = priv
	}

	d.NetworkID = base.NetworkID([]byte(u.NetworkID))

	switch i, err := u.Network.Decode(enc); {
	case err != nil:
		return e(err, "")
	default:
		d.Network = i
	}

	switch i, err := u.Storage.Decode(enc); {
	case err != nil:
		return e(err, "")
	default:
		d.Storage = i
	}

	return nil
}

type NodeNetworkDesign struct {
	Bind        *netip.AddrPort `yaml:"bind"`
	Publish     string          `yaml:"publish"`
	TLSInsecure bool            `yaml:"tls_insecure"`
}

func (d *NodeNetworkDesign) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid NodeNetworkDesign")

	switch {
	case d.Bind == nil:
		d.Bind = DefaultNetworkBind
	case !d.Bind.IsValid():
		return e(nil, "invalid bind")
	}

	if len(d.Publish) > 0 {
		switch host, port, err := net.SplitHostPort(d.Publish); {
		case err != nil:
			return e(err, "invalid publish")
		case len(host) < 1:
			return e(err, "invalid publish; empty host")
		case len(port) < 1:
			return e(err, "invalid publish; empty port")
		}
	}

	return nil
}

type NodeNetworkDesignYAMLMarshaler struct {
	Bind        string `yaml:"bind,omitempty"`
	Publish     string `yaml:"publish"`
	TLSInsecure bool   `yaml:"tls_insecure"`
}

func (y *NodeNetworkDesignYAMLMarshaler) Decode(enc *jsonenc.Encoder) (d NodeNetworkDesign, _ error) {
	e := util.StringErrorFunc("failed to unmarshal NodeNetworkDesign")

	if s := strings.TrimSpace(y.Bind); len(s) > 0 {
		ip, err := netip.ParseAddrPort(y.Bind)
		if err != nil {
			return d, e(err, "invalid bind")
		}

		d.Bind = &ip
	}

	if s := strings.TrimSpace(y.Publish); len(s) > 0 {
		if _, _, err := net.SplitHostPort(s); err != nil {
			return d, e(err, "invalid publish")
		}
	}

	d.Publish = y.Publish
	d.TLSInsecure = y.TLSInsecure

	return d, nil
}

func (d *NodeNetworkDesign) DecodeYAML(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to unmarshal NodeNetworkDesign")

	var u NodeNetworkDesignYAMLMarshaler

	if err := yaml.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := u.Decode(enc); {
	case err != nil:
		return err
	default:
		*d = i

		return nil
	}
}

type NodeStorageDesign struct {
	Base     string   `yaml:"base"`
	Database *url.URL `yaml:"database"`
}

type NodeStorageDesignYAMLMarshal struct {
	Base     string `yaml:"base"`
	Database string `yaml:"database"`
}

func (d *NodeStorageDesign) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid NodeStorageDesign")

	if len(d.Base) < 1 {
		d.Base = DefaultStorageBase
	}

	switch {
	case d.Database == nil:
		d.Database = DefaultStorageDatabase
	case len(d.Database.Scheme) < 1:
		return e(nil, "wrong database; empty scheme")
	}

	return nil
}

func (y *NodeStorageDesignYAMLMarshal) Decode(enc *jsonenc.Encoder) (d NodeStorageDesign, _ error) {
	e := util.StringErrorFunc("failed to unmarshal NodeStorageDesign")

	d.Base = strings.TrimSpace(y.Base)

	if s := strings.TrimSpace(y.Database); len(s) > 0 {
		switch i, err := url.Parse(s); {
		case err != nil:
			return d, e(err, "invalid database")
		default:
			d.Database = i
		}
	}

	return d, nil
}

func (d NodeStorageDesign) MarshalYAML() (interface{}, error) {
	return NodeStorageDesignYAMLMarshal{
		Base:     d.Base,
		Database: d.Database.String(),
	}, nil
}

func (d *NodeStorageDesign) DecodeYAML(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to unmarshal NodeStorageDesign")

	var u NodeStorageDesignYAMLMarshal

	if err := yaml.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := u.Decode(enc); {
	case err != nil:
		return err
	default:
		*d = i

		return nil
	}
}

type GenesisOpertionsDesign []base.Fact

func (d *GenesisOpertionsDesign) DecodeYAML(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode GenesisOpertionsDesign")

	var u []interface{}

	if err := yaml.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	facts := make([]base.Fact, len(u))
	for i := range u {
		bj, err := util.MarshalJSON(u[i])
		if err != nil {
			return e(err, "")
		}

		if err := encoder.Decode(enc, bj, &facts[i]); err != nil {
			return e(err, "")
		}
	}

	*d = GenesisOpertionsDesign(facts)

	return nil
}
