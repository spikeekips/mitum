package launch

import (
	"net"
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
	DefaultNetworkBind     = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4321} //nolint:gomnd //...
	defaultStorageBase     string
	DefaultStorageBase     string
	DefaultStorageDatabase *url.URL
)

func init() {
	{
		a, err := os.Getwd()
		if err != nil {
			panic(err)
		}

		defaultStorageBase = filepath.Join(a, "tmp", "mitum")

		DefaultStorageBase = defaultStorageBase
		DefaultStorageDatabase = &url.URL{
			Scheme: LeveldbURIScheme,
			Path:   filepath.Join(defaultStorageBase, "perm"),
		}
	}
}

type NodeDesign struct {
	Address    base.Address
	Privatekey base.Privatekey
	Storage    NodeStorageDesign
	Network    NodeNetworkDesign
	NetworkID  base.NetworkID
}

func NodeDesignFromFile(f string, enc *jsonenc.Encoder) (d NodeDesign, _ []byte, _ error) {
	e := util.StringErrorFunc("failed to load NodeDesign from file")

	b, err := os.ReadFile(filepath.Clean(f))
	if err != nil {
		return d, nil, e(err, "")
	}

	if err := d.DecodeYAML(b, enc); err != nil {
		return d, b, e(err, "")
	}

	if err := d.IsValid(nil); err != nil {
		return d, b, e(err, "")
	}

	return d, b, nil
}

func (d *NodeDesign) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NodeDesign")

	if err := util.CheckIsValid(nil, false, d.Address, d.Privatekey, d.NetworkID); err != nil {
		return e.Wrap(err)
	}

	if err := d.Network.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := d.Storage.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := d.Storage.Patch(d.Address); err != nil {
		return e.Wrap(err)
	}

	return nil
}

type NodeDesignYAMLUnmarshaler struct {
	Storage    NodeStorageDesignYAMLMarshal   `yaml:"storage"`
	Address    string                         `yaml:"address"`
	Privatekey string                         `yaml:"privatekey"`
	NetworkID  string                         `yaml:"network_id"`
	Network    NodeNetworkDesignYAMLMarshaler `yaml:"network"`
}

type NodeDesignYAMLMarshaler struct {
	Address    base.Address      `yaml:"address"`
	Privatekey base.Privatekey   `yaml:"privatekey"`
	Storage    NodeStorageDesign `yaml:"storage"`
	NetworkID  string            `yaml:"network_id"`
	Network    NodeNetworkDesign `yaml:"network"`
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
	Bind          *net.UDPAddr `yaml:"bind"`
	publish       *net.UDPAddr
	PublishString string `yaml:"publish"` //nolint:tagliatelle //...
	TLSInsecure   bool   `yaml:"tls_insecure"`
}

func (d *NodeNetworkDesign) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NodeNetworkDesign")

	switch {
	case d.Bind == nil:
		d.Bind = DefaultNetworkBind
	case d.Bind.Port < 1:
		return e.Errorf("invalid bind port")
	}

	switch {
	case len(d.PublishString) < 1:
		d.PublishString = DefaultNetworkBind.String()
		d.publish = DefaultNetworkBind
	default:
		addr, err := net.ResolveUDPAddr("udp", d.PublishString)
		if err != nil {
			return e.Wrapf(err, "invalid publish")
		}

		d.publish = addr
	}

	return nil
}

func (d NodeNetworkDesign) Publish() *net.UDPAddr {
	return d.publish
}

type NodeNetworkDesignYAMLMarshaler struct {
	Bind        string `yaml:"bind,omitempty"`
	Publish     string `yaml:"publish"`
	TLSInsecure bool   `yaml:"tls_insecure"`
}

func (d NodeNetworkDesign) MarshalYAML() (interface{}, error) {
	var bind string

	if d.Bind != nil {
		bind = d.Bind.String()
	}

	return NodeNetworkDesignYAMLMarshaler{
		Bind:        bind,
		Publish:     d.PublishString,
		TLSInsecure: d.TLSInsecure,
	}, nil
}

func (y *NodeNetworkDesignYAMLMarshaler) Decode(*jsonenc.Encoder) (d NodeNetworkDesign, _ error) {
	e := util.StringErrorFunc("failed to unmarshal NodeNetworkDesign")

	if s := strings.TrimSpace(y.Bind); len(s) > 0 {
		addr, err := net.ResolveUDPAddr("udp", y.Bind)
		if err != nil {
			return d, e(err, "invalid bind")
		}

		d.Bind = addr
	}

	d.PublishString = y.Publish

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
	Database *url.URL `yaml:"database"`
	Base     string   `yaml:"base"`
}

type NodeStorageDesignYAMLMarshal struct {
	Base     string `yaml:"base"`
	Database string `yaml:"database"`
}

func (d *NodeStorageDesign) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NodeStorageDesign")

	if len(d.Base) < 1 {
		d.Base = DefaultStorageBase
	}

	switch i, err := filepath.Abs(d.Base); {
	case err != nil:
		return e.Wrap(err)
	default:
		d.Base = i
	}

	switch {
	case d.Database == nil:
		d.Database = DefaultStorageDatabase
	case len(d.Database.Scheme) < 1:
		return e.Errorf("wrong database; empty scheme")
	}

	return nil
}

func (d *NodeStorageDesign) Patch(node base.Address) error {
	if d.Base == DefaultStorageBase {
		d.Base = filepath.Join(defaultStorageBase, node.String())
	}

	if d.Database.String() == DefaultStorageDatabase.String() {
		d.Database = &url.URL{
			Scheme: LeveldbURIScheme,
			Path:   filepath.Join(defaultStorageBase, node.String(), "perm"),
		}
	}

	return nil
}

func (y *NodeStorageDesignYAMLMarshal) Decode(*jsonenc.Encoder) (d NodeStorageDesign, _ error) {
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

type GenesisDesign struct {
	Facts []base.Fact `yaml:"facts" json:"facts"`
}

func GenesisDesignFromFile(f string, enc *jsonenc.Encoder) (d GenesisDesign, _ []byte, _ error) {
	e := util.StringErrorFunc("failed to load GenesisDesign from file")

	b, err := os.ReadFile(filepath.Clean(f))
	if err != nil {
		return d, nil, e(err, "")
	}

	if err := d.DecodeYAML(b, enc); err != nil {
		return d, b, e(err, "")
	}

	if err := d.IsValid(nil); err != nil {
		return d, b, e(err, "")
	}

	return d, b, nil
}

type GenesisDesignYAMLUnmarshaler struct {
	Facts []interface{} `yaml:"facts" json:"facts"`
}

func (*GenesisDesign) IsValid([]byte) error {
	return nil
}

func (d *GenesisDesign) DecodeYAML(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode GenesisOpertionsDesign")

	var u GenesisDesignYAMLUnmarshaler

	if err := yaml.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	d.Facts = make([]base.Fact, len(u.Facts))

	for i := range u.Facts {
		bj, err := util.MarshalJSON(u.Facts[i])
		if err != nil {
			return e(err, "")
		}

		if err := encoder.Decode(enc, bj, &d.Facts[i]); err != nil {
			return e(err, "")
		}
	}

	return nil
}
