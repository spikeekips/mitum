package launch

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"gopkg.in/yaml.v3"
)

var (
	DefaultNetworkBind                  = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4321} //nolint:gomnd //...
	DefaultNetworkPublish               = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4321} //nolint:gomnd //...
	DefaultStorageBase                  string
	DefaultStorageDatabaseDirectoryName = "db"
)

func init() {
	{
		a, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		a, err = filepath.Abs(a)
		if err != nil {
			panic(err)
		}

		DefaultStorageBase = filepath.Join(a, "tmp", "mitum")
	}
}

type NodeDesign struct {
	Address     base.Address
	Privatekey  base.Privatekey
	Storage     NodeStorageDesign
	Network     NodeNetworkDesign
	NetworkID   base.NetworkID
	LocalParams *isaac.LocalParams
	SyncSources []SyncSourceDesign
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

	for i := range d.SyncSources {
		s := d.SyncSources[i]
		if err := s.IsValid(nil); err != nil {
			return e.Wrap(err)
		}

		var ci isaac.NodeConnInfo

		switch t := s.Source.Source.(type) {
		case isaac.NodeConnInfo:
			if t.Address().Equal(d.Address) {
				return e.Errorf("same node address with local")
			}

			ci = t
		case quicstream.UDPConnInfo,
			quicmemberlist.NamedConnInfo:
			ci = t.(isaac.NodeConnInfo) //nolint:forcetypeassert //...
		default:
			continue
		}

		switch {
		case ci.Addr().String() == d.Network.PublishString:
			return e.Errorf("sync source has same with publish address")
		case ci.Addr().String() == d.Network.publish.String():
			return e.Errorf("sync source has same with publish resolved address")
		}
	}

	switch {
	case d.LocalParams == nil:
		d.LocalParams = isaac.DefaultLocalParams(d.NetworkID)
	default:
		if err := d.LocalParams.IsValid(d.NetworkID); err != nil {
			return e.Wrap(err)
		}
	}

	if err := d.Storage.Patch(d.Address); err != nil {
		return e.Wrap(err)
	}

	return nil
}

type NodeDesignYAMLMarshaler struct {
	Address     base.Address       `yaml:"address"`
	Privatekey  base.Privatekey    `yaml:"privatekey"`
	Storage     NodeStorageDesign  `yaml:"storage"`
	NetworkID   string             `yaml:"network_id"`
	Network     NodeNetworkDesign  `yaml:"network"`
	LocalParams *isaac.LocalParams `yaml:"parameters"` //nolint:tagliatelle //...
	SyncSources []SyncSourceDesign `yaml:"sync_sources"`
}

type NodeDesignYAMLUnmarshaler struct {
	Storage     NodeStorageDesignYAMLMarshal   `yaml:"storage"`
	Address     string                         `yaml:"address"`
	Privatekey  string                         `yaml:"privatekey"`
	NetworkID   string                         `yaml:"network_id"`
	LocalParams map[string]interface{}         `yaml:"parameters"` //nolint:tagliatelle //...
	Network     NodeNetworkDesignYAMLMarshaler `yaml:"network"`
	SyncSources []interface{}                  `yaml:"sync_sources"`
}

func (d NodeDesign) MarshalYAML() (interface{}, error) {
	return NodeDesignYAMLMarshaler{
		Address:     d.Address,
		Privatekey:  d.Privatekey,
		NetworkID:   string(d.NetworkID),
		Network:     d.Network,
		Storage:     d.Storage,
		LocalParams: d.LocalParams,
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

	d.SyncSources = make([]SyncSourceDesign, len(u.SyncSources))

	for i := range u.SyncSources {
		j, err := yaml.Marshal(u.SyncSources[i])
		if err != nil {
			return e(err, "")
		}

		if err := d.SyncSources[i].DecodeYAML(j, enc); err != nil {
			return e(err, "")
		}
	}

	d.LocalParams = isaac.DefaultLocalParams(d.NetworkID)

	switch lb, err := util.MarshalJSON(u.LocalParams); {
	case err != nil:
		return e(err, "")
	default:
		if err := util.UnmarshalJSON(lb, d.LocalParams); err != nil {
			return e(err, "")
		}

		d.LocalParams.BaseHinter = hint.NewBaseHinter(isaac.LocalParamsHint)
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
		d.PublishString = fmt.Sprintf("%s:%d", DefaultNetworkPublish.IP, d.Bind.Port)
		d.publish = &net.UDPAddr{IP: DefaultNetworkPublish.IP, Port: d.Bind.Port}
	default:
		addr, err := net.ResolveUDPAddr("udp", d.PublishString)

		switch {
		case err != nil:
			return e.Wrapf(err, "invalid publish")
		case addr.Port < 1:
			return e.Wrapf(err, "invalid publish port")
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

	switch {
	case d.Database == nil:
	case len(d.Database.Scheme) < 1:
		return e.Errorf("wrong database; empty scheme")
	}

	return nil
}

func (d *NodeStorageDesign) Patch(node base.Address) error {
	switch {
	case len(d.Base) < 1:
		d.Base = filepath.Join(DefaultStorageBase, node.String())
	default:
		switch i, err := filepath.Abs(d.Base); {
		case err != nil:
			return errors.Wrapf(err, "invalid base directory, %q", d.Base)
		default:
			d.Base = i
		}
	}

	if d.Database == nil {
		d.Database = defaultDatabaseURL(d.Base)
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

type SyncSourceDesign struct {
	Source isaacnetwork.SyncSource
}

func (d *SyncSourceDesign) IsValid([]byte) error {
	if err := d.Source.IsValid(nil); err != nil {
		return errors.WithMessage(err, "invalid SyncSourceDesign")
	}

	return nil
}

func (d *SyncSourceDesign) DecodeYAML(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SyncSourceDesign")

	var v interface{}

	if err := yaml.Unmarshal(b, &v); err != nil {
		return e(err, "")
	}

	switch t := v.(type) {
	case string:
		i, err := d.decodeString(t)
		if err != nil {
			return e(err, "")
		}

		d.Source = i
	case map[string]interface{}:
		var ty string

		switch i, found := t["type"]; {
		case !found:
			return e(nil, "missing type")
		default:
			j, ok := i.(string)
			if !ok {
				return e(nil, "type should be string, not %T", i)
			}

			ty = j
		}

		i, err := d.decodeYAMLMap(ty, b, enc)
		if err != nil {
			return e(err, "")
		}

		if err := i.IsValid(nil); err != nil {
			return e(err, "")
		}

		d.Source = i
	default:
		return e(nil, "unsupported format found, %q", string(b))
	}

	return nil
}

func (SyncSourceDesign) decodeString(s string) (source isaacnetwork.SyncSource, _ error) {
	u, err := url.Parse(s)
	if err != nil {
		return source, errors.Wrap(err, "failed to string for SyncSourceDesign")
	}

	switch {
	case len(u.Scheme) < 1:
		return source, errors.Errorf("missing scheme for SyncSourceDesign")
	case len(u.Host) < 1:
		return source, errors.Errorf("missing host for SyncSourceDesign")
	case len(u.Port()) < 1:
		return source, errors.Errorf("missing port for SyncSourceDesign")
	default:
		return isaacnetwork.SyncSource{Type: isaacnetwork.SyncSourceTypeURL, Source: u}, nil
	}
}

func (d SyncSourceDesign) decodeYAMLMap(
	t string, b []byte, enc *jsonenc.Encoder,
) (source isaacnetwork.SyncSource, _ error) {
	ty := isaacnetwork.SyncSourceType(t)

	switch ty {
	case isaacnetwork.SyncSourceTypeNode:
		i, err := d.decodeYAMLNodeConnInfo(b, enc)
		if err != nil {
			return source, err
		}

		return isaacnetwork.SyncSource{Type: ty, Source: i}, nil
	case isaacnetwork.SyncSourceTypeSuffrageNodes,
		isaacnetwork.SyncSourceTypeSyncSources:
		i, err := d.decodeYAMLConnInfo(b, enc)
		if err != nil {
			return source, err
		}

		return isaacnetwork.SyncSource{Type: ty, Source: i}, nil
	default:
		return source, errors.Errorf("unsupported type, %q", ty)
	}
}

type syncSourceNodeUnmarshaler struct {
	Address     string
	Publickey   string
	Publish     string `yaml:"publish"`
	TLSInsecure bool   `yaml:"tls_insecure"`
}

func (SyncSourceDesign) decodeYAMLNodeConnInfo(b []byte, enc *jsonenc.Encoder) (isaac.NodeConnInfo, error) {
	e := util.StringErrorFunc("failed to decode node of SyncSourceDesign")

	var u syncSourceNodeUnmarshaler

	if err := yaml.Unmarshal(b, &u); err != nil {
		return nil, e(err, "")
	}

	address, err := base.DecodeAddress(u.Address, enc)
	if err != nil {
		return nil, e(err, "")
	}

	pub, err := base.DecodePublickeyFromString(u.Publickey, enc)
	if err != nil {
		return nil, e(err, "")
	}

	if err := network.IsValidAddr(u.Publish); err != nil {
		return nil, e(err, "")
	}

	return isaacnetwork.NewNodeConnInfo(isaac.NewNode(pub, address), u.Publish, u.TLSInsecure), nil
}

type syncSourceConnInfoUnmarshaler struct {
	Publish     string `yaml:"publish"`
	TLSInsecure bool   `yaml:"tls_insecure"`
}

func (SyncSourceDesign) decodeYAMLConnInfo(
	b []byte, _ *jsonenc.Encoder,
) (ci quicmemberlist.NamedConnInfo, _ error) {
	e := util.StringErrorFunc("failed to decode conninfo of SyncSourceDesign")

	var u syncSourceConnInfoUnmarshaler

	if err := yaml.Unmarshal(b, &u); err != nil {
		return ci, e(err, "")
	}

	if err := network.IsValidAddr(u.Publish); err != nil {
		return ci, e(err, "")
	}

	return quicmemberlist.NewNamedConnInfo(u.Publish, u.TLSInsecure), nil
}

func defaultDatabaseURL(root string) *url.URL {
	return &url.URL{
		Scheme: LeveldbURIScheme,
		Path:   filepath.Join(root, DefaultStorageDatabaseDirectoryName),
	}
}
