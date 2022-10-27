package launch

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	consulapi "github.com/hashicorp/consul/api"
	vault "github.com/hashicorp/vault/api"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
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
	SyncSources SyncSourcesDesign
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

	return d, b, nil
}

func NodeDesignFromHTTP(u string, tlsInsecure bool, enc *jsonenc.Encoder) (design NodeDesign, _ error) {
	e := util.StringErrorFunc("failed to load NodeDesign thru http")

	httpclient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: tlsInsecure,
			},
		},
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	if err != nil {
		return design, e(err, "")
	}

	res, err := httpclient.Do(req)
	if err != nil {
		return design, e(err, "")
	}

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return design, e(err, "")
	}

	defer func() {
		_ = res.Body.Close()
	}()

	if res.StatusCode != http.StatusOK {
		return design, e(nil, "design not found")
	}

	if err := design.DecodeYAML(b, enc); err != nil {
		return design, e(err, "")
	}

	return design, nil
}

func NodeDesignFromConsul(addr, key string, enc *jsonenc.Encoder) (design NodeDesign, _ error) {
	e := util.StringErrorFunc("failed to load NodeDesign thru consul")

	client, err := consulClient(addr)
	if err != nil {
		return design, e(err, "")
	}

	switch v, _, err := client.KV().Get(key, nil); {
	case err != nil:
		return design, e(err, "")
	default:
		if err := design.DecodeYAML(v.Value, enc); err != nil {
			return design, e(err, "")
		}

		return design, nil
	}
}

func (d *NodeDesign) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NodeDesign")

	if err := util.CheckIsValiders(nil, false, d.Address, d.Privatekey, d.NetworkID); err != nil {
		return e.Wrap(err)
	}

	if err := d.Network.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := d.Storage.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := IsValidSyncSourcesDesign(
		d.SyncSources,
		d.Address,
		d.Network.PublishString,
		d.Network.publish.String(),
	); err != nil {
		return e.Wrap(err)
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

func (d *NodeDesign) Check(devflags DevFlags) error {
	if !devflags.AllowRiskyThreshold {
		if t := d.LocalParams.Threshold(); t < base.SafeThreshold {
			return util.ErrInvalid.Errorf("risky threshold under %v; %v", t, base.SafeThreshold)
		}
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
	SyncSources SyncSourcesDesign  `yaml:"sync_sources"`
}

type NodeDesignYAMLUnmarshaler struct {
	SyncSources interface{}                    `yaml:"sync_sources"`
	Storage     NodeStorageDesignYAMLMarshal   `yaml:"storage"`
	Address     string                         `yaml:"address"`
	Privatekey  string                         `yaml:"privatekey"`
	NetworkID   string                         `yaml:"network_id"`
	LocalParams map[string]interface{}         `yaml:"parameters"` //nolint:tagliatelle //...
	Network     NodeNetworkDesignYAMLMarshaler `yaml:"network"`
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

	switch sb, err := yaml.Marshal(u.SyncSources); {
	case err != nil:
		return e(err, "")
	default:
		if err := d.SyncSources.DecodeYAML(sb, enc); err != nil {
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

type SyncSourcesDesign []isaacnetwork.SyncSource

func (d *SyncSourcesDesign) IsValid([]byte) error {
	for i := range *d {
		s := (*d)[i]
		if err := s.IsValid(nil); err != nil {
			return errors.WithMessage(err, "invalid SyncSourcesDesign")
		}
	}

	return nil
}

func IsValidSyncSourcesDesign(
	d SyncSourcesDesign,
	localAddress base.Address,
	localPublishString, localPublishResolved string,
) error {
	e := util.ErrInvalid.Errorf("invalid SyncSourcesDesign")

	for i := range d {
		s := d[i]
		if err := s.IsValid(nil); err != nil {
			return e.Wrap(err)
		}

		var ci isaac.NodeConnInfo

		switch t := s.Source.(type) {
		case isaac.NodeConnInfo:
			if t.Address().Equal(localAddress) {
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
		case ci.Addr().String() == localPublishString:
			return e.Errorf("sync source has same with publish address")
		case ci.Addr().String() == localPublishResolved:
			return e.Errorf("sync source has same with publish resolved address")
		}
	}

	return nil
}

func (d *SyncSourcesDesign) DecodeYAML(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SyncSourcesDesign")

	var v []interface{}
	if err := yaml.Unmarshal(b, &v); err != nil {
		return e(err, "")
	}

	sources := make([]isaacnetwork.SyncSource, len(v))

	for i := range v {
		vb, err := yaml.Marshal(v[i])
		if err != nil {
			return e(err, "")
		}

		var s isaacnetwork.SyncSource

		switch err := s.DecodeYAML(vb, enc); {
		case err != nil:
			return e(err, "")
		default:
			sources[i] = s
		}
	}

	*d = sources

	return nil
}

func defaultDatabaseURL(root string) *url.URL {
	return &url.URL{
		Scheme: LeveldbURIScheme,
		Path:   filepath.Join(root, DefaultStorageDatabaseDirectoryName),
	}
}

func (d NodeDesign) MarshalZerologObject(e *zerolog.Event) {
	var priv base.Publickey
	if d.Privatekey != nil {
		priv = d.Privatekey.Publickey()
	}

	e.
		Interface("address", d.Address).
		Interface("privatekey*", priv).
		Interface("storage", d.Storage).
		Interface("network_id", d.NetworkID).
		Interface("network", d.Network).
		Interface("parameters", d.LocalParams).
		Interface("sync_sources", d.SyncSources)
}

func loadPrivatekeyFromVault(path string, enc *jsonenc.Encoder) (base.Privatekey, error) {
	e := util.StringErrorFunc("failed to load privatekey from vault")

	config := vault.DefaultConfig()

	client, err := vault.NewClient(config)
	if err != nil {
		return nil, e(err, "failed to create vault client")
	}

	secret, err := client.KVv2("secret").Get(context.Background(), path)
	if err != nil {
		return nil, e(err, "failed to read secret")
	}

	i := secret.Data["string"]

	privs, ok := i.(string)
	if !ok {
		return nil, e(nil, "failed to read secret; expected string but %T", i)
	}

	switch priv, err := base.DecodePrivatekeyFromString(privs, enc); {
	case err != nil:
		return nil, e(err, "invalid privatekey")
	default:
		return priv, nil
	}
}

func consulClient(addr string) (*consulapi.Client, error) {
	config := consulapi.DefaultConfig()
	if len(addr) > 0 {
		config.Address = addr
	}

	client, err := consulapi.NewClient(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new consul api Client")
	}

	return client, nil
}
