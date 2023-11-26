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
	"strconv"
	"strings"
	"sync"

	consulapi "github.com/hashicorp/consul/api"
	vault "github.com/hashicorp/vault/api"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
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

type NodeDesign struct { //nolint:govet //...
	Address        base.Address
	Privatekey     base.Privatekey
	Storage        NodeStorageDesign
	Network        NodeNetworkDesign
	NetworkID      base.NetworkID
	LocalParams    *LocalParams
	SyncSources    *SyncSourcesDesign
	TimeServerPort int
	TimeServer     string
}

func NodeDesignFromFile(f string, jsonencoder encoder.Encoder) (d NodeDesign, _ []byte, _ error) {
	e := util.StringError("load NodeDesign from file")

	switch b, err := os.ReadFile(filepath.Clean(f)); {
	case err != nil:
		return d, nil, e.Wrap(err)
	default:
		if err := d.DecodeYAML(b, jsonencoder); err != nil {
			return d, b, e.Wrap(err)
		}

		return d, b, nil
	}
}

func NodeDesignFromHTTP(
	u string,
	tlsinsecure bool,
	jsonencoder encoder.Encoder,
) (design NodeDesign, _ []byte, _ error) {
	e := util.StringError("load NodeDesign thru http")

	switch b, err := getFromHTTP(u, tlsinsecure); {
	case err != nil:
		return design, nil, e.Wrap(err)
	default:
		if err := design.DecodeYAML(b, jsonencoder); err != nil {
			return design, nil, e.Wrap(err)
		}

		return design, b, nil
	}
}

func NodeDesignFromConsul(addr, key string, jsonencoder encoder.Encoder) (design NodeDesign, _ []byte, _ error) {
	e := util.StringError("load NodeDesign thru consul")

	switch b, err := getFromConsul(addr, key); {
	case err != nil:
		return design, nil, e.Wrap(err)
	default:
		if err := design.DecodeYAML(b, jsonencoder); err != nil {
			return design, nil, e.Wrap(err)
		}

		return design, b, nil
	}
}

func (d *NodeDesign) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NodeDesign")

	if len(d.TimeServer) > 0 {
		switch i, err := url.Parse("http://" + d.TimeServer); {
		case err != nil:
			return e.WithMessage(err, "invalid time server, %q", d.TimeServer)
		case len(i.Hostname()) < 1:
			return e.Errorf("invalid time server, %q", d.TimeServer)
		case i.Host != d.TimeServer && len(i.Port()) < 1:
			return e.Errorf("invalid time server, %q", d.TimeServer)
		default:
			s := d.TimeServer
			if len(i.Port()) < 1 {
				s = net.JoinHostPort(d.TimeServer, "123")
			}

			if _, err := net.ResolveUDPAddr("udp", s); err != nil {
				return e.WithMessage(err, "invalid time server, %q", d.TimeServer)
			}

			if len(i.Port()) > 0 {
				p, err := strconv.ParseInt(i.Port(), 10, 64)
				if err != nil {
					return e.WithMessage(err, "invalid time server, %q", d.TimeServer)
				}

				d.TimeServer = i.Hostname()
				d.TimeServerPort = int(p)
			}
		}
	}

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
		d.Network.PublishString,
		d.Network.publish.String(),
	); err != nil {
		return e.Wrap(err)
	}

	switch {
	case d.LocalParams == nil:
		d.LocalParams = defaultLocalParams(d.NetworkID)
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
		if t := d.LocalParams.ISAAC.Threshold(); t < base.SafeThreshold {
			return util.ErrInvalid.Errorf("risky threshold under %v; %v", t, base.SafeThreshold)
		}
	}

	return nil
}

type NodeDesignMarshaler struct {
	LocalParams *LocalParams       `json:"parameters" yaml:"parameters"` //nolint:tagliatelle //...
	SyncSources *SyncSourcesDesign `json:"sync_sources" yaml:"sync_sources"`
	Address     base.Address       `json:"address" yaml:"address"`
	Privatekey  base.Privatekey    `json:"privatekey" yaml:"privatekey"`
	Storage     NodeStorageDesign  `json:"storage" yaml:"storage"`
	NetworkID   string             `json:"network_id" yaml:"network_id"`
	TimeServer  string             `json:"time_server,omitempty" yaml:"time_server,omitempty"`
	Network     NodeNetworkDesign  `json:"network" yaml:"network"`
}

type NodeDesignYAMLUnmarshaler struct {
	SyncSources interface{}                 `json:"sync_sources" yaml:"sync_sources"`
	Storage     NodeStorageDesignLMarshaler `json:"storage" yaml:"storage"`
	Address     string                      `json:"address" yaml:"address"`
	Privatekey  string                      `json:"privatekey" yaml:"privatekey"`
	NetworkID   string                      `json:"network_id" yaml:"network_id"`
	TimeServer  string                      `json:"time_server,omitempty" yaml:"time_server,omitempty"`
	LocalParams interface{}                 `json:"parameters" yaml:"parameters"` //nolint:tagliatelle //...
	Network     NodeNetworkDesignMarshaler  `json:"network" yaml:"network"`
}

func (d NodeDesign) marshaler() NodeDesignMarshaler {
	return NodeDesignMarshaler{
		Address:     d.Address,
		Privatekey:  d.Privatekey,
		NetworkID:   string(d.NetworkID),
		Network:     d.Network,
		Storage:     d.Storage,
		LocalParams: d.LocalParams,
		TimeServer:  d.TimeServer,
		SyncSources: d.SyncSources,
	}
}

func (d NodeDesign) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(d.marshaler())
}

func (d NodeDesign) MarshalYAML() (interface{}, error) {
	return d.marshaler(), nil
}

func (d *NodeDesign) DecodeYAML(b []byte, jsonencoder encoder.Encoder) error {
	e := util.StringError("decode NodeDesign")

	var u NodeDesignYAMLUnmarshaler
	if err := yaml.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	switch address, err := base.DecodeAddress(u.Address, jsonencoder); {
	case err != nil:
		return e.WithMessage(err, "invalid address")
	default:
		d.Address = address
	}

	switch priv, err := base.DecodePrivatekeyFromString(u.Privatekey, jsonencoder); {
	case err != nil:
		return e.WithMessage(err, "invalid privatekey")
	default:
		d.Privatekey = priv
	}

	d.NetworkID = base.NetworkID([]byte(u.NetworkID))

	switch i, err := u.Network.Decode(jsonencoder); {
	case err != nil:
		return e.Wrap(err)
	default:
		d.Network = i
	}

	switch i, err := u.Storage.Decode(jsonencoder); {
	case err != nil:
		return e.Wrap(err)
	default:
		d.Storage = i
	}

	switch sb, err := yaml.Marshal(u.SyncSources); {
	case err != nil:
		return e.Wrap(err)
	default:
		d.SyncSources = NewSyncSourcesDesign(nil)
		if err := d.SyncSources.DecodeYAML(sb, jsonencoder); err != nil {
			return e.Wrap(err)
		}
	}

	d.LocalParams = defaultLocalParams(d.NetworkID)

	switch lb, err := yaml.Marshal(u.LocalParams); {
	case err != nil:
		return e.Wrap(err)
	default:
		if err := d.LocalParams.DecodeYAML(lb, jsonencoder); err != nil {
			return e.Wrap(err)
		}
	}

	d.TimeServer = strings.TrimSpace(u.TimeServer)

	return nil
}

type NodeNetworkDesign struct {
	publishConnInfo quicstream.ConnInfo
	Bind            *net.UDPAddr `yaml:"bind"`
	publish         *net.UDPAddr
	PublishString   string `yaml:"publish"` //nolint:tagliatelle //...
	TLSInsecure     bool   `yaml:"tls_insecure"`
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
			return e.WithMessage(err, "invalid publish")
		case addr.Port < 1:
			return e.Errorf("invalid publish port")
		}

		d.publish = addr
	}

	switch i, err := quicstream.NewConnInfo(d.publish, d.TLSInsecure); {
	case err != nil:
		return e.WithMessage(err, "publish conninfo")
	default:
		d.publishConnInfo = i
	}

	return nil
}

func (d NodeNetworkDesign) Publish() *net.UDPAddr {
	return d.publish
}

func (d NodeNetworkDesign) PublishConnInfo() quicstream.ConnInfo {
	return d.publishConnInfo
}

type NodeNetworkDesignMarshaler struct {
	Bind        string `json:"bind,omitempty" yaml:"bind,omitempty"`
	Publish     string `json:"publish" yaml:"publish"`
	TLSInsecure bool   `json:"tls_insecure" yaml:"tls_insecure"`
}

func (d NodeNetworkDesign) marshaler() NodeNetworkDesignMarshaler {
	var bind string

	if d.Bind != nil {
		bind = d.Bind.String()
	}

	return NodeNetworkDesignMarshaler{
		Bind:        bind,
		Publish:     d.PublishString,
		TLSInsecure: d.TLSInsecure,
	}
}

func (d NodeNetworkDesign) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(d.marshaler())
}

func (d NodeNetworkDesign) MarshalYAML() (interface{}, error) {
	return d.marshaler(), nil
}

func (y *NodeNetworkDesignMarshaler) Decode(encoder.Encoder) (d NodeNetworkDesign, _ error) {
	e := util.StringError("decode NodeNetworkDesign")

	if s := strings.TrimSpace(y.Bind); len(s) > 0 {
		addr, err := net.ResolveUDPAddr("udp", y.Bind)
		if err != nil {
			return d, e.WithMessage(err, "invalid bind")
		}

		d.Bind = addr
	}

	d.PublishString = y.Publish
	d.TLSInsecure = y.TLSInsecure

	return d, nil
}

func (d *NodeNetworkDesign) DecodeYAML(b []byte, jsonencoder encoder.Encoder) error {
	e := util.StringError("decode NodeNetworkDesign")

	var u NodeNetworkDesignMarshaler

	if err := yaml.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	switch i, err := u.Decode(jsonencoder); {
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

type NodeStorageDesignLMarshaler struct {
	Base     string `json:"base" yaml:"base"`
	Database string `json:"database" yaml:"database"`
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

func (y *NodeStorageDesignLMarshaler) Decode(encoder.Encoder) (d NodeStorageDesign, _ error) {
	e := util.StringError("decode NodeStorageDesign")

	d.Base = strings.TrimSpace(y.Base)

	if s := strings.TrimSpace(y.Database); len(s) > 0 {
		switch i, err := url.Parse(s); {
		case err != nil:
			return d, e.WithMessage(err, "invalid database")
		default:
			d.Database = i
		}
	}

	return d, nil
}

func (d NodeStorageDesign) marshaler() NodeStorageDesignLMarshaler {
	var db string
	if d.Database != nil {
		db = d.Database.String()
	}

	return NodeStorageDesignLMarshaler{
		Base:     d.Base,
		Database: db,
	}
}

func (d NodeStorageDesign) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(d.marshaler())
}

func (d NodeStorageDesign) MarshalYAML() (interface{}, error) {
	return d.marshaler(), nil
}

func (d *NodeStorageDesign) DecodeYAML(b []byte, jsonencoder encoder.Encoder) error {
	e := util.StringError("decode NodeStorageDesign")

	var u NodeStorageDesignLMarshaler

	if err := yaml.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	switch i, err := u.Decode(jsonencoder); {
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

func GenesisDesignFromFile(f string, jsonencoder encoder.Encoder) (d GenesisDesign, _ []byte, _ error) {
	e := util.StringError("load GenesisDesign from file")

	b, err := os.ReadFile(filepath.Clean(f))
	if err != nil {
		return d, nil, e.Wrap(err)
	}

	if err := d.DecodeYAML(b, jsonencoder); err != nil {
		return d, b, e.Wrap(err)
	}

	if err := d.IsValid(nil); err != nil {
		return d, b, e.Wrap(err)
	}

	return d, b, nil
}

type GenesisDesignYAMLUnmarshaler struct {
	Facts []interface{} `json:"facts" yaml:"facts"`
}

func (*GenesisDesign) IsValid([]byte) error {
	return nil
}

func (d *GenesisDesign) DecodeYAML(b []byte, jsonencoder encoder.Encoder) error {
	e := util.StringError("decode GenesisOpertionsDesign")

	var u GenesisDesignYAMLUnmarshaler

	if err := yaml.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	d.Facts = make([]base.Fact, len(u.Facts))

	for i := range u.Facts {
		bj, err := util.MarshalJSON(u.Facts[i])
		if err != nil {
			return e.Wrap(err)
		}

		if err := encoder.Decode(jsonencoder, bj, &d.Facts[i]); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

type SyncSourcesDesign struct {
	l []isaacnetwork.SyncSource
	sync.RWMutex
}

func NewSyncSourcesDesign(l []isaacnetwork.SyncSource) *SyncSourcesDesign {
	return &SyncSourcesDesign{l: l}
}

func (d *SyncSourcesDesign) IsValid([]byte) error {
	for i := range d.l {
		if err := d.l[i].IsValid(nil); err != nil {
			return errors.WithMessage(err, "invalid SyncSourcesDesign")
		}
	}

	return nil
}

func (d *SyncSourcesDesign) Sources() []isaacnetwork.SyncSource {
	d.RLock()
	defer d.RUnlock()

	return d.l
}

func (d *SyncSourcesDesign) Update(l []isaacnetwork.SyncSource) {
	d.Lock()
	defer d.Unlock()

	d.l = l
}

func IsValidSyncSourcesDesign(
	d *SyncSourcesDesign,
	localPublishString, localPublishResolved string,
) error {
	if d == nil {
		return nil
	}

	e := util.ErrInvalid.Errorf("invalid SyncSourcesDesign")

	for i := range d.l {
		s := d.l[i]
		if err := s.IsValid(nil); err != nil {
			return e.Wrap(err)
		}

		var ci isaac.NodeConnInfo

		switch t := s.Source.(type) {
		case isaac.NodeConnInfo:
			ci = t
		case quicstream.ConnInfo,
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

func (d *SyncSourcesDesign) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(d.l)
}

func (d *SyncSourcesDesign) MarshalYAML() (interface{}, error) {
	return d.l, nil
}

func (d *SyncSourcesDesign) DecodeYAML(b []byte, jsonencoder encoder.Encoder) error {
	e := util.StringError("decode SyncSourcesDesign")

	var v []interface{}
	if err := yaml.Unmarshal(b, &v); err != nil {
		return e.Wrap(err)
	}

	sources := make([]isaacnetwork.SyncSource, len(v))

	for i := range v {
		vb, err := yaml.Marshal(v[i])
		if err != nil {
			return e.Wrap(err)
		}

		var s isaacnetwork.SyncSource

		switch err := s.DecodeYAML(vb, jsonencoder); {
		case err != nil:
			return e.Wrap(err)
		default:
			sources[i] = s
		}
	}

	d.l = sources

	return nil
}

func defaultDatabaseURL(root string) *url.URL {
	return &url.URL{
		Scheme: LeveldbURIScheme,
		Path:   filepath.Join(root, DefaultStorageDatabaseDirectoryName),
	}
}

func consulClient(addr string) (*consulapi.Client, error) {
	config := consulapi.DefaultConfig()
	if len(addr) > 0 {
		config.Address = addr
	}

	client, err := consulapi.NewClient(config)
	if err != nil {
		return nil, errors.Wrap(err, "create new consul api Client")
	}

	return client, nil
}

func getFromConsul(addr, key string) ([]byte, error) {
	client, err := consulClient(addr)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	switch v, _, err := client.KV().Get(key, nil); {
	case err != nil:
		return nil, errors.WithStack(err)
	default:
		return v.Value, nil
	}
}

func getFromHTTP(u string, tlsinsecure bool) ([]byte, error) {
	httpclient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: tlsinsecure,
			},
		},
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	res, err := httpclient.Do(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	if res.StatusCode != http.StatusOK {
		return nil, errors.Errorf("design not found")
	}

	return b, nil
}

func getFromVault(u string) ([]byte, error) {
	client, err := vault.NewClient(vault.DefaultConfig())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	secret, err := client.KVv2("secret").Get(context.Background(), u)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	i := secret.Data["string"]

	s, ok := i.(string)
	if !ok {
		return nil, errors.Errorf("expected string but %T", i)
	}

	return []byte(s), nil
}
