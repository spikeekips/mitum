package isaacnetwork

import (
	"net/url"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"gopkg.in/yaml.v3"
)

func (d *SyncSource) DecodeYAML(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SyncSource")

	var v interface{}
	if err := yaml.Unmarshal(b, &v); err != nil {
		return e(err, "")
	}

	switch t := v.(type) {
	case string:
		if err := d.decodeString(t); err != nil {
			return e(err, "")
		}

		return nil
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

		if err := d.decodeYAMLMap(ty, b, enc); err != nil {
			return e(err, "")
		}

		return nil
	default:
		return e(nil, "unsupported format found, %q", string(b))
	}
}

func (d *SyncSource) decodeString(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return errors.Wrap(err, "failed to string for SyncSource")
	}

	switch {
	case len(u.Scheme) < 1:
		return errors.Errorf("missing scheme for SyncSource")
	case len(u.Host) < 1:
		return errors.Errorf("missing host for SyncSource")
	case len(u.Port()) < 1:
		return errors.Errorf("missing port for SyncSource")
	default:
		d.Type = SyncSourceTypeURL
		d.Source = u

		return nil
	}
}

func (d *SyncSource) decodeYAMLMap(t string, b []byte, enc *jsonenc.Encoder) error {
	ty := SyncSourceType(t)

	switch ty {
	case SyncSourceTypeNode:
		i, err := d.decodeYAMLNodeConnInfo(b, enc)
		if err != nil {
			return err
		}

		d.Type = ty
		d.Source = i

		return nil
	case SyncSourceTypeSuffrageNodes,
		SyncSourceTypeSyncSources:
		i, err := d.decodeYAMLConnInfo(b, enc)
		if err != nil {
			return err
		}

		d.Type = ty
		d.Source = i

		return nil
	default:
		return errors.Errorf("unsupported type, %q", ty)
	}
}

type syncSourceNodeUnmarshaler struct {
	Address     string
	Publickey   string
	Publish     string `yaml:"publish"`
	TLSInsecure bool   `yaml:"tls_insecure"`
}

func (SyncSource) decodeYAMLNodeConnInfo(b []byte, enc *jsonenc.Encoder) (isaac.NodeConnInfo, error) {
	e := util.StringErrorFunc("failed to decode node of SyncSource")

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

	return NewNodeConnInfo(isaac.NewNode(pub, address), u.Publish, u.TLSInsecure), nil
}

type syncSourceConnInfoUnmarshaler struct {
	Publish     string `yaml:"publish"`
	TLSInsecure bool   `yaml:"tls_insecure"`
}

func (SyncSource) decodeYAMLConnInfo(
	b []byte, _ *jsonenc.Encoder,
) (ci quicmemberlist.NamedConnInfo, _ error) {
	e := util.StringErrorFunc("failed to decode conninfo of SyncSource")

	var u syncSourceConnInfoUnmarshaler

	if err := yaml.Unmarshal(b, &u); err != nil {
		return ci, e(err, "")
	}

	if err := network.IsValidAddr(u.Publish); err != nil {
		return ci, e(err, "")
	}

	return quicmemberlist.NewNamedConnInfo(u.Publish, u.TLSInsecure), nil
}
