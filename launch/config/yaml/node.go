package yamlconfig

import (
	"context"

	"github.com/spikeekips/mitum/launch/config"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
)

type Node struct {
	Address *string `yaml:",omitempty"`
}

type RemoteNode struct {
	Node        `yaml:",inline"`
	NodeNetwork `yaml:",inline"`
	Publickey   *string                `yaml:",omitempty"`
	Extras      map[string]interface{} `yaml:",inline"`
}

func (no RemoteNode) Load(ctx context.Context) (config.RemoteNode, error) {
	var conf *config.BaseRemoteNode
	var enc *jsonenc.Encoder
	if err := config.LoadJSONEncoderContextValue(ctx, &enc); err != nil {
		return nil, err
	} else {
		conf = config.NewBaseRemoteNode(enc)
	}

	if no.Address != nil {
		if err := conf.SetAddress(*no.Address); err != nil {
			return nil, err
		}
	}

	if no.URL != nil {
		if err := conf.SetURL(*no.URL); err != nil {
			return nil, err
		}
	}

	if no.Publickey != nil {
		if err := conf.SetPublickey(*no.Publickey); err != nil {
			return nil, err
		}
	}

	return conf, nil
}
