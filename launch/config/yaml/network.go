package yamlconfig

import (
	"context"

	"github.com/spikeekips/mitum/launch/config"
	"golang.org/x/xerrors"
)

type NodeNetwork struct {
	URL    *string
	Extras map[string]interface{} `yaml:",inline"`
}

type LocalNetwork struct {
	NodeNetwork `yaml:",inline"`
	Bind        *string
	CertKeyFile *string                `yaml:"cert-key,omitempty"`
	CertFile    *string                `yaml:"cert,omitempty"`
	Extras      map[string]interface{} `yaml:",inline"`
}

func (no LocalNetwork) Set(ctx context.Context) (context.Context, error) {
	var l config.LocalNode
	var conf config.LocalNetwork
	if err := config.LoadConfigContextValue(ctx, &l); err != nil {
		return ctx, err
	} else {
		conf = l.Network()
	}

	if no.NodeNetwork.URL != nil {
		if err := conf.SetURL(*no.NodeNetwork.URL); err != nil {
			return ctx, err
		}
	}

	if no.Bind != nil {
		if err := conf.SetBind(*no.Bind); err != nil {
			return ctx, err
		}
	}

	if (no.CertKeyFile != nil || no.CertFile != nil) && (no.CertKeyFile == nil || no.CertFile == nil) {
		return ctx, xerrors.Errorf("cert-key and cert should be given both")
	} else if no.CertKeyFile != nil {
		if err := conf.SetCertFiles(*no.CertFile, *no.CertKeyFile); err != nil {
			return ctx, err
		}
	}

	return ctx, nil
}
