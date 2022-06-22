package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type keyNewCommand struct {
	baseCommand
	Seed string `arg:"" name:"seed" optional:"" help:"seed for generating key"`
}

func (cmd *keyNewCommand) Run() error {
	log.Debug().
		Str("seed", cmd.Seed).
		Msg("flags")

	if err := cmd.prepareEncoder(); err != nil {
		return err
	}

	var key base.Privatekey

	switch {
	case len(cmd.Seed) > 0:
		if len(strings.TrimSpace(cmd.Seed)) < 1 {
			log.Warn().Msg("seed consists with empty spaces")
		}

		i, err := base.NewMPrivatekeyFromSeed(cmd.Seed)
		if err != nil {
			return err
		}

		key = i
	default:
		key = base.NewMPrivatekey()
	}

	o := struct {
		PrivateKey base.PKKey  `json:"privatekey"` //nolint:tagliatelle //...
		Publickey  base.PKKey  `json:"publickey"`
		Hint       interface{} `json:"hint,omitempty"`
		Seed       string      `json:"seed"`
		Type       string      `json:"type"`
	}{
		Seed:       cmd.Seed,
		PrivateKey: key,
		Publickey:  key.Publickey(),
		Type:       "privatekey",
	}

	if hinter, ok := (interface{})(key).(hint.Hinter); ok {
		o.Hint = hinter.Hint()
	}

	b, err := util.MarshalJSONIndent(o)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintln(os.Stdout, string(b))

	return nil
}

type keyLoadCommand struct {
	baseCommand
	KeyString string `arg:"" name:"key string" help:"key string"`
}

func (cmd *keyLoadCommand) Run() error {
	log.Debug().
		Str("key_string", cmd.KeyString).
		Msg("flags")

	if err := cmd.prepareEncoder(); err != nil {
		return err
	}

	if len(cmd.KeyString) < 1 {
		return errors.Errorf("empty key string")
	}

	if key, err := base.DecodePrivatekeyFromString(cmd.KeyString, cmd.enc); err == nil {
		o := struct {
			PrivateKey base.PKKey  `json:"privatekey"` //nolint:tagliatelle //...
			Publickey  base.PKKey  `json:"publickey"`
			Hint       interface{} `json:"hint,omitempty"`
			String     string      `json:"string"`
			Type       string      `json:"type"`
		}{
			String:     cmd.KeyString,
			PrivateKey: key,
			Publickey:  key.Publickey(),
			Type:       "privatekey",
		}

		if hinter, ok := key.(hint.Hinter); ok {
			o.Hint = hinter.Hint()
		}

		b, err := util.MarshalJSONIndent(o)
		if err != nil {
			return err
		}

		_, _ = fmt.Fprintln(os.Stdout, string(b))

		return nil
	}

	if key, err := base.DecodePublickeyFromString(cmd.KeyString, cmd.enc); err == nil {
		o := struct {
			Publickey base.PKKey  `json:"publickey"`
			Hint      interface{} `json:"hint,omitempty"`
			String    string      `json:"string"`
			Type      string      `json:"type"`
		}{
			String:    cmd.KeyString,
			Publickey: key,
			Type:      "publickey",
		}

		if hinter, ok := key.(hint.Hinter); ok {
			o.Hint = hinter.Hint()
		}

		b, err := util.MarshalJSONIndent(o)
		if err != nil {
			return err
		}

		_, _ = fmt.Fprintln(os.Stdout, string(b))

		return nil
	}

	return nil
}
