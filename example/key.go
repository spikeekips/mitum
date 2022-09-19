package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type KeyNewCommand struct {
	baseCommand
	Seed string `arg:"" name:"seed" optional:"" help:"seed for generating key"`
}

func (cmd *KeyNewCommand) Run(pctx context.Context) error {
	if _, err := cmd.prepare(pctx); err != nil {
		return err
	}

	cmd.log.Debug().
		Str("seed", cmd.Seed).
		Msg("flags")

	if _, err := cmd.prepare(pctx); err != nil {
		return err
	}

	var key base.Privatekey

	switch {
	case len(cmd.Seed) > 0:
		if len(strings.TrimSpace(cmd.Seed)) < 1 {
			cmd.log.Warn().Msg("seed consists with empty spaces")
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

type KeyLoadCommand struct {
	baseCommand
	KeyString string `arg:"" name:"key string" help:"key string"`
}

func (cmd *KeyLoadCommand) Run(pctx context.Context) error {
	if _, err := cmd.prepare(pctx); err != nil {
		return err
	}

	cmd.log.Debug().
		Str("key_string", cmd.KeyString).
		Msg("flags")

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

type KeySignCommand struct {
	baseCommand
	KeyString string             `arg:"" name:"privatekey" help:"privatekey string"`
	NetworkID string             `arg:"" name:"network-id" help:"network-id"`
	Body      *os.File           `arg:"" help:"body"`
	Node      launch.AddressFlag `help:"node address"`
	Token     string             `help:"set fact token"`
	priv      base.Privatekey
	networkID base.NetworkID
}

func (cmd *KeySignCommand) Run(pctx context.Context) error {
	if err := cmd.prepare(pctx); err != nil {
		return err
	}

	cmd.log.Debug().
		Str("privatekey", cmd.KeyString).
		Str("network_id", cmd.NetworkID).
		Stringer("node", cmd.Node.Address()).
		Str("token", cmd.Token).
		Msg("flags")

	defer func() {
		_ = cmd.Body.Close()
	}()

	var elem, ptr interface{}

	switch i, j, err := cmd.loadBody(); {
	case err != nil:
		return err
	default:
		elem = i
		ptr = j
	}

	switch ptr.(type) {
	case base.NodeHashSigner, base.NodeSigner:
		if cmd.Node.Address() == nil {
			return errors.Errorf("--node is missing")
		}
	}

	if err := util.InterfaceSetValue(elem, ptr); err != nil {
		return err
	}

	switch {
	case len(cmd.Token) < 1:
		cmd.log.Debug().Msg("token not updated")
	default:
		if i, ok := ptr.(base.TokenSetter); ok {
			if err := i.SetToken(base.Token([]byte(cmd.Token))); err != nil {
				return err
			}

			cmd.log.Debug().Str("new_token", cmd.Token).Msg("token updated")
		}
	}

	if err := cmd.sign(ptr); err != nil {
		return err
	}

	cmd.log.Debug().Msg("successfully sign")

	b, err := util.MarshalJSONIndent(ptr)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintln(os.Stdout, string(b))

	return nil
}

func (cmd *KeySignCommand) prepare(pctx context.Context) error {
	if _, err := cmd.baseCommand.prepare(pctx); err != nil {
		return err
	}

	switch key, err := base.DecodePrivatekeyFromString(cmd.KeyString, cmd.enc); {
	case err != nil:
		return err
	default:
		if err := key.IsValid(nil); err != nil {
			return err
		}

		cmd.priv = key
	}

	cmd.networkID = base.NetworkID([]byte(cmd.NetworkID))

	return cmd.networkID.IsValid(nil)
}

func (cmd *KeySignCommand) loadBody() (interface{}, interface{}, error) {
	var body []byte

	switch i, err := io.ReadAll(cmd.Body); {
	case err != nil:
		return nil, nil, errors.WithStack(err)
	default:
		body = i
	}

	var u map[string]interface{}
	if err := util.UnmarshalJSON(body, &u); err != nil {
		return nil, nil, err
	}

	switch i, err := util.MarshalJSONIndent(u); {
	case err != nil:
		return nil, nil, err
	default:
		_, _ = fmt.Fprintln(os.Stderr, string(i))
	}

	cmd.log.Debug().Str("raw_body", string(body)).Msg("read body")

	elem, err := cmd.enc.Decode(body)
	if err != nil {
		return nil, nil, err
	}

	if elem == nil {
		return nil, nil, errors.Errorf("failed to load body")
	}

	cmd.log.Debug().Str("body_type", fmt.Sprintf("%T", elem)).Msg("body loaded")

	return elem, reflect.New(reflect.ValueOf(elem).Type()).Interface(), nil
}

func (cmd *KeySignCommand) sign(ptr interface{}) error {
	var sign func() error

	switch t := ptr.(type) {
	case base.NodeHashSigner:
		sign = func() error {
			return t.HashSign(cmd.priv, cmd.networkID, cmd.Node.Address())
		}
	case base.NodeSigner:
		sign = func() error {
			return t.Sign(cmd.priv, cmd.networkID, cmd.Node.Address())
		}
	case base.HashSigner:
		sign = func() error {
			return t.HashSign(cmd.priv, cmd.networkID)
		}
	case base.Signer:
		sign = func() error {
			return t.Sign(cmd.priv, cmd.networkID)
		}
	default:
		return errors.Errorf("it's not Signer, %T", ptr)
	}

	if err := sign(); err != nil {
		return err
	}

	if i, ok := ptr.(util.IsValider); ok {
		if err := i.IsValid(cmd.networkID); err != nil {
			return err
		}
	}

	return nil
}
