package launchcmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"gopkg.in/yaml.v3"
)

type baseNetworkClientRWDesignCommand struct { //nolint:govet //...
	BaseNetworkClientCommand
	Privatekey string `arg:"" name:"privatekey" help:"privatekey string"`
	Key        string `arg:"" name:"key" help:"design key"`
	Format     string `name:"format" help:"output format, {json, yaml}" default:"yaml"`
	priv       base.Privatekey
}

func (cmd *baseNetworkClientRWDesignCommand) Prepare(pctx context.Context) error {
	if err := cmd.BaseNetworkClientCommand.Prepare(pctx); err != nil {
		return err
	}

	if len(cmd.Key) < 1 {
		return errors.Errorf("empty key")
	}

	switch cmd.Format {
	case "json", "yaml":
	default:
		return errors.Errorf("unsupported format, %q", cmd.Format)
	}

	switch key, err := launch.DecodePrivatekey(cmd.Privatekey, cmd.Encoder); {
	case err != nil:
		return err
	default:
		cmd.priv = key
	}

	return nil
}

func (cmd *baseNetworkClientRWDesignCommand) printDesignValue(
	ctx context.Context,
) error {
	stream, _, err := cmd.Client.Dial(ctx, cmd.Remote.ConnInfo())
	if err != nil {
		return err
	}

	switch i, found, err := launch.ReadDesignFromNetworkHandler(
		ctx,
		cmd.priv,
		base.NetworkID(cmd.NetworkID),
		cmd.Key,
		stream,
	); {
	case err != nil:
		return err
	case !found:
		return util.ErrNotFound.Errorf("unknown key, %q", cmd.Key)
	case cmd.Format == "json":
		b, err := util.MarshalJSON(i)
		if err != nil {
			return err
		}

		_, _ = fmt.Fprintln(os.Stdout, string(b))
	case cmd.Format == "yaml":
		var v string

		if i != nil {
			b, err := yaml.Marshal(i)
			if err != nil {
				return errors.WithStack(err)
			}

			v = string(b)
		}

		_, _ = fmt.Fprintln(os.Stdout, v)
	}

	return nil
}

type NetworkClientReadDesignCommand struct { //nolint:govet //...
	baseNetworkClientRWDesignCommand
}

func (cmd *NetworkClientReadDesignCommand) Run(pctx context.Context) error {
	if err := cmd.Prepare(pctx); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	defer func() {
		_ = cmd.Client.Close()
	}()

	return cmd.printDesignValue(ctx)
}

type NetworkClientWriteDesignCommand struct { //nolint:govet //...
	baseNetworkClientRWDesignCommand
	Value string `arg:"" name:"value" help:"design value" default:""`
}

func (cmd *NetworkClientWriteDesignCommand) Run(pctx context.Context) error {
	if err := cmd.Prepare(pctx); err != nil {
		return err
	}

	var value string

	switch {
	case len(cmd.Value) > 0:
		value = cmd.Value
	case cmd.Body != nil:
		i, err := io.ReadAll(cmd.Body)
		if err != nil {
			return errors.WithStack(err)
		}

		value = strings.TrimRight(string(i), "\n")
	}

	if len(value) < 1 {
		return errors.Errorf("empty value")
	}

	cmd.Log.Debug().
		Str("key", cmd.Key).
		Str("value", value).
		Msg("flags")

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	stream, _, err := cmd.Client.Dial(ctx, cmd.Remote.ConnInfo())
	if err != nil {
		return err
	}

	defer func() {
		_ = cmd.Client.Close()
	}()

	switch found, err := launch.WriteDesignFromNetworkHandler(
		ctx,
		cmd.priv,
		base.NetworkID(cmd.NetworkID),
		cmd.Key,
		value,
		stream,
	); {
	case err != nil:
		return err
	case !found:
		return util.ErrNotFound.Errorf("unknown key, %q", cmd.Key)
	}

	return cmd.printDesignValue(ctx)
}
