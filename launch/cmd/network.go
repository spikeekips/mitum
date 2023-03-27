package launchcmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type NetworkCommand struct {
	Client NetworkClientCommand `cmd:"" help:"network client"`
}

type NetworkClientCommand struct { //nolint:govet //...
	NodeInfo      NetworkClientNodeInfoCommand      `cmd:"" name:"node-info" help:"remote node info"`
	SendOperation NetworkClientSendOperationCommand `cmd:"" name:"send-operation" help:"send operation"`
	State         NetworkClientStateCommand         `cmd:"" name:"state" help:"get state"`
	LastBlockMap  NetworkClientLastBlockMapCommand  `cmd:"" name:"last-blockmap" help:"get last blockmap"`
}

type BaseNetworkClientNodeInfoFlags struct { //nolint:govet //...
	NetworkID string              `arg:"" name:"network-id" help:"network-id" default:""`
	Remote    launch.ConnInfoFlag `arg:"" help:"remote node conn info" placeholder:"ConnInfo" default:"localhost:4321"`
	Timeout   time.Duration       `help:"timeout" placeholder:"duration" default:"9s"`
	Body      *os.File            `help:"body"`
}

type BaseNetworkClientCommand struct { //nolint:govet //...
	BaseCommand
	BaseNetworkClientNodeInfoFlags
	Client *isaacnetwork.QuicstreamClient
}

func (cmd *BaseNetworkClientCommand) Prepare(pctx context.Context) error {
	if _, err := cmd.BaseCommand.prepare(pctx); err != nil {
		return err
	}

	if len(cmd.NetworkID) < 1 {
		return errors.Errorf(`expected "<network-id>"`)
	}

	if _, err := cmd.Remote.ConnInfo(); err != nil {
		return err
	}

	if cmd.Timeout < 1 {
		cmd.Timeout = time.Second * 9 //nolint:gomnd //...
	}

	cmd.Client = launch.NewNetworkClient(cmd.Encoders, cmd.Encoder, base.NetworkID(cmd.NetworkID))

	cmd.Log.Debug().
		Stringer("remote", cmd.Remote).
		Stringer("timeout", cmd.Timeout).
		Str("network_id", cmd.NetworkID).
		Bool("has_body", cmd.Body != nil).
		Msg("flags")

	return nil
}

func (cmd *BaseNetworkClientCommand) Print(v interface{}, out io.Writer) error {
	l := cmd.Log.Debug().
		Str("type", fmt.Sprintf("%T", v))

	if ht, ok := v.(hint.Hinter); ok {
		l = l.Stringer("hint", ht.Hint())
	}

	l.Msg("body loaded")

	b, err := util.MarshalJSONIndent(v)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(out, string(b))

	return errors.WithStack(err)
}
