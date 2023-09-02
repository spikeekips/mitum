package launchcmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type NetworkCommand struct {
	Client NetworkClientCommand `cmd:"" help:"network client"`
}

type NetworkClientCommand struct { //nolint:govet //...
	//revive:disable:line-length-limit
	//revive:disable:nested-structs
	NodeInfo      NetworkClientNodeInfoCommand      `cmd:"" name:"node-info" help:"remote node info"`
	SendOperation NetworkClientSendOperationCommand `cmd:"" name:"send-operation" help:"send operation"`
	State         NetworkClientStateCommand         `cmd:"" name:"state" help:"get state"`
	LastBlockMap  NetworkClientLastBlockMapCommand  `cmd:"" name:"last-blockmap" help:"get last blockmap"`
	Node          struct {
		Read  NetworkClientReadNodeCommand  `cmd:"" name:"read" help:"read node value"`
		Write NetworkClientWriteNodeCommand `cmd:"" name:"write" help:"write node value"`
	} `cmd:"" name:"node" help:""`
	//revive:enable:nested-structs
	//revive:enable:line-length-limit
}

type BaseNetworkClientNodeInfoFlags struct { //nolint:govet //...
	//revive:disable:line-length-limit
	NetworkID string              `arg:"" name:"network-id" help:"network-id"`
	Remote    launch.ConnInfoFlag `arg:"" help:"remote node conn info" placeholder:"ConnInfo"`
	Timeout   time.Duration       `help:"timeout" placeholder:"duration" default:"9s"`
	Body      *os.File            `help:"body"`
	//revive:enable:line-length-limit
}

type BaseNetworkClientCommand struct { //nolint:govet //...
	BaseCommand
	BaseNetworkClientNodeInfoFlags
	Client   *isaacnetwork.BaseClient `kong:"-"`
	ClientID string                   `name:"client-id" help:"client id"`
}

func (cmd *BaseNetworkClientCommand) Prepare(pctx context.Context) error {
	if _, err := cmd.BaseCommand.prepare(pctx); err != nil {
		return err
	}

	if len(cmd.NetworkID) < 1 {
		return errors.Errorf(`expected "<network-id>"`)
	}

	if cmd.Timeout < 1 {
		cmd.Timeout = isaac.DefaultTimeoutRequest * 2
	}

	connectionPool, err := launch.NewConnectionPool(
		1<<9, //nolint:gomnd //...
		base.NetworkID(cmd.NetworkID),
		nil,
	)
	if err != nil {
		return err
	}

	cmd.Client = isaacnetwork.NewBaseClient(
		cmd.Encoders, cmd.Encoder,
		connectionPool.Dial,
		connectionPool.CloseAll,
	)
	cmd.Client.SetClientID(cmd.ClientID)

	cmd.Log.Debug().
		Stringer("remote", cmd.Remote).
		Stringer("timeout", cmd.Timeout).
		Str("network_id", cmd.NetworkID).
		Str("client_id", cmd.ClientID).
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
