package launchcmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
)

type NetworkClientEventLoggingCommand struct { //nolint:govet //...
	BaseNetworkClientCommand
	Privatekey string                 `arg:"" name:"privatekey" help:"privatekey string"`
	Logger     launch.EventLoggerName `arg:"" name:"logger" help:"logger name" default:""`
	Offsets    launch.RangeFlag       `name:"offsets" help:"offset, '<start-end>'"`
	Limit      uint64                 `name:"limit" help:"limit" default:"9"`
	Sort       bool                   `name:"sort" help:"sort, true means latest first"`
	priv       base.Privatekey
	offsets    [2]int64
}

func (cmd *NetworkClientEventLoggingCommand) Run(pctx context.Context) error {
	if err := cmd.Prepare(pctx); err != nil {
		return err
	}

	header := launch.NewEventLoggingHeader(cmd.Logger, cmd.offsets, cmd.Limit, cmd.Sort, cmd.priv.Publickey())
	if err := header.IsValid(nil); err != nil {
		return err
	}

	header.SetClientID(cmd.ClientID)

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	defer func() {
		_ = cmd.Client.Close()
	}()

	stream, _, err := cmd.Client.Dial(ctx, cmd.Remote.ConnInfo())
	if err != nil {
		return err
	}

	return launch.EventLoggingFromNetworkHandler(
		ctx,
		stream,
		header,
		cmd.priv,
		base.NetworkID(cmd.NetworkID),
		cmd.printLog,
	)
}

func (*NetworkClientEventLoggingCommand) Help() string {
	s := "## available logger name\n\n"

	for i := range launch.AllEventLoggerNames {
		j := launch.AllEventLoggerNames[i]
		if j == launch.AllEventLogger {
			continue
		}

		s += fmt.Sprintf("  - %s\n", j)
	}

	return s
}

func (cmd *NetworkClientEventLoggingCommand) Prepare(pctx context.Context) error {
	if err := cmd.BaseNetworkClientCommand.Prepare(pctx); err != nil {
		return err
	}

	switch key, err := launch.DecodePrivatekey(cmd.Privatekey, cmd.JSONEncoder); {
	case err != nil:
		return err
	default:
		cmd.priv = key
	}

	if cmd.Limit < 1 {
		return errors.Errorf("--limit under 1")
	}

	if i := cmd.Offsets.From(); i != nil {
		cmd.offsets[0] = int64(*i)
	}

	if i := cmd.Offsets.To(); i != nil {
		cmd.offsets[1] = int64(*i)
	}

	return nil
}

func (cmd *NetworkClientEventLoggingCommand) printLog(addedAt time.Time, offset int64, raw []byte) (bool, error) {
	m := map[string]interface{}{
		"time":   util.RFC3339(addedAt),
		"offset": offset,
		"log":    json.RawMessage(raw),
	}

	if err := cmd.JSONEncoder.StreamEncoder(os.Stdout).Encode(m); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to encode: %q\n", string(raw))
	}

	return true, nil
}
