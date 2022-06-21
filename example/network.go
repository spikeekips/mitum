package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type networkClientCommand struct { //nolint:govet //...
	baseCommand
	Header  string              `arg:"" help:"json header"`
	Remote  launch.ConnInfoFlag `arg:"" help:"remote" placeholder:"ConnInfo" default:"localhost:4321"`
	Timeout time.Duration       `help:"timeout" placeholder:"duration" default:"10s"`
	List    bool                `help:"list requests"`
}

func (cmd *networkClientCommand) Run() error {
	if cmd.List {
		_, _ = fmt.Fprintln(os.Stdout, "requests:")

		for _, prefix := range []string{
			isaacnetwork.HandlerPrefixRequestProposal,
			isaacnetwork.HandlerPrefixProposal,
			isaacnetwork.HandlerPrefixLastSuffrageProof,
			isaacnetwork.HandlerPrefixSuffrageProof,
			isaacnetwork.HandlerPrefixLastBlockMap,
			isaacnetwork.HandlerPrefixBlockMap,
			isaacnetwork.HandlerPrefixBlockMapItem,
			handlerPrefixRequestState,
			handlerPrefixRequestExistsInStateOperation,
		} {
			_, _ = fmt.Fprintln(os.Stdout, "  -", prefix)
		}

		_, _ = fmt.Fprintln(os.Stdout, "* see isaac/network/header.go")

		return nil
	}

	log.Debug().
		Stringer("remote", cmd.Remote).
		Stringer("timeout", cmd.Timeout).
		Str("header", cmd.Header).
		Msg("flags")

	if err := cmd.prepareEncoder(); err != nil {
		return errors.Wrap(err, "")
	}

	ci, err := cmd.Remote.ConnInfo()
	if err != nil {
		return errors.Wrap(err, "")
	}

	var header isaac.NetworkHeader
	if err = encoder.Decode(cmd.enc, []byte(cmd.Header), &header); err != nil {
		return errors.Wrap(err, "")
	}

	client := launch.NewNetworkClient(cmd.encs, cmd.enc, cmd.Timeout) //nolint:gomnd //...

	ctx, cancel := context.WithTimeout(context.Background(), cmd.Timeout)
	defer cancel()

	response, v, err := client.Request(ctx, ci, header)

	b, err := util.MarshalJSON(map[string]interface{}{
		"response": response,
		"v":        v,
		"error":    err,
	})
	if err != nil {
		return errors.Wrap(err, "")
	}

	_, _ = fmt.Fprintln(os.Stdout, string(b))

	return nil
}
