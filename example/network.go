package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
)

var headerExamples = map[string]isaac.NetworkHeader{}

func init() {
	headerExamples = map[string]isaac.NetworkHeader{
		isaacnetwork.HandlerPrefixRequestProposal: isaacnetwork.NewRequestProposalRequestHeader(
			base.RawPoint(33, 1), base.NewStringAddress("proposer")), //nolint:gomnd //...
		isaacnetwork.HandlerPrefixProposal: isaacnetwork.NewProposalRequestHeader(
			valuehash.RandomSHA256()),
		isaacnetwork.HandlerPrefixLastSuffrageProof: isaacnetwork.NewLastSuffrageProofRequestHeader(
			valuehash.RandomSHA256()),
		isaacnetwork.HandlerPrefixSuffrageProof: isaacnetwork.NewSuffrageProofRequestHeader(
			base.Height(44)), //nolint:gomnd //...
		isaacnetwork.HandlerPrefixLastBlockMap: isaacnetwork.NewLastBlockMapRequestHeader(
			valuehash.RandomSHA256()),
		isaacnetwork.HandlerPrefixBlockMap: isaacnetwork.NewBlockMapRequestHeader(base.Height(33)), //nolint:gomnd //...
		isaacnetwork.HandlerPrefixBlockMapItem: isaacnetwork.NewBlockMapItemRequestHeader(
			base.Height(33), base.BlockMapItemTypeOperations), //nolint:gomnd //...
		handlerPrefixRequestState:                  newStateRequestHeader(isaac.SuffrageStateKey),
		handlerPrefixRequestExistsInStateOperation: newExistsInStateOperationRequestHeader(valuehash.RandomSHA256()),
	}
}

type networkClientCommand struct { //nolint:govet //...
	baseCommand
	Header  string              `arg:"" help:"json header"`
	Remote  launch.ConnInfoFlag `arg:"" help:"remote" placeholder:"ConnInfo" default:"localhost:4321"`
	Timeout time.Duration       `help:"timeout" placeholder:"duration" default:"10s"`
}

func (cmd *networkClientCommand) Run() error {
	log.Debug().
		Stringer("remote", cmd.Remote).
		Stringer("timeout", cmd.Timeout).
		Str("header", cmd.Header).
		Msg("flags")

	if err := cmd.prepareEncoder(); err != nil {
		return errors.Wrap(err, "")
	}

	if cmd.Header == "example" {
		_, _ = fmt.Fprintln(os.Stdout, "example headers:")

		for desc := range headerExamples {
			b, err := util.MarshalJSON(headerExamples[desc])
			if err != nil {
				return errors.Wrap(err, "")
			}

			_, _ = fmt.Fprintf(os.Stdout, " - %s:\n%s\n", desc, string(b))
		}

		_, _ = fmt.Fprintln(os.Stdout, "\n* see isaac/network/header.go")

		return nil
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
