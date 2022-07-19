package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	headerExamples     = map[string]isaac.NetworkHeader{}
	headerExamplesDesc = map[string]string{
		isaacnetwork.HandlerPrefixSendOperation: `$ cmd <header> --body=<json body>`,
	}
)

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
		isaacnetwork.HandlerPrefixNodeChallenge:        isaacnetwork.NewNodeChallengeRequestHeader(util.UUID().Bytes()),
		isaacnetwork.HandlerPrefixSuffrageNodeConnInfo: isaacnetwork.NewSuffrageNodeConnInfoRequestHeader(),
		isaacnetwork.HandlerPrefixSyncSourceConnInfo:   isaacnetwork.NewSyncSourceConnInfoRequestHeader(),
		isaacnetwork.HandlerPrefixOperation: isaacnetwork.NewOperationRequestHeader(
			valuehash.RandomSHA256()),
		isaacnetwork.HandlerPrefixSendOperation:    isaacnetwork.NewSendOperationRequestHeader(),
		HandlerPrefixRequestState:                  NewStateRequestHeader(isaac.SuffrageStateKey),
		HandlerPrefixRequestExistsInStateOperation: NewExistsInStateOperationRequestHeader(valuehash.RandomSHA256()),
	}
}

type networkClientCommand struct { //nolint:govet //...
	baseCommand
	Header  string              `arg:"" help:"json header; 'example' will print example headers"`
	Remote  launch.ConnInfoFlag `arg:"" help:"remote" placeholder:"ConnInfo" default:"localhost:4321"`
	Timeout time.Duration       `help:"timeout" placeholder:"duration" default:"10s"`
	Body    *os.File            `help:"body"`
	body    io.Reader
	remote  quicstream.UDPConnInfo
}

func (cmd *networkClientCommand) Run() error {
	if cmd.Header == "example" {
		_, _ = fmt.Fprintln(os.Stdout, "example headers:")

		for desc := range headerExamples {
			b, err := util.MarshalJSON(headerExamples[desc])
			if err != nil {
				return err
			}

			help := headerExamplesDesc[desc]
			_, _ = fmt.Fprintf(os.Stdout, "- %s: %s\n", desc, help)
			_, _ = fmt.Fprintln(os.Stdout, "   ", string(b))
		}

		_, _ = fmt.Fprintln(os.Stdout, "\n* see isaac/network/header.go")

		return nil
	}

	if err := cmd.prepare(); err != nil {
		return err
	}

	log.Debug().
		Stringer("remote", cmd.Remote).
		Stringer("timeout", cmd.Timeout).
		Str("header", cmd.Header).
		Bool("has_body", cmd.body != nil).
		Msg("flags")

	var header isaac.NetworkHeader
	if err := encoder.Decode(cmd.enc, []byte(cmd.Header), &header); err != nil {
		return err
	}

	client := launch.NewNetworkClient(cmd.encs, cmd.enc, cmd.Timeout) //nolint:gomnd //...

	ctx, cancel := context.WithTimeout(context.Background(), cmd.Timeout)
	defer cancel()

	response, v, err := client.Request(ctx, cmd.remote, header, cmd.body)

	var errstring string
	if err != nil {
		errstring = err.Error()
	}

	b, err := util.MarshalJSON(map[string]interface{}{
		"response": response,
		"v":        v,
		"error":    errstring,
	})
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintln(os.Stdout, string(b))

	return nil
}

func (cmd *networkClientCommand) prepare() error {
	if cmd.Body != nil {
		cmd.body = cmd.Body
	}

	if err := cmd.prepareEncoder(); err != nil {
		return err
	}

	ci, err := cmd.Remote.ConnInfo()
	if err != nil {
		return err
	}

	cmd.remote = ci

	return nil
}
