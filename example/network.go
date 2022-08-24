package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
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
	headerExamplesKeys []string
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
		isaacnetwork.HandlerPrefixSendOperation: isaacnetwork.NewSendOperationRequestHeader(),
		isaacnetwork.HandlerPrefixState: isaacnetwork.NewStateRequestHeader(
			isaac.SuffrageStateKey, valuehash.RandomSHA256()),
		isaacnetwork.HandlerPrefixExistsInStateOperation: isaacnetwork.NewExistsInStateOperationRequestHeader(
			valuehash.RandomSHA256()),
		isaacnetwork.HandlerPrefixNodeInfo: isaacnetwork.NewNodeInfoRequestHeader(),
		launch.HandlerPrefixPprof: launch.NewPprofRequestHeader(
			"heap", 5, true), //nolint:gomnd //...
	}

	headerExamplesKeys = make([]string, len(headerExamples))

	var i int

	for k := range headerExamples {
		headerExamplesKeys[i] = k
		i++
	}

	sort.Slice(headerExamplesKeys, func(i, j int) bool {
		return strings.Compare(headerExamplesKeys[i], headerExamplesKeys[j]) < 0
	})
}

type NetworkClientCommand struct { //nolint:govet //...
	baseCommand
	NetworkID string              `arg:"" name:"network-id" help:"network-id"`
	Header    string              `arg:"" help:"request header; 'example' will print example headers"`
	Remote    launch.ConnInfoFlag `arg:"" help:"remote node conn info" placeholder:"ConnInfo" default:"localhost:4321"`
	Timeout   time.Duration       `help:"timeout" placeholder:"duration" default:"10s"`
	Body      *os.File            `help:"body"`
	DryRun    bool                `name:"dry-run" help:"don't send"`
	body      io.Reader
	remote    quicstream.UDPConnInfo
}

func (cmd *NetworkClientCommand) Run(pctx context.Context) error {
	if cmd.Header == "example" {
		_, _ = fmt.Fprintln(os.Stdout, "example headers:")

		for i := range headerExamplesKeys {
			k := headerExamplesKeys[i]

			b, err := util.MarshalJSON(headerExamples[k])
			if err != nil {
				return err
			}

			help := headerExamplesDesc[k]
			_, _ = fmt.Fprintf(os.Stdout, "- %s: %s\n", k, help)
			_, _ = fmt.Fprintln(os.Stdout, "   ", string(b))
		}

		_, _ = fmt.Fprintln(os.Stdout, "\n* see isaac/network/header.go")

		return nil
	}

	if err := cmd.prepare(pctx); err != nil {
		return err
	}

	cmd.log.Debug().
		Stringer("remote", cmd.Remote).
		Stringer("timeout", cmd.Timeout).
		Str("network_id", cmd.NetworkID).
		Str("header", cmd.Header).
		Bool("has_body", cmd.body != nil).
		Msg("flags")

	var header isaac.NetworkHeader
	if err := encoder.Decode(cmd.enc, []byte(cmd.Header), &header); err != nil {
		return errors.WithMessage(err, "failed to load header")
	}

	if cmd.DryRun {
		return cmd.dryRun(header)
	}

	return cmd.response(header)
}

func (cmd *NetworkClientCommand) response(header isaac.NetworkHeader) error {
	client := launch.NewNetworkClient( //nolint:gomnd //...
		cmd.encs, cmd.enc, cmd.Timeout,
		base.NetworkID([]byte(cmd.NetworkID)),
	)
	defer func() {
		if err := client.Close(); err != nil {
			cmd.log.Error().Err(err).Msg("failed to close client")
		}
	}()

	response, v, cancel, err := client.Request(context.Background(), cmd.remote, header, cmd.body)

	switch {
	case err != nil:
		cmd.log.Error().Err(err).Interface("response", response).Msg("got respond")

		return err
	case response.Err() != nil:
		cmd.log.Error().Err(err).Interface("response", response).Msg("got respond")

		return err
	}

	defer func() {
		_ = cancel()
	}()

	cmd.log.Info().Interface("response", response).Msg("got respond")
	cmd.log.Trace().Interface("response", response).Interface("body", v).Msg("got respond")

	switch response.Type() {
	case isaac.NetworkResponseHinterContentType:
		b, err := util.MarshalJSONIndent(v)
		if err != nil {
			return err
		}

		_, _ = fmt.Fprintln(os.Stdout, string(b))
	case isaac.NetworkResponseRawContentType:
		r, ok := v.(io.Reader)
		if !ok {
			return errors.Errorf("expected io.Reader, but %T", v)
		}

		_, err := io.Copy(os.Stdout, r)
		if err != nil {
			return errors.Wrap(err, "")
		}
	}

	return nil
}

func (cmd *NetworkClientCommand) prepare(pctx context.Context) error {
	if _, err := cmd.baseCommand.prepare(pctx); err != nil {
		return err
	}

	if cmd.Body != nil {
		buf := bytes.NewBuffer(nil)

		if _, err := io.Copy(buf, cmd.Body); err != nil {
			return errors.Wrap(err, "")
		}

		cmd.body = buf
	}

	ci, err := cmd.Remote.ConnInfo()
	if err != nil {
		return err
	}

	cmd.remote = ci

	if cmd.Timeout < 1 {
		cmd.Timeout = time.Second * 5 //nolint:gomnd //...
	}

	return nil
}

func (cmd *NetworkClientCommand) dryRun(header isaac.NetworkHeader) error {
	hb, err := util.MarshalJSONIndent(header)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(os.Stdout, "header: %s\n", string(hb))

	if cmd.body != nil {
		raw, err := io.ReadAll(cmd.body)
		if err != nil {
			return errors.Wrap(err, "")
		}

		var u interface{}

		if err = util.UnmarshalJSON(raw, &u); err != nil {
			return err
		}

		bb, err := util.MarshalJSONIndent(u)
		if err != nil {
			return err
		}

		_, _ = fmt.Fprintf(os.Stdout, "body: %s\n", string(bb))
	}

	return nil
}
