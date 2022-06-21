package main

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

var networkHandlerIdleTimeout = time.Second * 10

var (
	handlerPrefixRequestState                  = "state"
	handlerPrefixRequestExistsInStateOperation = "exists_instate_operation"
)

func (cmd *runCommand) prepareNetwork() error {
	cmd.client = launch.NewNetworkClient(cmd.encs, cmd.enc, time.Second*2) //nolint:gomnd //...

	cmd.quicstreamserver = quicstream.NewServer(
		cmd.design.Network.Bind,
		launch.GenerateNewTLSConfig(),
		launch.DefaultQuicConfig(),
		cmd.networkHandlers(),
	)
	_ = cmd.quicstreamserver.SetLogging(logging)

	return nil
}

func (cmd *runCommand) networkHandlers() quicstream.Handler {
	handlers := isaacnetwork.NewQuicstreamHandlers(
		cmd.local,
		cmd.encs,
		cmd.enc,
		networkHandlerIdleTimeout,
		cmd.pool,
		cmd.proposalMaker(),
		func(last util.Hash) (base.SuffrageProof, bool, error) {
			switch proof, found, err := cmd.db.LastSuffrageProof(); {
			case err != nil:
				return nil, false, errors.Wrap(err, "")
			case !found:
				return nil, false, storage.NotFoundError.Errorf("last SuffrageProof not found")
			case last != nil && last.Equal(proof.Map().Manifest().Suffrage()):
				return nil, false, nil
			default:
				return proof, true, nil
			}
		},
		cmd.db.SuffrageProof,
		func(last util.Hash) (base.BlockMap, bool, error) {
			switch m, found, err := cmd.db.LastBlockMap(); {
			case err != nil:
				return nil, false, errors.Wrap(err, "")
			case !found:
				return nil, false, storage.NotFoundError.Errorf("last BlockMap not found")
			case last != nil && last.Equal(m.Manifest().Hash()):
				return nil, false, nil
			default:
				return m, true, nil
			}
		},
		cmd.db.BlockMap,
		func(height base.Height, item base.BlockMapItemType) (io.ReadCloser, bool, error) {
			e := util.StringErrorFunc("failed to get BlockMapItem")

			var enc encoder.Encoder

			switch m, found, err := cmd.db.BlockMap(height); {
			case err != nil:
				return nil, false, e(err, "")
			case !found:
				return nil, false, e(storage.NotFoundError.Errorf("BlockMap not found"), "")
			default:
				enc = cmd.encs.Find(m.Encoder())
			}

			// FIXME use cache with singleflight

			reader, err := isaacblock.NewLocalFSReaderFromHeight(
				launch.LocalFSDataDirectory(cmd.design.Storage.Base), height, enc,
			)
			if err != nil {
				return nil, false, e(err, "")
			}
			defer func() {
				_ = reader.Close()
			}()

			return reader.Reader(item)
		},
	)

	prefix := launch.Handlers(handlers)

	cmd.addNetworkHandlers(prefix)

	return prefix.Handler
}

func (cmd *runCommand) addNetworkHandlers(prefix *quicstream.PrefixHandler) {
	prefix.
		Add(handlerPrefixRequestState, cmd.networkHandlerState).
		Add(handlerPrefixRequestExistsInStateOperation, cmd.networkHandlerExistsInStateOperation)
}

func (cmd *runCommand) networkHandlerState(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle get state by key")

	ctx, cancel := context.WithTimeout(context.Background(), networkHandlerIdleTimeout)
	defer cancel()

	enc, err := isaacnetwork.ReadEncoder(ctx, cmd.encs, r)
	if err != nil {
		return e(err, "")
	}

	var body stateRequestHeader
	if err = encoder.DecodeReader(enc, r, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	st, found, err := cmd.db.State(body.key)
	header := isaacnetwork.NewResponseHeader(found, err)

	if err := isaacnetwork.Response(w, header, st, enc); err != nil {
		return e(err, "")
	}

	return nil
}

func (cmd *runCommand) networkHandlerExistsInStateOperation(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle exists instate operation")

	ctx, cancel := context.WithTimeout(context.Background(), networkHandlerIdleTimeout)
	defer cancel()

	enc, err := isaacnetwork.ReadEncoder(ctx, cmd.encs, r)
	if err != nil {
		return e(err, "")
	}

	var body existsInStateOperationRequestHeader
	if err = encoder.DecodeReader(enc, r, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	found, err := cmd.db.ExistsInStateOperation(body.op)
	header := isaacnetwork.NewResponseHeader(found, err)

	if err := isaacnetwork.Response(w, header, nil, enc); err != nil {
		return e(err, "")
	}

	return nil
}
