package launch

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var PNameStatesNetworkHandlers = ps.Name("states-network-handlers")

func PStatesNetworkHandlers(pctx context.Context) (context.Context, error) {
	if err := AttachHandlerOperation(pctx); err != nil {
		return pctx, err
	}

	if err := AttachHandlerSendOperation(pctx); err != nil {
		return pctx, err
	}

	if err := AttachHandlerStreamOperations(pctx); err != nil {
		return pctx, err
	}

	if err := AttachHandlerProposals(pctx); err != nil {
		return pctx, err
	}

	return pctx, nil
}

func AttachHandlerOperation(pctx context.Context) error {
	var encs *encoder.Encoders
	var pool *isaacdatabase.TempPool
	var connectionPool *quicstream.ConnectionPool
	var states *isaacstates.States

	if err := util.LoadFromContext(pctx,
		EncodersContextKey, &encs,
		PoolDatabaseContextKey, &pool,
		ConnectionPoolContextKey, &connectionPool,
		StatesContextKey, &states,
	); err != nil {
		return err
	}

	headerdial := quicstreamheader.NewDialFunc(connectionPool.Dial, encs, encs.Default())

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerPrefixOperationString,
		isaacnetwork.QuicstreamHandlerOperation(
			pool,
			func(ctx context.Context, header isaacnetwork.OperationRequestHeader) (
				enchint string, body []byte, found bool, _ error,
			) {
				var ci quicstream.ConnInfo

				switch xbroker := states.HandoverYBroker(); {
				case xbroker == nil:
					return enchint, nil, false, nil
				default:
					ci = xbroker.ConnInfo()
				}

				var stream quicstreamheader.StreamFunc

				switch i, _, err := headerdial(ctx, ci); {
				case err != nil:
					return enchint, body, found, err
				default:
					stream = i
				}

				err := stream(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
					if ok, err := isaacnetwork.HCReqResBodyDecOK(
						ctx,
						broker,
						header,
						func(enc encoder.Encoder, r io.Reader) error {
							enchint = enc.Hint().String()

							switch b, err := io.ReadAll(r); {
							case err != nil:
								return errors.WithStack(err)
							default:
								body = b
							}

							found = true

							return nil
						},
					); err != nil || !ok {
						return err
					}

					return nil
				})

				return enchint, body, found, err
			},
		),
		nil,
	)

	return gerror
}

func AttachHandlerSendOperation(pctx context.Context) error {
	var log *logging.Logging
	var params *LocalParams
	var db isaac.Database
	var pool *isaacdatabase.TempPool
	var states *isaacstates.States
	var svvotef isaac.SuffrageVoteFunc
	var memberlist *quicmemberlist.Memberlist

	if err := util.LoadFromContext(pctx,
		LoggingContextKey, &log,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
		PoolDatabaseContextKey, &pool,
		StatesContextKey, &states,
		SuffrageVotingVoteFuncContextKey, &svvotef,
		MemberlistContextKey, &memberlist,
	); err != nil {
		return err
	}

	sendOperationFilterf, err := SendOperationFilterFunc(pctx)
	if err != nil {
		return err
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerPrefixSendOperationString,
		isaacnetwork.QuicstreamHandlerSendOperation(
			params.ISAAC.NetworkID(),
			pool,
			db.ExistsInStateOperation,
			sendOperationFilterf,
			svvotef,
			func(ctx context.Context, id string, op base.Operation, b []byte) error {
				if broker := states.HandoverXBroker(); broker != nil {
					if err := broker.SendData(ctx, isaacstates.HandoverMessageDataTypeOperation, op); err != nil {
						log.Log().Error().Err(err).
							Interface("operation", op.Hash()).
							Msg("failed to send operation data to handover y broker; ignored")
					}
				}

				return memberlist.CallbackBroadcast(b, id, nil)
			},
			params.MISC.MaxMessageSize,
		),
		nil,
	)

	return gerror
}

func AttachHandlerStreamOperations(pctx context.Context) error {
	var local base.LocalNode
	var params *LocalParams
	var pool *isaacdatabase.TempPool

	if err := util.LoadFromContext(pctx,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		PoolDatabaseContextKey, &pool,
	); err != nil {
		return err
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerPrefixStreamOperationsString,
		isaacnetwork.QuicstreamHandlerStreamOperations(
			local.Publickey(),
			params.ISAAC.NetworkID(),
			333, //nolint:gomnd // big enough
			func(
				ctx context.Context,
				offset []byte,
				callback func(string, isaacdatabase.FrameHeaderPoolOperation, []byte, []byte) (bool, error),
			) error {
				return pool.TraverseOperationsBytes(ctx, offset,
					func(
						enchint string,
						meta isaacdatabase.FrameHeaderPoolOperation,
						body,
						offset []byte,
					) (bool, error) {
						return callback(enchint, meta, body, offset)
					},
				)
			},
		),
		nil,
	)

	return gerror
}

func AttachHandlerProposals(pctx context.Context) error {
	var encs *encoder.Encoders
	var local base.LocalNode
	var states *isaacstates.States
	var pool *isaacdatabase.TempPool
	var proposalMaker *isaac.ProposalMaker
	var db isaac.Database
	var client isaac.NetworkClient

	if err := util.LoadFromContext(pctx,
		EncodersContextKey, &encs,
		LocalContextKey, &local,
		StatesContextKey, &states,
		PoolDatabaseContextKey, &pool,
		ProposalMakerContextKey, &proposalMaker,
		CenterDatabaseContextKey, &db,
		QuicstreamClientContextKey, &client,
	); err != nil {
		return err
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerPrefixRequestProposalString,
		isaacnetwork.QuicstreamHandlerRequestProposal(
			local.Address(), pool, proposalMaker,
			func(ctx context.Context, header isaacnetwork.RequestProposalRequestHeader) (
				base.ProposalSignFact, error,
			) {
				var connInfo quicstream.ConnInfo

				switch broker := states.HandoverYBroker(); {
				case broker == nil:
					return nil, nil
				default:
					connInfo = broker.ConnInfo()
				}

				switch pr, _, err := client.RequestProposal(ctx, connInfo,
					header.Point(),
					header.Proposer(),
					header.PreviousBlock(),
				); {
				case err != nil:
					return nil, err
				default:
					return pr, nil
				}
			},
		), nil,
	)

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerPrefixProposalString,
		isaacnetwork.QuicstreamHandlerProposal(
			pool,
			func(ctx context.Context, header isaacnetwork.ProposalRequestHeader) (string, []byte, bool, error) {
				var connInfo quicstream.ConnInfo

				switch broker := states.HandoverYBroker(); {
				case broker == nil:
					return "", nil, false, nil
				default:
					connInfo = broker.ConnInfo()
				}

				switch pr, _, err := client.Proposal(ctx, connInfo, header.Proposal()); {
				case err != nil:
					return "", nil, false, err
				case pr == nil:
					return "", nil, false, nil
				default:
					b, err := encs.Default().Marshal(pr)
					if err != nil {
						return "", nil, false, err
					}

					return encs.Default().Hint().String(), b, true, nil
				}
			},
		),
		nil,
	)

	return gerror
}
