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
	var log *logging.Logging
	var local base.LocalNode
	var params *LocalParams
	var states *isaacstates.States

	if err := util.LoadFromContext(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		StatesContextKey, &states,
	); err != nil {
		return pctx, err
	}

	if err := attachHandlerOperation(pctx); err != nil {
		return pctx, err
	}

	if err := attachHandlerSendOperation(pctx); err != nil {
		return pctx, err
	}

	if err := attachHandlerStreamOperations(pctx); err != nil {
		return pctx, err
	}

	if err := attachHandlerProposals(pctx); err != nil {
		return pctx, err
	}

	var gerror error

	ensureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerPrefixSetAllowConsensusString,
		isaacnetwork.QuicstreamHandlerSetAllowConsensus(
			local.Publickey(),
			params.ISAAC.NetworkID(),
			states.SetAllowConsensus,
		),
		nil,
	)

	return pctx, gerror
}

func attachHandlerOperation(pctx context.Context) error {
	var log *logging.Logging
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var params *LocalParams
	var pool *isaacdatabase.TempPool
	var client *isaacnetwork.BaseClient
	var connectionPool *quicstream.ConnectionPool
	var states *isaacstates.States

	if err := util.LoadFromContext(pctx,
		LoggingContextKey, &log,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		LocalParamsContextKey, &params,
		PoolDatabaseContextKey, &pool,
		QuicstreamClientContextKey, &client,
		ConnectionPoolContextKey, &connectionPool,
		StatesContextKey, &states,
	); err != nil {
		return err
	}

	headerdial := quicstreamheader.NewDialFunc(connectionPool.Dial, encs, enc)

	var gerror error

	ensureHandlerAdd(pctx, &gerror,
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

				stream, _, err := headerdial(ctx, ci)
				if err != nil {
					return enchint, body, found, err
				}

				err = stream(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
					if ok, rerr := isaacnetwork.HCReqResBodyDecOK(
						ctx,
						broker,
						header,
						func(enc encoder.Encoder, r io.Reader) error {
							enchint = enc.Hint().String()

							switch b, rerr := io.ReadAll(r); {
							case rerr != nil:
								return errors.WithStack(rerr)
							default:
								body = b
							}

							found = true

							return nil
						},
					); rerr != nil || !ok {
						return rerr
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

func attachHandlerSendOperation(pctx context.Context) error {
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

	sendOperationFilterf, err := sendOperationFilterFunc(pctx)
	if err != nil {
		return err
	}

	var gerror error

	ensureHandlerAdd(pctx, &gerror,
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

func attachHandlerStreamOperations(pctx context.Context) error {
	var log *logging.Logging
	var local base.LocalNode
	var params *LocalParams
	var pool *isaacdatabase.TempPool

	if err := util.LoadFromContext(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		PoolDatabaseContextKey, &pool,
	); err != nil {
		return err
	}

	var gerror error

	ensureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerPrefixStreamOperationsString,
		isaacnetwork.QuicstreamHandlerStreamOperations(
			local.Publickey(),
			params.ISAAC.NetworkID(),
			333, //nolint:gomnd // big enough
			func(
				ctx context.Context,
				offset []byte,
				callback func(string, isaacdatabase.PoolOperationRecordMeta, []byte, []byte) (bool, error),
			) error {
				return pool.TraverseOperationsBytes(ctx, offset,
					func(
						enchint string,
						meta isaacdatabase.PoolOperationRecordMeta,
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

func attachHandlerProposals(pctx context.Context) error {
	var log *logging.Logging
	var enc encoder.Encoder
	var local base.LocalNode
	var params *LocalParams
	var states *isaacstates.States
	var pool *isaacdatabase.TempPool
	var proposalMaker *isaac.ProposalMaker
	var db isaac.Database
	var client isaac.NetworkClient

	if err := util.LoadFromContext(pctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		StatesContextKey, &states,
		PoolDatabaseContextKey, &pool,
		ProposalMakerContextKey, &proposalMaker,
		CenterDatabaseContextKey, &db,
		QuicstreamClientContextKey, &client,
	); err != nil {
		return err
	}

	var gerror error

	ensureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerPrefixRequestProposalString,
		isaacnetwork.QuicstreamHandlerRequestProposal(
			local, pool, proposalMaker, db.LastBlockMap,
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

	ensureHandlerAdd(pctx, &gerror,
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
					b, err := enc.Marshal(pr)
					if err != nil {
						return "", nil, false, err
					}

					return enc.Hint().String(), b, true, nil
				}
			},
		),
		nil,
	)

	return gerror
}
