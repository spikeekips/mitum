package launch

import (
	"context"
	"io"
	"time"

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
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var PNameStatesNetworkHandlers = ps.Name("states-network-handlers")

func PStatesNetworkHandlers(pctx context.Context) (context.Context, error) {
	var encs *encoder.Encoders
	var local base.LocalNode
	var params *isaac.LocalParams
	var handlers *quicstream.PrefixHandler
	var states *isaacstates.States

	if err := util.LoadFromContext(pctx,
		EncodersContextKey, &encs,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		QuicstreamHandlersContextKey, &handlers,
		StatesContextKey, &states,
	); err != nil {
		return pctx, err
	}

	if err := attachHandlerOperation(pctx, handlers); err != nil {
		return pctx, err
	}

	if err := attachHandlerSendOperation(pctx, handlers); err != nil {
		return pctx, err
	}

	if err := attachHandlerStreamOperations(pctx, handlers); err != nil {
		return pctx, err
	}

	if err := attachHandlerProposals(pctx, handlers); err != nil {
		return pctx, err
	}

	handlers.
		Add(isaacnetwork.HandlerPrefixSetAllowConsensus,
			quicstreamheader.NewHandler(encs,
				time.Second*2, //nolint:gomnd //...
				isaacnetwork.QuicstreamHandlerSetAllowConsensus(
					local.Publickey(),
					params.NetworkID(),
					states.SetAllowConsensus,
				),
				nil,
			),
		)

	return pctx, nil
}

func attachHandlerOperation(pctx context.Context, handlers *quicstream.PrefixHandler) error {
	var encs *encoder.Encoders
	var pool *isaacdatabase.TempPool
	var client *isaacnetwork.QuicstreamClient
	var states *isaacstates.States

	if err := util.LoadFromContext(pctx,
		EncodersContextKey, &encs,
		PoolDatabaseContextKey, &pool,
		QuicstreamClientContextKey, &client,
		StatesContextKey, &states,
	); err != nil {
		return err
	}

	handlers.Add(isaacnetwork.HandlerPrefixOperation, quicstreamheader.NewHandler(encs, 0,
		isaacnetwork.QuicstreamHandlerOperation(
			pool,
			func(ctx context.Context, header isaacnetwork.OperationRequestHeader) (
				enchint hint.Hint, body []byte, found bool, _ error,
			) {
				var ci quicstream.UDPConnInfo

				switch xbroker := states.HandoverXBroker(); {
				case xbroker == nil:
					return enchint, nil, false, nil
				default:
					ci = xbroker.ConnInfo()
				}

				if ok, err := isaacnetwork.HCReqResBodyDecOK(
					ctx,
					ci,
					header,
					client.Broker,
					func(enc encoder.Encoder, r io.Reader) error {
						enchint = enc.Hint()

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
					return enchint, nil, ok, err
				}

				return enchint, body, found, nil
			},
		),
		nil))

	return nil
}

func attachHandlerSendOperation(pctx context.Context, handlers *quicstream.PrefixHandler) error {
	var log *logging.Logging
	var encs *encoder.Encoders
	var params *isaac.LocalParams
	var db isaac.Database
	var pool *isaacdatabase.TempPool
	var states *isaacstates.States
	var svvotef isaac.SuffrageVoteFunc
	var memberlist *quicmemberlist.Memberlist

	if err := util.LoadFromContext(pctx,
		LoggingContextKey, &log,
		EncodersContextKey, &encs,
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

	handlers.Add(isaacnetwork.HandlerPrefixSendOperation, quicstreamheader.NewHandler(encs, 0,
		isaacnetwork.QuicstreamHandlerSendOperation(
			params,
			pool,
			db.ExistsInStateOperation,
			sendOperationFilterf,
			svvotef,
			func(ctx context.Context, id string, op base.Operation, b []byte) error {
				switch broker := states.HandoverXBroker(); {
				case broker == nil:
				default:
					if err := broker.SendData(ctx, isaacstates.HandoverMessageDataTypeOperation, op); err != nil {
						log.Log().Error().Err(err).
							Interface("operation", op.Hash()).
							Msg("failed to send operation data to handover y broker; ignored")
					}
				}

				return memberlist.CallbackBroadcast(b, id, nil)
			},
		),
		nil))

	return nil
}

func attachHandlerStreamOperations(pctx context.Context, handlers *quicstream.PrefixHandler) error {
	var encs *encoder.Encoders
	var local base.LocalNode
	var params *isaac.LocalParams
	var pool *isaacdatabase.TempPool

	if err := util.LoadFromContext(pctx,
		EncodersContextKey, &encs,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		PoolDatabaseContextKey, &pool,
	); err != nil {
		return err
	}

	handlers.Add(isaacnetwork.HandlerPrefixStreamOperations, quicstreamheader.NewHandler(encs, 0,
		isaacnetwork.QuicstreamHandlerStreamOperations(
			local.Publickey(),
			params.NetworkID(),
			func(ctx context.Context, offset []byte) (
				func(context.Context) (hint.Hint, []byte, []byte, error),
				func(),
			) {
				ch := make(chan [3]interface{}, 1)

				nctx, cancel := context.WithCancel(ctx)

				go func() {
					defer cancel()

					_ = pool.TraverseOperationsBytes(nctx, offset,
						func(meta isaacdatabase.PoolOperationRecordMeta, body []byte, offset []byte) (bool, error) {
							if nctx.Err() != nil {
								return false, nctx.Err()
							}

							ch <- [3]interface{}{meta.Hint(), body, offset}

							return true, nil
						},
					)
				}()

				return func(ctx context.Context) (enchint hint.Hint, body, offset []byte, _ error) {
					select {
					case <-ctx.Done():
						return enchint, nil, nil, isaacnetwork.ErrNoMoreNext.WithStack()
					case <-nctx.Done():
						return enchint, nil, nil, isaacnetwork.ErrNoMoreNext.WithStack()
					case i := <-ch:
						return i[0].(hint.Hint), //nolint:forcetypeassert //...
							i[1].([]byte), //nolint:forcetypeassert //...
							i[2].([]byte), //nolint:forcetypeassert //...
							nil
					}
				}, cancel
			},
		),
		nil))

	return nil
}

func attachHandlerProposals(pctx context.Context, handlers *quicstream.PrefixHandler) error {
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var local base.LocalNode
	var states *isaacstates.States
	var pool *isaacdatabase.TempPool
	var proposalMaker *isaac.ProposalMaker
	var db isaac.Database
	var client isaac.NetworkClient

	if err := util.LoadFromContext(pctx,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		LocalContextKey, &local,
		StatesContextKey, &states,
		PoolDatabaseContextKey, &pool,
		ProposalMakerContextKey, &proposalMaker,
		CenterDatabaseContextKey, &db,
		QuicstreamClientContextKey, &client,
	); err != nil {
		return err
	}

	handlers.
		Add(isaacnetwork.HandlerPrefixRequestProposal, quicstreamheader.NewHandler(encs, 0,
			isaacnetwork.QuicstreamHandlerRequestProposal(
				local, pool, proposalMaker, db.LastBlockMap,
				func(ctx context.Context, header isaacnetwork.RequestProposalRequestHeader) (
					base.ProposalSignFact, error,
				) {
					var connInfo quicstream.UDPConnInfo

					switch broker := states.HandoverXBroker(); {
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
			), nil)).
		Add(isaacnetwork.HandlerPrefixProposal, quicstreamheader.NewHandler(encs, 0,
			isaacnetwork.QuicstreamHandlerProposal(
				pool,
				func(ctx context.Context, header isaacnetwork.ProposalRequestHeader) (hint.Hint, []byte, bool, error) {
					var connInfo quicstream.UDPConnInfo

					switch broker := states.HandoverXBroker(); {
					case broker == nil:
						return hint.Hint{}, nil, false, nil
					default:
						connInfo = broker.ConnInfo()
					}

					switch pr, _, err := client.Proposal(ctx, connInfo, header.Proposal()); {
					case err != nil:
						return hint.Hint{}, nil, false, err
					case pr == nil:
						return hint.Hint{}, nil, false, nil
					default:
						b, err := enc.Marshal(pr)
						if err != nil {
							return hint.Hint{}, nil, false, err
						}

						return enc.Hint(), b, true, nil
					}
				},
			), nil))

	return nil
}
