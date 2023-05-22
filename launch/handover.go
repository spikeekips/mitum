package launch

import (
	"context"
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
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var PNameHandoverNetworkHandlers = ps.Name("handover-network-handlers")

func patchStatesArgsForHandover(pctx context.Context, args *isaacstates.StatesArgs) (context.Context, error) {
	{
		var err error

		if args.NewHandoverXBroker, err = newHandoverXBrokerFunc(pctx); err != nil {
			return pctx, err
		}

		if args.NewHandoverYBroker, err = newHandoverYBrokerFunc(pctx); err != nil {
			return pctx, err
		}
	}

	return pctx, nil
}

func newHandoverXBrokerFunc(pctx context.Context) (isaacstates.NewHandoverXBrokerFunc, error) {
	var log *logging.Logging
	var local base.LocalNode
	var params *isaac.LocalParams
	var client *isaacnetwork.QuicstreamClient
	var pool *isaacdatabase.TempPool
	var memberlist *quicmemberlist.Memberlist

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		QuicstreamClientContextKey, &client,
		PoolDatabaseContextKey, &pool,
		MemberlistContextKey, &memberlist,
	); err != nil {
		return nil, err
	}

	args := isaacstates.NewHandoverXBrokerArgs(local, params.NetworkID())

	args.SendMessageFunc = func(ctx context.Context, ci quicstream.UDPConnInfo, msg isaacstates.HandoverMessage) error {
		return client.HandoverMessage(ctx, ci, msg)
	}
	args.CheckIsReady = func() (bool, error) { return true, nil }
	args.WhenCanceled = func(err error) {
		log.Log().Debug().Err(err).Msg("handover x canceled")
	}

	whenFinished := isaacstates.NewHandoverXFinishedFunc(
		func() error {
			return memberlist.Leave(time.Second * 33) //nolint:gomnd // long enough
		},
	)
	args.WhenFinished = func(vp base.INITVoteproof) error {
		log.Log().Debug().Interface("init_voteproof", vp).Msg("handover x finished")

		return whenFinished(vp)
	}

	args.GetProposal = func(facthash util.Hash) (base.ProposalSignFact, bool, error) {
		return pool.Proposal(facthash)
	}

	return func(ctx context.Context, yci quicstream.UDPConnInfo) (*isaacstates.HandoverXBroker, error) {
		return isaacstates.NewHandoverXBroker(ctx, args, yci), nil
	}, nil
}

//revive:disable:function-length

func newHandoverYBrokerFunc(pctx context.Context) (isaacstates.NewHandoverYBrokerFunc, error) {
	var log *logging.Logging
	var design NodeDesign
	var local base.LocalNode
	var params *isaac.LocalParams
	var client *isaacnetwork.QuicstreamClient
	var pool *isaacdatabase.TempPool
	var long *LongRunningMemberlistJoin
	var svvotef isaac.SuffrageVoteFunc
	var memberlist *quicmemberlist.Memberlist
	var syncSourcePool *isaac.SyncSourcePool
	var ballotbox *isaacstates.Ballotbox

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		QuicstreamClientContextKey, &client,
		PoolDatabaseContextKey, &pool,
		LongRunningMemberlistJoinContextKey, &long,
		SuffrageVotingVoteFuncContextKey, &svvotef,
		MemberlistContextKey, &memberlist,
		SyncSourcePoolContextKey, &syncSourcePool,
		BallotboxContextKey, &ballotbox,
	); err != nil {
		return nil, err
	}

	localci := quicstream.NewUDPConnInfo(design.Network.Publish(), design.Network.TLSInsecure)

	args := isaacstates.NewHandoverYBrokerArgs(params.NetworkID())

	args.SendMessageFunc = func(ctx context.Context, ci quicstream.UDPConnInfo, msg isaacstates.HandoverMessage) error {
		return client.HandoverMessage(ctx, ci, msg)
	}
	args.NewDataFunc = func(d isaacstates.HandoverMessageDataType, i interface{}) error {
		switch d {
		case isaacstates.HandoverMessageDataTypeVoteproof,
			isaacstates.HandoverMessageDataTypeINITVoteproof:
		case isaacstates.HandoverMessageDataTypeProposal:
			pr, ok := i.(base.ProposalSignFact)
			if !ok {
				return errors.Errorf("expected ProposalSignFact, but %T", i)
			}

			_, err := pool.SetProposal(pr)

			return err
		case isaacstates.HandoverMessageDataTypeOperation,
			isaacstates.HandoverMessageDataTypeSuffrageVoting:
			op, ok := i.(base.Operation)
			if !ok {
				return errors.Errorf("expected Operation, but %T", i)
			}

			switch t := op.(type) {
			case base.SuffrageExpelOperation:
				_, err := svvotef(t)

				return err
			default:
				_, err := pool.SetOperation(context.Background(), t)

				return err
			}
		case isaacstates.HandoverMessageDataTypeBallot:
			ballot, ok := i.(base.Ballot)
			if !ok {
				return errors.Errorf("expected Ballot, but %T", i)
			}

			_, err := ballotbox.Vote(ballot)

			return err
		default:
			return errors.Errorf("unknown data type, %v", d)
		}

		return nil
	}

	whenFinished := isaacstates.NewHandoverYFinishedFunc(
		func() error {
			return memberlist.Leave(time.Second * 33) //nolint:gomnd // long enough
		},
		func(xci quicstream.UDPConnInfo) error {
			nci := isaacnetwork.NewNodeConnInfo(
				isaac.NewNode(local.Publickey(), local.Address()),
				xci.Addr().String(),
				xci.TLSInsecure(),
			)

			_ = syncSourcePool.RemoveNonFixed(nci)

			return nil
		},
	)
	args.WhenFinished = func(vp base.INITVoteproof, xci quicstream.UDPConnInfo) error {
		log.Log().Debug().Interface("init_voteproof", vp).Msg("handover x finished")

		return whenFinished(vp, xci)
	}

	whenCanceled := isaacstates.NewHandoverYCanceledFunc(
		func() error {
			return memberlist.Leave(time.Second * 33) //nolint:gomnd // long enough
		},
		func(xci quicstream.UDPConnInfo) error {
			nci := isaacnetwork.NewNodeConnInfo(
				isaac.NewNode(local.Publickey(), local.Address()),
				xci.Addr().String(),
				xci.TLSInsecure(),
			)

			_ = syncSourcePool.RemoveNonFixed(nci)

			return nil
		},
	)

	args.WhenCanceled = func(err error, xci quicstream.UDPConnInfo) {
		log.Log().Debug().Err(err).Msg("handover x canceled")

		whenCanceled(err, xci)
	}
	args.AskRequestFunc = isaacstates.NewAskHandoverFunc(
		local.Address(),
		func(ctx context.Context, ci quicstream.UDPConnInfo) error {
			donech := long.Join()
			if donech == nil {
				return nil
			}

			select {
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "join")
			case <-donech:
				return nil
			}
		},
		func(ctx context.Context, x base.Address, xci quicstream.UDPConnInfo) (string, bool, error) {
			return client.AskHandover(ctx, xci, local.Privatekey(), params.NetworkID(), local.Address(), localci)
		},
	)

	return func(ctx context.Context, xci quicstream.UDPConnInfo) (*isaacstates.HandoverYBroker, error) {
		return isaacstates.NewHandoverYBroker(ctx, args, xci), nil
	}, nil
}

//revive:enable:function-length

func PHandoverNetworkHandlers(pctx context.Context) (context.Context, error) {
	var design NodeDesign
	var encs *encoder.Encoders
	var local base.LocalNode
	var params *isaac.LocalParams
	var handlers *quicstream.PrefixHandler
	var states *isaacstates.States
	var client *isaacnetwork.QuicstreamClient
	var syncSourcePool *isaac.SyncSourcePool
	var memberlist *quicmemberlist.Memberlist

	if err := util.LoadFromContext(pctx,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		QuicstreamHandlersContextKey, &handlers,
		StatesContextKey, &states,
		QuicstreamClientContextKey, &client,
		SyncSourcePoolContextKey, &syncSourcePool,
		MemberlistContextKey, &memberlist,
	); err != nil {
		return pctx, err
	}

	localci := quicstream.NewUDPConnInfo(design.Network.Publish(), design.Network.TLSInsecure)

	if err := attachStartHandoverHandler(handlers, encs, local, params, localci,
		states, client, syncSourcePool,
	); err != nil {
		return pctx, err
	}

	if err := attachCancelHandoverHandler(handlers, encs, local, params, states); err != nil {
		return pctx, err
	}

	if err := attachCheckHandoverHandler(handlers, encs, local, params, localci, states, memberlist); err != nil {
		return pctx, err
	}

	if err := attachAskHandoverHandler(handlers, encs, local, params, localci, states, memberlist); err != nil {
		return pctx, err
	}

	if err := attachHandoverMessageHandler(handlers, encs, states); err != nil {
		return pctx, err
	}

	if err := attachCheckHandoverXHandler(handlers, encs, local, params, states, memberlist); err != nil {
		return pctx, err
	}

	return pctx, nil
}

func attachStartHandoverHandler(
	handlers *quicstream.PrefixHandler,
	encs *encoder.Encoders,
	local base.LocalNode,
	params *isaac.LocalParams,
	localci quicstream.UDPConnInfo,
	states *isaacstates.States,
	client *isaacnetwork.QuicstreamClient,
	syncSourcePool *isaac.SyncSourcePool,
) error {
	_ = handlers.Add(isaacnetwork.HandlerPrefixStartHandover, quicstreamheader.NewHandler(encs, 0,
		isaacnetwork.QuicstreamHandlerStartHandover(
			local,
			params.NetworkID(),
			isaacstates.NewStartHandoverYFunc(
				local.Address(),
				localci,
				states.AllowedConsensus,
				func() bool {
					return states.HandoverXBroker() != nil || states.HandoverYBroker() != nil
				},
				func(ctx context.Context, x base.Address, xci quicstream.UDPConnInfo) error {
					switch ok, err := client.CheckHandover(
						ctx, xci, local.Privatekey(), params.NetworkID(), local.Address(), localci); {
					case err != nil:
						return err
					case !ok:
						return errors.Errorf("x not available for handover")
					default:
						return nil
					}
				},
				func(x base.Address, xci quicstream.UDPConnInfo) error {
					nci := isaacnetwork.NewNodeConnInfo(
						isaac.NewNode(local.Publickey(), x),
						xci.Addr().String(),
						xci.TLSInsecure(),
					)
					_ = syncSourcePool.AddNonFixed(nci)

					return nil
				},
				states.NewHandoverYBroker,
			),
		),
		nil,
	))

	return nil
}

func attachCancelHandoverHandler(
	handlers *quicstream.PrefixHandler,
	encs *encoder.Encoders,
	local base.LocalNode,
	params *isaac.LocalParams,
	states *isaacstates.States,
) error {
	_ = handlers.Add(isaacnetwork.HandlerPrefixCancelHandover, quicstreamheader.NewHandler(encs, 0,
		isaacnetwork.QuicstreamHandlerCancelHandover(
			local,
			params.NetworkID(),
			func() error {
				xch := make(chan error)
				ych := make(chan error)

				go func() {
					xch <- states.CancelHandoverXBroker()
				}()

				go func() {
					ych <- states.CancelHandoverYBroker()
				}()

				return util.JoinErrors(<-xch, <-ych)
			},
		),
		nil,
	))

	return nil
}

func attachCheckHandoverHandler(
	handlers *quicstream.PrefixHandler,
	encs *encoder.Encoders,
	local base.LocalNode,
	params *isaac.LocalParams,
	localci quicstream.UDPConnInfo,
	states *isaacstates.States,
	memberlist *quicmemberlist.Memberlist,
) error {
	_ = handlers.Add(isaacnetwork.HandlerPrefixCheckHandover, quicstreamheader.NewHandler(encs, 0,
		isaacnetwork.QuicstreamHandlerCheckHandover(
			local,
			params.NetworkID(),
			isaacstates.NewCheckHandoverFunc(local.Address(), localci,
				states.AllowedConsensus,
				func() bool {
					return states.HandoverXBroker() != nil || states.HandoverYBroker() != nil
				},
				func() (bool, error) {
					return memberlist.IsJoined(), nil
				},
				states.Current,
			),
		),
		nil,
	))

	return nil
}

func attachAskHandoverHandler(
	handlers *quicstream.PrefixHandler,
	encs *encoder.Encoders,
	local base.LocalNode,
	params *isaac.LocalParams,
	localci quicstream.UDPConnInfo,
	states *isaacstates.States,
	memberlist *quicmemberlist.Memberlist,
) error {
	_ = handlers.Add(isaacnetwork.HandlerPrefixAskHandover, quicstreamheader.NewHandler(encs, 0,
		isaacnetwork.QuicstreamHandlerAskHandover(
			local,
			params.NetworkID(),
			isaacstates.NewAskHandoverReceivedFunc(local.Address(), localci,
				states.AllowedConsensus,
				func() bool {
					return states.HandoverXBroker() != nil || states.HandoverYBroker() != nil
				},
				func(yci quicstream.UDPConnInfo) (bool, error) {
					return memberlist.Exists(yci.UDPAddr()), nil
				},
				states.Current,
				func() {
					_ = states.SetAllowConsensus(false)
				},
				states.NewHandoverXBroker,
			),
		),
		nil,
	))

	return nil
}

func attachHandoverMessageHandler(
	handlers *quicstream.PrefixHandler,
	encs *encoder.Encoders,
	states *isaacstates.States,
) error {
	_ = handlers.Add(isaacnetwork.HandlerPrefixHandoverMessage, quicstreamheader.NewHandler(encs, 0,
		isaacnetwork.QuicstreamHandlerHandoverMessage(
			func(msg isaacstates.HandoverMessage) error {
				var receive func(interface{}) error

				if broker := states.HandoverXBroker(); broker != nil {
					receive = broker.Receive
				}

				if receive == nil {
					if broker := states.HandoverYBroker(); broker != nil {
						receive = broker.Receive
					}
				}

				if receive == nil {
					return errors.Errorf("not under handover")
				}

				return receive(msg)
			},
		),
		nil,
	))

	return nil
}

func attachCheckHandoverXHandler(
	handlers *quicstream.PrefixHandler,
	encs *encoder.Encoders,
	local base.LocalNode,
	params *isaac.LocalParams,
	states *isaacstates.States,
	memberlist *quicmemberlist.Memberlist,
) error {
	_ = handlers.Add(isaacnetwork.HandlerPrefixCheckHandoverX, quicstreamheader.NewHandler(encs, 0,
		isaacnetwork.QuicstreamHandlerCheckHandoverX(
			local,
			params.NetworkID(),
			isaacstates.NewCheckHandoverXFunc(
				states.AllowedConsensus,
				func() bool {
					return states.HandoverXBroker() != nil || states.HandoverYBroker() != nil
				},
				func() (bool, error) {
					return memberlist.IsJoined(), nil
				},
				states.Current,
			),
		),
		nil,
	))

	return nil
}
