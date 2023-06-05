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
	var syncSourcePool *isaac.SyncSourcePool
	var syncSourceChecker *isaacnetwork.SyncSourceChecker

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		QuicstreamClientContextKey, &client,
		PoolDatabaseContextKey, &pool,
		MemberlistContextKey, &memberlist,
		SyncSourcePoolContextKey, &syncSourcePool,
		SyncSourceCheckerContextKey, &syncSourceChecker,
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
		func(y base.Address, yci quicstream.UDPConnInfo) error {
			nci := isaacnetwork.NewNodeConnInfo(
				isaac.NewNode(local.Publickey(), y),
				yci.Addr().String(),
				yci.TLSInsecure(),
			)

			if syncSourcePool.Len() < 1 {
				err := syncSourceChecker.UpdateSources(context.Background(), []isaacnetwork.SyncSource{
					{Source: nci, Type: isaacnetwork.SyncSourceTypeNode},
				})

				log.Log().Debug().Err(err).Msg("handover y broker added to sync sourcess")
			}

			return nil
		},
	)
	args.WhenFinished = func(vp base.INITVoteproof, y base.Address, yci quicstream.UDPConnInfo) error {
		return whenFinished(vp, y, yci)
	}

	args.GetProposal = func(facthash util.Hash) (base.ProposalSignFact, bool, error) {
		return pool.Proposal(facthash)
	}

	return func(ctx context.Context, yci quicstream.UDPConnInfo) (*isaacstates.HandoverXBroker, error) {
		broker := isaacstates.NewHandoverXBroker(ctx, args, yci)

		_ = broker.SetLogging(log)

		return broker, nil
	}, nil
}

//revive:disable:function-length

func newHandoverYBrokerFunc(pctx context.Context) (isaacstates.NewHandoverYBrokerFunc, error) {
	var log *logging.Logging
	var design NodeDesign
	var encs *encoder.Encoders
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
		EncodersContextKey, &encs,
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

	lastoffsetop := util.EmptyLocked[util.Hash]()

	if err := attachNewDataFuncForHandoverY(pctx, args, func(op util.Hash) {
		_, _ = lastoffsetop.Set(func(_ util.Hash, isempty bool) (util.Hash, error) {
			if !isempty {
				return nil, util.ErrLockedSetIgnore.WithStack()
			}

			return op, nil
		})
	}); err != nil {
		return nil, err
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
		log.Log().Debug().Interface("init_voteproof", vp).Msg("handover y finished")

		return whenFinished(vp, xci)
	}

	if err := attachSyncDataFuncForHandoverY(pctx, args, func() util.Hash {
		h, _ := lastoffsetop.Value()

		return h
	}); err != nil {
		return nil, err
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
		log.Log().Debug().Err(err).Msg("handover y canceled")

		whenCanceled(err, xci)
	}
	args.AskRequestFunc = isaacstates.NewAskHandoverFunc(
		local.Address(),
		func(ctx context.Context, ci quicstream.UDPConnInfo) error {
			donech := long.Join(ci)
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
		broker := isaacstates.NewHandoverYBroker(ctx, args, xci)

		_ = broker.SetLogging(log)

		return broker, nil
	}, nil
}

//revive:enable:function-length

func PHandoverNetworkHandlers(pctx context.Context) (context.Context, error) {
	var design NodeDesign
	var encs *encoder.Encoders
	var local base.LocalNode
	var params *isaac.LocalParams
	var handlers *quicstream.PrefixHandler

	if err := util.LoadFromContext(pctx,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		QuicstreamHandlersContextKey, &handlers,
	); err != nil {
		return pctx, err
	}

	localci := quicstream.NewUDPConnInfo(design.Network.Publish(), design.Network.TLSInsecure)

	if err := attachStartHandoverHandler(pctx, handlers, encs, local, params, localci); err != nil {
		return pctx, err
	}

	if err := attachCancelHandoverHandler(pctx, handlers, encs, local, params); err != nil {
		return pctx, err
	}

	if err := attachCheckHandoverHandler(pctx, handlers, encs, local, params, localci); err != nil {
		return pctx, err
	}

	if err := attachAskHandoverHandler(pctx, handlers, encs, local, params, localci); err != nil {
		return pctx, err
	}

	if err := attachHandoverMessageHandler(pctx, handlers, encs, params); err != nil {
		return pctx, err
	}

	if err := attachCheckHandoverXHandler(pctx, handlers, encs, local, params); err != nil {
		return pctx, err
	}

	return pctx, nil
}

func attachStartHandoverHandler(
	pctx context.Context,
	handlers *quicstream.PrefixHandler,
	encs *encoder.Encoders,
	local base.LocalNode,
	params *isaac.LocalParams,
	localci quicstream.UDPConnInfo,
) error {
	var states *isaacstates.States
	var client *isaacnetwork.QuicstreamClient
	var syncSourcePool *isaac.SyncSourcePool

	if err := util.LoadFromContextOK(pctx,
		StatesContextKey, &states,
		QuicstreamClientContextKey, &client,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return err
	}

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
	pctx context.Context,
	handlers *quicstream.PrefixHandler,
	encs *encoder.Encoders,
	local base.LocalNode,
	params *isaac.LocalParams,
) error {
	var states *isaacstates.States

	if err := util.LoadFromContextOK(pctx,
		StatesContextKey, &states,
	); err != nil {
		return err
	}

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
	pctx context.Context,
	handlers *quicstream.PrefixHandler,
	encs *encoder.Encoders,
	local base.LocalNode,
	params *isaac.LocalParams,
	localci quicstream.UDPConnInfo,
) error {
	var states *isaacstates.States
	var memberlist *quicmemberlist.Memberlist
	var sp *SuffragePool

	if err := util.LoadFromContextOK(pctx,
		StatesContextKey, &states,
		MemberlistContextKey, &memberlist,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return err
	}

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
					return isMemberlistJoined(local.Address(), memberlist, sp)
				},
				states.Current,
			),
		),
		nil,
	))

	return nil
}

func attachAskHandoverHandler(
	pctx context.Context,
	handlers *quicstream.PrefixHandler,
	encs *encoder.Encoders,
	local base.LocalNode,
	params *isaac.LocalParams,
	localci quicstream.UDPConnInfo,
) error {
	var states *isaacstates.States
	var memberlist *quicmemberlist.Memberlist
	var sp *SuffragePool

	if err := util.LoadFromContextOK(pctx,
		StatesContextKey, &states,
		MemberlistContextKey, &memberlist,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return err
	}

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
					switch ok, err := isMemberlistJoined(local.Address(), memberlist, sp); {
					case err != nil:
						return false, err
					case !ok:
						return false, nil
					default:
						return memberlist.Exists(yci.UDPAddr()), nil
					}
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
	pctx context.Context,
	handlers *quicstream.PrefixHandler,
	encs *encoder.Encoders,
	params *isaac.LocalParams,
) error {
	var states *isaacstates.States

	if err := util.LoadFromContextOK(pctx,
		StatesContextKey, &states,
	); err != nil {
		return err
	}

	_ = handlers.Add(isaacnetwork.HandlerPrefixHandoverMessage, quicstreamheader.NewHandler(encs, 0,
		isaacnetwork.QuicstreamHandlerHandoverMessage(
			params.NetworkID(),
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

				err := receive(msg)

				return err
			},
		),
		nil,
	))

	return nil
}

func attachCheckHandoverXHandler(
	pctx context.Context,
	handlers *quicstream.PrefixHandler,
	encs *encoder.Encoders,
	local base.LocalNode,
	params *isaac.LocalParams,
) error {
	var states *isaacstates.States
	var memberlist *quicmemberlist.Memberlist
	var sp *SuffragePool

	if err := util.LoadFromContextOK(pctx,
		StatesContextKey, &states,
		MemberlistContextKey, &memberlist,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return err
	}

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
					return isMemberlistJoined(local.Address(), memberlist, sp)
				},
				states.Current,
			),
		),
		nil,
	))

	return nil
}

func attachNewDataFuncForHandoverY(
	pctx context.Context,
	args *isaacstates.HandoverYBrokerArgs,
	setlastoffsetop func(util.Hash),
) error {
	var pool *isaacdatabase.TempPool
	var svvotef isaac.SuffrageVoteFunc
	var ballotbox *isaacstates.Ballotbox

	if err := util.LoadFromContextOK(pctx,
		PoolDatabaseContextKey, &pool,
		SuffrageVotingVoteFuncContextKey, &svvotef,
		BallotboxContextKey, &ballotbox,
	); err != nil {
		return err
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
				if err == nil {
					setlastoffsetop(t.Hash())
				}

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

	return nil
}

func attachSyncDataFuncForHandoverY(
	pctx context.Context,
	args *isaacstates.HandoverYBrokerArgs,
	lastoffsetop func() util.Hash,
) error {
	var local base.LocalNode
	var params *isaac.LocalParams
	var client *isaacnetwork.QuicstreamClient
	var pool *isaacdatabase.TempPool

	if err := util.LoadFromContextOK(pctx,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		QuicstreamClientContextKey, &client,
		PoolDatabaseContextKey, &pool,
	); err != nil {
		return err
	}

	args.SyncDataFunc = func(ctx context.Context, xci quicstream.UDPConnInfo, readych chan<- struct{}) error {
		var lastoffset []byte

		ticker := time.NewTicker(time.Millisecond * 333)
		defer ticker.Stop()

		var count uint64

		for range ticker.C {
			switch {
			case ctx.Err() != nil:
				return ctx.Err()
			case count >= params.MaxTryHandoverYBrokerSyncData():
				return nil
			}

			if err := client.StreamOperations(ctx, xci, local.Privatekey(), params.NetworkID(), lastoffset,
				func(op base.Operation, offset []byte) error {
					if op.Hash().Equal(lastoffsetop()) {
						ticker.Stop()

						return nil
					}

					if _, err := pool.SetOperation(ctx, op); err != nil {
						return err
					}

					lastoffset = offset

					return nil
				},
			); err != nil {
				return err
			}

			readych <- struct{}{}

			count++
		}

		return nil
	}

	return nil
}

func isMemberlistJoined(local base.Address, memberlist *quicmemberlist.Memberlist, sp *SuffragePool) (bool, error) {
	switch suf, found, err := sp.Last(); {
	case err != nil:
		return false, err
	case !found:
		return false, nil
	case !suf.Exists(local):
		return false, nil
	case suf.Len() == 1:
		return true, nil
	default:
		return memberlist.IsJoined(), nil
	}
}
