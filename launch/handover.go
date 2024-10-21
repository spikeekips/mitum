package launch

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var PNameHandoverNetworkHandlers = ps.Name("handover-network-handlers")

var HandoverEventLogger EventLoggerName = "handover"

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
	var isaacparams *isaac.Params
	var client *isaacnetwork.BaseClient
	var pool *isaacdatabase.TempPool
	var memberlist *quicmemberlist.Memberlist
	var syncSourcePool *isaac.SyncSourcePool
	var syncSourceChecker *isaacnetwork.SyncSourceChecker
	var eventLogging *EventLogging

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		ISAACParamsContextKey, &isaacparams,
		QuicstreamClientContextKey, &client,
		PoolDatabaseContextKey, &pool,
		MemberlistContextKey, &memberlist,
		SyncSourcePoolContextKey, &syncSourcePool,
		SyncSourceCheckerContextKey, &syncSourceChecker,
		EventLoggingContextKey, &eventLogging,
	); err != nil {
		return nil, err
	}

	var el zerolog.Logger

	switch i, found := eventLogging.Logger(HandoverEventLogger); {
	case !found:
		return nil, errors.Errorf("handover event logger not found")
	default:
		el = i.With().Str("module", "handover_x").Logger()
	}

	args := isaacstates.NewHandoverXBrokerArgs(local, isaacparams.NetworkID())

	args.SendMessageFunc = func(ctx context.Context, ci quicstream.ConnInfo, msg isaacstates.HandoverMessage) error {
		return client.HandoverMessage(ctx, ci, msg)
	}
	args.CheckIsReady = func() (bool, error) { return true, nil }
	args.WhenCanceled = func(brokerID string, err error) {
		log.Log().Debug().Err(err).Str("broker_id", brokerID).Msg("handover x canceled")

		el.Error().Err(err).
			Str("broker_id", brokerID).
			Msg("handover x canceled")
	}

	whenFinished := isaacstates.NewHandoverXFinishedFunc(
		func() error {
			return memberlist.Leave(time.Second * 33) //nolint:mnd // long enough
		},
		func(y base.Address, yci quicstream.ConnInfo) error {
			nci := isaacnetwork.NewNodeConnInfoFromConnInfo(isaac.NewNode(local.Publickey(), y), yci)

			if syncSourcePool.Len() < 1 {
				err := syncSourceChecker.UpdateSources(context.Background(), []isaacnetwork.SyncSource{
					{Source: nci, Type: isaacnetwork.SyncSourceTypeNode},
				})

				log.Log().Debug().Err(err).Msg("handover y broker added to sync sourcess")
			}

			return nil
		},
	)
	args.WhenFinished = func(brokerID string, vp base.INITVoteproof, y base.Address, yci quicstream.ConnInfo) error {
		el.Debug().
			Str("broker_id", brokerID).
			Interface("y_conninfo", yci).
			Interface("point", vp.Point()).
			Msg("handover x finished")

		return whenFinished(vp, y, yci)
	}

	args.GetProposal = func(facthash util.Hash) (base.ProposalSignFact, bool, error) {
		return pool.Proposal(facthash)
	}

	return func(ctx context.Context, yci quicstream.ConnInfo) (*isaacstates.HandoverXBroker, error) {
		broker := isaacstates.NewHandoverXBroker(ctx, args, yci)

		_ = broker.SetLogging(log)

		el.Debug().
			Str("broker_id", broker.ID()).
			Interface("y_conninfo", yci).
			Msg("handover x started")

		return broker, nil
	}, nil
}

//revive:disable:function-length

func newHandoverYBrokerFunc(pctx context.Context) (isaacstates.NewHandoverYBrokerFunc, error) {
	var log *logging.Logging
	var design NodeDesign
	var local base.LocalNode
	var isaacparams *isaac.Params
	var client *isaacnetwork.BaseClient
	var pool *isaacdatabase.TempPool
	var long *LongRunningMemberlistJoin
	var svvotef isaac.SuffrageVoteFunc
	var memberlist *quicmemberlist.Memberlist
	var syncSourcePool *isaac.SyncSourcePool
	var ballotbox *isaacstates.Ballotbox
	var eventLogging *EventLogging

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		ISAACParamsContextKey, &isaacparams,
		QuicstreamClientContextKey, &client,
		PoolDatabaseContextKey, &pool,
		LongRunningMemberlistJoinContextKey, &long,
		SuffrageVotingVoteFuncContextKey, &svvotef,
		MemberlistContextKey, &memberlist,
		SyncSourcePoolContextKey, &syncSourcePool,
		BallotboxContextKey, &ballotbox,
		EventLoggingContextKey, &eventLogging,
	); err != nil {
		return nil, err
	}

	var el zerolog.Logger

	switch i, found := eventLogging.Logger(HandoverEventLogger); {
	case !found:
		return nil, errors.Errorf("handover event logger not found")
	default:
		el = i.With().Str("module", "handover_y").Logger()
	}

	localci := design.Network.PublishConnInfo()

	args := isaacstates.NewHandoverYBrokerArgs(isaacparams.NetworkID())

	args.SendMessageFunc = func(ctx context.Context, ci quicstream.ConnInfo, msg isaacstates.HandoverMessage) error {
		return client.HandoverMessage(ctx, ci, msg)
	}

	lastoffsetop := util.EmptyLocked[util.Hash]()

	if err := attachNewDataFuncForHandoverY(pctx, args, func(op util.Hash) {
		_, _ = lastoffsetop.Set(func(_ util.Hash, isempty bool) (util.Hash, error) {
			if !isempty {
				return nil, util.ErrLockedSetIgnore
			}

			return op, nil
		})
	}); err != nil {
		return nil, err
	}

	whenFinished := isaacstates.NewHandoverYFinishedFunc(
		func(xci quicstream.ConnInfo) error {
			nci := isaacnetwork.NewNodeConnInfoFromConnInfo(isaac.NewNode(local.Publickey(), local.Address()), xci)

			_ = syncSourcePool.RemoveNonFixed(nci)

			return nil
		},
	)
	args.WhenFinished = func(brokerID string, vp base.INITVoteproof, xci quicstream.ConnInfo) error {
		log.Log().Debug().Interface("init_voteproof", vp).Str("broker_id", brokerID).Msg("handover y finished")

		el.Debug().
			Str("broker_id", brokerID).
			Interface("x_conninfo", xci).
			Interface("point", vp.Point()).
			Msg("handover y finished")

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
			return memberlist.Leave(time.Second * 33) //nolint:mnd // long enough
		},
		func(xci quicstream.ConnInfo) error {
			nci := isaacnetwork.NewNodeConnInfoFromConnInfo(isaac.NewNode(local.Publickey(), local.Address()), xci)

			_ = syncSourcePool.RemoveNonFixed(nci)

			return nil
		},
	)

	args.WhenCanceled = func(brokerID string, err error, xci quicstream.ConnInfo) {
		log.Log().Debug().Err(err).Str("broker_id", brokerID).Msg("handover y canceled")

		whenCanceled(err, xci)

		el.Error().Err(err).
			Str("broker_id", brokerID).
			Interface("x_conninfo", xci).
			Msg("handover y canceled")
	}

	args.AskRequestFunc = isaacstates.NewAskHandoverFunc(
		local.Address(),
		func(ctx context.Context, ci quicstream.ConnInfo) error {
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
		func(ctx context.Context, _ base.Address, xci quicstream.ConnInfo) (string, bool, error) {
			return client.AskHandover(ctx, xci, local.Privatekey(), isaacparams.NetworkID(), local.Address(), localci)
		},
	)

	return func(ctx context.Context, xci quicstream.ConnInfo) (*isaacstates.HandoverYBroker, error) {
		broker := isaacstates.NewHandoverYBroker(ctx, args, xci)

		_ = broker.SetLogging(log)

		el.Debug().
			Interface("x_conninfo", xci).
			Msg("handover y started")

		return broker, nil
	}, nil
}

//revive:enable:function-length

func PHandoverNetworkHandlers(pctx context.Context) (context.Context, error) {
	var design NodeDesign
	var local base.LocalNode
	var params *LocalParams

	if err := util.LoadFromContext(pctx,
		DesignContextKey, &design,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
	); err != nil {
		return pctx, err
	}

	isaacparams := params.ISAAC

	localci := design.Network.PublishConnInfo()

	if err := attachStartHandoverHandler(pctx, local, isaacparams, localci); err != nil {
		return pctx, err
	}

	if err := attachCancelHandoverHandler(pctx, isaacparams); err != nil {
		return pctx, err
	}

	if err := attachCheckHandoverHandler(pctx, local, isaacparams, localci); err != nil {
		return pctx, err
	}

	if err := attachAskHandoverHandler(pctx, local, isaacparams, localci); err != nil {
		return pctx, err
	}

	if err := attachHandoverMessageHandler(pctx, isaacparams); err != nil {
		return pctx, err
	}

	if err := attachCheckHandoverXHandler(pctx, local, isaacparams); err != nil {
		return pctx, err
	}

	return pctx, nil
}

var HandoverACLScope = ACLScope("handover")

func attachStartHandoverHandler(
	pctx context.Context,
	local base.LocalNode,
	isaacparams *isaac.Params,
	localci quicstream.ConnInfo,
) error {
	var log *logging.Logging
	var states *isaacstates.States
	var client *isaacnetwork.BaseClient
	var syncSourcePool *isaac.SyncSourcePool

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		StatesContextKey, &states,
		QuicstreamClientContextKey, &client,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return err
	}

	var aclallow ACLAllowFunc

	switch i, err := pACLAllowFunc(pctx); {
	case err != nil:
		return err
	default:
		aclallow = i
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameStartHandover,
		isaacnetwork.QuicstreamHandlerStartHandover(
			ACLNetworkHandler[isaacnetwork.StartHandoverHeader](
				aclallow,
				HandoverACLScope,
				WriteAllowACLPerm,
				isaacparams.NetworkID(),
			),
			isaacstates.NewStartHandoverYFunc(
				local.Address(),
				localci,
				states.AllowedConsensus,
				func() bool {
					return states.HandoverXBroker() != nil || states.HandoverYBroker() != nil
				},
				func(ctx context.Context, _ base.Address, xci quicstream.ConnInfo) error {
					switch ok, err := client.CheckHandover(
						ctx, xci, local.Privatekey(), isaacparams.NetworkID(), local.Address(), localci); {
					case err != nil:
						return err
					case !ok:
						return errors.Errorf("x not available for handover")
					default:
						return nil
					}
				},
				func(x base.Address, xci quicstream.ConnInfo) error {
					nci := isaacnetwork.NewNodeConnInfoFromConnInfo(isaac.NewNode(local.Publickey(), x), xci)

					_ = syncSourcePool.AddNonFixed(nci)

					return nil
				},
				states.NewHandoverYBroker,
			),
		),
		nil,
	)

	return gerror
}

func attachCancelHandoverHandler(
	pctx context.Context,
	isaacparams *isaac.Params,
) error {
	var log *logging.Logging
	var states *isaacstates.States

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		StatesContextKey, &states,
	); err != nil {
		return err
	}

	var aclallow ACLAllowFunc

	switch i, err := pACLAllowFunc(pctx); {
	case err != nil:
		return err
	default:
		aclallow = i
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameCancelHandover,
		isaacnetwork.QuicstreamHandlerCancelHandover(
			ACLNetworkHandler[isaacnetwork.CancelHandoverHeader](
				aclallow,
				HandoverACLScope,
				WriteAllowACLPerm,
				isaacparams.NetworkID(),
			),
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
	)

	return gerror
}

func attachCheckHandoverHandler(
	pctx context.Context,
	local base.LocalNode,
	isaacparams *isaac.Params,
	localci quicstream.ConnInfo,
) error {
	var log *logging.Logging
	var states *isaacstates.States
	var memberlist *quicmemberlist.Memberlist
	var sp *SuffragePool

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		StatesContextKey, &states,
		MemberlistContextKey, &memberlist,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return err
	}

	var aclallow ACLAllowFunc

	switch i, err := pACLAllowFunc(pctx); {
	case err != nil:
		return err
	default:
		aclallow = i
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameCheckHandover,
		isaacnetwork.QuicstreamHandlerCheckHandover(
			ACLNetworkHandler[isaacnetwork.CheckHandoverHeader](
				aclallow,
				HandoverACLScope,
				WriteAllowACLPerm,
				isaacparams.NetworkID(),
			),
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
	)

	return gerror
}

func attachAskHandoverHandler(
	pctx context.Context,
	local base.LocalNode,
	isaacparams *isaac.Params,
	localci quicstream.ConnInfo,
) error {
	var log *logging.Logging
	var states *isaacstates.States
	var memberlist *quicmemberlist.Memberlist
	var sp *SuffragePool

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		StatesContextKey, &states,
		MemberlistContextKey, &memberlist,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return err
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameAskHandover,
		isaacnetwork.QuicstreamHandlerAskHandover(
			local,
			isaacparams.NetworkID(),
			isaacstates.NewAskHandoverReceivedFunc(local.Address(), localci,
				states.AllowedConsensus,
				func() bool {
					return states.HandoverXBroker() != nil || states.HandoverYBroker() != nil
				},
				func(yci quicstream.ConnInfo) (bool, error) {
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
	)

	return gerror
}

func attachHandoverMessageHandler(
	pctx context.Context,
	isaacparams *isaac.Params,
) error {
	var log *logging.Logging
	var states *isaacstates.States

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		StatesContextKey, &states,
	); err != nil {
		return err
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameHandoverMessage,
		isaacnetwork.QuicstreamHandlerHandoverMessage(
			isaacparams.NetworkID(),
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
	)

	return gerror
}

func attachCheckHandoverXHandler(
	pctx context.Context,
	local base.LocalNode,
	isaacparams *isaac.Params,
) error {
	var log *logging.Logging
	var states *isaacstates.States
	var memberlist *quicmemberlist.Memberlist
	var sp *SuffragePool

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		StatesContextKey, &states,
		MemberlistContextKey, &memberlist,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return err
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameCheckHandoverX,
		isaacnetwork.QuicstreamHandlerCheckHandoverX(
			local,
			isaacparams.NetworkID(),
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
	)

	return gerror
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
			pr, err := util.AssertInterfaceValue[base.ProposalSignFact](i)
			if err != nil {
				return err
			}

			_, err = pool.SetProposal(pr)

			return err
		case isaacstates.HandoverMessageDataTypeOperation,
			isaacstates.HandoverMessageDataTypeSuffrageVoting:
			var op base.Operation
			if err := util.SetInterfaceValue(i, &op); err != nil {
				return err
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
			switch ballot, err := util.AssertInterfaceValue[base.Ballot](i); {
			case err != nil:
				return err
			default:
				_, err := ballotbox.Vote(ballot)

				return err
			}
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
	var isaacparams *isaac.Params
	var client *isaacnetwork.BaseClient
	var pool *isaacdatabase.TempPool

	if err := util.LoadFromContextOK(pctx,
		LocalContextKey, &local,
		ISAACParamsContextKey, &isaacparams,
		QuicstreamClientContextKey, &client,
		PoolDatabaseContextKey, &pool,
	); err != nil {
		return err
	}

	args.SyncDataFunc = func(ctx context.Context, xci quicstream.ConnInfo, readych chan<- struct{}) error {
		var lastoffset []byte

		ticker := time.NewTicker(time.Millisecond * 333)
		defer ticker.Stop()

		var count uint64

		for range ticker.C {
			switch {
			case ctx.Err() != nil:
				return ctx.Err()
			case count >= isaacparams.MaxTryHandoverYBrokerSyncData():
				return nil
			}

			if err := client.StreamOperations(ctx, xci, local.Privatekey(), isaacparams.NetworkID(), lastoffset,
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
