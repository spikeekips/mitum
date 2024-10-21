package launch

import (
	"context"
	"io"
	"net/url"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	"github.com/spikeekips/mitum/util/valuehash"
	"golang.org/x/exp/slices"
)

var (
	PNameNetworkHandlers             = ps.Name("network-handlers")
	PNameOperationProcessorsMap      = ps.Name("operation-processors-map")
	OperationProcessorsMapContextKey = util.ContextKey("operation-processors-map")
)

var (
	//revive:disable:line-length-limit
	HandlerNameMemberlistCallbackBroadcastMessage quicstream.HandlerName = "memberlist_callback_broadcast_message"
	HandlerNameMemberlistEnsureBroadcastMessage   quicstream.HandlerName = "memberlist_ensure_broadcast_message"

	handlerPrefixMemberlistCallbackBroadcastMessage = quicstream.HashPrefix(HandlerNameMemberlistCallbackBroadcastMessage)
	handlerPrefixMemberlistEnsureBroadcastMessage   = quicstream.HashPrefix(HandlerNameMemberlistEnsureBroadcastMessage)
	//revive:enable:line-length-limit
)

//revive:disable:function-length

func PNetworkHandlers(pctx context.Context) (context.Context, error) {
	e := util.StringError("prepare network handlers")

	var log *logging.Logging
	var encs *encoder.Encoders
	var design NodeDesign
	var local base.LocalNode
	var params *LocalParams
	var db isaac.Database
	var pool *isaacdatabase.TempPool
	var proposalMaker *isaac.ProposalMaker
	var m *quicmemberlist.Memberlist
	var syncSourcePool *isaac.SyncSourcePool
	var nodeinfo *isaacnetwork.NodeInfoUpdater
	var svvotef isaac.SuffrageVoteFunc
	var ballotbox *isaacstates.Ballotbox
	var filternotifymsg quicmemberlist.FilterNotifyMsgFunc
	var lvps *isaac.LastVoteproofsHandler

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EncodersContextKey, &encs,
		DesignContextKey, &design,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
		PoolDatabaseContextKey, &pool,
		ProposalMakerContextKey, &proposalMaker,
		MemberlistContextKey, &m,
		SyncSourcePoolContextKey, &syncSourcePool,
		NodeInfoContextKey, &nodeinfo,
		SuffrageVotingVoteFuncContextKey, &svvotef,
		BallotboxContextKey, &ballotbox,
		FilterMemberlistNotifyMsgFuncContextKey, &filternotifymsg,
		LastVoteproofsHandlerContextKey, &lvps,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	isaacparams := params.ISAAC

	lastBlockMapf := QuicstreamHandlerLastBlockMapFunc(db)
	suffrageNodeConnInfof := QuicstreamHandlerSuffrageNodeConnInfoFunc(db, m)

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameLastSuffrageProof,
		isaacnetwork.QuicstreamHandlerLastSuffrageProof(
			func(last util.Hash) (string, []byte, []byte, bool, error) {
				enchint, metab, body, found, lastheight, err := db.LastSuffrageProofBytes()

				switch {
				case err != nil:
					return enchint, nil, nil, false, err
				case !found:
					return enchint, nil, nil, false, storage.ErrNotFound.Errorf("last SuffrageProof not found")
				}

				switch {
				case last != nil && len(metab) > 0 && valuehash.NewBytes(metab).Equal(last):
					nbody, _ := util.NewLengthedBytesSlice([][]byte{lastheight.Bytes(), nil})

					return enchint, nil, nbody, false, nil
				default:
					nbody, _ := util.NewLengthedBytesSlice([][]byte{lastheight.Bytes(), body})

					return enchint, metab, nbody, true, nil
				}
			},
		), nil)

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameSuffrageProof,
		isaacnetwork.QuicstreamHandlerSuffrageProof(db.SuffrageProofBytes), nil)

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameLastBlockMap,
		isaacnetwork.QuicstreamHandlerLastBlockMap(lastBlockMapf), nil)

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameBlockMap,
		isaacnetwork.QuicstreamHandlerBlockMap(db.BlockMapBytes), nil)

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameNodeChallenge,
		isaacnetwork.QuicstreamHandlerNodeChallenge(isaacparams.NetworkID(), local), nil)

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameSuffrageNodeConnInfo,
		isaacnetwork.QuicstreamHandlerSuffrageNodeConnInfo(suffrageNodeConnInfof), nil)

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameSyncSourceConnInfo,
		isaacnetwork.QuicstreamHandlerSyncSourceConnInfo(
			func() ([]isaac.NodeConnInfo, error) {
				addMembers, doneMembers := util.CompactAppendSlice[isaac.NodeConnInfo](syncSourcePool.Len() * 2)

				syncSourcePool.Actives(func(nci isaac.NodeConnInfo) bool {
					addMembers(nci)

					return true
				})

				return doneMembers(), nil
			},
		), nil)

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameState,
		isaacnetwork.QuicstreamHandlerState(db.StateBytes), nil)

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameExistsInStateOperation,
		isaacnetwork.QuicstreamHandlerExistsInStateOperation(db.ExistsInStateOperation), nil)

	if vp := lvps.Last().Cap(); vp != nil {
		_ = nodeinfo.SetLastVote(vp.Point(), vp.Result())
	}

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameNodeInfo,
		isaacnetwork.QuicstreamHandlerNodeInfo(QuicstreamHandlerGetNodeInfoFunc(encs.Default(), nodeinfo)), nil)

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameSendBallots,
		isaacnetwork.QuicstreamHandlerSendBallots(isaacparams.NetworkID(),
			func(bl base.BallotSignFact) error {
				switch passed, err := filternotifymsg(bl); {
				case err != nil:
					log.Log().Trace().
						Str("module", "filter-notify-msg-send-ballots").
						Err(err).
						Interface("handover_message", bl).
						Msg("filter error")

					fallthrough
				case !passed:
					log.Log().Trace().
						Str("module", "filter-notify-msg-send-ballots").
						Interface("handover_message", bl).
						Msg("filtered")

					return nil
				}

				_, err := ballotbox.VoteSignFact(bl)

				return err
			},
			params.MISC.MaxMessageSize,
		), nil)

	if gerror != nil {
		return pctx, gerror
	}

	if err := AttachBlockItemsNetworkHandlers(pctx); err != nil {
		return pctx, err
	}

	if err := AttachMemberlistNetworkHandlers(pctx); err != nil {
		return pctx, err
	}

	return pctx, nil
}

//revive:enable:function-length

func POperationProcessorsMap(pctx context.Context) (context.Context, error) {
	var isaacparams *isaac.Params
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		ISAACParamsContextKey, &isaacparams,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return pctx, err
	}

	limiterf, err := NewSuffrageCandidateLimiterFunc(pctx)
	if err != nil {
		return pctx, err
	}

	set := hint.NewCompatibleSet[isaac.NewOperationProcessorInternalFunc](1 << 9) //nolint:mnd //...

	_ = set.Add(isaacoperation.SuffrageCandidateHint,
		func(height base.Height, getStatef base.GetStateFunc) (base.OperationProcessor, error) {
			policy := db.LastNetworkPolicy()
			if policy == nil { // NOTE Usually it means empty block data
				return nil, nil
			}

			return isaacoperation.NewSuffrageCandidateProcessor(
				height,
				getStatef,
				limiterf,
				nil,
				policy.SuffrageCandidateLifespan(),
			)
		})

	_ = set.Add(isaacoperation.SuffrageJoinHint,
		func(height base.Height, getStatef base.GetStateFunc) (base.OperationProcessor, error) {
			policy := db.LastNetworkPolicy()
			if policy == nil { // NOTE Usually it means empty block data
				return nil, nil
			}

			return isaacoperation.NewSuffrageJoinProcessor(
				height,
				isaacparams.Threshold(),
				getStatef,
				nil,
				nil,
			)
		})

	_ = set.Add(isaac.SuffrageExpelOperationHint,
		func(height base.Height, getStatef base.GetStateFunc) (base.OperationProcessor, error) {
			policy := db.LastNetworkPolicy()
			if policy == nil { // NOTE Usually it means empty block data
				return nil, nil
			}

			return isaacoperation.NewSuffrageExpelProcessor(
				height,
				getStatef,
				nil,
				nil,
			)
		})

	_ = set.Add(isaacoperation.SuffrageDisjoinHint,
		func(height base.Height, getStatef base.GetStateFunc) (base.OperationProcessor, error) {
			return isaacoperation.NewSuffrageDisjoinProcessor(
				height,
				getStatef,
				nil,
				nil,
			)
		})

	_ = set.Add(isaacoperation.NetworkPolicyHint,
		func(height base.Height, getStatef base.GetStateFunc) (base.OperationProcessor, error) {
			return isaacoperation.NewNetworkPolicyProcessor(
				height,
				isaacparams.Threshold(),
				getStatef,
				nil,
				nil,
			)
		})

	return context.WithValue(pctx, OperationProcessorsMapContextKey, set), nil
}

func SendOperationFilterFunc(pctx context.Context) (
	func(base.Operation) (bool, error),
	error,
) {
	var db isaac.Database
	var oprs *hint.CompatibleSet[isaac.NewOperationProcessorInternalFunc]

	if err := util.LoadFromContextOK(pctx,
		CenterDatabaseContextKey, &db,
		OperationProcessorsMapContextKey, &oprs,
	); err != nil {
		return nil, err
	}

	operationfilterf := IsSupportedProposalOperationFactHintFunc()

	return func(op base.Operation) (bool, error) {
		switch hinter, ok := op.Fact().(hint.Hinter); {
		case !ok:
			return false, nil
		case !operationfilterf(hinter.Hint()):
			return false, nil
		}

		var height base.Height

		switch m, found, err := db.LastBlockMap(); {
		case err != nil:
			return false, err
		case !found:
			return true, nil
		default:
			height = m.Manifest().Height()
		}

		f, closef, err := OperationPreProcess(db, oprs, op, height)
		if err != nil {
			return false, err
		}

		defer func() {
			_ = closef()
		}()

		_, reason, err := f(context.Background(), db.State)
		if err != nil {
			return false, err
		}

		return reason == nil, reason
	}, nil
}

func QuicstreamHandlerLastBlockMapFunc(
	db isaac.Database,
) func(last util.Hash) (string, []byte, []byte, bool, error) {
	return func(last util.Hash) (string, []byte, []byte, bool, error) {
		enchint, metab, body, found, err := db.LastBlockMapBytes()

		switch {
		case err != nil:
			return enchint, nil, nil, false, err
		case !found:
			return enchint, nil, nil, false, storage.ErrNotFound.Errorf("last BlockMap not found")
		}

		switch {
		case last != nil && len(metab) > 0 && last.Equal(valuehash.NewBytes(metab)):
			return enchint, nil, nil, false, nil
		default:
			return enchint, metab, body, true, nil
		}
	}
}

func QuicstreamHandlerSuffrageNodeConnInfoFunc(
	db isaac.Database,
	memberlist *quicmemberlist.Memberlist,
) func() ([]isaac.NodeConnInfo, error) {
	return func() ([]isaac.NodeConnInfo, error) {
		var suf base.Suffrage

		switch proof, found, err := db.LastSuffrageProof(); {
		case err != nil:
			return nil, err
		case !found:
			return nil, storage.ErrNotFound.Errorf("last SuffrageProof not found")
		default:
			i, err := proof.Suffrage()
			if err != nil {
				return nil, err
			}

			suf = i
		}

		addMembers, doneMembers := util.CompactAppendSlice[isaac.NodeConnInfo](memberlist.MembersLen() * 2)

		memberlist.Remotes(func(node quicmemberlist.Member) bool {
			if !suf.ExistsPublickey(node.Address(), node.Publickey()) {
				return true
			}

			addMembers(isaacnetwork.NewNodeConnInfoFromMemberlistNode(node))

			return true
		})

		return doneMembers(), nil
	}
}

func QuicstreamHandlerGetNodeInfoFunc(
	enc encoder.Encoder,
	nodeinfo *isaacnetwork.NodeInfoUpdater,
) func() ([]byte, error) {
	lastid := util.EmptyLocked[[2]interface{}]()

	return func() ([]byte, error) {
		var b []byte

		if _, err := lastid.Set(func(v [2]interface{}, isempty bool) (vv [2]interface{}, _ error) {
			switch {
			case isempty:
			case v[0].(string) == nodeinfo.ID(): //nolint:forcetypeassert //...
				b = v[1].([]byte) //nolint:forcetypeassert //...

				return vv, util.ErrLockedSetIgnore
			}

			switch i, err := enc.Marshal(nodeinfo.NodeInfo()); {
			case err != nil:
				return vv, err
			default:
				b = i

				return [2]interface{}{nodeinfo.ID(), i}, nil
			}
		}); err != nil {
			return nil, err
		}

		return b, nil
	}
}

func OperationPreProcess(
	db isaac.Database,
	oprs *hint.CompatibleSet[isaac.NewOperationProcessorInternalFunc],
	op base.Operation,
	height base.Height,
) (
	preprocess func(context.Context, base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error),
	cancel func() error,
	_ error,
) {
	f, found := oprs.Find(op.Hint())
	if !found {
		return op.PreProcess, util.EmptyCancelFunc, nil
	}

	switch opp, err := f(height, db.State); {
	case err != nil:
		return nil, nil, err
	default:
		return func(pctx context.Context, getStateFunc base.GetStateFunc) (
			context.Context, base.OperationProcessReasonError, error,
		) {
			return opp.PreProcess(pctx, op, getStateFunc)
		}, opp.Close, nil
	}
}

func AttachMemberlistNetworkHandlers(pctx context.Context) error {
	var log *logging.Logging
	var params *LocalParams
	var m *quicmemberlist.Memberlist
	var sp *SuffragePool

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalParamsContextKey, &params,
		MemberlistContextKey, &m,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return err
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		HandlerNameMemberlistCallbackBroadcastMessage,
		m.CallbackBroadcastHandler(), nil)

	EnsureHandlerAdd(pctx, &gerror,
		HandlerNameMemberlistEnsureBroadcastMessage,
		m.EnsureBroadcastHandler(
			params.ISAAC.NetworkID(),
			func(node base.Address) (base.Publickey, bool, error) {
				switch suf, found, err := sp.Last(); {
				case err != nil:
					return nil, false, err
				case !found, suf == nil, !suf.Exists(node):
					return nil, false, nil
				default:
					var foundnode base.Node

					_ = slices.IndexFunc(suf.Nodes(), func(n base.Node) bool {
						if node.Equal(n.Address()) {
							foundnode = n

							return true
						}

						return false
					})

					return foundnode.Publickey(), true, nil
				}
			},
		), nil)

	return gerror
}

func AttachBlockItemsNetworkHandlers(pctx context.Context) error {
	var params *LocalParams
	var readers *isaac.BlockItemReaders

	if err := util.LoadFromContextOK(pctx,
		LocalParamsContextKey, &params,
		BlockItemReadersContextKey, &readers,
	); err != nil {
		return err
	}

	isaacparams := params.ISAAC

	var aclallow ACLAllowFunc

	switch i, err := pACLAllowFunc(pctx); {
	case err != nil:
		return err
	default:
		aclallow = i
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameBlockItem,
		isaacnetwork.QuicstreamHandlerBlockItem(
			func(height base.Height, item base.BlockItemType, f func(io.Reader, bool, url.URL, string) error) error {
				switch bfile, found, err := readers.Item(height, item, func(ir isaac.BlockItemReader) error {
					return f(ir.Reader(), true, url.URL{}, ir.Reader().Format)
				}); {
				case err != nil:
					return err
				case !found && bfile != nil:
					// NOTE if not in local, but itemfile exists, found is true
					return f(nil, true, bfile.URI(), bfile.CompressFormat())
				case !found:
					return f(nil, false, url.URL{}, "")
				default:
					return nil
				}
			},
		), nil)

	EnsureHandlerAdd(pctx, &gerror,
		isaacnetwork.HandlerNameBlockItemFiles,
		ACLNetworkHandler[isaacnetwork.BlockItemFilesRequestHeader](
			aclallow,
			BlockItemFilesACLScope,
			ReadAllowACLPerm,
			isaacparams.NetworkID(),
		).Handler(
			isaacnetwork.QuicstreamHandlerBlockItemFiles(
				func(height base.Height, f func(io.Reader, bool) error) error {
					switch found, err := readers.ItemFilesReader(height, func(r io.Reader) error {
						return f(r, true)
					}); {
					case err != nil:
						return err
					case !found:
						return f(nil, false)
					default:
						return nil
					}
				},
			),
		),
		nil)

	return gerror
}

func EnsureHandlerAdd[T quicstreamheader.RequestHeader](
	pctx context.Context,
	gerr *error, //nolint:gocritic //...
	name quicstream.HandlerName,
	handler quicstreamheader.Handler[T],
	errhandler quicstreamheader.ErrorHandler,
) {
	if *gerr != nil {
		return
	}

	var params *LocalParams
	var encs *encoder.Encoders
	var handlers *quicstream.PrefixHandler
	var rateLimitHandler *RateLimitHandler

	if err := util.LoadFromContextOK(pctx,
		EncodersContextKey, &encs,
		LocalParamsContextKey, &params,
		QuicstreamHandlersContextKey, &handlers,
		RateLimiterContextKey, &rateLimitHandler,
	); err != nil {
		*gerr = err

		return
	}

	var timeoutf func() time.Duration

	switch f, err := params.Network.HandlerTimeoutFunc(name); {
	case err != nil:
		*gerr = err

		return
	default:
		timeoutf = f
	}

	rhandler := rateLimitHeaderHandlerFunc[T](
		rateLimitHandler,
		func(quicstream.HandlerPrefix) (string, bool) { return string(name), true },
		handler,
	)

	newhandler := quicstreamheader.NewHandler(encs, rhandler, errhandler)
	newhandler = quicstream.TimeoutHandler(newhandler, timeoutf)

	_ = handlers.Add(
		name,
		newhandler,
	)
}
