package launch

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
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
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	"golang.org/x/exp/slices"
)

var (
	PNameNetworkHandlers             = ps.Name("network-handlers")
	PNameOperationProcessorsMap      = ps.Name("operation-processors-map")
	OperationProcessorsMapContextKey = util.ContextKey("operation-processors-map")
)

var (
	//revive:disable:line-length-limit
	HandlerPrefixMemberlistCallbackBroadcastMessageString = "memberlist-callback-broadcast-message"
	HandlerPrefixMemberlistEnsureBroadcastMessageString   = "memberlist-ensure-broadcast-message"

	HandlerPrefixMemberlistCallbackBroadcastMessage = quicstream.HashPrefix(HandlerPrefixMemberlistCallbackBroadcastMessageString)
	HandlerPrefixMemberlistEnsureBroadcastMessage   = quicstream.HashPrefix(HandlerPrefixMemberlistEnsureBroadcastMessageString)
	//revive:enable:line-length-limit
)

func PNetworkHandlers(pctx context.Context) (context.Context, error) {
	e := util.StringError("prepare network handlers")

	var log *logging.Logging
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var design NodeDesign
	var local base.LocalNode
	var params *isaac.LocalParams
	var db isaac.Database
	var pool *isaacdatabase.TempPool
	var proposalMaker *isaac.ProposalMaker
	var m *quicmemberlist.Memberlist
	var syncSourcePool *isaac.SyncSourcePool
	var handlers *quicstream.PrefixHandler
	var nodeinfo *isaacnetwork.NodeInfoUpdater
	var svvotef isaac.SuffrageVoteFunc
	var ballotbox *isaacstates.Ballotbox
	var filternotifymsg quicmemberlist.FilterNotifyMsgFunc

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
		PoolDatabaseContextKey, &pool,
		ProposalMakerContextKey, &proposalMaker,
		MemberlistContextKey, &m,
		SyncSourcePoolContextKey, &syncSourcePool,
		QuicstreamHandlersContextKey, &handlers,
		NodeInfoContextKey, &nodeinfo,
		SuffrageVotingVoteFuncContextKey, &svvotef,
		BallotboxContextKey, &ballotbox,
		FilterMemberlistNotifyMsgFuncContextKey, &filternotifymsg,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	lastBlockMapf := quicstreamHandlerLastBlockMapFunc(db)
	suffrageNodeConnInfof := quicstreamHandlerSuffrageNodeConnInfoFunc(db, m)

	handlers.
		Add(isaacnetwork.HandlerPrefixLastSuffrageProof,
			quicstreamheader.NewHandler(encs, 0, isaacnetwork.QuicstreamHandlerLastSuffrageProof(
				func(last util.Hash) (hint.Hint, []byte, []byte, bool, error) {
					enchint, metabytes, body, found, lastheight, err := db.LastSuffrageProofBytes()

					switch {
					case err != nil:
						return enchint, nil, nil, false, err
					case !found:
						return enchint, nil, nil, false, storage.ErrNotFound.Errorf("last SuffrageProof not found")
					}

					switch h, err := isaacdatabase.ReadHashRecordMeta(metabytes); {
					case err != nil:
						return enchint, nil, nil, true, err
					case last != nil && last.Equal(h):
						nbody, _ := util.NewLengthedBytesSlice(0x01, [][]byte{lastheight.Bytes(), nil})

						return enchint, nil, nbody, false, nil
					default:
						nbody, _ := util.NewLengthedBytesSlice(0x01, [][]byte{lastheight.Bytes(), body})

						return enchint, metabytes, nbody, true, nil
					}
				},
			), nil)).
		Add(isaacnetwork.HandlerPrefixSuffrageProof,
			quicstreamheader.NewHandler(encs, 0,
				isaacnetwork.QuicstreamHandlerSuffrageProof(db.SuffrageProofBytes), nil)).
		Add(isaacnetwork.HandlerPrefixLastBlockMap,
			quicstreamheader.NewHandler(encs, 0, isaacnetwork.QuicstreamHandlerLastBlockMap(lastBlockMapf), nil)).
		Add(isaacnetwork.HandlerPrefixBlockMap,
			quicstreamheader.NewHandler(encs, 0, isaacnetwork.QuicstreamHandlerBlockMap(db.BlockMapBytes), nil)).
		Add(isaacnetwork.HandlerPrefixBlockMapItem,
			quicstreamheader.NewHandler(encs, 0,
				isaacnetwork.QuicstreamHandlerBlockMapItem(
					func(height base.Height, item base.BlockMapItemType) (io.ReadCloser, bool, error) {
						e := util.StringError("get BlockMapItem")

						var menc encoder.Encoder

						switch m, found, err := db.BlockMap(height); {
						case err != nil:
							return nil, false, e.Wrap(err)
						case !found:
							return nil, false, e.Wrap(storage.ErrNotFound.Errorf("BlockMap not found"))
						default:
							menc = encs.Find(m.Encoder())
							if menc == nil {
								return nil, false, e.Wrap(storage.ErrNotFound.Errorf("encoder of BlockMap not found"))
							}
						}

						reader, err := isaacblock.NewLocalFSReaderFromHeight(
							LocalFSDataDirectory(design.Storage.Base), height, menc,
						)
						if err != nil {
							return nil, false, e.Wrap(err)
						}
						defer func() {
							_ = reader.Close()
						}()

						return reader.Reader(item)
					},
				), nil)).
		Add(isaacnetwork.HandlerPrefixNodeChallenge,
			quicstreamheader.NewHandler(encs, 0,
				isaacnetwork.QuicstreamHandlerNodeChallenge(local, params), nil)).
		Add(isaacnetwork.HandlerPrefixSuffrageNodeConnInfo,
			quicstreamheader.NewHandler(encs, 0,
				isaacnetwork.QuicstreamHandlerSuffrageNodeConnInfo(suffrageNodeConnInfof), nil)).
		Add(isaacnetwork.HandlerPrefixSyncSourceConnInfo,
			quicstreamheader.NewHandler(encs, 0, isaacnetwork.QuicstreamHandlerSyncSourceConnInfo(
				func() ([]isaac.NodeConnInfo, error) {
					members := make([]isaac.NodeConnInfo, syncSourcePool.Len()*2)

					var i int
					syncSourcePool.Actives(func(nci isaac.NodeConnInfo) bool {
						members[i] = nci
						i++

						return true
					})

					return members[:i], nil
				},
			), nil)).
		Add(isaacnetwork.HandlerPrefixState,
			quicstreamheader.NewHandler(encs, 0, isaacnetwork.QuicstreamHandlerState(db.StateBytes), nil)).
		Add(isaacnetwork.HandlerPrefixExistsInStateOperation,
			quicstreamheader.NewHandler(encs, 0,
				isaacnetwork.QuicstreamHandlerExistsInStateOperation(db.ExistsInStateOperation), nil)).
		Add(isaacnetwork.HandlerPrefixNodeInfo,
			quicstreamheader.NewHandler(encs, 0, isaacnetwork.QuicstreamHandlerNodeInfo(
				quicstreamHandlerGetNodeInfoFunc(enc, nodeinfo)), nil)).
		Add(isaacnetwork.HandlerPrefixSendBallots,
			quicstreamheader.NewHandler(encs, 0, isaacnetwork.QuicstreamHandlerSendBallots(
				params,
				func(bl base.BallotSignFact) error {
					switch passed, err := filternotifymsg(bl); {
					case err != nil:
						log.Log().Trace().
							Str("module", "filter-notify-msg-send-ballots").
							Err(err).
							Interface("message", bl).
							Msg("filter error")

						fallthrough
					case !passed:
						log.Log().Trace().
							Str("module", "filter-notify-msg-send-ballots").
							Interface("message", bl).
							Msg("filtered")

						return nil
					}

					_, err := ballotbox.VoteSignFact(bl)

					return err
				},
			), nil))

	if err := attachMemberlistNetworkHandlers(pctx); err != nil {
		return pctx, err
	}

	return pctx, nil
}

func POperationProcessorsMap(pctx context.Context) (context.Context, error) {
	var params *isaac.LocalParams
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return pctx, err
	}

	limiterf, err := newSuffrageCandidateLimiterFunc(pctx)
	if err != nil {
		return pctx, err
	}

	set := hint.NewCompatibleSet()

	_ = set.Add(isaacoperation.SuffrageCandidateHint, func(height base.Height) (base.OperationProcessor, error) {
		policy := db.LastNetworkPolicy()
		if policy == nil { // NOTE Usually it means empty block data
			return nil, nil
		}

		return isaacoperation.NewSuffrageCandidateProcessor(
			height,
			db.State,
			limiterf,
			nil,
			policy.SuffrageCandidateLifespan(),
		)
	})

	_ = set.Add(isaacoperation.SuffrageJoinHint, func(height base.Height) (base.OperationProcessor, error) {
		policy := db.LastNetworkPolicy()
		if policy == nil { // NOTE Usually it means empty block data
			return nil, nil
		}

		return isaacoperation.NewSuffrageJoinProcessor(
			height,
			params.Threshold(),
			db.State,
			nil,
			nil,
		)
	})

	_ = set.Add(isaac.SuffrageExpelOperationHint, func(height base.Height) (base.OperationProcessor, error) {
		policy := db.LastNetworkPolicy()
		if policy == nil { // NOTE Usually it means empty block data
			return nil, nil
		}

		return isaacoperation.NewSuffrageExpelProcessor(
			height,
			db.State,
			nil,
			nil,
		)
	})

	_ = set.Add(isaacoperation.SuffrageDisjoinHint, func(height base.Height) (base.OperationProcessor, error) {
		return isaacoperation.NewSuffrageDisjoinProcessor(
			height,
			db.State,
			nil,
			nil,
		)
	})

	return context.WithValue(pctx, OperationProcessorsMapContextKey, set), nil
}

func sendOperationFilterFunc(pctx context.Context) (
	func(base.Operation) (bool, error),
	error,
) {
	var db isaac.Database
	var oprs *hint.CompatibleSet

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

		f, closef, err := OperationPreProcess(oprs, op, height)
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

func quicstreamHandlerLastBlockMapFunc(
	db isaac.Database,
) func(last util.Hash) (hint.Hint, []byte, []byte, bool, error) {
	return func(last util.Hash) (hint.Hint, []byte, []byte, bool, error) {
		enchint, metabytes, body, found, err := db.LastBlockMapBytes()

		switch {
		case err != nil:
			return enchint, nil, nil, false, err
		case !found:
			return enchint, nil, nil, false, storage.ErrNotFound.Errorf("last BlockMap not found")
		}

		switch h, err := isaacdatabase.ReadHashRecordMeta(metabytes); {
		case err != nil:
			return enchint, nil, nil, false, err
		case last != nil && last.Equal(h):
			return enchint, nil, nil, false, nil
		default:
			return enchint, metabytes, body, true, nil
		}
	}
}

func quicstreamHandlerSuffrageNodeConnInfoFunc(
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

		members := make([]isaac.NodeConnInfo, memberlist.MembersLen()*2)

		var i int
		memberlist.Remotes(func(node quicmemberlist.Member) bool {
			if !suf.ExistsPublickey(node.Address(), node.Publickey()) {
				return true
			}

			members[i] = isaacnetwork.NewNodeConnInfoFromMemberlistNode(node)
			i++

			return true
		})

		return members[:i], nil
	}
}

func quicstreamHandlerGetNodeInfoFunc(
	enc encoder.Encoder,
	nodeinfo *isaacnetwork.NodeInfoUpdater,
) func() ([]byte, error) {
	startedAt := nodeinfo.StartedAt()
	lastid := util.EmptyLocked[string]()

	uptimet := []byte("<uptime>")
	updateUptime := func(b []byte) []byte {
		return bytes.Replace(
			b,
			uptimet,
			[]byte(fmt.Sprintf("%0.3f", localtime.Now().UTC().Sub(startedAt).Seconds())),
			1,
		)
	}

	var lastb []byte

	return func() ([]byte, error) {
		var b []byte

		if _, err := lastid.Set(func(last string, isempty bool) (string, error) {
			if !isempty && nodeinfo.ID() == last {
				b = updateUptime(lastb)

				return "", util.ErrLockedSetIgnore.WithStack()
			}

			jm := nodeinfo.NodeInfo().JSONMarshaler()
			jm.Local.Uptime = string(uptimet)

			switch i, err := enc.Marshal(jm); {
			case err != nil:
				return "", err
			default:
				lastb = i

				b = updateUptime(i)

				return nodeinfo.ID(), nil
			}
		}); err != nil {
			return nil, err
		}

		return b, nil
	}
}

func OperationPreProcess(
	oprs *hint.CompatibleSet,
	op base.Operation,
	height base.Height,
) (
	preprocess func(context.Context, base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error),
	cancel func() error,
	_ error,
) {
	v := oprs.Find(op.Hint())
	if v == nil {
		return op.PreProcess, util.EmptyCancelFunc, nil
	}

	f := v.(func(height base.Height) (base.OperationProcessor, error)) //nolint:forcetypeassert //...

	switch opp, err := f(height); {
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

func attachMemberlistNetworkHandlers(pctx context.Context) error {
	var params *isaac.LocalParams
	var encs *encoder.Encoders
	var handlers *quicstream.PrefixHandler
	var m *quicmemberlist.Memberlist
	var sp *SuffragePool

	if err := util.LoadFromContextOK(pctx,
		EncodersContextKey, &encs,
		LocalParamsContextKey, &params,
		QuicstreamHandlersContextKey, &handlers,
		MemberlistContextKey, &m,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return err
	}

	handlers.
		Add(HandlerPrefixMemberlistCallbackBroadcastMessage,
			quicstreamheader.NewHandler(encs, 0, m.CallbackBroadcastHandler(), nil)).
		Add(HandlerPrefixMemberlistEnsureBroadcastMessage,
			quicstreamheader.NewHandler(encs, 0, m.EnsureBroadcastHandler(
				params.NetworkID(),
				func(node base.Address) (base.Publickey, bool, error) {
					switch suf, found, err := sp.Last(); {
					case err != nil:
						return nil, false, err
					case !found, suf == nil, !suf.Exists(node):
						return nil, false, nil
					default:
						var foundnode base.Node

						_ = slices.IndexFunc[base.Node](suf.Nodes(), func(n base.Node) bool {
							if node.Equal(n.Address()) {
								foundnode = n

								return true
							}

							return false
						})

						return foundnode.Publickey(), true, nil
					}
				},
			), nil))

	return nil
}
