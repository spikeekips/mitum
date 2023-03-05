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
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameNetworkHandlers             = ps.Name("network-handlers")
	PNameOperationProcessorsMap      = ps.Name("operation-processors-map")
	OperationProcessorsMapContextKey = util.ContextKey("operation-processors-map")
)

var HandlerPrefixMemberlistCallbackBroadcastMessage = "memberlist-callback-message"

func PNetworkHandlers(pctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare network handlers")

	var log *logging.Logging
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var design NodeDesign
	var local base.LocalNode
	var params *isaac.LocalParams
	var db isaac.Database
	var pool *isaacdatabase.TempPool
	var proposalMaker *isaac.ProposalMaker
	var memberlist *quicmemberlist.Memberlist
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
		MemberlistContextKey, &memberlist,
		SyncSourcePoolContextKey, &syncSourcePool,
		QuicstreamHandlersContextKey, &handlers,
		NodeInfoContextKey, &nodeinfo,
		SuffrageVotingVoteFuncContextKey, &svvotef,
		BallotboxContextKey, &ballotbox,
		FilterMemberlistNotifyMsgFuncContextKey, &filternotifymsg,
	); err != nil {
		return pctx, e(err, "")
	}

	sendOperationFilterf, err := sendOperationFilterFunc(pctx)
	if err != nil {
		return pctx, e(err, "")
	}

	lastBlockMapf := quicstreamHandlerLastBlockMapFunc(db)
	suffrageNodeConnInfof := quicstreamHandlerSuffrageNodeConnInfoFunc(db, memberlist)

	handlers.
		Add(isaacnetwork.HandlerPrefixOperation,
			quicstream.NewHeaderHandler(encs, 0, isaacnetwork.QuicstreamHandlerOperation(pool))).
		Add(isaacnetwork.HandlerPrefixSendOperation,
			quicstream.NewHeaderHandler(encs, 0, isaacnetwork.QuicstreamHandlerSendOperation(
				params, pool,
				db.ExistsInStateOperation,
				sendOperationFilterf,
				svvotef,
				func(id string, b []byte) error {
					return memberlist.CallbackBroadcast(b, id, nil)
				},
			))).
		Add(isaacnetwork.HandlerPrefixRequestProposal,
			quicstream.NewHeaderHandler(encs, 0, isaacnetwork.QuicstreamHandlerRequestProposal(
				local, pool, proposalMaker, db.LastBlockMap,
			))).
		Add(isaacnetwork.HandlerPrefixProposal,
			quicstream.NewHeaderHandler(encs, 0, isaacnetwork.QuicstreamHandlerProposal(pool))).
		Add(isaacnetwork.HandlerPrefixLastSuffrageProof,
			quicstream.NewHeaderHandler(encs, 0, isaacnetwork.QuicstreamHandlerLastSuffrageProof(
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
			))).
		Add(isaacnetwork.HandlerPrefixSuffrageProof,
			quicstream.NewHeaderHandler(encs, 0,
				isaacnetwork.QuicstreamHandlerSuffrageProof(db.SuffrageProofBytes))).
		Add(isaacnetwork.HandlerPrefixLastBlockMap,
			quicstream.NewHeaderHandler(encs, 0, isaacnetwork.QuicstreamHandlerLastBlockMap(lastBlockMapf))).
		Add(isaacnetwork.HandlerPrefixBlockMap,
			quicstream.NewHeaderHandler(encs, 0, isaacnetwork.QuicstreamHandlerBlockMap(db.BlockMapBytes))).
		Add(isaacnetwork.HandlerPrefixBlockMapItem,
			quicstream.NewHeaderHandler(encs, 0,
				isaacnetwork.QuicstreamHandlerBlockMapItem(
					func(height base.Height, item base.BlockMapItemType) (io.ReadCloser, bool, error) {
						e := util.StringErrorFunc("failed to get BlockMapItem")

						var menc encoder.Encoder

						switch m, found, err := db.BlockMap(height); {
						case err != nil:
							return nil, false, e(err, "")
						case !found:
							return nil, false, e(storage.ErrNotFound.Errorf("BlockMap not found"), "")
						default:
							menc = encs.Find(m.Encoder())
							if menc == nil {
								return nil, false, e(storage.ErrNotFound.Errorf("encoder of BlockMap not found"), "")
							}
						}

						reader, err := isaacblock.NewLocalFSReaderFromHeight(
							LocalFSDataDirectory(design.Storage.Base), height, menc,
						)
						if err != nil {
							return nil, false, e(err, "")
						}
						defer func() {
							_ = reader.Close()
						}()

						return reader.Reader(item)
					},
				))).
		Add(isaacnetwork.HandlerPrefixNodeChallenge,
			quicstream.NewHeaderHandler(encs, 0,
				isaacnetwork.QuicstreamHandlerNodeChallenge(local, params))).
		Add(isaacnetwork.HandlerPrefixSuffrageNodeConnInfo,
			quicstream.NewHeaderHandler(encs, 0,
				isaacnetwork.QuicstreamHandlerSuffrageNodeConnInfo(suffrageNodeConnInfof))).
		Add(isaacnetwork.HandlerPrefixSyncSourceConnInfo,
			quicstream.NewHeaderHandler(encs, 0, isaacnetwork.QuicstreamHandlerSyncSourceConnInfo(
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
			))).
		Add(isaacnetwork.HandlerPrefixState,
			quicstream.NewHeaderHandler(encs, 0, isaacnetwork.QuicstreamHandlerState(db.StateBytes))).
		Add(isaacnetwork.HandlerPrefixExistsInStateOperation,
			quicstream.NewHeaderHandler(encs, 0,
				isaacnetwork.QuicstreamHandlerExistsInStateOperation(db.ExistsInStateOperation))).
		Add(isaacnetwork.HandlerPrefixNodeInfo,
			quicstream.NewHeaderHandler(encs, 0, isaacnetwork.QuicstreamHandlerNodeInfo(
				quicstreamHandlerGetNodeInfoFunc(enc, nodeinfo)))).
		Add(HandlerPrefixMemberlistCallbackBroadcastMessage,
			quicstream.NewHeaderHandler(encs, 0, memberlist.CallbackBroadcastHandler())).
		Add(isaacnetwork.HandlerPrefixSendBallots,
			quicstream.NewHeaderHandler(encs, 0, isaacnetwork.QuicstreamHandlerSendBallots(
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

					_, err := ballotbox.VoteSignFact(bl, params.Threshold())

					return err
				},
			)))

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

	_ = set.Add(isaac.SuffrageWithdrawOperationHint, func(height base.Height) (base.OperationProcessor, error) {
		policy := db.LastNetworkPolicy()
		if policy == nil { // NOTE Usually it means empty block data
			return nil, nil
		}

		return isaacoperation.NewSuffrageWithdrawProcessor(
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
	lastid := util.EmptyLocked("")

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

				return "", util.ErrLockedSetIgnore.Call()
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
