package launch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameNetworkHandlers             = ps.PName("network-handlers")
	PNameOperationProcessorsMap      = ps.PName("operation-processors-map")
	OperationProcessorsMapContextKey = ps.ContextKey("operation-processors-map")
)

func PNetworkHandlers(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare network handlers")

	var encs *encoder.Encoders
	var enc encoder.Encoder
	var design NodeDesign
	var local base.LocalNode
	var policy *isaac.NodePolicy
	var db isaac.Database
	var pool *isaacdatabase.TempPool
	var proposalMaker *isaac.ProposalMaker
	var memberlist *quicmemberlist.Memberlist
	var syncSourcePool *isaac.SyncSourcePool
	var handlers *quicstream.PrefixHandler
	var nodeinfo *isaacnetwork.NodeInfoUpdater

	if err := ps.LoadsFromContextOK(ctx,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LocalContextKey, &local,
		NodePolicyContextKey, &policy,
		CenterDatabaseContextKey, &db,
		PoolDatabaseContextKey, &pool,
		ProposalMakerContextKey, &proposalMaker,
		MemberlistContextKey, &memberlist,
		SyncSourcePoolContextKey, &syncSourcePool,
		QuicstreamHandlersContextKey, &handlers,
		NodeInfoContextKey, &nodeinfo,
	); err != nil {
		return ctx, e(err, "")
	}

	sendOperationFilterf, err := sendOperationFilterFunc(ctx)
	if err != nil {
		return ctx, e(err, "")
	}

	idletimeout := time.Second * 2 //nolint:gomnd //...
	lastBlockMapf := quicstreamHandlerLastBlockMapFunc(db)
	suffrageNodeConnInfof := quicstreamHandlerSuffrageNodeConnInfoFunc(db, memberlist)

	handlers.
		Add(isaacnetwork.HandlerPrefixOperation, isaacnetwork.QuicstreamHandlerOperation(encs, idletimeout, pool)).
		Add(isaacnetwork.HandlerPrefixSendOperation,
			isaacnetwork.QuicstreamHandlerSendOperation(
				encs, idletimeout, policy, pool, db.ExistsInStateOperation, sendOperationFilterf),
		).
		Add(isaacnetwork.HandlerPrefixRequestProposal,
			isaacnetwork.QuicstreamHandlerRequestProposal(encs, idletimeout,
				local, pool, proposalMaker, lastBlockMapf,
			),
		).
		Add(isaacnetwork.HandlerPrefixProposal,
			isaacnetwork.QuicstreamHandlerProposal(encs, idletimeout, pool),
		).
		Add(isaacnetwork.HandlerPrefixLastSuffrageProof,
			isaacnetwork.QuicstreamHandlerLastSuffrageProof(encs, idletimeout,
				func(last util.Hash) (base.SuffrageProof, bool, error) {
					switch proof, found, err := db.LastSuffrageProof(); {
					case err != nil:
						return nil, false, err
					case !found:
						return nil, false, storage.NotFoundError.Errorf("last SuffrageProof not found")
					case last != nil && last.Equal(proof.Map().Manifest().Suffrage()):
						return nil, false, nil
					default:
						return proof, true, nil
					}
				},
			),
		).
		Add(isaacnetwork.HandlerPrefixSuffrageProof,
			isaacnetwork.QuicstreamHandlerSuffrageProof(encs, idletimeout, db.SuffrageProof),
		).
		Add(isaacnetwork.HandlerPrefixLastBlockMap,
			isaacnetwork.QuicstreamHandlerLastBlockMap(encs, idletimeout, lastBlockMapf),
		).
		Add(isaacnetwork.HandlerPrefixBlockMap,
			isaacnetwork.QuicstreamHandlerBlockMap(encs, idletimeout, db.BlockMap),
		).
		Add(isaacnetwork.HandlerPrefixBlockMapItem,
			isaacnetwork.QuicstreamHandlerBlockMapItem(encs, idletimeout, idletimeout*2, //nolint:gomnd //...
				func(height base.Height, item base.BlockMapItemType) (io.ReadCloser, bool, error) {
					e := util.StringErrorFunc("failed to get BlockMapItem")

					var menc encoder.Encoder

					switch m, found, err := db.BlockMap(height); {
					case err != nil:
						return nil, false, e(err, "")
					case !found:
						return nil, false, e(storage.NotFoundError.Errorf("BlockMap not found"), "")
					default:
						menc = encs.Find(m.Encoder())
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
			),
		).
		Add(isaacnetwork.HandlerPrefixNodeChallenge,
			isaacnetwork.QuicstreamHandlerNodeChallenge(encs, idletimeout, local, policy),
		).
		Add(isaacnetwork.HandlerPrefixSuffrageNodeConnInfo,
			isaacnetwork.QuicstreamHandlerSuffrageNodeConnInfo(encs, idletimeout, suffrageNodeConnInfof),
		).
		Add(isaacnetwork.HandlerPrefixSyncSourceConnInfo,
			isaacnetwork.QuicstreamHandlerSyncSourceConnInfo(encs, idletimeout,
				func() ([]isaac.NodeConnInfo, error) {
					// FIXME cache result

					members := make([]isaac.NodeConnInfo, syncSourcePool.Len()*2)

					var i int
					syncSourcePool.Actives(func(nci isaac.NodeConnInfo) bool {
						members[i] = nci
						i++

						return true
					})

					return members[:i], nil
				},
			),
		).
		Add(isaacnetwork.HandlerPrefixState,
			isaacnetwork.QuicstreamHandlerState(encs, idletimeout, db.State),
		).
		Add(isaacnetwork.HandlerPrefixExistsInStateOperation,
			isaacnetwork.QuicstreamHandlerExistsInStateOperation(encs, idletimeout, db.ExistsInStateOperation),
		).
		Add(isaacnetwork.HandlerPrefixNodeInfo,
			isaacnetwork.QuicstreamHandlerNodeInfo(encs, idletimeout, quicstreamHandlerGetNodeInfoFunc(enc, nodeinfo)),
		).
		Add(HandlerPrefixPprof, NetworkHandlerPprofFunc(encs))

	return ctx, nil
}

func POperationProcessorsMap(ctx context.Context) (context.Context, error) {
	var nodepolicy *isaac.NodePolicy
	var db isaac.Database

	if err := ps.LoadsFromContextOK(ctx,
		NodePolicyContextKey, &nodepolicy,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return ctx, err
	}

	limiterf, err := NewSuffrageCandidateLimiterFunc(ctx)
	if err != nil {
		return ctx, err
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
			nodepolicy.Threshold(),
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

	ctx = context.WithValue(ctx, OperationProcessorsMapContextKey, set) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func sendOperationFilterFunc(ctx context.Context) (
	func(base.Operation) (bool, error),
	error,
) {
	var db isaac.Database
	var oprs *hint.CompatibleSet

	if err := ps.LoadsFromContextOK(ctx,
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
		case !operationfilterf(hinter.Hint().Bytes()):
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

		reason, err := f(context.Background(), db.State)
		if err != nil {
			return false, err
		}

		return reason == nil, reason
	}, nil
}

func quicstreamHandlerLastBlockMapFunc(
	db isaac.Database,
) func(last util.Hash) (base.BlockMap, bool, error) {
	return func(last util.Hash) (base.BlockMap, bool, error) {
		switch m, found, err := db.LastBlockMap(); {
		case err != nil:
			return nil, false, err
		case !found:
			return nil, false, storage.NotFoundError.Errorf("last BlockMap not found")
		case last != nil && last.Equal(m.Manifest().Hash()):
			return nil, false, nil
		default:
			return m, true, nil
		}
	}
}

func quicstreamHandlerSuffrageNodeConnInfoFunc(
	db isaac.Database,
	memberlist *quicmemberlist.Memberlist,
) func() ([]isaac.NodeConnInfo, error) {
	return func() ([]isaac.NodeConnInfo, error) {
		// FIXME cache result

		var suf base.Suffrage

		switch proof, found, err := db.LastSuffrageProof(); {
		case err != nil:
			return nil, err
		case !found:
			return nil, storage.NotFoundError.Errorf("last SuffrageProof not found")
		default:
			i, err := proof.Suffrage()
			if err != nil {
				return nil, err
			}

			suf = i
		}

		members := make([]isaac.NodeConnInfo, memberlist.MembersLen()*2)

		var i int
		memberlist.Remotes(func(node quicmemberlist.Node) bool {
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
	var lastid string
	var lastb []byte

	startedAt := nodeinfo.StartedAt()

	uptimet := []byte("<uptime>")
	updateUptime := func(b []byte) []byte {
		return bytes.Replace(
			b,
			uptimet,
			[]byte(fmt.Sprintf("%0.3f", localtime.UTCNow().Sub(startedAt).Seconds())),
			1,
		)
	}

	return func() ([]byte, error) {
		if nodeinfo.ID() == lastid {
			return updateUptime(lastb), nil
		}

		jm := nodeinfo.NodeInfo().JSONMarshaler()
		jm.Local.Uptime = string(uptimet)

		b, err := enc.Marshal(jm)
		if err != nil {
			return nil, err
		}

		lastid = nodeinfo.ID()
		lastb = b

		return updateUptime(b), nil
	}
}

func OperationPreProcess(
	oprs *hint.CompatibleSet,
	op base.Operation,
	height base.Height,
) (
	preprocess func(context.Context, base.GetStateFunc) (base.OperationProcessReasonError, error),
	cancelf func() error,
	_ error,
) {
	v := oprs.Find(op.Hint())
	if v == nil {
		return op.PreProcess, func() error { return nil }, nil
	}

	f := v.(func(height base.Height) (base.OperationProcessor, error)) //nolint:forcetypeassert //...

	switch opp, err := f(height); {
	case err != nil:
		return nil, nil, err
	default:
		return func(ctx context.Context, getStateFunc base.GetStateFunc) (base.OperationProcessReasonError, error) {
			return opp.PreProcess(ctx, op, getStateFunc)
		}, opp.Close, nil
	}
}
