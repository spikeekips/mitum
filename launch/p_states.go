package launch

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	PNameStatesReady                                = ps.Name("states-ready")
	PNameStates                                     = ps.Name("states")
	PNameBallotbox                                  = ps.Name("ballotbox")
	PNameProposalProcessors                         = ps.Name("proposal-processors")
	PNameStatesSetHandlers                          = ps.Name("states-set-handlers")
	BallotboxContextKey                             = ps.ContextKey("ballotbox")
	StatesContextKey                                = ps.ContextKey("states")
	ProposalProcessorsContextKey                    = ps.ContextKey("proposal-processors")
	ProposalSelectorContextKey                      = ps.ContextKey("proposal-selector")
	LastVoteproofsHandlerContextKey                 = ps.ContextKey("last-voteproofs-handler")
	WhenNewBlockSavedInSyncingStateFuncContextKey   = ps.ContextKey("when-new-block-saved-in-syncing-state-func")
	WhenNewBlockSavedInConsensusStateFuncContextKey = ps.ContextKey("when-new-block-saved-in-consensus-state-func")
)

func PBallotbox(ctx context.Context) (context.Context, error) {
	var db isaac.Database

	if err := ps.LoadsFromContextOK(ctx, CenterDatabaseContextKey, &db); err != nil {
		return ctx, err
	}

	ballotbox := isaacstates.NewBallotbox(
		func(blockheight base.Height) (base.Suffrage, bool, error) {
			return isaac.GetSuffrageFromDatabase(db, blockheight)
		},
	)

	ctx = context.WithValue(ctx, BallotboxContextKey, ballotbox) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func PStates(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare states")

	var enc encoder.Encoder
	var ballotbox *isaacstates.Ballotbox
	var pps *isaac.ProposalProcessors
	var memberlist *quicmemberlist.Memberlist

	if err := ps.LoadsFromContextOK(ctx,
		EncoderContextKey, &enc,
		BallotboxContextKey, &ballotbox,
		ProposalProcessorsContextKey, &pps,
		MemberlistContextKey, &memberlist,
	); err != nil {
		return ctx, e(err, "")
	}

	lvps := isaacstates.NewLastVoteproofsHandler()

	states := isaacstates.NewStates(
		ballotbox,
		lvps,
		func(ballot base.Ballot) error {
			ee := util.StringErrorFunc("failed to broadcast ballot")

			b, err := enc.Marshal(ballot)
			if err != nil {
				return ee(err, "")
			}

			id := valuehash.NewSHA256(ballot.HashBytes()).String()

			if err := BroadcastThruMemberlist(memberlist, id, b, nil); err != nil {
				return ee(err, "")
			}

			return nil
		},
	)

	proposalSelector, err := NewProposalSelector(ctx)
	if err != nil {
		return ctx, e(err, "")
	}

	//revive:disable:modifies-parameter
	ctx = context.WithValue(ctx, LastVoteproofsHandlerContextKey, lvps)
	ctx = context.WithValue(ctx, StatesContextKey, states)
	ctx = context.WithValue(ctx, ProposalSelectorContextKey, proposalSelector)
	//revive:enable:modifies-parameter

	return ctx, nil
}

func PCloseStates(ctx context.Context) (context.Context, error) {
	var states *isaacstates.States
	if err := ps.LoadFromContext(ctx, StatesContextKey, &states); err != nil {
		return ctx, err
	}

	if states != nil {
		if err := states.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
			return ctx, err
		}
	}

	return ctx, nil
}

func PStatesSetHandlers(ctx context.Context) (context.Context, error) { //revive:disable-line:function-length
	e := util.StringErrorFunc("failed to set states handler")

	var log *logging.Logging
	var local base.LocalNode
	var params *isaac.LocalParams
	var db isaac.Database
	var states *isaacstates.States
	var nodeinfo *isaacnetwork.NodeInfoUpdater
	var proposalSelector *isaac.BaseProposalSelector
	var nodeInConsensusNodesf func(base.Node, base.Height) (base.Suffrage, bool, error)
	var ballotbox *isaacstates.Ballotbox
	var pool *isaacdatabase.TempPool
	var lvps *isaacstates.LastVoteproofsHandler
	var pps *isaac.ProposalProcessors

	if err := ps.LoadsFromContextOK(ctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
		StatesContextKey, &states,
		NodeInfoContextKey, &nodeinfo,
		ProposalSelectorContextKey, &proposalSelector,
		NodeInConsensusNodesFuncContextKey, &nodeInConsensusNodesf,
		BallotboxContextKey, &ballotbox,
		PoolDatabaseContextKey, &pool,
		LastVoteproofsHandlerContextKey, &lvps,
		ProposalProcessorsContextKey, &pps,
	); err != nil {
		return ctx, e(err, "")
	}

	voteFunc := func(bl base.Ballot) (bool, error) {
		voted, err := ballotbox.Vote(bl, params.Threshold())
		if err != nil {
			return false, err
		}

		return voted, nil
	}

	joinMemberlistForStateHandlerf, err := joinMemberlistForStateHandlerFunc(ctx)
	if err != nil {
		return ctx, e(err, "")
	}

	leaveMemberlistForStateHandlerf, err := leaveMemberlistForStateHandlerFunc(ctx)
	if err != nil {
		return ctx, e(err, "")
	}

	var whenNewBlockSavedInSyncingStatef func(base.Height)

	switch err = ps.LoadFromContext(
		ctx, WhenNewBlockSavedInSyncingStateFuncContextKey, &whenNewBlockSavedInSyncingStatef); {
	case err != nil:
		return ctx, e(err, "")
	case whenNewBlockSavedInSyncingStatef == nil:
		whenNewBlockSavedInSyncingStatef = WhenNewBlockSavedInSyncingStateFunc(db, nodeinfo)
	}

	var whenNewBlockSavedInConsensusStatef func(base.Height)

	switch err = ps.LoadFromContext(
		ctx, WhenNewBlockSavedInConsensusStateFuncContextKey, &whenNewBlockSavedInConsensusStatef); {
	case err != nil:
		return ctx, e(err, "")
	case whenNewBlockSavedInConsensusStatef == nil:
		whenNewBlockSavedInConsensusStatef = WhenNewBlockSavedInConsensusStateFunc(params, ballotbox, db, nodeinfo)
	}

	newsyncerf, err := newSyncerFunc(ctx, params, db, lvps, whenNewBlockSavedInSyncingStatef)
	if err != nil {
		return ctx, e(err, "")
	}

	getLastManifestf := getLastManifestFunc(db)
	getManifestf := getManifestFunc(db)

	states.SetWhenStateSwitched(func(_, next isaacstates.StateType) {
		_ = nodeinfo.SetConsensusState(next)
	})

	syncinghandler := isaacstates.NewNewSyncingHandlerType(
		local, params, proposalSelector, newsyncerf, nodeInConsensusNodesf,
		joinMemberlistForStateHandlerf,
		leaveMemberlistForStateHandlerf,
		whenNewBlockSavedInSyncingStatef,
	)
	syncinghandler.SetWhenFinished(func(base.Height) {
		ballotbox.Count(params.Threshold())
	})

	states.
		SetHandler(isaacstates.StateBroken, isaacstates.NewNewBrokenHandlerType(local, params)).
		SetHandler(isaacstates.StateStopped, isaacstates.NewNewStoppedHandlerType(local, params)).
		SetHandler(
			isaacstates.StateBooting,
			isaacstates.NewNewBootingHandlerType(local, params,
				getLastManifestf, nodeInConsensusNodesf),
		).
		SetHandler(
			isaacstates.StateJoining,
			isaacstates.NewNewJoiningHandlerType(
				local, params, proposalSelector, getLastManifestf, nodeInConsensusNodesf,
				voteFunc, joinMemberlistForStateHandlerf, leaveMemberlistForStateHandlerf,
			),
		).
		SetHandler(
			isaacstates.StateConsensus,
			isaacstates.NewNewConsensusHandlerType(
				local, params, proposalSelector,
				getManifestf, nodeInConsensusNodesf, voteFunc, whenNewBlockSavedInConsensusStatef,
				pps,
			)).
		SetHandler(isaacstates.StateSyncing, syncinghandler)

	_ = states.SetLogging(log)

	// NOTE load last init, accept voteproof and last majority voteproof
	switch ivp, avp, found, err := pool.LastVoteproofs(); {
	case err != nil:
		return ctx, e(err, "")
	case !found:
	default:
		_ = states.LastVoteproofsHandler().Set(ivp)
		_ = states.LastVoteproofsHandler().Set(avp)
	}

	return ctx, nil
}

func BroadcastThruMemberlist(
	memberlist *quicmemberlist.Memberlist,
	id string,
	b []byte,
	notifych chan struct{},
) error {
	body := quicmemberlist.NewBroadcast(b, id, notifych)

	memberlist.Broadcast(body)

	if notifych != nil {
		<-notifych
	}

	return nil
}

func newSyncerFunc(
	pctx context.Context,
	params *isaac.LocalParams,
	db isaac.Database,
	lvps *isaacstates.LastVoteproofsHandler,
	whenNewBlockSavedInSyncingStatef func(base.Height),
) (
	func(height base.Height) (isaac.Syncer, error),
	error,
) {
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var design NodeDesign
	var st *leveldbstorage.Storage
	var perm isaac.PermanentDatabase

	if err := ps.LoadsFromContextOK(pctx,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LeveldbStorageContextKey, &st,
		PermanentDatabaseContextKey, &perm,
	); err != nil {
		return nil, err
	}

	syncerLastBlockMapf, err := syncerLastBlockMapFunc(pctx)
	if err != nil {
		return nil, err
	}

	syncerBlockMapf, err := syncerBlockMapFunc(pctx)
	if err != nil {
		return nil, err
	}

	syncerBlockMapItemf, err := syncerBlockMapItemFunc(pctx)
	if err != nil {
		return nil, err
	}

	setLastVoteproofsfFromBlockReaderf, err := setLastVoteproofsfFromBlockReaderFunc(pctx, lvps)
	if err != nil {
		return nil, err
	}

	newSyncerDeferredf, err := newSyncerDeferredFunc(pctx, db)
	if err != nil {
		return nil, err
	}

	return func(height base.Height) (isaac.Syncer, error) {
		e := util.StringErrorFunc("failed newSyncer")

		var prev base.BlockMap

		switch m, found, err := db.LastBlockMap(); {
		case err != nil:
			return nil, e(isaacstates.ErrUnpromising.Wrap(err), "")
		case found:
			prev = m
		}

		var tempsyncpool isaac.TempSyncPool

		switch i, err := isaacdatabase.NewLeveldbTempSyncPool(height, st, encs, enc); {
		case err != nil:
			return nil, e(isaacstates.ErrUnpromising.Wrap(err), "")
		default:
			tempsyncpool = i
		}

		syncer, err := isaacstates.NewSyncer(
			design.Storage.Base,
			func(height base.Height) (isaac.BlockWriteDatabase, func(context.Context) error, error) {
				bwdb, err := db.NewBlockWriteDatabase(height)
				if err != nil {
					return nil, nil, err
				}

				return bwdb,
					func(ctx context.Context) error {
						if err := MergeBlockWriteToPermanentDatabase(ctx, bwdb, perm); err != nil {
							return err
						}

						whenNewBlockSavedInSyncingStatef(height)

						return nil
					},
					nil
			},
			func(root string, blockmap base.BlockMap, bwdb isaac.BlockWriteDatabase) (isaac.BlockImporter, error) {
				return isaacblock.NewBlockImporter(
					LocalFSDataDirectory(root),
					encs,
					blockmap,
					bwdb,
					params.NetworkID(),
				)
			},
			prev,
			syncerLastBlockMapf,
			syncerBlockMapf,
			syncerBlockMapItemf,
			tempsyncpool,
			setLastVoteproofsfFromBlockReaderf,
		)
		if err != nil {
			return nil, e(err, "")
		}

		go newSyncerDeferredf(height, syncer)

		return syncer, nil
	}, nil
}

func syncerLastBlockMapFunc(pctx context.Context) (isaacstates.SyncerLastBlockMapFunc, error) {
	var client *isaacnetwork.QuicstreamClient
	var params base.LocalParams
	var syncSourcePool *isaac.SyncSourcePool

	if err := ps.LoadsFromContextOK(pctx,
		QuicstreamClientContextKey, &client,
		LocalParamsContextKey, &params,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	f := func(
		ctx context.Context, manifest util.Hash, ci quicstream.UDPConnInfo,
	) (_ base.BlockMap, updated bool, _ error) {
		switch m, updated, err := client.LastBlockMap(ctx, ci, manifest); {
		case err != nil, !updated:
			return m, updated, err
		default:
			if err := m.IsValid(params.NetworkID()); err != nil {
				return m, updated, err
			}

			return m, updated, nil
		}
	}

	return func(ctx context.Context, manifest util.Hash) (base.BlockMap, bool, error) {
		ml := util.EmptyLocked()

		numnodes := 3 // NOTE choose top 3 sync nodes

		if err := isaac.DistributeWorkerWithSyncSourcePool(
			ctx,
			syncSourcePool,
			numnodes,
			uint64(numnodes),
			nil,
			func(ctx context.Context, i, _ uint64, nci isaac.NodeConnInfo) error {
				ci, err := nci.UDPConnInfo()
				if err != nil {
					return err
				}

				m, updated, err := f(ctx, manifest, ci)
				switch {
				case err != nil:
					return err
				case !updated:
					return nil
				}

				_, err = ml.Set(func(v interface{}) (interface{}, error) {
					switch {
					case v == nil,
						m.Manifest().Height() > v.(base.BlockMap).Manifest().Height(): //nolint:forcetypeassert //...
						return m, nil
					default:
						return nil, util.ErrLockedSetIgnore.Errorf("old BlockMap")
					}
				})

				return err
			},
		); err != nil {
			return nil, false, err
		}

		switch v, isnil := ml.Value(); {
		case isnil:
			return nil, false, nil
		default:
			return v.(base.BlockMap), true, nil //nolint:forcetypeassert //...
		}
	}, nil
}

func syncerBlockMapFunc(pctx context.Context) ( //revive:disable-line:cognitive-complexity
	isaacstates.SyncerBlockMapFunc, error,
) {
	var client *isaacnetwork.QuicstreamClient
	var params base.LocalParams
	var syncSourcePool *isaac.SyncSourcePool

	if err := ps.LoadsFromContextOK(pctx,
		QuicstreamClientContextKey, &client,
		LocalParamsContextKey, &params,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	f := func(ctx context.Context, height base.Height, ci quicstream.UDPConnInfo) (base.BlockMap, bool, error) {
		switch m, found, err := client.BlockMap(ctx, ci, height); {
		case err != nil, !found:
			return m, found, err
		default:
			if err := m.IsValid(params.NetworkID()); err != nil {
				return m, true, err
			}

			return m, true, nil
		}
	}

	return func(ctx context.Context, height base.Height) (m base.BlockMap, found bool, _ error) {
		err := util.Retry(
			ctx,
			func() (bool, error) {
				numnodes := 3 // NOTE choose top 3 sync nodes
				result := util.EmptyLocked()

				_ = isaac.ErrGroupWorkerWithSyncSourcePool(
					ctx,
					syncSourcePool,
					numnodes,
					uint64(numnodes),
					func(ctx context.Context, i, _ uint64, nci isaac.NodeConnInfo) error {
						ci, err := nci.UDPConnInfo()
						if err != nil {
							return err
						}

						switch a, b, err := f(ctx, height, ci); {
						case err != nil:
							if quicstream.IsNetworkError(err) {
								return err
							}

							return nil
						case !b:
							return nil
						default:
							_, _ = result.Set(func(i interface{}) (interface{}, error) {
								if i != nil {
									return nil, errors.Errorf("already set")
								}

								return [2]interface{}{a, b}, nil
							})

							return errors.Errorf("stop")
						}
					},
				)

				v, isnil := result.Value()
				if isnil {
					return true, nil
				}

				i := v.([2]interface{}) //nolint:forcetypeassert //...

				m, found = i[0].(base.BlockMap), i[1].(bool) //nolint:forcetypeassert //...

				return false, nil
			},
			-1,
			time.Second,
		)

		return m, found, err
	}, nil
}

func syncerBlockMapItemFunc(pctx context.Context) (isaacstates.SyncerBlockMapItemFunc, error) {
	var client *isaacnetwork.QuicstreamClient
	var params base.LocalParams
	var syncSourcePool *isaac.SyncSourcePool

	if err := ps.LoadsFromContextOK(pctx,
		QuicstreamClientContextKey, &client,
		LocalParamsContextKey, &params,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	// FIXME support remote item like https or ftp?
	f := func(
		ctx context.Context, height base.Height, item base.BlockMapItemType, ci quicstream.UDPConnInfo,
	) (io.ReadCloser, func() error, bool, error) {
		r, cancel, found, err := client.BlockMapItem(ctx, ci, height, item)

		return r, cancel, found, err
	}

	return func(ctx context.Context, height base.Height, item base.BlockMapItemType) (
		reader io.ReadCloser, closef func() error, found bool, _ error,
	) {
		err := util.Retry(
			ctx,
			func() (bool, error) {
				numnodes := 3 // NOTE choose top 3 sync nodes
				result := util.EmptyLocked()

				_ = isaac.ErrGroupWorkerWithSyncSourcePool(
					ctx,
					syncSourcePool,
					numnodes,
					uint64(numnodes),
					func(ctx context.Context, i, _ uint64, nci isaac.NodeConnInfo) error {
						ci, err := nci.UDPConnInfo()
						if err != nil {
							return err
						}

						switch a, b, c, err := f(ctx, height, item, ci); {
						case err != nil:
							if quicstream.IsNetworkError(err) {
								return err
							}

							return nil
						case !c:
							return nil
						default:
							_, _ = result.Set(func(i interface{}) (interface{}, error) {
								if i != nil {
									_ = a.Close()
									_ = b()

									return nil, errors.Errorf("already set")
								}

								return [3]interface{}{a, b, c}, nil
							})

							return errors.Errorf("stop")
						}
					},
				)

				v, isnil := result.Value()
				if isnil {
					return true, nil
				}

				i := v.([3]interface{}) //nolint:forcetypeassert //...

				reader, closef, found = i[0].(io.ReadCloser), //nolint:forcetypeassert //...
					i[1].(func() error), i[2].(bool)

				return false, nil
			},
			-1,
			time.Second,
		)

		return reader, closef, found, err
	}, nil
}

func setLastVoteproofsfFromBlockReaderFunc(
	pctx context.Context,
	lvps *isaacstates.LastVoteproofsHandler,
) (func(isaac.BlockReader) error, error) {
	var pool *isaacdatabase.TempPool

	if err := ps.LoadsFromContextOK(pctx,
		PoolDatabaseContextKey, &pool,
	); err != nil {
		return nil, err
	}

	return func(reader isaac.BlockReader) error {
		switch v, found, err := reader.Item(base.BlockMapItemTypeVoteproofs); {
		case err != nil:
			return err
		case !found:
			return errors.Errorf("voteproofs not found at last")
		default:
			vps := v.([]base.Voteproof) //nolint:forcetypeassert //...

			ivp := vps[0].(base.INITVoteproof)   //nolint:forcetypeassert //...
			avp := vps[1].(base.ACCEPTVoteproof) //nolint:forcetypeassert //...

			if err := pool.SetLastVoteproofs(ivp, avp); err != nil {
				return err
			}

			_ = lvps.Set(ivp)
			_ = lvps.Set(avp)

			return nil
		}
	}, nil
}

func joinMemberlistForStateHandlerFunc(pctx context.Context) (
	func(context.Context, base.Suffrage) error,
	error,
) {
	var discoveries []quicstream.UDPConnInfo
	var m *quicmemberlist.Memberlist

	if err := ps.LoadsFromContextOK(pctx,
		DiscoveryContextKey, &discoveries,
		MemberlistContextKey, &m,
	); err != nil {
		return nil, err
	}

	return func(ctx context.Context, suf base.Suffrage) error {
		if len(discoveries) < 1 {
			return nil
		}

		if m.IsJoined() {
			return nil
		}

		return util.Retry(
			ctx,
			func() (bool, error) {
				if err := m.EnsureJoin(discoveries); err != nil {
					if !errors.Is(err, quicmemberlist.ErrNotYetJoined) {
						return false, err
					}
				}

				return !m.IsJoined(), nil // NOTE keep trying until joined to remotes
			},
			-1,
			time.Second,
		)
	}, nil
}

func leaveMemberlistForStateHandlerFunc(pctx context.Context) (
	func(time.Duration) error,
	error,
) {
	var m *quicmemberlist.Memberlist
	if err := ps.LoadFromContextOK(pctx, MemberlistContextKey, &m); err != nil {
		return nil, err
	}

	return func(timeout time.Duration) error {
		switch {
		case m.MembersLen() < 1:
			return nil
		default:
			return m.Leave(timeout)
		}
	}, nil
}

func getLastManifestFunc(db isaac.Database) func() (base.Manifest, bool, error) {
	return func() (base.Manifest, bool, error) {
		switch m, found, err := db.LastBlockMap(); {
		case err != nil || !found:
			return nil, found, err
		default:
			return m.Manifest(), true, nil
		}
	}
}

func getManifestFunc(db isaac.Database) func(height base.Height) (base.Manifest, error) {
	return func(height base.Height) (base.Manifest, error) {
		switch m, found, err := db.BlockMap(height); {
		case err != nil:
			return nil, err
		case !found:
			return nil, nil
		default:
			return m.Manifest(), nil
		}
	}
}

func WhenNewBlockSavedInSyncingStateFunc(
	db isaac.Database,
	nodeinfo *isaacnetwork.NodeInfoUpdater,
) func(base.Height) {
	return func(height base.Height) {
		_ = UpdateNodeInfoWithNewBlock(db, nodeinfo)
	}
}

func WhenNewBlockSavedInConsensusStateFunc(
	params *isaac.LocalParams,
	ballotbox *isaacstates.Ballotbox,
	db isaac.Database,
	nodeinfo *isaacnetwork.NodeInfoUpdater,
) func(base.Height) {
	return func(height base.Height) {
		ballotbox.Count(params.Threshold())

		_ = UpdateNodeInfoWithNewBlock(db, nodeinfo)
	}
}

func newSyncerDeferredFunc(pctx context.Context, db isaac.Database) (
	func(base.Height, *isaacstates.Syncer),
	error,
) {
	var log *logging.Logging

	if err := ps.LoadsFromContextOK(pctx, LoggingContextKey, &log); err != nil {
		return nil, err
	}

	return func(height base.Height, syncer *isaacstates.Syncer) {
		l := log.Log().With().Str("module", "new-syncer").Logger()

		if err := db.MergeAllPermanent(); err != nil {
			l.Error().Err(err).Msg("failed to merge temps")

			return
		}

		log.Log().Debug().Msg("SuffrageProofs built")

		err := syncer.Start()
		if err != nil {
			l.Error().Err(err).Msg("syncer stopped")

			return
		}

		_ = syncer.Add(height)

		l.Debug().Interface("height", height).Msg("new syncer created")
	}, nil
}
