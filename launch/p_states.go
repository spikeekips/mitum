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
	PNameProposerSelector                           = ps.Name("proposer-selector")
	PNameBallotStuckResolver                        = ps.Name("ballot-stuck-resolver")
	BallotboxContextKey                             = util.ContextKey("ballotbox")
	StatesContextKey                                = util.ContextKey("states")
	ProposalProcessorsContextKey                    = util.ContextKey("proposal-processors")
	ProposerSelectorContextKey                      = util.ContextKey("proposer-selector")
	ProposalSelectorContextKey                      = util.ContextKey("proposal-selector")
	WhenNewBlockSavedInSyncingStateFuncContextKey   = util.ContextKey("when-new-block-saved-in-syncing-state-func")
	WhenNewBlockSavedInConsensusStateFuncContextKey = util.ContextKey("when-new-block-saved-in-consensus-state-func")
	BallotStuckResolverContextKey                   = util.ContextKey("ballot-stuck-resolver")
)

func PBallotbox(ctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var local base.LocalNode
	var params *isaac.LocalParams
	var db isaac.Database

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return ctx, err
	}

	getLastSuffragef, err := GetLastSuffrageFunc(ctx)
	if err != nil {
		return ctx, err
	}

	ballotbox := isaacstates.NewBallotbox(
		local.Address(),
		getLastSuffragef,
		isaac.IsValidVoteproofWithSuffrage,
		params.WaitPreparingINITBallot(),
	)

	_ = ballotbox.SetLogging(log)

	if err := ballotbox.Start(context.Background()); err != nil {
		return ctx, err
	}

	ctx = context.WithValue(ctx, BallotboxContextKey, ballotbox) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func PBallotStuckResolver(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to load ballot stuck resolver")

	var log *logging.Logging
	var design NodeDesign
	var local base.LocalNode
	var params *isaac.LocalParams
	var ballotbox *isaacstates.Ballotbox
	var cb *isaacnetwork.CallbackBroadcaster
	var sv *isaac.SuffrageVoting

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		BallotboxContextKey, &ballotbox,
		CallbackBroadcasterContextKey, &cb,
		SuffrageVotingContextKey, &sv,
	); err != nil {
		return ctx, e(err, "")
	}

	getLastSuffragef, err := GetLastSuffrageFunc(ctx)
	if err != nil {
		return ctx, e(err, "")
	}

	findMissingBallotsf := isaacstates.FindMissingBallotsFromBallotboxFunc(
		local.Address(),
		params,
		getLastSuffragef,
		ballotbox,
	)

	requestMissingBallotsf := isaacstates.RequestMissingBallots(
		quicstream.NewUDPConnInfo(design.Network.Publish(), design.Network.TLSInsecure),
		cb.Broadcast,
	)

	voteSuffrageVotingf := isaacstates.VoteSuffrageVotingFunc(
		local,
		params,
		ballotbox,
		sv,
		getLastSuffragef,
	)

	r := isaacstates.NewDefaultBallotStuckResolver(
		params.BallotStuckWait(),
		params.BallotStuckResolveAfter(),
		params.IntervalBroadcastBallot(),
		findMissingBallotsf,
		requestMissingBallotsf,
		voteSuffrageVotingf,
	)

	_ = r.SetLogging(log)

	return context.WithValue(ctx, BallotStuckResolverContextKey, r), nil
}

func PStates(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare states")

	var enc encoder.Encoder
	var local base.LocalNode
	var params *isaac.LocalParams
	var ballotbox *isaacstates.Ballotbox
	var pps *isaac.ProposalProcessors
	var memberlist *quicmemberlist.Memberlist
	var lvps *isaacstates.LastVoteproofsHandler
	var syncSourcePool *isaac.SyncSourcePool
	var cb *isaacnetwork.CallbackBroadcaster
	var resolver isaacstates.BallotStuckResolver

	if err := util.LoadFromContextOK(ctx,
		EncoderContextKey, &enc,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		BallotboxContextKey, &ballotbox,
		ProposalProcessorsContextKey, &pps,
		MemberlistContextKey, &memberlist,
		LastVoteproofsHandlerContextKey, &lvps,
		SyncSourcePoolContextKey, &syncSourcePool,
		CallbackBroadcasterContextKey, &cb,
		BallotStuckResolverContextKey, &resolver,
	); err != nil {
		return ctx, e(err, "")
	}

	states := isaacstates.NewStates(
		local,
		params,
		ballotbox,
		resolver,
		lvps,
		syncSourcePool.IsInFixed,
		func(bl base.Ballot) error {
			ee := util.StringErrorFunc("failed to broadcast ballot")

			b, err := enc.Marshal(bl)
			if err != nil {
				return ee(err, "")
			}

			id := valuehash.NewSHA256(bl.HashBytes()).String()
			if err := cb.Broadcast(id, b, nil); err != nil {
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
	ctx = context.WithValue(ctx, StatesContextKey, states)
	ctx = context.WithValue(ctx, ProposalSelectorContextKey, proposalSelector)
	//revive:enable:modifies-parameter

	return ctx, nil
}

func PCloseStates(ctx context.Context) (context.Context, error) {
	var states *isaacstates.States
	var ballotbox *isaacstates.Ballotbox

	if err := util.LoadFromContext(ctx,
		StatesContextKey, &states,
		BallotboxContextKey, &ballotbox,
	); err != nil {
		return ctx, err
	}

	if states != nil {
		if err := states.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
			return ctx, err
		}
	}

	if ballotbox != nil {
		if err := ballotbox.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
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
	var sv *isaac.SuffrageVoting

	if err := util.LoadFromContextOK(ctx,
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
		SuffrageVotingContextKey, &sv,
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

	joinMemberlistForJoiningeHandlerf, err := joinMemberlistForJoiningeHandlerFunc(ctx)
	if err != nil {
		return ctx, e(err, "")
	}

	leaveMemberlistForStateHandlerf, err := leaveMemberlistForStateHandlerFunc(ctx)
	if err != nil {
		return ctx, e(err, "")
	}

	leaveMemberlistForSyncingHandlerf, err := leaveMemberlistForSyncingHandlerFunc(ctx)
	if err != nil {
		return ctx, e(err, "")
	}

	var whenNewBlockSavedInSyncingStatef func(base.Height)

	switch err = util.LoadFromContext(
		ctx, WhenNewBlockSavedInSyncingStateFuncContextKey, &whenNewBlockSavedInSyncingStatef); {
	case err != nil:
		return ctx, e(err, "")
	case whenNewBlockSavedInSyncingStatef == nil:
		whenNewBlockSavedInSyncingStatef = WhenNewBlockSavedInSyncingStateFunc(db, nodeinfo)
	}

	var whenNewBlockSavedInConsensusStatef func(base.Height)

	switch err = util.LoadFromContext(
		ctx, WhenNewBlockSavedInConsensusStateFuncContextKey, &whenNewBlockSavedInConsensusStatef); {
	case err != nil:
		return ctx, e(err, "")
	case whenNewBlockSavedInConsensusStatef == nil:
		whenNewBlockSavedInConsensusStatef = WhenNewBlockSavedInConsensusStateFunc(params, ballotbox, db, nodeinfo)
	}

	suffrageVotingFindf := func(
		ctx context.Context,
		height base.Height,
		suf base.Suffrage,
	) ([]base.SuffrageWithdrawOperation, error) {
		return sv.Find(ctx, height, suf)
	}

	onEmptyMembersf, err := onEmptyMembersStateHandlerFunc(ctx, states)
	if err != nil {
		return ctx, e(err, "")
	}

	newsyncerf, err := newSyncerFunc(ctx, params, db, lvps, whenNewBlockSavedInSyncingStatef)
	if err != nil {
		return ctx, e(err, "")
	}

	getLastManifestf := getLastManifestFunc(db)
	getManifestf := getManifestFunc(db)

	whenSyncingFinished := func(base.Height) {
		ballotbox.Count(params.Threshold())
	}

	states.SetWhenStateSwitched(func(next isaacstates.StateType) {
		_ = nodeinfo.SetConsensusState(next)
	})

	syncinghandler := isaacstates.NewNewSyncingHandlerType(
		local, params, newsyncerf, nodeInConsensusNodesf,
		joinMemberlistForStateHandlerf,
		leaveMemberlistForSyncingHandlerf,
		whenNewBlockSavedInSyncingStatef,
	)
	syncinghandler.SetWhenFinished(whenSyncingFinished)

	consensusHandler := isaacstates.NewNewConsensusHandlerType(
		local, params, proposalSelector, pps,
		getManifestf, nodeInConsensusNodesf, voteFunc, whenNewBlockSavedInConsensusStatef, suffrageVotingFindf,
	)

	consensusHandler.SetOnEmptyMembers(onEmptyMembersf)

	joiningHandler := isaacstates.NewNewJoiningHandlerType(
		local, params, proposalSelector,
		getLastManifestf, nodeInConsensusNodesf,
		voteFunc, joinMemberlistForJoiningeHandlerf, leaveMemberlistForStateHandlerf, suffrageVotingFindf,
	)
	joiningHandler.SetOnEmptyMembers(onEmptyMembersf)

	states.
		SetHandler(isaacstates.StateBroken, isaacstates.NewNewBrokenHandlerType(local, params)).
		SetHandler(isaacstates.StateStopped, isaacstates.NewNewStoppedHandlerType(local, params)).
		SetHandler(
			isaacstates.StateBooting,
			isaacstates.NewNewBootingHandlerType(local, params,
				getLastManifestf, nodeInConsensusNodesf),
		).
		SetHandler(isaacstates.StateJoining, joiningHandler).
		SetHandler(isaacstates.StateConsensus, consensusHandler).
		SetHandler(isaacstates.StateSyncing, syncinghandler)

	_ = states.SetLogging(log)

	return ctx, nil
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
	var devflags DevFlags
	var design NodeDesign
	var client *isaacnetwork.QuicstreamClient
	var st *leveldbstorage.Storage
	var perm isaac.PermanentDatabase
	var syncSourcePool *isaac.SyncSourcePool

	if err := util.LoadFromContextOK(pctx,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		DevFlagsContextKey, &devflags,
		DesignContextKey, &design,
		QuicstreamClientContextKey, &client,
		LeveldbStorageContextKey, &st,
		PermanentDatabaseContextKey, &perm,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	setLastVoteproofsfFromBlockReaderf, err := setLastVoteproofsfFromBlockReaderFunc(lvps)
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

		newclient := client.Clone()

		conninfocache, _ := util.NewShardedMap(base.NilHeight, quicstream.UDPConnInfo{}, 1<<9) //nolint:gomnd //...

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
			syncerLastBlockMapFunc(newclient, params, syncSourcePool),
			syncerBlockMapFunc(newclient, params, syncSourcePool, conninfocache, devflags.DelaySyncer),
			syncerBlockMapItemFunc(newclient, conninfocache),
			tempsyncpool,
			setLastVoteproofsfFromBlockReaderf,
			func() error {
				conninfocache.Close()

				return newclient.Close()
			},
		)
		if err != nil {
			return nil, e(err, "")
		}

		go newSyncerDeferredf(height, syncer)

		return syncer, nil
	}, nil
}

func syncerLastBlockMapFunc(
	client *isaacnetwork.QuicstreamClient,
	params base.LocalParams,
	syncSourcePool *isaac.SyncSourcePool,
) isaacstates.SyncerLastBlockMapFunc {
	f := func(
		ctx context.Context, manifest util.Hash, ci quicstream.UDPConnInfo,
	) (_ base.BlockMap, updated bool, _ error) {
		cctx, cancel := context.WithTimeout(ctx, time.Second*2) //nolint:gomnd //...
		defer cancel()

		switch m, updated, err := client.LastBlockMap(cctx, ci, manifest); {
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
		ml := util.EmptyLocked((base.BlockMap)(nil))

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

				_, err = ml.Set(func(v base.BlockMap, _ bool) (base.BlockMap, error) {
					switch {
					case v == nil,
						m.Manifest().Height() > v.Manifest().Height():

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

		switch v, _ := ml.Value(); {
		case v == nil:
			return nil, false, nil
		default:
			return v, true, nil
		}
	}
}

func syncerBlockMapFunc( //revive:disable-line:cognitive-complexity
	client *isaacnetwork.QuicstreamClient,
	params base.LocalParams,
	syncSourcePool *isaac.SyncSourcePool,
	conninfocache util.LockedMap[base.Height, quicstream.UDPConnInfo],
	devdelay time.Duration,
) isaacstates.SyncerBlockMapFunc {
	f := func(ctx context.Context, height base.Height, ci quicstream.UDPConnInfo) (base.BlockMap, bool, error) {
		cctx, cancel := context.WithTimeout(ctx, time.Second*2) //nolint:gomnd //...
		defer cancel()

		if devdelay > 0 {
			<-time.After(devdelay) // NOTE for testing
		}

		switch m, found, err := client.BlockMap(cctx, ci, height); {
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
				result := util.EmptyLocked([2]interface{}{})

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
							_, _ = result.Set(func(i [2]interface{}, isempty bool) ([2]interface{}, error) {
								if !isempty {
									return [2]interface{}{}, errors.Errorf("already set")
								}

								_ = conninfocache.SetValue(height, ci)

								return [2]interface{}{a, b}, nil
							})

							return errors.Errorf("stop")
						}
					},
				)

				i, isempty := result.Value()
				if isempty {
					return true, nil
				}

				m, found = i[0].(base.BlockMap), i[1].(bool) //nolint:forcetypeassert //...

				return false, nil
			},
			-1,
			time.Second,
		)

		return m, found, err
	}
}

func syncerBlockMapItemFunc(
	client *isaacnetwork.QuicstreamClient,
	conninfocache util.LockedMap[base.Height, quicstream.UDPConnInfo],
) isaacstates.SyncerBlockMapItemFunc {
	// FIXME support remote item like https or ftp?

	return func(ctx context.Context, height base.Height, item base.BlockMapItemType) (
		reader io.ReadCloser, closef func() error, found bool, _ error,
	) {
		e := util.StringErrorFunc("failed to fetch blockmap item")

		var ci quicstream.UDPConnInfo

		switch i, cfound := conninfocache.Value(height); {
		case !cfound:
			return nil, nil, false, e(nil, "conninfo not found")
		default:
			ci = i
		}

		cctx, ctxcancel := context.WithTimeout(ctx, time.Second*2) //nolint:gomnd //...
		defer ctxcancel()

		r, cancel, found, err := client.BlockMapItem(cctx, ci, height, item)
		if err != nil {
			return nil, nil, false, e(err, "")
		}

		return r, cancel, found, err
	}
}

func setLastVoteproofsfFromBlockReaderFunc(
	lvps *isaacstates.LastVoteproofsHandler,
) (func(isaac.BlockReader) error, error) {
	return func(reader isaac.BlockReader) error {
		switch v, found, err := reader.Item(base.BlockMapItemTypeVoteproofs); {
		case err != nil:
			return err
		case !found:
			return errors.Errorf("voteproofs not found at last")
		default:
			vps := v.([]base.Voteproof) //nolint:forcetypeassert //...

			_ = lvps.Set(vps[0].(base.INITVoteproof))   //nolint:forcetypeassert //...
			_ = lvps.Set(vps[1].(base.ACCEPTVoteproof)) //nolint:forcetypeassert //...

			return nil
		}
	}, nil
}

func joinMemberlistForStateHandlerFunc(pctx context.Context) (
	func(context.Context, base.Suffrage) error,
	error,
) {
	var long *LongRunningMemberlistJoin
	if err := util.LoadFromContextOK(pctx, LongRunningMemberlistJoinContextKey, &long); err != nil {
		return nil, err
	}

	return func(ctx context.Context, _ base.Suffrage) error {
		donech := long.Join()
		if donech == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "failed to join")
		case <-donech:
			return nil
		}
	}, nil
}

func joinMemberlistForJoiningeHandlerFunc(pctx context.Context) (
	func(context.Context, base.Suffrage) error,
	error,
) {
	var long *LongRunningMemberlistJoin
	if err := util.LoadFromContextOK(pctx, LongRunningMemberlistJoinContextKey, &long); err != nil {
		return nil, err
	}

	return func(ctx context.Context, _ base.Suffrage) error {
		_ = long.Join()

		return nil
	}, nil
}

func leaveMemberlistForStateHandlerFunc(pctx context.Context) (
	func(time.Duration) error,
	error,
) {
	var long *LongRunningMemberlistJoin
	var m *quicmemberlist.Memberlist

	if err := util.LoadFromContextOK(pctx,
		MemberlistContextKey, &m,
		LongRunningMemberlistJoinContextKey, &long,
	); err != nil {
		return nil, err
	}

	return func(timeout time.Duration) error {
		_ = long.Cancel()

		switch {
		case m.MembersLen() < 1:
			return nil
		default:
			return m.Leave(timeout)
		}
	}, nil
}

func leaveMemberlistForSyncingHandlerFunc(pctx context.Context) (
	func(time.Duration) error,
	error,
) {
	var long *LongRunningMemberlistJoin
	if err := util.LoadFromContextOK(pctx, LongRunningMemberlistJoinContextKey, &long); err != nil {
		return nil, err
	}

	return func(timeout time.Duration) error {
		_ = long.Cancel()

		return nil
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

	if err := util.LoadFromContextOK(pctx, LoggingContextKey, &log); err != nil {
		return nil, err
	}

	return func(height base.Height, syncer *isaacstates.Syncer) {
		l := log.Log().With().Str("module", "new-syncer").Logger()

		if err := db.MergeAllPermanent(); err != nil {
			l.Error().Err(err).Msg("failed to merge temps")

			return
		}

		log.Log().Debug().Msg("SuffrageProofs built")

		err := syncer.Start(context.Background())
		if err != nil {
			l.Error().Err(err).Msg("syncer stopped")

			return
		}

		_ = syncer.Add(height)

		l.Debug().Interface("height", height).Msg("new syncer created")
	}, nil
}

func onEmptyMembersStateHandlerFunc(
	pctx context.Context,
	states *isaacstates.States,
) (func(), error) {
	var log *logging.Logging
	var pps *ps.PS
	var long *LongRunningMemberlistJoin

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EventOnEmptyMembersContextKey, &pps,
		LongRunningMemberlistJoinContextKey, &long,
	); err != nil {
		return nil, err
	}

	_ = pps.Add("in-state-handler", func(ctx context.Context) (context.Context, error) {
		_ = long.Join()
		log.Log().Debug().Msg("start LongRunningMemberlistJoin")

		return ctx, nil
	}, nil)

	return func() {
		states.OnEmptyMembers()
	}, nil
}
