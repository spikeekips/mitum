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
	WhenNewBlockConfirmedFuncContextKey             = util.ContextKey("when-new-block-confirmed-func")
	BallotStuckResolverContextKey                   = util.ContextKey("ballot-stuck-resolver")
)

func PBallotbox(ctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var local base.LocalNode
	var params *isaac.LocalParams
	var sp *SuffragePool

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return ctx, err
	}

	ballotbox := isaacstates.NewBallotbox(local.Address(), sp.Height)
	_ = ballotbox.SetCountAfter(params.WaitPreparingINITBallot())
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
	var sp *SuffragePool
	var cb *isaacnetwork.CallbackBroadcaster
	var svf *isaac.SuffrageVoting

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		BallotboxContextKey, &ballotbox,
		SuffragePoolContextKey, &sp,
		CallbackBroadcasterContextKey, &cb,
		SuffrageVotingContextKey, &svf,
	); err != nil {
		return ctx, e(err, "")
	}

	findMissingBallotsf := isaacstates.FindMissingBallotsFromBallotboxFunc(
		local.Address(),
		params,
		sp.Height,
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
		svf,
		sp.Height,
	)

	switch {
	case params.BallotStuckWait() < params.WaitPreparingINITBallot():
		return ctx, util.ErrInvalid.Errorf("too short ballot stuck wait; it should be over wait_preparing_init_ballot")
	case params.BallotStuckWait() < params.WaitPreparingINITBallot()*2:
		log.Log().Warn().
			Dur("ballot_stuck_wait", params.BallotStuckWait()).
			Msg("too short ballot stuck wait; proper valud is over 2 * wait_preparing_init_ballot")
	}

	r := isaacstates.NewDefaultBallotStuckResolver(
		params.BallotStuckWait(),
		time.Second,
		params.BallotStuckResolveAfter(),
		findMissingBallotsf,
		requestMissingBallotsf,
		voteSuffrageVotingf,
	)

	_ = r.SetLogging(log)

	return context.WithValue(ctx, BallotStuckResolverContextKey, r), nil
}

func PStates(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare states")

	args := isaacstates.NewStatesArgs()

	var log *logging.Logging
	var enc encoder.Encoder
	var local base.LocalNode
	var params *isaac.LocalParams
	var syncSourcePool *isaac.SyncSourcePool
	var cb *isaacnetwork.CallbackBroadcaster
	var pool *isaacdatabase.TempPool

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		BallotboxContextKey, &args.Ballotbox,
		LastVoteproofsHandlerContextKey, &args.LastVoteproofsHandler,
		SyncSourcePoolContextKey, &syncSourcePool,
		CallbackBroadcasterContextKey, &cb,
		BallotStuckResolverContextKey, &args.BallotStuckResolver,
		PoolDatabaseContextKey, &pool,
	); err != nil {
		return ctx, e(err, "")
	}

	if vp := args.LastVoteproofsHandler.Last().Cap(); vp != nil {
		last := args.Ballotbox.LastPoint()

		isset := args.Ballotbox.SetLastPointFromVoteproof(vp)

		log.Log().Debug().
			Interface("lastpoint", last).
			Interface("last_voteproof", vp).
			Bool("is_set", isset).
			Msg("last voteproof updated to ballotbox")
	}

	args.IsInSyncSourcePoolFunc = syncSourcePool.IsInFixed
	args.BallotBroadcaster = isaacstates.NewDefaultBallotBroadcaster(
		local.Address(),
		pool,
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

	states := isaacstates.NewStates(local, params, args)
	_ = states.SetLogging(log)

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

func PStatesSetHandlers(pctx context.Context) (context.Context, error) { //revive:disable-line:function-length
	e := util.StringErrorFunc("failed to set states handler")

	var log *logging.Logging
	var local base.LocalNode
	var params *isaac.LocalParams
	var db isaac.Database
	var states *isaacstates.States
	var nodeinfo *isaacnetwork.NodeInfoUpdater
	var ballotbox *isaacstates.Ballotbox
	var sv *isaac.SuffrageVoting

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
		StatesContextKey, &states,
		NodeInfoContextKey, &nodeinfo,
		BallotboxContextKey, &ballotbox,
		SuffrageVotingContextKey, &sv,
	); err != nil {
		return pctx, e(err, "")
	}

	votef := func(bl base.Ballot) (bool, error) {
		return ballotbox.Vote(bl, params.Threshold())
	}

	suffrageVotingFindf := func(ctx context.Context, height base.Height, suf base.Suffrage) (
		[]base.SuffrageWithdrawOperation, error,
	) {
		return sv.Find(ctx, height, suf)
	}

	whenEmptyMembersf, err := whenEmptyMembersStateHandlerFunc(pctx, states)
	if err != nil {
		return pctx, e(err, "")
	}

	getLastManifestf := getLastManifestFunc(db)
	getManifestf := getManifestFunc(db)

	states.SetWhenStateSwitched(func(next isaacstates.StateType) {
		_ = nodeinfo.SetConsensusState(next)
	})

	syncingargs, err := newSyncingHandlerArgs(pctx)
	if err != nil {
		return pctx, e(err, "")
	}

	consensusargs, err := consensusHandlerArgs(pctx)
	if err != nil {
		return pctx, e(err, "")
	}

	consensusargs.VoteFunc = votef
	consensusargs.SuffrageVotingFindFunc = suffrageVotingFindf
	consensusargs.GetManifestFunc = getManifestf
	consensusargs.WhenEmptyMembersFunc = whenEmptyMembersf

	joiningargs, err := newJoiningHandlerArgs(pctx)
	if err != nil {
		return pctx, e(err, "")
	}

	joiningargs.VoteFunc = votef
	joiningargs.SuffrageVotingFindFunc = suffrageVotingFindf
	joiningargs.LastManifestFunc = getLastManifestf
	joiningargs.WhenEmptyMembersFunc = whenEmptyMembersf

	bootingargs, err := newBootingHandlerArgs(pctx)
	if err != nil {
		return pctx, e(err, "")
	}

	bootingargs.LastManifestFunc = getLastManifestf

	states.
		SetHandler(isaacstates.StateBroken, isaacstates.NewNewBrokenHandlerType(local, params)).
		SetHandler(isaacstates.StateStopped, isaacstates.NewNewStoppedHandlerType(local, params)).
		SetHandler(isaacstates.StateBooting, isaacstates.NewNewBootingHandlerType(local, params, bootingargs)).
		SetHandler(isaacstates.StateJoining, isaacstates.NewNewJoiningHandlerType(local, params, joiningargs)).
		SetHandler(isaacstates.StateConsensus, isaacstates.NewNewConsensusHandlerType(local, params, consensusargs)).
		SetHandler(isaacstates.StateSyncing, isaacstates.NewNewSyncingHandlerType(local, params, syncingargs))

	_ = states.SetLogging(log)

	return pctx, nil
}

func newSyncerFunc(pctx context.Context) (
	func(height base.Height) (isaac.Syncer, error),
	error,
) {
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return nil, err
	}

	newSyncerDeferredf, err := newSyncerDeferredFunc(pctx, db)
	if err != nil {
		return nil, err
	}

	newArgsf, err := newSyncerArgsFunc(pctx)
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

		args, err := newArgsf(height)
		if err != nil {
			return nil, e(err, "")
		}

		syncer := isaacstates.NewSyncer(prev, args)

		go newSyncerDeferredf(height, syncer)

		return syncer, nil
	}, nil
}

func consensusHandlerArgs(pctx context.Context) (*isaacstates.ConsensusHandlerArgs, error) {
	var log *logging.Logging
	var params *isaac.LocalParams
	var ballotbox *isaacstates.Ballotbox
	var db isaac.Database
	var proposalSelector *isaac.BaseProposalSelector
	var pps *isaac.ProposalProcessors
	var nodeinfo *isaacnetwork.NodeInfoUpdater
	var nodeInConsensusNodesf func(base.Node, base.Height) (base.Suffrage, bool, error)

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalParamsContextKey, &params,
		BallotboxContextKey, &ballotbox,
		CenterDatabaseContextKey, &db,
		ProposalSelectorContextKey, &proposalSelector,
		ProposalProcessorsContextKey, &pps,
		NodeInfoContextKey, &nodeinfo,
		NodeInConsensusNodesFuncContextKey, &nodeInConsensusNodesf,
	); err != nil {
		return nil, err
	}

	var whenNewBlockSavedf func(base.Height)

	switch err := util.LoadFromContext(pctx, WhenNewBlockSavedInConsensusStateFuncContextKey, &whenNewBlockSavedf); {
	case err != nil:
		return nil, err
	case whenNewBlockSavedf == nil:
		whenNewBlockSavedf = func(base.Height) {}
	}

	defaultWhenNewBlockSavedf := DefaultWhenNewBlockSavedInConsensusStateFunc(log, params, ballotbox, db, nodeinfo)

	var whenNewBlockConfirmedf func(base.Height)

	switch err := util.LoadFromContext(
		pctx, WhenNewBlockConfirmedFuncContextKey, &whenNewBlockConfirmedf); {
	case err != nil:
		return nil, err
	case whenNewBlockConfirmedf == nil:
		whenNewBlockConfirmedf = func(base.Height) {}
	}

	defaultWhenNewBlockConfirmedf := DefaultWhenNewBlockConfirmedFunc(log)

	args := isaacstates.NewConsensusHandlerArgs()
	args.NodeInConsensusNodesFunc = nodeInConsensusNodesf
	args.ProposalSelector = proposalSelector
	args.ProposalProcessors = pps
	args.WhenNewBlockSaved = func(height base.Height) {
		defaultWhenNewBlockSavedf(height)

		whenNewBlockSavedf(height)
	}
	args.WhenNewBlockConfirmed = func(height base.Height) {
		defaultWhenNewBlockConfirmedf(height)

		whenNewBlockConfirmedf(height)
	}

	return args, nil
}

func newJoiningHandlerArgs(pctx context.Context) (*isaacstates.JoiningHandlerArgs, error) {
	var params *isaac.LocalParams
	var proposalSelector *isaac.BaseProposalSelector
	var nodeInConsensusNodesf func(base.Node, base.Height) (base.Suffrage, bool, error)

	if err := util.LoadFromContextOK(pctx,
		LocalParamsContextKey, &params,
		ProposalSelectorContextKey, &proposalSelector,
		NodeInConsensusNodesFuncContextKey, &nodeInConsensusNodesf,
	); err != nil {
		return nil, err
	}

	joinMemberlistf, err := joinMemberlistForJoiningeHandlerFunc(pctx)
	if err != nil {
		return nil, err
	}

	leaveMemberlistf, err := leaveMemberlistForJoiningHandlerFunc(pctx)
	if err != nil {
		return nil, err
	}

	args := isaacstates.NewJoiningHandlerArgs(params)
	args.NodeInConsensusNodesFunc = nodeInConsensusNodesf
	args.ProposalSelector = proposalSelector
	args.JoinMemberlistFunc = joinMemberlistf
	args.LeaveMemberlistFunc = leaveMemberlistf

	return args, nil
}

func newBootingHandlerArgs(pctx context.Context) (*isaacstates.BootingHandlerArgs, error) {
	var nodeInConsensusNodesf func(base.Node, base.Height) (base.Suffrage, bool, error)

	if err := util.LoadFromContextOK(pctx,
		NodeInConsensusNodesFuncContextKey, &nodeInConsensusNodesf,
	); err != nil {
		return nil, err
	}

	args := isaacstates.NewBootingHandlerArgs()
	args.NodeInConsensusNodesFunc = nodeInConsensusNodesf

	return args, nil
}

func newSyncingHandlerArgs(pctx context.Context) (*isaacstates.SyncingHandlerArgs, error) {
	var log *logging.Logging
	var params *isaac.LocalParams
	var db isaac.Database
	var ballotbox *isaacstates.Ballotbox
	var nodeinfo *isaacnetwork.NodeInfoUpdater
	var nodeInConsensusNodesf func(base.Node, base.Height) (base.Suffrage, bool, error)

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
		BallotboxContextKey, &ballotbox,
		NodeInfoContextKey, &nodeinfo,
		NodeInConsensusNodesFuncContextKey, &nodeInConsensusNodesf,
	); err != nil {
		return nil, err
	}

	newsyncerf, err := newSyncerFunc(pctx)
	if err != nil {
		return nil, err
	}

	joinMemberlistf, err := joinMemberlistForSyncingHandlerFunc(pctx)
	if err != nil {
		return nil, err
	}

	leaveMemberlistf, err := leaveMemberlistForSyncingHandlerFunc(pctx)
	if err != nil {
		return nil, err
	}

	var whenNewBlockSavedf func(base.Height)

	switch err = util.LoadFromContext(pctx, WhenNewBlockSavedInSyncingStateFuncContextKey, &whenNewBlockSavedf); {
	case err != nil:
		return nil, err
	case whenNewBlockSavedf == nil:
		whenNewBlockSavedf = func(base.Height) {}
	}

	defaultWhenNewBlockSavedf := DefaultWhenNewBlockSavedInSyncingStateFunc(log, db, nodeinfo)

	args := isaacstates.NewSyncingHandlerArgs(params)
	args.NodeInConsensusNodesFunc = nodeInConsensusNodesf
	args.NewSyncerFunc = newsyncerf
	args.WhenFinishedFunc = func(base.Height) {
		ballotbox.Count(params.Threshold())
	}
	args.JoinMemberlistFunc = joinMemberlistf
	args.LeaveMemberlistFunc = leaveMemberlistf
	args.WhenNewBlockSavedFunc = func(height base.Height) {
		defaultWhenNewBlockSavedf(height)

		whenNewBlockSavedf(height)
	}

	return args, nil
}

func newSyncerArgsFunc(pctx context.Context) (func(base.Height) (isaacstates.SyncerArgs, error), error) {
	var log *logging.Logging
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var devflags DevFlags
	var design NodeDesign
	var params *isaac.LocalParams
	var client *isaacnetwork.QuicstreamClient
	var st *leveldbstorage.Storage
	var db isaac.Database
	var syncSourcePool *isaac.SyncSourcePool
	var lvps *isaacstates.LastVoteproofsHandler
	var nodeinfo *isaacnetwork.NodeInfoUpdater

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		DevFlagsContextKey, &devflags,
		DesignContextKey, &design,
		LocalParamsContextKey, &params,
		QuicstreamClientContextKey, &client,
		LeveldbStorageContextKey, &st,
		CenterDatabaseContextKey, &db,
		CenterDatabaseContextKey, &db,
		SyncSourcePoolContextKey, &syncSourcePool,
		LastVoteproofsHandlerContextKey, &lvps,
		NodeInfoContextKey, &nodeinfo,
	); err != nil {
		return nil, err
	}

	setLastVoteproofsfFromBlockReaderf, err := setLastVoteproofsfFromBlockReaderFunc(lvps)
	if err != nil {
		return nil, err
	}

	removePrevBlockf, err := removePrevBlockFunc(pctx)
	if err != nil {
		return nil, err
	}

	var whenNewBlockSavedInSyncingStatef func(base.Height)

	switch err = util.LoadFromContext(
		pctx, WhenNewBlockSavedInSyncingStateFuncContextKey, &whenNewBlockSavedInSyncingStatef); {
	case err != nil:
		return nil, err
	case whenNewBlockSavedInSyncingStatef == nil:
		whenNewBlockSavedInSyncingStatef = func(base.Height) {}
	}

	defaultWhenNewBlockSavedInSyncingStatef := DefaultWhenNewBlockSavedInSyncingStateFunc(log, db, nodeinfo)

	var whenNewBlockConfirmedf func(base.Height)

	switch err = util.LoadFromContext(
		pctx, WhenNewBlockConfirmedFuncContextKey, &whenNewBlockConfirmedf); {
	case err != nil:
		return nil, err
	case whenNewBlockConfirmedf == nil:
		whenNewBlockConfirmedf = func(base.Height) {}
	}

	defaultWhenNewBlockConfirmedf := DefaultWhenNewBlockConfirmedFunc(log)

	return func(height base.Height) (args isaacstates.SyncerArgs, _ error) {
		var tempsyncpool isaac.TempSyncPool

		switch i, err := isaacdatabase.NewLeveldbTempSyncPool(height, st, encs, enc); {
		case err != nil:
			return args, isaacstates.ErrUnpromising.Wrap(err)
		default:
			tempsyncpool = i
		}

		newclient := client.Clone()

		conninfocache, _ := util.NewShardedMap(base.NilHeight, quicstream.UDPConnInfo{}, 1<<9) //nolint:gomnd //...

		args = isaacstates.NewSyncerArgs()
		args.LastBlockMapFunc = syncerLastBlockMapFunc(newclient, params, syncSourcePool)
		args.BlockMapFunc = syncerBlockMapFunc(newclient, params, syncSourcePool, conninfocache, devflags.DelaySyncer)
		args.TempSyncPool = tempsyncpool
		args.WhenStoppedFunc = func() error {
			conninfocache.Close()

			return newclient.Close()
		}
		args.RemovePrevBlockFunc = removePrevBlockf
		args.NewImportBlocksFunc = func(
			ctx context.Context,
			from, to base.Height,
			batchlimit int64,
			blockMapf func(context.Context, base.Height) (base.BlockMap, bool, error),
		) error {
			return isaacstates.ImportBlocks(
				ctx,
				from, to,
				batchlimit,
				blockMapf,
				syncerBlockMapItemFunc(newclient, conninfocache),
				func(blockmap base.BlockMap) (isaac.BlockImporter, error) {
					bwdb, err := db.NewBlockWriteDatabase(blockmap.Manifest().Height())
					if err != nil {
						return nil, err
					}

					return isaacblock.NewBlockImporter(
						LocalFSDataDirectory(design.Storage.Base),
						encs,
						blockmap,
						bwdb,
						func(context.Context) error {
							return db.MergeBlockWriteDatabase(bwdb)
						},
						params.NetworkID(),
					)
				},
				setLastVoteproofsfFromBlockReaderf,
				func(context.Context) error {
					defaultWhenNewBlockSavedInSyncingStatef(to)

					if c := to.SafePrev(); c >= from {
						defaultWhenNewBlockConfirmedf(c)
						whenNewBlockConfirmedf(c)
					}

					whenNewBlockSavedInSyncingStatef(to)

					return db.MergeAllPermanent()
				},
			)
		}

		return args, nil
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

func joinMemberlistForSyncingHandlerFunc(pctx context.Context) (
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

func leaveMemberlistForJoiningHandlerFunc(pctx context.Context) (
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

func DefaultWhenNewBlockSavedInSyncingStateFunc(
	log *logging.Logging,
	db isaac.Database,
	nodeinfo *isaacnetwork.NodeInfoUpdater,
) func(base.Height) {
	return func(height base.Height) {
		log.Log().Debug().
			Interface("height", height).
			Stringer("state", isaacstates.StateSyncing).
			Msg("new block saved")

		_ = UpdateNodeInfoWithNewBlock(db, nodeinfo)
	}
}

func DefaultWhenNewBlockSavedInConsensusStateFunc(
	log *logging.Logging,
	params *isaac.LocalParams,
	ballotbox *isaacstates.Ballotbox,
	db isaac.Database,
	nodeinfo *isaacnetwork.NodeInfoUpdater,
) func(base.Height) {
	return func(height base.Height) {
		log.Log().Debug().
			Interface("height", height).
			Stringer("state", isaacstates.StateConsensus).
			Msg("new block saved")

		ballotbox.Count(params.Threshold())

		_ = UpdateNodeInfoWithNewBlock(db, nodeinfo)
	}
}

func DefaultWhenNewBlockConfirmedFunc(log *logging.Logging) func(base.Height) {
	return func(height base.Height) {
		log.Log().Debug().Interface("height", height).Msg("new block confirmed")
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

		_ = syncer.Add(height)

		l.Debug().Interface("height", height).Msg("new syncer created")
	}, nil
}

func whenEmptyMembersStateHandlerFunc(
	pctx context.Context,
	states *isaacstates.States,
) (func(), error) {
	var log *logging.Logging
	var pps *ps.PS
	var long *LongRunningMemberlistJoin

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EventWhenEmptyMembersContextKey, &pps,
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
		states.WhenEmptyMembers()
	}, nil
}

func removePrevBlockFunc(pctx context.Context) (func(base.Height) (bool, error), error) {
	var db isaac.Database
	var design NodeDesign
	var sp *SuffragePool

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		CenterDatabaseContextKey, &db,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return nil, err
	}

	return func(height base.Height) (bool, error) {
		// NOTE remove from database
		switch removed, err := db.RemoveBlocks(height); {
		case err != nil:
			return false, err
		case !removed:
			return false, nil
		}

		switch p, found, err := db.LastSuffrageProof(); {
		case err != nil || !found:
			return false, err
		case height <= p.Map().Manifest().Height():
			sp.Purge() // NOTE purge suffrage cache
		}

		// NOTE remove from localfs
		switch removed, err := isaacblock.RemoveBlocksFromLocalFS(design.Storage.Base, height); {
		case err != nil:
			return false, err
		case !removed:
			return false, nil
		}

		return true, nil
	}, nil
}
