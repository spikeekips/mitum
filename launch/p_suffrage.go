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
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameSuffrageCandidateLimiterSet      = ps.Name("suffrage-candidate-limiter-set")
	PNamePatchLastConsensusNodesWatcher   = ps.Name("patch-last-consensus-nodes-watcher")
	PNameLastConsensusNodesWatcher        = ps.Name("last-consensus-nodes-watcher")
	PNameStartLastConsensusNodesWatcher   = ps.Name("start-last-consensus-nodes-watcher")
	PNameNodeInConsensusNodesFunc         = ps.Name("node-in-consensus-nodes-func")
	SuffrageCandidateLimiterSetContextKey = util.ContextKey("suffrage-candidate-limiter-set")
	LastConsensusNodesWatcherContextKey   = util.ContextKey("last-consensus-nodes-watcher")
	NodeInConsensusNodesFuncContextKey    = util.ContextKey("node-in-consensus-nodes-func")
	SuffragePoolContextKey                = util.ContextKey("suffrage-pool")
)

func PSuffrageCandidateLimiterSet(pctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("prepare SuffrageCandidateLimiterSet")

	var db isaac.Database
	if err := util.LoadFromContextOK(pctx, CenterDatabaseContextKey, &db); err != nil {
		return pctx, e(err, "")
	}

	set := hint.NewCompatibleSet()

	if err := set.Add(
		isaac.FixedSuffrageCandidateLimiterRuleHint,
		base.SuffrageCandidateLimiterFunc(FixedSuffrageCandidateLimiterFunc()),
	); err != nil {
		return pctx, e(err, "")
	}

	if err := set.Add(
		isaac.MajoritySuffrageCandidateLimiterRuleHint,
		base.SuffrageCandidateLimiterFunc(MajoritySuffrageCandidateLimiterFunc(db)),
	); err != nil {
		return pctx, e(err, "")
	}

	return context.WithValue(pctx, SuffrageCandidateLimiterSetContextKey, set), nil
}

func PLastConsensusNodesWatcher(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var local base.LocalNode
	var params *isaac.LocalParams
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return pctx, err
	}

	getLastSuffrageProoff, err := getLastSuffrageProofFunc(pctx)
	if err != nil {
		return pctx, err
	}

	getSuffrageProofFromRemotef, err := getSuffrageProofFromRemoteFunc(pctx)
	if err != nil {
		return pctx, err
	}

	getLastSuffrageCandidatef, err := getLastSuffrageCandidateFunc(pctx)
	if err != nil {
		return pctx, err
	}

	builder := isaac.NewSuffrageStateBuilder(
		params.NetworkID(),
		getLastSuffrageProoff,
		getSuffrageProofFromRemotef,
		getLastSuffrageCandidatef,
	)

	watcher, err := isaac.NewLastConsensusNodesWatcher(
		func() (base.Height, base.SuffrageProof, base.State, bool, error) {
			var lastheight base.Height

			switch m, found, err := db.LastBlockMap(); { //nolint:govet //...
			case err != nil:
				return lastheight, nil, nil, false, err
			case !found:
				return lastheight, nil, nil, false, nil
			default:
				lastheight = m.Manifest().Height()
			}

			proof, found, err := db.LastSuffrageProof() //nolint:govet //...
			if err != nil {
				return lastheight, nil, nil, false, err
			}

			st, _, err := db.State(isaac.SuffrageCandidateStateKey)
			if err != nil {
				return lastheight, nil, nil, false, err
			}

			return lastheight, proof, st, found, nil
		},
		builder.Build,
		nil,
		params.WaitPreparingINITBallot(),
	)
	if err != nil {
		return pctx, err
	}

	_ = watcher.SetLogging(log)

	pctx = context.WithValue(pctx, //revive:disable-line:modifies-parameter
		LastConsensusNodesWatcherContextKey, watcher)

	sp := NewSuffragePool(
		func(height base.Height) (base.Suffrage, bool, error) {
			switch suf, found, err := isaac.GetSuffrageFromDatabase(db, height); {
			case err != nil:
				return nil, false, err
			case found:
				return suf, true, nil
			}

			return watcher.GetSuffrage(height)
		},
		func() (base.Height, base.Suffrage, bool, error) {
			var proof base.SuffrageProof

			switch i, _, err := watcher.Last(); {
			case err != nil:
				return base.NilHeight, nil, false, err
			case i == nil:
				switch j, _, err := db.LastSuffrageProof(); {
				case err != nil, j == nil:
					return base.NilHeight, nil, false, err
				default:
					proof = j
				}
			default:
				proof = i
			}

			suf, err := proof.Suffrage()
			if err != nil {
				return base.NilHeight, nil, false, err
			}

			return proof.Map().Manifest().Height(), suf, true, nil
		},
	)

	return context.WithValue(pctx, SuffragePoolContextKey, sp), nil
}

func PPatchLastConsensusNodesWatcher(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var local base.LocalNode
	var params *isaac.LocalParams
	var db isaac.Database
	var watcher *isaac.LastConsensusNodesWatcher
	var states *isaacstates.States
	var long *LongRunningMemberlistJoin
	var mlist *quicmemberlist.Memberlist
	var syncSourcePool *isaac.SyncSourcePool
	var ballotbox *isaacstates.Ballotbox

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
		LastConsensusNodesWatcherContextKey, &watcher,
		StatesContextKey, &states,
		LongRunningMemberlistJoinContextKey, &long,
		MemberlistContextKey, &mlist,
		SyncSourcePoolContextKey, &syncSourcePool,
		BallotboxContextKey, &ballotbox,
	); err != nil {
		return pctx, err
	}

	watcher.SetWhenUpdated(func(_ context.Context, previous, updated base.SuffrageProof, candidatesst base.State) {
		if updated != nil {
			log.Log().Debug().Msg("suffrage updated")
		}

		if candidatesst != nil {
			log.Log().Debug().Msg("candiates updated")
		}

		ballotbox.Count(params.Threshold())

		if updated != nil {
			// NOTE remove expel nodes from SyncSourcePool
			if err := removeExpelsFromSyncSourcePoolByWatcher(previous, updated, syncSourcePool, log); err != nil {
				log.Log().Error().Err(err).Msg("failed to remove expels from sync source pool")
			}
		}

		if updated != nil || candidatesst != nil && states.AllowedConsensus() {
			if err := joinLocalIsInConsensusNodesByWatcher(
				updated, candidatesst, local, mlist, long, log,
			); err != nil {
				log.Log().Error().Err(err).Msg("failed to check local is in consensus nodes")
			}
		}
	})

	return pctx, nil
}

func PNodeInConsensusNodesFunc(pctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("NodeInConsensusNodesFunc")

	var db isaac.Database
	var sp *SuffragePool

	if err := util.LoadFromContextOK(
		pctx,
		CenterDatabaseContextKey, &db,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return pctx, e(err, "")
	}

	return context.WithValue(pctx, NodeInConsensusNodesFuncContextKey, nodeInConsensusNodesFunc(db, sp.Height)), nil
}

func getCandidatesFunc(
	db isaac.Database,
) func(height base.Height) (
	[]base.SuffrageCandidateStateValue, []base.SuffrageCandidateStateValue, error,
) {
	var prevcandidateslocked [2]interface{}

	lastcandidateslocked := util.EmptyLocked([2]interface{}{})

	return func(height base.Height) (
		[]base.SuffrageCandidateStateValue, []base.SuffrageCandidateStateValue, error,
	) {
		var prevcandidates []base.SuffrageCandidateStateValue
		var lastcandidates []base.SuffrageCandidateStateValue
		var cerr error

		_, _ = lastcandidateslocked.Set(func(i [2]interface{}, isempty bool) (v [2]interface{}, _ error) {
			var lastheight base.Height
			var last []base.SuffrageCandidateStateValue
			var prev []base.SuffrageCandidateStateValue

			if !isempty {
				lastheight = i[0].(base.Height) //nolint:forcetypeassert //...
				last = isaac.FilterCandidates(  //nolint:forcetypeassert //...
					height, i[1].([]base.SuffrageCandidateStateValue))
			}

			stheight, c, err := isaac.LastCandidatesFromState(height, db.State)
			if err != nil {
				cerr = err

				return v, err
			}

			if j := prevcandidateslocked[1]; j != nil {
				prev = isaac.FilterCandidates( //nolint:forcetypeassert //...
					height-1, j.([]base.SuffrageCandidateStateValue))
			}

			if stheight == lastheight {
				prevcandidates = prev
				lastcandidates = last

				return v, errors.Errorf("stop")
			}

			prevcandidates = last
			lastcandidates = c

			prevcandidateslocked = [2]interface{}{lastheight, last}

			return [2]interface{}{stheight, c}, nil
		})

		if cerr != nil {
			return nil, nil, cerr
		}

		return prevcandidates, lastcandidates, nil
	}
}

func nodeInConsensusNodesFunc(
	db isaac.Database,
	getSuffragef isaac.GetSuffrageByBlockHeight,
) func(node base.Node, height base.Height) (base.Suffrage, bool, error) {
	getCandidatesf := getCandidatesFunc(db)

	return func(node base.Node, height base.Height) (base.Suffrage, bool, error) {
		suf, found, err := getSuffragef(height)

		switch {
		case err != nil:
			return nil, false, err
		case !found:
			return nil, false, nil
		case suf.ExistsPublickey(node.Address(), node.Publickey()):
			return suf, true, nil
		}

		prev, last, err := getCandidatesf(height)
		if err != nil {
			return nil, false, err
		}

		if isaac.InCandidates(node, last) {
			return suf, true, nil
		}

		if isaac.InCandidates(node, prev) {
			return suf, true, nil
		}

		return suf, false, nil
	}
}

func PSuffrageVoting(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var local base.LocalNode
	var enc encoder.Encoder
	var db isaac.Database
	var pool *isaacdatabase.TempPool
	var memberlist *quicmemberlist.Memberlist
	var ballotbox *isaacstates.Ballotbox
	var sp *SuffragePool

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		EncoderContextKey, &enc,
		CenterDatabaseContextKey, &db,
		PoolDatabaseContextKey, &pool,
		MemberlistContextKey, &memberlist,
		BallotboxContextKey, &ballotbox,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return pctx, err
	}

	sv := isaac.NewSuffrageVoting(
		local.Address(),
		pool,
		db.ExistsInStateOperation,
		broadcastSuffrageVotingFunc(log, memberlist),
	)

	ballotbox.SetSuffrageVoteFunc(func(op base.SuffrageExpelOperation) error {
		_, err := sv.Vote(op)

		return err
	})

	pctx = context.WithValue(pctx, SuffrageVotingVoteFuncContextKey, //revive:disable-line:modifies-parameter
		isaac.SuffrageVoteFunc(func(op base.SuffrageExpelOperation) (bool, error) {
			var height base.Height

			switch m, found, err := db.LastBlockMap(); {
			case err != nil:
				return false, err
			case !found:
				return false, nil
			default:
				height = m.Manifest().Height()
			}

			var suf base.Suffrage

			switch i, found, err := sp.Height(height); {
			case err != nil:
				return false, err
			case !found:
				return false, nil
			default:
				suf = i
			}

			policy := db.LastNetworkPolicy()

			if err := isaac.IsValidExpelWithSuffrageLifespan(
				height, op, suf, policy.SuffrageExpelLifespan(),
			); err != nil {
				return false, err
			}

			return sv.Vote(op)
		}),
	)

	return context.WithValue(pctx, SuffrageVotingContextKey, sv), nil
}

func FixedSuffrageCandidateLimiterFunc() func(
	base.SuffrageCandidateLimiterRule,
) (base.SuffrageCandidateLimiter, error) {
	return func(rule base.SuffrageCandidateLimiterRule) (base.SuffrageCandidateLimiter, error) {
		i, ok := rule.(isaac.FixedSuffrageCandidateLimiterRule)
		if !ok {
			return nil, errors.Errorf("expected FixedSuffrageCandidateLimiterRule, not %T", rule)
		}

		return isaac.NewFixedSuffrageCandidateLimiter(i), nil
	}
}

func MajoritySuffrageCandidateLimiterFunc(
	db isaac.Database,
) func(base.SuffrageCandidateLimiterRule) (base.SuffrageCandidateLimiter, error) {
	return func(rule base.SuffrageCandidateLimiterRule) (base.SuffrageCandidateLimiter, error) {
		i, ok := rule.(isaac.MajoritySuffrageCandidateLimiterRule)
		if !ok {
			return nil, errors.Errorf("expected MajoritySuffrageCandidateLimiterRule, not %T", rule)
		}

		proof, found, err := db.LastSuffrageProof()

		switch {
		case err != nil:
			return nil, errors.WithMessagef(err, "get last suffrage for MajoritySuffrageCandidateLimiter")
		case !found:
			return nil, errors.Errorf("last suffrage not found for MajoritySuffrageCandidateLimiter")
		}

		suf, err := proof.Suffrage()
		if err != nil {
			return nil, errors.WithMessagef(err, "get suffrage for MajoritySuffrageCandidateLimiter")
		}

		return isaac.NewMajoritySuffrageCandidateLimiter(
			i,
			func() (uint64, error) {
				return uint64(suf.Len()), nil
			},
		), nil
	}
}

func getLastSuffrageProofFunc(pctx context.Context) (isaac.GetLastSuffrageProofFromRemoteFunc, error) {
	var params *isaac.LocalParams
	var client *isaacnetwork.QuicstreamClient
	var syncSourcePool *isaac.SyncSourcePool

	if err := util.LoadFromContextOK(pctx,
		QuicstreamClientContextKey, &client,
		LocalParamsContextKey, &params,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	lastl := util.EmptyLocked((util.Hash)(nil))

	f := func(ctx context.Context, ci quicstream.UDPConnInfo) (base.Height, base.SuffrageProof, bool, error) {
		cctx, cancel := context.WithTimeout(ctx, params.TimeoutRequest())
		defer cancel()

		last, _ := lastl.Value()

		switch lastheight, proof, updated, err := client.LastSuffrageProof(cctx, ci, last); {
		case err != nil, !updated:
			return lastheight, proof, updated, nil
		default:
			if err := proof.IsValid(params.NetworkID()); err != nil {
				return lastheight, nil, updated, err
			}

			_ = lastl.SetValue(proof.Map().Manifest().Suffrage())

			return lastheight, proof, updated, nil
		}
	}

	return func(ctx context.Context) (lastheight base.Height, proof base.SuffrageProof, found bool, _ error) {
		ml := util.EmptyLocked((base.SuffrageProof)(nil))

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

				h, proof, updated, err := f(ctx, ci)
				if err != nil {
					return err
				}

				_, _ = ml.Set(func(v base.SuffrageProof, _ bool) (base.SuffrageProof, error) {
					lastheight = h

					if !updated {
						return nil, util.ErrLockedSetIgnore.Errorf("not updated")
					}

					switch {
					case v == nil,
						proof.Map().Manifest().Height() > v.Map().Manifest().Height():

						return proof, nil
					default:
						return nil, util.ErrLockedSetIgnore.Errorf("old SuffrageProof")
					}
				})

				return nil
			},
		); err != nil {
			if errors.Is(err, isaac.ErrEmptySyncSources) {
				return lastheight, nil, false, nil
			}

			return lastheight, nil, false, err
		}

		switch v, _ := ml.Value(); {
		case v == nil:
			return lastheight, nil, false, nil
		default:
			return lastheight, v, true, nil
		}
	}, nil
}

func getSuffrageProofFromRemoteFunc(pctx context.Context) ( //revive:disable-line:cognitive-complexity
	isaac.GetSuffrageProofFromRemoteFunc, error,
) {
	var params *isaac.LocalParams
	var client *isaacnetwork.QuicstreamClient
	var syncSourcePool *isaac.SyncSourcePool

	if err := util.LoadFromContextOK(pctx,
		QuicstreamClientContextKey, &client,
		LocalParamsContextKey, &params,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	return func(ctx context.Context, suffrageheight base.Height) (proof base.SuffrageProof, found bool, _ error) {
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

						cctx, cancel := context.WithTimeout(ctx, params.TimeoutRequest())
						defer cancel()

						switch a, b, err := client.SuffrageProof(cctx, ci, suffrageheight); {
						case err != nil:
							if quicstream.IsNetworkError(err) {
								return err
							}

							return nil
						case !b:
							return nil
						default:
							if err := a.IsValid(params.NetworkID()); err != nil {
								return nil
							}

							_, _ = result.Set(func(_ [2]interface{}, isempty bool) ([2]interface{}, error) {
								if !isempty {
									return [2]interface{}{}, util.ErrLockedSetIgnore.Errorf("already set")
								}

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

				proof, found = i[0].(base.SuffrageProof), i[1].(bool) //nolint:forcetypeassert //...

				return false, nil
			},
			-1,
			time.Second,
		)

		return proof, found, err
	}, nil
}

func getLastSuffrageCandidateFunc(pctx context.Context) (isaac.GetLastSuffrageCandidateStateRemoteFunc, error) {
	var params *isaac.LocalParams
	var client *isaacnetwork.QuicstreamClient
	var syncSourcePool *isaac.SyncSourcePool

	if err := util.LoadFromContextOK(pctx,
		LocalParamsContextKey, &params,
		QuicstreamClientContextKey, &client,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	lastl := util.EmptyLocked((util.Hash)(nil))

	f := func(ctx context.Context, ci quicstream.UDPConnInfo) (base.State, bool, error) {
		last, _ := lastl.Value()

		cctx, cancel := context.WithTimeout(ctx, params.TimeoutRequest())
		defer cancel()

		st, found, err := client.State(cctx, ci, isaac.SuffrageCandidateStateKey, last)

		switch {
		case err != nil, !found, st == nil:
			return st, found, err
		default:
			if err := st.IsValid(nil); err != nil {
				return nil, false, err
			}

			_ = lastl.SetValue(st.Hash())

			return st, true, nil
		}
	}

	return func(ctx context.Context) (base.State, bool, error) {
		ml := util.EmptyLocked((base.State)(nil))

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

				st, found, err := f(ctx, ci)
				switch {
				case err != nil, !found, st == nil:
					return err
				}

				_, err = ml.Set(func(v base.State, _ bool) (base.State, error) {
					switch {
					case v == nil, st.Height() > v.Height():
						return st, nil
					default:
						return nil, util.ErrLockedSetIgnore.Errorf("old SuffrageProof")
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
	}, nil
}

func newSuffrageCandidateLimiterFunc(pctx context.Context) ( //revive:disable-line:cognitive-complexity
	func(base.Height, base.GetStateFunc) (base.OperationProcessorProcessFunc, error),
	error,
) {
	var db isaac.Database
	var limiterset *hint.CompatibleSet

	if err := util.LoadFromContextOK(pctx,
		CenterDatabaseContextKey, &db,
		SuffrageCandidateLimiterSetContextKey, &limiterset,
	); err != nil {
		return nil, err
	}

	return func(height base.Height, getStateFunc base.GetStateFunc) (base.OperationProcessorProcessFunc, error) {
		e := util.StringErrorFunc("get SuffrageCandidateLimiterFunc")

		policy := db.LastNetworkPolicy()
		if policy == nil {
			return nil, e(nil, "empty network policy")
		}

		var suf base.Suffrage

		switch proof, found, err := db.LastSuffrageProof(); {
		case err != nil:
			return nil, e(err, "get last suffrage")
		case !found:
			return nil, e(nil, "last suffrage not found")
		default:
			i, err := proof.Suffrage()
			if err != nil {
				return nil, e(err, "get suffrage")
			}

			suf = i
		}

		var existings uint64

		switch _, i, err := isaac.LastCandidatesFromState(height, getStateFunc); {
		case err != nil:
			return nil, e(err, "")
		default:
			existings = uint64(len(i))
		}

		rule := policy.SuffrageCandidateLimiterRule()

		var limit uint64

		switch i := limiterset.Find(rule.Hint()); {
		case i == nil:
			return nil, e(nil, "unknown limiter rule, %q", rule.Hint())
		default:
			f, ok := i.(base.SuffrageCandidateLimiterFunc)
			if !ok {
				return nil, e(nil, "expected SuffrageCandidateLimiterFunc, not %T", i)
			}

			limiter, err := f(rule)
			if err != nil {
				return nil, e(err, "")
			}

			j, err := limiter()
			if err != nil {
				return nil, e(err, "")
			}

			limit = j
		}

		switch {
		case existings >= policy.MaxSuffrageSize():
			return func(
				_ context.Context, op base.Operation, _ base.GetStateFunc,
			) (base.OperationProcessReasonError, error) {
				return base.NewBaseOperationProcessReasonError("reached limit, %d", policy.MaxSuffrageSize()), nil
			}, nil
		case limit > policy.MaxSuffrageSize()-uint64(suf.Len()):
			limit = policy.MaxSuffrageSize() - uint64(suf.Len())
		}

		if limit < 1 {
			return func(
				_ context.Context, op base.Operation, _ base.GetStateFunc,
			) (base.OperationProcessReasonError, error) {
				return base.NewBaseOperationProcessReasonError("reached limit, %d", limit), nil
			}, nil
		}

		var counted uint64

		return func(
			_ context.Context, op base.Operation, _ base.GetStateFunc,
		) (base.OperationProcessReasonError, error) {
			if counted >= limit {
				return base.NewBaseOperationProcessReasonError("reached limit, %d", limit), nil
			}

			counted++

			return nil, nil
		}, nil
	}, nil
}

func PStartLastConsensusNodesWatcher(pctx context.Context) (context.Context, error) {
	var watcher *isaac.LastConsensusNodesWatcher
	if err := util.LoadFromContextOK(pctx, LastConsensusNodesWatcherContextKey, &watcher); err != nil {
		return pctx, err
	}

	return pctx, watcher.Start(context.Background())
}

func PCloseLastConsensusNodesWatcher(pctx context.Context) (context.Context, error) {
	var watcher *isaac.LastConsensusNodesWatcher
	if err := util.LoadFromContext(pctx, LastConsensusNodesWatcherContextKey, &watcher); err != nil {
		return pctx, err
	}

	if watcher != nil {
		if err := watcher.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
			return pctx, err
		}
	}

	return pctx, nil
}

func joinLocalIsInConsensusNodesByWatcher(
	updated base.SuffrageProof,
	candidatesst base.State,
	local base.LocalNode,
	mlist *quicmemberlist.Memberlist,
	long *LongRunningMemberlistJoin,
	log *logging.Logging,
) error {
	suf, err := updated.Suffrage()
	if err != nil {
		return err
	}

	var inConsensusNodes bool

	switch {
	case suf.Exists(local.Address()):
		inConsensusNodes = true
	case candidatesst == nil:
	case isaac.InCandidates(local,
		candidatesst.Value().(base.SuffrageCandidatesStateValue).Nodes()): //nolint:forcetypeassert //...
		inConsensusNodes = true
	}

	if inConsensusNodes && !mlist.IsJoined() {
		// NOTE if local is in consensus nodes, try to join
		log.Log().Debug().
			Msg("watcher updated suffrage and local is in consensus nodes, but not yet joined; tries to join")

		go func() {
			_ = long.Join()
		}()
	}

	return nil
}

func removeExpelsFromSyncSourcePoolByWatcher(
	previous, updated base.SuffrageProof,
	syncSourcePool *isaac.SyncSourcePool,
	log *logging.Logging,
) error {
	if previous == nil {
		return nil
	}

	suf, err := updated.Suffrage()
	if err != nil {
		return err
	}

	previoussuf, err := previous.Suffrage()
	if err != nil {
		return err
	}

	expels := util.Filter2Slices(previoussuf.Nodes(), suf.Nodes(), func(p, n base.Node) bool {
		return p.Address().Equal(n.Address())
	})

	if len(expels) < 1 {
		return nil
	}

	expelnodes := make([]base.Address, len(expels))
	for i := range expels {
		expelnodes[i] = expels[i].Address()
	}

	removed := syncSourcePool.RemoveNonFixedNode(expelnodes...)

	log.Log().Debug().
		Interface("expels", expelnodes).
		Bool("removed", removed).
		Msg("expel nodes removed from sync source pool")

	return nil
}

func broadcastSuffrageVotingFunc(
	log *logging.Logging,
	memberlist *quicmemberlist.Memberlist,
) func(base.SuffrageExpelOperation) error {
	return func(op base.SuffrageExpelOperation) error {
		var b []byte

		switch i, err := util.MarshalJSON(op); {
		case err != nil:
			return errors.WithMessage(err, "marshal SuffrageExpelOperation")
		default:
			b = i
		}

		notifych := make(chan error, 1)

		go func() {
			if err := <-notifych; err != nil {
				log.Log().Error().Err(err).
					Stringer("operation", op.Fact().Hash()).
					Msg("failed to broadcast suffrage voting operation")

				return
			}

			log.Log().Debug().
				Stringer("operation", op.Fact().Hash()).
				Msg("suffrage voting operation fully broadcasted")
		}()

		// NOTE trying to broadcast until,
		// - no members in memberlist
		// - local not joined
		return memberlist.EnsureBroadcast(
			b,
			op.Fact().Hash().String(),
			notifych,
			func(i uint64) time.Duration {
				switch {
				case !memberlist.CanBroadcast():
					return 0
				case i < 1:
					return time.Nanosecond
				}

				return time.Second * 2 //nolint:gomnd //...
			},
			base.MaxThreshold.Float64(),
			0, // NOTE infinite loop
		)
	}
}
