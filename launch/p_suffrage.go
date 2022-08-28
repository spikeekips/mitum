package launch

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameSuffrageCandidateLimiterSet                 = ps.Name("suffrage-candidate-limiter-set")
	PNamePatchLastSuffrageProofWatcherWithMemberlist = ps.Name("patch-last-suffrage-proof-watcher-with-memberlist")
	PNameLastSuffrageProofWatcher                    = ps.Name("last-suffrage-proof-watcher")
	PNameStartLastSuffrageProofWatcher               = ps.Name("start-last-suffrage-proof-watcher")
	PNameNodeInConsensusNodesFunc                    = ps.Name("node-in-consensus-nodes-func")
	SuffrageCandidateLimiterSetContextKey            = ps.ContextKey("suffrage-candidate-limiter-set")
	LastSuffrageProofWatcherContextKey               = ps.ContextKey("last-suffrage-proof-watcher")
	NodeInConsensusNodesFuncContextKey               = ps.ContextKey("node-in-consensus-nodes-func")
)

func PSuffrageCandidateLimiterSet(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare SuffrageCandidateLimiterSet")

	var db isaac.Database
	if err := ps.LoadFromContextOK(ctx, CenterDatabaseContextKey, &db); err != nil {
		return ctx, e(err, "")
	}

	set := hint.NewCompatibleSet()

	if err := set.Add(
		isaac.FixedSuffrageCandidateLimiterRuleHint,
		base.SuffrageCandidateLimiterFunc(FixedSuffrageCandidateLimiterFunc()),
	); err != nil {
		return ctx, e(err, "")
	}

	if err := set.Add(
		isaac.MajoritySuffrageCandidateLimiterRuleHint,
		base.SuffrageCandidateLimiterFunc(MajoritySuffrageCandidateLimiterFunc(db)),
	); err != nil {
		return ctx, e(err, "")
	}

	ctx = context.WithValue(ctx, SuffrageCandidateLimiterSetContextKey, set) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func PLastSuffrageProofWatcher(ctx context.Context) (context.Context, error) {
	var local base.LocalNode
	var params base.LocalParams
	var db isaac.Database

	if err := ps.LoadFromContextOK(ctx,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return ctx, err
	}

	getLastSuffrageProoff, err := GetLastSuffrageProofFunc(ctx)
	if err != nil {
		return ctx, err
	}

	getSuffrageProoff, err := GetSuffrageProofFunc(ctx)
	if err != nil {
		return ctx, err
	}

	getLastSuffrageCandidatef, err := GetLastSuffrageCandidateFunc(ctx)
	if err != nil {
		return ctx, err
	}

	builder := isaac.NewSuffrageStateBuilder(
		params.NetworkID(),
		getLastSuffrageProoff,
		getSuffrageProoff,
		getLastSuffrageCandidatef,
	)

	watcher := isaac.NewLastConsensusNodesWatcher(
		func() (base.SuffrageProof, base.State, bool, error) {
			proof, found, err := db.LastSuffrageProof()
			if err != nil {
				return nil, nil, false, err
			}

			st, _, err := db.State(isaac.SuffrageCandidateStateKey)
			if err != nil {
				return nil, nil, false, err
			}

			return proof, st, found, nil
		},
		builder.Build,
		nil,
	)

	ctx = context.WithValue(ctx, LastSuffrageProofWatcherContextKey, watcher) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func PPatchLastSuffrageProofWatcherWithMemberlist(ctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var local base.LocalNode
	var watcher *isaac.LastConsensusNodesWatcher
	var memberlist *quicmemberlist.Memberlist

	if err := ps.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LastSuffrageProofWatcherContextKey, &watcher,
		MemberlistContextKey, &memberlist,
	); err != nil {
		return ctx, err
	}

	watcher.SetWhenUpdated(func(ctx context.Context, proof base.SuffrageProof, st base.State) {
		// NOTE set blank
	})

	return ctx, nil
}

func PNodeInConsensusNodesFunc(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed NodeInConsensusNodesFunc")

	var db isaac.Database
	if err := ps.LoadFromContextOK(ctx, CenterDatabaseContextKey, &db); err != nil {
		return ctx, e(err, "")
	}

	lastcandidateslocked := util.EmptyLocked()
	prevcandidateslocked := util.EmptyLocked()

	getCandidates := func(height base.Height) (
		[]base.SuffrageCandidateStateValue, []base.SuffrageCandidateStateValue, error,
	) {
		var prevcandidates []base.SuffrageCandidateStateValue
		var lastcandidates []base.SuffrageCandidateStateValue
		var cerr error

		_, _ = lastcandidateslocked.Set(func(_ bool, i interface{}) (interface{}, error) {
			var lastheight base.Height
			var last []base.SuffrageCandidateStateValue
			var prev []base.SuffrageCandidateStateValue

			if i != nil {
				j := i.([2]interface{}) //nolint:forcetypeassert //...

				lastheight = j[0].(base.Height) //nolint:forcetypeassert //...
				last = isaac.FilterCandidates(  //nolint:forcetypeassert //...
					height, j[1].([]base.SuffrageCandidateStateValue))
			}

			stheight, c, err := isaac.LastCandidatesFromState(height, db.State)
			if err != nil {
				cerr = err

				return nil, err
			}

			switch j, _ := prevcandidateslocked.Value(); {
			case j == nil:
			default:
				j := i.([2]interface{}) //nolint:forcetypeassert //...

				prev = isaac.FilterCandidates( //nolint:forcetypeassert //...
					height-1, j[1].([]base.SuffrageCandidateStateValue))
			}

			if stheight == lastheight {
				prevcandidates = prev
				lastcandidates = last

				return nil, errors.Errorf("stop")
			}

			prevcandidates = last
			lastcandidates = c

			_ = prevcandidateslocked.SetValue([2]interface{}{
				lastheight,
				last,
			})

			return [2]interface{}{stheight, c}, nil
		})

		if cerr != nil {
			return nil, nil, cerr
		}

		return prevcandidates, lastcandidates, nil
	}

	f := func(node base.Node, height base.Height) (base.Suffrage, bool, error) {
		suf, found, err := isaac.GetSuffrageFromDatabase(db, height)

		switch {
		case err != nil:
			return nil, false, err
		case !found:
			return nil, false, nil
		case suf.ExistsPublickey(node.Address(), node.Publickey()):
			return suf, true, nil
		}

		prev, last, err := getCandidates(height)
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

	ctx = context.WithValue(ctx, NodeInConsensusNodesFuncContextKey, f) //revive:disable-line:modifies-parameter

	return ctx, nil
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
			return nil, errors.WithMessagef(err, "failed to get last suffrage for MajoritySuffrageCandidateLimiter")
		case !found:
			return nil, errors.Errorf("last suffrage not found for MajoritySuffrageCandidateLimiter")
		}

		suf, err := proof.Suffrage()
		if err != nil {
			return nil, errors.WithMessagef(err, "failed to get suffrage for MajoritySuffrageCandidateLimiter")
		}

		return isaac.NewMajoritySuffrageCandidateLimiter(
			i,
			func() (uint64, error) {
				return uint64(suf.Len()), nil
			},
		), nil
	}
}

func GetLastSuffrageProofFunc(ctx context.Context) (isaac.GetLastSuffrageProofFromRemoteFunc, error) {
	var params base.LocalParams
	var client *isaacnetwork.QuicstreamClient
	var syncSourcePool *isaac.SyncSourcePool

	if err := ps.LoadFromContextOK(ctx,
		QuicstreamClientContextKey, &client,
		LocalParamsContextKey, &params,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	lastl := util.EmptyLocked()

	f := func(ctx context.Context, ci quicstream.UDPConnInfo) (base.SuffrageProof, bool, error) {
		var last util.Hash

		if i, _ := lastl.Value(); i != nil {
			last = i.(util.Hash) //nolint:forcetypeassert //...
		}

		cctx, cancel := context.WithTimeout(ctx, time.Second*2) //nolint:gomnd //...
		defer cancel()

		proof, updated, err := client.LastSuffrageProof(cctx, ci, last)

		switch {
		case err != nil:
			return proof, updated, err
		case !updated:
			return proof, updated, nil
		default:
			if err := proof.IsValid(params.NetworkID()); err != nil {
				return nil, updated, err
			}

			_ = lastl.SetValue(proof.Map().Manifest().Suffrage())

			return proof, updated, nil
		}
	}

	return func(ctx context.Context) (proof base.SuffrageProof, found bool, _ error) {
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

				proof, updated, err := f(ctx, ci)
				switch {
				case err != nil:
					return err
				case !updated:
					return nil
				}

				_, err = ml.Set(func(_ bool, v interface{}) (interface{}, error) {
					switch {
					case v == nil,
						proof.Map().Manifest().Height() >
							v.(base.SuffrageProof).Map().Manifest().Height(): //nolint:forcetypeassert //...

						return proof, nil
					default:
						return nil, util.ErrLockedSetIgnore.Errorf("old SuffrageProof")
					}
				})

				return err
			},
		); err != nil {
			if errors.Is(err, isaac.ErrEmptySyncSources) {
				return nil, false, nil
			}

			return nil, false, err
		}

		switch v, _ := ml.Value(); {
		case v == nil:
			return nil, false, nil
		default:
			return v.(base.SuffrageProof), true, nil //nolint:forcetypeassert //...
		}
	}, nil
}

func GetSuffrageProofFunc(ctx context.Context) ( //revive:disable-line:cognitive-complexity
	isaac.GetSuffrageProofFromRemoteFunc, error,
) {
	var params base.LocalParams
	var client *isaacnetwork.QuicstreamClient
	var syncSourcePool *isaac.SyncSourcePool

	if err := ps.LoadFromContextOK(ctx,
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

						cctx, cancel := context.WithTimeout(ctx, time.Second*2) //nolint:gomnd //...
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

							_, _ = result.Set(func(_ bool, i interface{}) (interface{}, error) {
								if i != nil {
									return nil, errors.Errorf("already set")
								}

								return [2]interface{}{a, b}, nil
							})

							return errors.Errorf("stop")
						}
					},
				)

				v, _ := result.Value()
				if v == nil {
					return true, nil
				}

				i := v.([2]interface{}) //nolint:forcetypeassert //...

				proof, found = i[0].(base.SuffrageProof), i[1].(bool) //nolint:forcetypeassert //...

				return false, nil
			},
			-1,
			time.Second,
		)

		return proof, found, err
	}, nil
}

func GetLastSuffrageCandidateFunc(ctx context.Context) (isaac.GetLastSuffrageCandidateStateRemoteFunc, error) {
	var client *isaacnetwork.QuicstreamClient
	var syncSourcePool *isaac.SyncSourcePool

	if err := ps.LoadFromContextOK(ctx,
		QuicstreamClientContextKey, &client,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	lastl := util.EmptyLocked()

	f := func(ctx context.Context, ci quicstream.UDPConnInfo) (base.State, bool, error) {
		var last util.Hash

		if i, _ := lastl.Value(); i != nil {
			last = i.(util.Hash) //nolint:forcetypeassert //...
		}

		cctx, cancel := context.WithTimeout(ctx, time.Second*2) //nolint:gomnd //...
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

				st, found, err := f(ctx, ci)
				switch {
				case err != nil, !found, st == nil:
					return err
				}

				_, err = ml.Set(func(_ bool, v interface{}) (interface{}, error) {
					switch {
					case v == nil, st.Height() > v.(base.State).Height(): //nolint:forcetypeassert //...
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
			return v.(base.State), true, nil //nolint:forcetypeassert //...
		}
	}, nil
}

func NewSuffrageCandidateLimiterFunc(ctx context.Context) ( //revive:disable-line:cognitive-complexity
	func(base.Height, base.GetStateFunc) (base.OperationProcessorProcessFunc, error),
	error,
) {
	var db isaac.Database
	var limiterset *hint.CompatibleSet

	if err := ps.LoadFromContextOK(ctx,
		CenterDatabaseContextKey, &db,
		SuffrageCandidateLimiterSetContextKey, &limiterset,
	); err != nil {
		return nil, err
	}

	return func(height base.Height, getStateFunc base.GetStateFunc) (base.OperationProcessorProcessFunc, error) {
		e := util.StringErrorFunc("failed to get SuffrageCandidateLimiterFunc")

		policy := db.LastNetworkPolicy()
		if policy == nil {
			return nil, e(nil, "empty network policy")
		}

		var suf base.Suffrage

		switch proof, found, err := db.LastSuffrageProof(); {
		case err != nil:
			return nil, e(err, "failed to get last suffrage")
		case !found:
			return nil, e(nil, "last suffrage not found")
		default:
			i, err := proof.Suffrage()
			if err != nil {
				return nil, e(err, "failed to get suffrage")
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

func PStartLastSuffrageProofWatcher(ctx context.Context) (context.Context, error) {
	var watcher *isaac.LastConsensusNodesWatcher
	if err := ps.LoadFromContextOK(ctx, LastSuffrageProofWatcherContextKey, &watcher); err != nil {
		return ctx, err
	}

	return ctx, watcher.Start()
}

func PCloseLastSuffrageProofWatcher(ctx context.Context) (context.Context, error) {
	var watcher *isaac.LastConsensusNodesWatcher
	if err := ps.LoadFromContext(ctx, LastSuffrageProofWatcherContextKey, &watcher); err != nil {
		return ctx, err
	}

	if watcher != nil {
		if err := watcher.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
			return ctx, err
		}
	}

	return ctx, nil
}
