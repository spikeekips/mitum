package launch

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

func PProposalProcessors(ctx context.Context) (context.Context, error) {
	var log *logging.Logging

	if err := ps.LoadFromContextOK(ctx, LoggingContextKey, &log); err != nil {
		return ctx, err
	}

	newProposalProcessorf, err := newProposalProcessorFunc(ctx)
	if err != nil {
		return ctx, err
	}

	getProposalf, err := getProposalFunc(ctx)
	if err != nil {
		return ctx, err
	}

	pps := isaac.NewProposalProcessors(newProposalProcessorf, getProposalf)
	_ = pps.SetLogging(log)

	ctx = context.WithValue(ctx, ProposalProcessorsContextKey, pps) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func newProposalProcessorFunc(pctx context.Context) (
	func(base.ProposalSignedFact, base.Manifest) (isaac.ProposalProcessor, error),
	error,
) {
	var enc encoder.Encoder
	var design NodeDesign
	var local base.LocalNode
	var params base.LocalParams
	var db isaac.Database
	var oprs *hint.CompatibleSet

	if err := ps.LoadFromContextOK(pctx,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
		OperationProcessorsMapContextKey, &oprs,
	); err != nil {
		return nil, err
	}

	getProposalOperationFuncf, err := getProposalOperationFunc(pctx)
	if err != nil {
		return nil, err
	}

	return func(proposal base.ProposalSignedFact, previous base.Manifest) (
		isaac.ProposalProcessor, error,
	) {
		return isaac.NewDefaultProposalProcessor(
			proposal,
			previous,
			NewBlockWriterFunc(
				local,
				params.NetworkID(),
				LocalFSDataDirectory(design.Storage.Base),
				enc,
				db,
			),
			db.State,
			getProposalOperationFuncf(proposal),
			func(height base.Height, ht hint.Hint) (base.OperationProcessor, error) {
				v := oprs.Find(ht)
				if v == nil {
					return nil, nil
				}

				f := v.(func(height base.Height) (base.OperationProcessor, error)) //nolint:forcetypeassert //...

				return f(height)
			},
		)
	}, nil
}

func getProposalFunc(pctx context.Context) (
	func(context.Context, util.Hash) (base.ProposalSignedFact, error),
	error,
) {
	var pool *isaacdatabase.TempPool
	var client *isaacnetwork.QuicstreamClient
	var memberlist *quicmemberlist.Memberlist

	if err := ps.LoadFromContextOK(pctx,
		PoolDatabaseContextKey, &pool,
		QuicstreamClientContextKey, &client,
		MemberlistContextKey, &memberlist,
	); err != nil {
		return nil, err
	}

	return func(ctx context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
		switch pr, found, err := pool.Proposal(facthash); {
		case err != nil:
			return nil, err
		case found:
			return pr, nil
		}

		// NOTE if not found, request to remote node
		worker := util.NewErrgroupWorker(ctx, int64(memberlist.MembersLen()))
		defer worker.Close()

		prl := util.EmptyLocked()

		go func() {
			defer worker.Done()

			memberlist.Remotes(func(node quicmemberlist.Node) bool {
				ci := node.UDPConnInfo()

				return worker.NewJob(func(ctx context.Context, _ uint64) error {
					cctx, cancel := context.WithTimeout(ctx, time.Second*2) //nolint:gomnd //...
					defer cancel()

					pr, found, err := client.Proposal(cctx, ci, facthash)
					switch {
					case err != nil:
						return nil
					case !found:
						return nil
					}

					_, _ = prl.Set(func(_ bool, i interface{}) (interface{}, error) {
						if i != nil {
							return i, nil
						}

						return pr, nil
					})

					return errors.Errorf("stop")
				}) == nil
			})
		}()

		err := worker.Wait()

		switch i, _ := prl.Value(); {
		case i == nil:
			if err != nil {
				return nil, err
			}

			return nil, storage.ErrNotFound.Errorf("ProposalSignedFact not found")
		default:
			return i.(base.ProposalSignedFact), nil //nolint:forcetypeassert //...
		}
	}, nil
}

func getProposalOperationFunc(pctx context.Context) (
	func(base.ProposalSignedFact) isaac.OperationProcessorGetOperationFunction,
	error,
) {
	var params base.LocalParams
	var db isaac.Database

	if err := ps.LoadFromContextOK(pctx,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return nil, err
	}

	getProposalOperationFromPoolf, err := getProposalOperationFromPoolFunc(pctx)
	if err != nil {
		return nil, err
	}

	getProposalOperationFromRemotef, err := getProposalOperationFromRemoteFunc(pctx)
	if err != nil {
		return nil, err
	}

	return func(proposal base.ProposalSignedFact) isaac.OperationProcessorGetOperationFunction {
		return func(ctx context.Context, operationhash util.Hash) (base.Operation, error) {
			var op base.Operation

			switch i, found, err := getProposalOperationFromPoolf(ctx, operationhash); {
			case err != nil:
				return nil, err
			case found:
				op = i
			}

			if op == nil {
				switch i, found, err := getProposalOperationFromRemotef(ctx, proposal, operationhash); {
				case err != nil:
					return nil, err
				case !found:
					return nil, isaac.OperationNotFoundInProcessorError.Errorf("not found in remote")
				default:
					op = i
				}
			}

			if err := op.IsValid(params.NetworkID()); err != nil {
				return nil, isaac.InvalidOperationInProcessorError.Wrap(err)
			}

			switch found, err := db.ExistsInStateOperation(op.Fact().Hash()); {
			case err != nil:
				return nil, err
			case found:
				return nil, isaac.OperationAlreadyProcessedInProcessorError.Errorf("already processed")
			default:
				return op, nil
			}
		}
	}, nil
}

func getProposalOperationFromPoolFunc(pctx context.Context) (
	func(ctx context.Context, operationhash util.Hash) (base.Operation, bool, error),
	error,
) {
	var pool *isaacdatabase.TempPool

	if err := ps.LoadFromContextOK(pctx, PoolDatabaseContextKey, &pool); err != nil {
		return nil, err
	}

	return func(ctx context.Context, operationhash util.Hash) (base.Operation, bool, error) {
		op, found, err := pool.NewOperation(ctx, operationhash)

		switch {
		case err != nil:
			return nil, false, err
		case !found:
			return nil, false, nil
		default:
			return op, true, nil
		}
	}, nil
}

func getProposalOperationFromRemoteFunc(pctx context.Context) ( //nolint:gocognit //...
	func(context.Context, base.ProposalSignedFact, util.Hash) (base.Operation, bool, error),
	error,
) {
	var client *isaacnetwork.QuicstreamClient
	var syncSourcePool *isaac.SyncSourcePool

	if err := ps.LoadFromContextOK(pctx,
		QuicstreamClientContextKey, &client,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	getProposalOperationFromRemoteProposerf, err := getProposalOperationFromRemoteProposerFunc(pctx)
	if err != nil {
		return nil, err
	}

	return func(
		ctx context.Context, proposal base.ProposalSignedFact, operationhash util.Hash,
	) (base.Operation, bool, error) {
		if syncSourcePool.Len() < 1 {
			return nil, false, nil
		}

		switch isproposer, op, found, err := getProposalOperationFromRemoteProposerf(ctx, proposal, operationhash); {
		case err != nil:
			return nil, false, err
		case !isproposer:
		case !found:
			// NOTE proposer proposed this operation, but it does not have? weired.
		default:
			return op, true, nil
		}

		proposer := proposal.ProposalFact().Proposer()
		result := util.EmptyLocked()

		worker := util.NewErrgroupWorker(ctx, int64(syncSourcePool.Len()))
		defer worker.Close()

		syncSourcePool.Actives(func(nci isaac.NodeConnInfo) bool {
			if proposer.Equal(nci.Address()) {
				return true
			}

			ci, err := nci.UDPConnInfo()
			if err != nil {
				return true
			}

			if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
				_, err := result.Set(func(_ bool, i interface{}) (interface{}, error) {
					if i != nil {
						return nil, util.ErrLockedSetIgnore.Call()
					}

					cctx, cancel := context.WithTimeout(ctx, time.Second*2) //nolint:gomnd //...
					defer cancel()

					op, found, err := client.Operation(cctx, ci, operationhash)
					if err == nil && found {
						return op, nil
					}

					return nil, util.ErrLockedSetIgnore.Call()
				})

				if err == nil {
					return errors.Errorf("stop")
				}

				return nil
			}); err != nil {
				return false
			}

			return true
		})

		worker.Done()

		err := worker.Wait()

		i, _ := result.Value()
		if i == nil {
			return nil, false, err
		}

		return i.(base.Operation), true, nil //nolint:forcetypeassert //...
	}, nil
}

func getProposalOperationFromRemoteProposerFunc(pctx context.Context) (
	func(context.Context, base.ProposalSignedFact, util.Hash) (bool, base.Operation, bool, error),
	error,
) {
	var client *isaacnetwork.QuicstreamClient
	var syncSourcePool *isaac.SyncSourcePool

	if err := ps.LoadFromContextOK(pctx,
		QuicstreamClientContextKey, &client,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	return func(
		ctx context.Context, proposal base.ProposalSignedFact, operationhash util.Hash,
	) (bool, base.Operation, bool, error) {
		proposer := proposal.ProposalFact().Proposer()

		var proposernci isaac.NodeConnInfo

		syncSourcePool.Actives(func(nci isaac.NodeConnInfo) bool {
			if !proposer.Equal(nci.Address()) {
				return true
			}

			if _, err := nci.UDPConnInfo(); err == nil {
				proposernci = nci
			}

			return false
		})

		if proposernci == nil {
			return false, nil, false, nil
		}

		ci, err := proposernci.UDPConnInfo()
		if err != nil {
			return true, nil, false, err
		}

		cctx, cancel := context.WithTimeout(ctx, time.Second*2) //nolint:gomnd //...
		defer cancel()

		switch op, found, err := client.Operation(cctx, ci, operationhash); {
		case err != nil:
			return true, nil, false, err
		case !found:
			return true, nil, false, nil
		default:
			return true, op, true, nil
		}
	}, nil
}

func NewProposalSelector(pctx context.Context) (*isaac.BaseProposalSelector, error) {
	var log *logging.Logging
	var local base.LocalNode
	var params *isaac.LocalParams
	var db isaac.Database
	var pool *isaacdatabase.TempPool
	var proposalMaker *isaac.ProposalMaker
	var memberlist *quicmemberlist.Memberlist
	var client *isaacnetwork.QuicstreamClient

	if err := ps.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
		PoolDatabaseContextKey, &pool,
		ProposalMakerContextKey, &proposalMaker,
		MemberlistContextKey, &memberlist,
		QuicstreamClientContextKey, &client,
	); err != nil {
		return nil, err
	}

	return isaac.NewBaseProposalSelector(
		local,
		params,
		isaac.NewBlockBasedProposerSelector(
			func(height base.Height) (util.Hash, error) {
				switch m, found, err := db.BlockMap(height); {
				case err != nil:
					return nil, err
				case !found:
					return nil, nil
				default:
					return m.Manifest().Hash(), nil
				}
			},
		),
		// isaac.NewFixedProposerSelector(func(_ base.Point, nodes []base.Node) (base.Node, error) { // NOTE
		// 	log.Log().Debug().
		// 		Int("number_nodes", len(nodes)).
		// 		Interface("nodes", nodes).
		// 		Msg("selecting proposer from the given nodes")
		//
		// 	for i := range nodes {
		// 		n := nodes[i]
		// 		if n.Address().String() == "no0sas" {
		// 			return n, nil
		// 		}
		// 	}
		//
		// 	return nil, errors.Errorf("no0sas not found")
		// }),
		proposalMaker,
		func(height base.Height) ([]base.Node, bool, error) {
			var suf base.Suffrage

			switch i, found, err := isaac.GetSuffrageFromDatabase(db, height); {
			case err != nil:
				return nil, false, err
			case !found:
				return nil, false, errors.Errorf("suffrage not found")
			case i.Len() < 1:
				return nil, false, errors.Errorf("empty suffrage nodes")
			default:
				suf = i
			}

			if suf.Len() < 2 { //nolint:gomnd // only local
				return []base.Node{local}, true, nil
			}

			switch {
			case memberlist == nil:
				log.Log().Debug().Msg("tried to make proposal, but empty memberlist")

				return nil, false, isaac.ErrEmptyAvailableNodes.Errorf("nil memberlist")
			case !memberlist.IsJoined():
				log.Log().Debug().Msg("tried to make proposal, but memberlist, not yet joined")

				return nil, false, isaac.ErrEmptyAvailableNodes.Errorf("memberlist, not yet joined")
			}

			members := make([]base.Node, memberlist.MembersLen()*2)

			var i int
			memberlist.Members(func(node quicmemberlist.Node) bool {
				if !suf.Exists(node.Address()) {
					return true
				}

				members[i] = isaac.NewNode(node.Publickey(), node.Address())
				i++

				return true
			})

			members = members[:i]

			if len(members) < 1 {
				return nil, false, isaac.ErrEmptyAvailableNodes.Errorf("no alive members")
			}

			return members, true, nil
		},
		func(ctx context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			var ci quicstream.UDPConnInfo

			memberlist.Members(func(node quicmemberlist.Node) bool {
				if node.Address().Equal(proposer) {
					ci = node.UDPConnInfo()

					return false
				}

				return true
			})

			if ci.Addr() == nil {
				return nil, errors.Errorf("proposer not joined in memberlist")
			}

			cctx, cancel := context.WithTimeout(ctx, time.Second*2) //nolint:gomnd //...
			defer cancel()

			sf, found, err := client.RequestProposal(cctx, ci, point, proposer)
			switch {
			case err != nil:
				return nil, errors.WithMessage(err, "failed to get proposal from proposer")
			case !found:
				return nil, errors.Errorf("proposer can not make proposal")
			default:
				return sf, nil
			}
		},
		pool,
	), nil
}
