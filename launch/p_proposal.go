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
)

func PProposalProcessors(pctx context.Context) (context.Context, error) {
	var log *logging.Logging

	if err := util.LoadFromContextOK(pctx, LoggingContextKey, &log); err != nil {
		return pctx, err
	}

	newProposalProcessorf, err := newProposalProcessorFunc(pctx)
	if err != nil {
		return pctx, err
	}

	getProposalf, err := getProposalFunc(pctx)
	if err != nil {
		return pctx, err
	}

	pps := isaac.NewProposalProcessors(newProposalProcessorf, getProposalf)
	_ = pps.SetLogging(log)

	return context.WithValue(pctx, ProposalProcessorsContextKey, pps), nil
}

func PProposerSelector(pctx context.Context) (context.Context, error) {
	p := isaac.NewBlockBasedProposerSelector()

	/* FixedProposerSelector example,

	isaac.NewFixedProposerSelector(func(_ base.Point, nodes []base.Node) (base.Node, error) { // NOTE
		log.Log().Debug().
			Int("number_nodes", len(nodes)).
			Interface("nodes", nodes).
			Msg("selecting proposer from the given nodes")

		for i := range nodes {
			n := nodes[i]
			if n.Address().String() == "no0sas" {
				return n, nil
			}
		}

		return nil, errors.Errorf("no0sas not found")
	}),
	*/

	return context.WithValue(pctx, ProposerSelectorContextKey, p), nil
}

func newProposalProcessorFunc(pctx context.Context) (
	func(base.ProposalSignFact, base.Manifest) (isaac.ProposalProcessor, error),
	error,
) {
	var enc encoder.Encoder
	var design NodeDesign
	var local base.LocalNode
	var params base.LocalParams
	var db isaac.Database
	var oprs *hint.CompatibleSet

	if err := util.LoadFromContextOK(pctx,
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

	return func(proposal base.ProposalSignFact, previous base.Manifest) (
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
	func(context.Context, base.Point, util.Hash) (base.ProposalSignFact, error),
	error,
) {
	var params *isaac.LocalParams
	var pool *isaacdatabase.TempPool
	var client *isaacnetwork.QuicstreamClient
	var memberlist *quicmemberlist.Memberlist

	if err := util.LoadFromContextOK(pctx,
		LocalParamsContextKey, &params,
		PoolDatabaseContextKey, &pool,
		QuicstreamClientContextKey, &client,
		MemberlistContextKey, &memberlist,
	); err != nil {
		return nil, err
	}

	return func(ctx context.Context, point base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
		switch pr, found, err := pool.Proposal(facthash); {
		case err != nil:
			return nil, err
		case found:
			return pr, nil
		}

		// NOTE if not found, request to remote node
		worker := util.NewErrgroupWorker(ctx, int64(memberlist.MembersLen()))
		defer worker.Close()

		prl := util.EmptyLocked((base.ProposalSignFact)(nil))

		go func() {
			defer worker.Done()

			memberlist.Remotes(func(node quicmemberlist.Member) bool {
				ci := node.UDPConnInfo()

				return worker.NewJob(func(ctx context.Context, _ uint64) error {
					cctx, cancel := context.WithTimeout(ctx, params.TimeoutRequest())
					defer cancel()

					var pr base.ProposalSignFact

					switch i, found, err := client.Proposal(cctx, ci, facthash); {
					case err != nil || !found:
						return nil
					default:
						if err := i.IsValid(params.NetworkID()); err != nil {
							return err
						}

						pr = i
					}

					switch {
					case !point.Equal(pr.Point()):
						return nil
					case !facthash.Equal(pr.Fact().Hash()):
						return nil
					}

					_, _ = prl.GetOrCreate(func() (base.ProposalSignFact, error) {
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

			return nil, storage.ErrNotFound.Errorf("ProposalSignFact not found")
		default:
			_, _ = pool.SetProposal(i)

			return i, nil
		}
	}, nil
}

func getProposalOperationFunc(pctx context.Context) (
	func(base.ProposalSignFact) isaac.OperationProcessorGetOperationFunction,
	error,
) {
	var params base.LocalParams
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
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

	return func(proposal base.ProposalSignFact) isaac.OperationProcessorGetOperationFunction {
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
					return nil, isaac.ErrOperationNotFoundInProcessor.Errorf("not found in remote")
				default:
					op = i
				}
			}

			if err := op.IsValid(params.NetworkID()); err != nil {
				return nil, isaac.ErrInvalidOperationInProcessor.Wrap(err)
			}

			switch found, err := db.ExistsInStateOperation(op.Fact().Hash()); {
			case err != nil:
				return nil, err
			case found:
				return nil, isaac.ErrOperationAlreadyProcessedInProcessor.Errorf("already processed")
			default:
				return op, nil
			}
		}
	}, nil
}

func getProposalOperationFromPoolFunc(pctx context.Context) (
	func(pctx context.Context, operationhash util.Hash) (base.Operation, bool, error),
	error,
) {
	var pool *isaacdatabase.TempPool

	if err := util.LoadFromContextOK(pctx, PoolDatabaseContextKey, &pool); err != nil {
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
	func(context.Context, base.ProposalSignFact, util.Hash) (base.Operation, bool, error),
	error,
) {
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

	getProposalOperationFromRemoteProposerf, err := getProposalOperationFromRemoteProposerFunc(pctx)
	if err != nil {
		return nil, err
	}

	return func(
		ctx context.Context, proposal base.ProposalSignFact, operationhash util.Hash,
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
		result := util.EmptyLocked((base.Operation)(nil))

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
				cctx, cancel := context.WithTimeout(ctx, params.TimeoutRequest())
				defer cancel()

				op, _ := result.Set(func(i base.Operation, _ bool) (base.Operation, error) {
					if i != nil {
						return i, util.ErrLockedSetIgnore.Call()
					}

					switch op, found, err := client.Operation(cctx, ci, operationhash); {
					case err != nil, !found:
						return nil, util.ErrLockedSetIgnore.Call()
					default:
						return op, nil
					}
				})

				if op != nil {
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

		return i, true, nil
	}, nil
}

func getProposalOperationFromRemoteProposerFunc(pctx context.Context) (
	func(context.Context, base.ProposalSignFact, util.Hash) (bool, base.Operation, bool, error),
	error,
) {
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

	return func(
		ctx context.Context, proposal base.ProposalSignFact, operationhash util.Hash,
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

		cctx, cancel := context.WithTimeout(ctx, params.TimeoutRequest())
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
	var local base.LocalNode
	var params *isaac.LocalParams

	if err := util.LoadFromContextOK(pctx,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
	); err != nil {
		return nil, err
	}

	args, err := newBaseProposalSelectorArgs(pctx)
	if err != nil {
		return nil, err
	}

	return isaac.NewBaseProposalSelector(local, params, args), nil
}

func newBaseProposalSelectorArgs(pctx context.Context) (*isaac.BaseProposalSelectorArgs, error) {
	var log *logging.Logging
	var local base.LocalNode
	var params *isaac.LocalParams
	var pool *isaacdatabase.TempPool
	var proposalMaker *isaac.ProposalMaker
	var memberlist *quicmemberlist.Memberlist
	var client *isaacnetwork.QuicstreamClient
	var proposerSelector isaac.ProposerSelector

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		PoolDatabaseContextKey, &pool,
		ProposalMakerContextKey, &proposalMaker,
		MemberlistContextKey, &memberlist,
		QuicstreamClientContextKey, &client,
		ProposerSelectorContextKey, &proposerSelector,
	); err != nil {
		return nil, err
	}

	args := isaac.NewBaseProposalSelectorArgs()

	args.Pool = pool
	args.ProposerSelector = proposerSelector
	args.Maker = proposalMaker
	args.MinProposerWait = params.TimeoutRequest() + (time.Second * 2) //nolint:gomnd //...

	if err := getNodesFuncOfBaseProposalSelectorArgs(pctx, args); err != nil {
		return nil, err
	}

	if err := requestFuncOfBaseProposalSelectorArgs(pctx, args); err != nil {
		return nil, err
	}

	return args, nil
}

func getNodesFuncOfBaseProposalSelectorArgs(pctx context.Context, args *isaac.BaseProposalSelectorArgs) error {
	var log *logging.Logging
	var local base.LocalNode
	var memberlist *quicmemberlist.Memberlist
	var sp *SuffragePool

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		MemberlistContextKey, &memberlist,
		SuffragePoolContextKey, &sp,
	); err != nil {
		return err
	}

	args.GetNodesFunc = func(height base.Height) ([]base.Node, bool, error) {
		switch i, found, err := sp.Height(height); {
		case err != nil:
			return nil, false, err
		case !found:
			return nil, false, errors.Errorf("suffrage not found")
		case i.Len() < 1:
			return nil, false, errors.Errorf("empty suffrage nodes")
		default:
			return i.Nodes(), true, nil
		}
	}

	return nil
}

func requestFuncOfBaseProposalSelectorArgs(pctx context.Context, args *isaac.BaseProposalSelectorArgs) error {
	var params *isaac.LocalParams
	var memberlist *quicmemberlist.Memberlist
	var client *isaacnetwork.QuicstreamClient

	if err := util.LoadFromContextOK(pctx,
		LocalParamsContextKey, &params,
		MemberlistContextKey, &memberlist,
		QuicstreamClientContextKey, &client,
	); err != nil {
		return err
	}

	args.RequestFunc = func(
		ctx context.Context,
		point base.Point,
		proposer base.Node,
		previousBlock util.Hash,
	) (base.ProposalSignFact, bool, error) {
		members, err := quicmemberlist.RandomAliveMembers(
			memberlist,
			33, //nolint:gomnd //...
			func(node quicmemberlist.Member) bool {
				return node.UDPConnInfo().Addr() == nil || node.Address().Equal(proposer.Address())
			},
		)
		if err != nil {
			return nil, false, err
		}

		cis := make([]quicstream.UDPConnInfo, len(members))
		for i := range members {
			cis[i] = members[i].UDPConnInfo()
		}

		if len(cis) < 1 {
			return nil, false, errors.Errorf("no alive members")
		}

		// NOTE include proposer conn info
		memberlist.Members(func(node quicmemberlist.Member) bool {
			if node.Address().Equal(proposer.Address()) {
				if node.UDPConnInfo().Addr() != nil {
					cis = append(cis, node.UDPConnInfo()) //nolint:makezero //...
				}

				return false
			}

			return true
		})

		nctx, cancel := context.WithTimeout(ctx, params.TimeoutRequest())
		defer cancel()

		return isaac.ConcurrentRequestProposal(
			nctx,
			point,
			proposer,
			previousBlock,
			client,
			cis,
			params.NetworkID(),
		)
	}

	return nil
}
