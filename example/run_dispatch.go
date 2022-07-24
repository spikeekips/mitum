package main

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	suffrageCandidateFactHintTypeBytes = isaacoperation.SuffrageCandidateFactHint.Type().Bytes()
	suffrageJoinFactHintTypeBytes      = isaacoperation.SuffrageJoinFactHint.Type().Bytes()
)

func (cmd *runCommand) getSuffrageFunc() isaac.GetSuffrageByBlockHeight {
	return func(blockheight base.Height) (base.Suffrage, bool, error) {
		return isaac.GetSuffrageFromDatabase(cmd.db, blockheight)
	}
}

func (cmd *runCommand) nodeInConsensusNodesFunc() isaac.NodeInConsensusNodesFunc {
	return func(node base.Node, height base.Height) (base.Suffrage, bool, error) {
		suf, found, err := cmd.getSuffrage(height)

		switch {
		case err != nil:
			return nil, false, err
		case !found:
			return nil, false, nil
		case suf.ExistsPublickey(node.Address(), node.Publickey()):
			return suf, true, nil
		}

		candidates, err := isaac.LastCandidatesFromState(height, cmd.db.State)
		if err != nil {
			return nil, false, err
		}

		for i := range candidates {
			c := candidates[i]

			switch {
			case !node.Address().Equal(c.Address()):
			case !node.Publickey().Equal(c.Publickey()):
			default:
				return suf, true, nil
			}
		}

		return suf, false, nil
	}
}

func (cmd *runCommand) getManifestFunc() func(height base.Height) (base.Manifest, error) {
	return func(height base.Height) (base.Manifest, error) {
		switch m, found, err := cmd.db.BlockMap(height); {
		case err != nil:
			return nil, err
		case !found:
			return nil, nil
		default:
			return m.Manifest(), nil
		}
	}
}

func (cmd *runCommand) proposalMaker() *isaac.ProposalMaker {
	operationfilterf := launch.IsSupportedProposalOperationFactHintFunc()

	return isaac.NewProposalMaker(
		cmd.local,
		cmd.nodePolicy,
		func(ctx context.Context) ([]util.Hash, error) {
			policy := cmd.db.LastNetworkPolicy()
			if policy == nil { // NOTE Usually it means empty block data
				return nil, nil
			}

			n := policy.MaxOperationsInProposal()
			if n < 1 {
				return nil, nil
			}

			hs, err := cmd.pool.NewOperationHashes(
				ctx,
				n,
				func(operationhash, facthash util.Hash, header isaac.PoolOperationHeader) (bool, error) {
					// NOTE filter genesis operations
					if !operationfilterf(header.HintBytes()) {
						return false, nil
					}

					switch found, err := cmd.db.ExistsKnownOperation(operationhash); {
					case err != nil:
						return false, err
					case found:
						log.Trace().Stringer("operation", operationhash).Msg("already processed; known operation")

						return false, nil
					}

					switch found, err := cmd.db.ExistsInStateOperation(facthash); {
					case err != nil:
						return false, err
					case found:
						log.Trace().Stringer("operation", facthash).Msg("already processed; in state")

						return false, nil
					}

					// NOTE if bad operation and it is failed to be processed;
					// it can be included in next proposal; it should be
					// excluded.
					// NOTE if operation has not enough fact signs, it will
					// ignored. It must be filtered for not this kind of
					// operations.
					switch found, err := cmd.db.ExistsInStateOperation(facthash); {
					case err != nil:
						return false, err
					case found:
						return false, nil
					}

					addedat, err := util.BytesToInt64(header.AddedAt())
					if err != nil {
						return false, nil
					}

					var expire time.Duration
					switch ht := header.HintBytes(); {
					case bytes.HasPrefix(ht, suffrageCandidateFactHintTypeBytes),
						bytes.HasPrefix(ht, suffrageJoinFactHintTypeBytes):
						expire = cmd.nodePolicy.ValidProposalSuffrageOperationsExpire()
					default:
						expire = cmd.nodePolicy.ValidProposalOperationExpire()
					}

					if addedat < localtime.UTCNow().Add(expire*-1).UnixNano() {
						return false, nil
					}

					return true, nil
				},
			)
			if err != nil {
				return nil, err
			}

			return hs, nil
		},
		cmd.pool,
	)
}

func (cmd *runCommand) proposalSelectorFunc() *isaac.BaseProposalSelector {
	proposalMaker := cmd.proposalMaker()

	_ = proposalMaker.SetLogging(logging)

	return isaac.NewBaseProposalSelector(
		cmd.local,
		cmd.nodePolicy,
		isaac.NewBlockBasedProposerSelector(
			func(height base.Height) (util.Hash, error) {
				switch m, err := cmd.getManifest(height); {
				case err != nil:
					return nil, err
				case m == nil:
					return nil, nil
				default:
					return m.Hash(), nil
				}
			},
		),
		// isaac.NewFixedProposerSelector(func(_ base.Point, nodes []base.Node) (base.Node, error) { // NOTE
		// 	log.Debug().
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
			switch i, found, err := cmd.getSuffrage(height); {
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
				return []base.Node{cmd.local}, true, nil
			}

			switch {
			case cmd.memberlist == nil:
				log.Debug().Msg("tried to make proposal, but empty memberlist")

				return nil, false, isaac.ErrEmptyAvailableNodes.Errorf("nil memberlist")
			case !cmd.memberlist.IsJoined():
				log.Debug().Msg("tried to make proposal, but memberlist, not yet joined")

				return nil, false, isaac.ErrEmptyAvailableNodes.Errorf("memberlist, not yet joined")
			}

			members := make([]base.Node, cmd.memberlist.MembersLen()*2)

			var i int
			cmd.memberlist.Members(func(node quicmemberlist.Node) bool {
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

			cmd.memberlist.Members(func(node quicmemberlist.Node) bool {
				if node.Address().Equal(proposer) {
					ci = node.UDPConnInfo()

					return false
				}

				return true
			})

			if ci.Addr() == nil {
				return nil, errors.Errorf("proposer not joined in memberlist")
			}

			sf, found, err := cmd.client.RequestProposal(ctx, ci, point, proposer)
			switch {
			case err != nil:
				return nil, errors.WithMessage(err, "failed to get proposal from proposer")
			case !found:
				return nil, errors.Errorf("proposer can not make proposal")
			default:
				return sf, nil
			}
		},
		cmd.pool,
	)
}

func (cmd *runCommand) getLastManifestFunc() func() (base.Manifest, bool, error) {
	return func() (base.Manifest, bool, error) {
		switch m, found, err := cmd.db.LastBlockMap(); {
		case err != nil || !found:
			return nil, found, err
		default:
			return m.Manifest(), true, nil
		}
	}
}

func (cmd *runCommand) newProposalProcessorFunc(enc encoder.Encoder) newProposalProcessorFunc {
	set := cmd.operationProcessorsMap()

	return func(proposal base.ProposalSignedFact, previous base.Manifest) (
		isaac.ProposalProcessor, error,
	) {
		return isaac.NewDefaultProposalProcessor(
			proposal,
			previous,
			launch.NewBlockWriterFunc(
				cmd.local,
				cmd.nodePolicy.NetworkID(),
				launch.LocalFSDataDirectory(cmd.design.Storage.Base),
				enc,
				cmd.db,
			),
			cmd.db.State,
			cmd.getProposalOperationFunc(proposal),
			func(height base.Height, ht hint.Hint) (base.OperationProcessor, error) {
				v := set.Find(ht)
				if v == nil {
					return nil, nil
				}

				f := v.(func(height base.Height) (base.OperationProcessor, error)) //nolint:forcetypeassert //...

				return f(height)
			},
			cmd.pool.SetLastVoteproofs,
		)
	}
}

func (cmd *runCommand) newSyncer(
	lvps *isaacstates.LastVoteproofsHandler,
) func(height base.Height) (isaac.Syncer, error) {
	return func(height base.Height) (isaac.Syncer, error) {
		e := util.StringErrorFunc("failed newSyncer")

		var prev base.BlockMap

		switch m, found, err := cmd.db.LastBlockMap(); {
		case err != nil:
			return nil, e(isaacstates.ErrUnpromising.Wrap(err), "")
		case found:
			prev = m
		}

		var tempsyncpool isaac.TempSyncPool

		switch i, err := launch.NewTempSyncPoolDatabase(cmd.design.Storage.Base, height, cmd.encs, cmd.enc); {
		case err != nil:
			return nil, e(isaacstates.ErrUnpromising.Wrap(err), "")
		default:
			tempsyncpool = i
		}

		syncer, err := isaacstates.NewSyncer(
			cmd.design.Storage.Base,
			func(height base.Height) (isaac.BlockWriteDatabase, func(context.Context) error, error) {
				bwdb, err := cmd.db.NewBlockWriteDatabase(height)
				if err != nil {
					return nil, nil, err
				}

				return bwdb,
					func(ctx context.Context) error {
						return launch.MergeBlockWriteToPermanentDatabase(ctx, bwdb, cmd.perm)
					},
					nil
			},
			func(root string, blockmap base.BlockMap, bwdb isaac.BlockWriteDatabase) (isaac.BlockImporter, error) {
				return isaacblock.NewBlockImporter(
					launch.LocalFSDataDirectory(root),
					cmd.encs,
					blockmap,
					bwdb,
					cmd.nodePolicy.NetworkID(),
				)
			},
			prev,
			cmd.syncerLastBlockMapf(),
			cmd.syncerBlockMapf(),
			cmd.syncerBlockMapItemf(),
			tempsyncpool,
			cmd.setLastVoteproofsfFromBlockReader(lvps),
		)
		if err != nil {
			return nil, e(err, "")
		}

		go cmd.newSyncerDeferred(height, syncer)

		return syncer, nil
	}
}

func (cmd *runCommand) newSyncerDeferred(
	height base.Height,
	syncer *isaacstates.Syncer,
) {
	l := log.With().Str("module", "new-syncer").Logger()

	if err := cmd.db.MergeAllPermanent(); err != nil {
		l.Error().Err(err).Msg("failed to merge temps")

		return
	}

	log.Debug().Msg("SuffrageProofs built")

	err := syncer.Start()
	if err != nil {
		l.Error().Err(err).Msg("syncer stopped")

		return
	}

	_ = syncer.Add(height)

	l.Debug().Interface("height", height).Msg("new syncer created")
}

func (cmd *runCommand) getProposalFunc() func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
	return func(ctx context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
		switch pr, found, err := cmd.pool.Proposal(facthash); {
		case err != nil:
			return nil, err
		case found:
			return pr, nil
		}

		// NOTE if not found, request to remote node
		worker := util.NewErrgroupWorker(ctx, int64(cmd.memberlist.MembersLen()))
		defer worker.Close()

		prl := util.EmptyLocked()

		go func() {
			defer worker.Done()

			cmd.memberlist.Remotes(func(node quicmemberlist.Node) bool {
				ci := node.UDPConnInfo()

				return worker.NewJob(func(ctx context.Context, _ uint64) error {
					pr, found, err := cmd.client.Proposal(ctx, ci, facthash)
					switch {
					case err != nil:
						return nil
					case !found:
						return nil
					}

					_, _ = prl.Set(func(i interface{}) (interface{}, error) {
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

		switch i, isnil := prl.Value(); {
		case isnil:
			if err != nil {
				return nil, err
			}

			return nil, storage.NotFoundError.Errorf("ProposalSignedFact not found")
		default:
			return i.(base.ProposalSignedFact), nil //nolint:forcetypeassert //...
		}
	}
}

func (cmd *runCommand) syncerLastBlockMapf() isaacstates.SyncerLastBlockMapFunc {
	f := func(
		ctx context.Context, manifest util.Hash, ci quicstream.UDPConnInfo,
	) (_ base.BlockMap, updated bool, _ error) {
		switch m, updated, err := cmd.client.LastBlockMap(ctx, ci, manifest); {
		case err != nil, !updated:
			return m, updated, err
		default:
			if err := m.IsValid(cmd.nodePolicy.NetworkID()); err != nil {
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
			cmd.syncSourcePool,
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
	}
}

func (cmd *runCommand) syncerBlockMapf() isaacstates.SyncerBlockMapFunc {
	f := func(ctx context.Context, height base.Height, ci quicstream.UDPConnInfo) (base.BlockMap, bool, error) {
		switch m, found, err := cmd.client.BlockMap(ctx, ci, height); {
		case err != nil, !found:
			return m, found, err
		default:
			if err := m.IsValid(cmd.nodePolicy.NetworkID()); err != nil {
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
					cmd.syncSourcePool,
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
							_ = result.SetValue([2]interface{}{a, b})

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
			cmd.syncSourcesRetryInterval,
		)

		return m, found, err
	}
}

func (cmd *runCommand) syncerBlockMapItemf() isaacstates.SyncerBlockMapItemFunc {
	// FIXME support remote item like https or ftp?
	f := func(
		ctx context.Context, height base.Height, item base.BlockMapItemType, ci quicstream.UDPConnInfo,
	) (io.ReadCloser, func() error, bool, error) {
		r, cancel, found, err := cmd.client.BlockMapItem(ctx, ci, height, item)

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
					cmd.syncSourcePool,
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
							_ = result.SetValue([3]interface{}{a, b, c})

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
			cmd.syncSourcesRetryInterval,
		)

		return reader, closef, found, err
	}
}

func (cmd *runCommand) setLastVoteproofsfFromBlockReader(
	lvps *isaacstates.LastVoteproofsHandler,
) func(isaac.BlockReader) error {
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

			if err := cmd.pool.SetLastVoteproofs(ivp, avp); err != nil {
				return err
			}

			_ = lvps.Set(ivp)
			_ = lvps.Set(avp)

			return nil
		}
	}
}

func (cmd *runCommand) broadcastBallotFunc(ballot base.Ballot) error {
	e := util.StringErrorFunc("failed to broadcast ballot")

	b, err := cmd.enc.Marshal(ballot)
	if err != nil {
		return e(err, "")
	}

	id := valuehash.NewSHA256(ballot.HashBytes()).String()

	if err := launch.BroadcastThruMemberlist(cmd.memberlist, id, b); err != nil {
		return e(err, "")
	}

	return nil
}

func (cmd *runCommand) updateSyncSources(called int64, ncis []isaac.NodeConnInfo, err error) {
	cmd.syncSourcePool.UpdateFixed(ncis)

	if err != nil {
		log.Error().Err(err).
			Interface("node_conninfo", ncis).
			Msg("failed to check sync sources")

		return
	}

	log.Debug().
		Int64("called", called).
		Interface("node_conninfo", ncis).
		Msg("sync sources updated")
}

func (cmd *runCommand) getLastSuffrageProofFunc() isaac.GetLastSuffrageProofFromRemoteFunc {
	lastl := util.EmptyLocked()

	f := func(ctx context.Context, ci quicstream.UDPConnInfo) (base.SuffrageProof, bool, error) {
		var last util.Hash

		if i, isnil := lastl.Value(); !isnil {
			last = i.(util.Hash) //nolint:forcetypeassert //...
		}

		proof, updated, err := cmd.client.LastSuffrageProof(ctx, ci, last)

		switch {
		case err != nil:
			return proof, updated, err
		case !updated:
			return proof, updated, nil
		default:
			if err := proof.IsValid(cmd.nodePolicy.NetworkID()); err != nil {
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
			cmd.syncSourcePool,
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

				_, err = ml.Set(func(v interface{}) (interface{}, error) {
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

		switch v, isnil := ml.Value(); {
		case isnil:
			return nil, false, nil
		default:
			return v.(base.SuffrageProof), true, nil //nolint:forcetypeassert //...
		}
	}
}

func (cmd *runCommand) getLastSuffrageCandidateFunc() isaac.GetLastSuffrageCandidateStateRemoteFunc {
	lastl := util.EmptyLocked()

	f := func(ctx context.Context, ci quicstream.UDPConnInfo) (base.State, bool, error) {
		var last util.Hash

		if i, isnil := lastl.Value(); !isnil {
			last = i.(util.Hash) //nolint:forcetypeassert //...
		}

		st, found, err := cmd.client.State(ctx, ci, isaac.SuffrageCandidateStateKey, last)

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
			cmd.syncSourcePool,
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

				_, err = ml.Set(func(v interface{}) (interface{}, error) {
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

		switch v, isnil := ml.Value(); {
		case isnil, v == nil:
			return nil, false, nil
		default:
			return v.(base.State), true, nil //nolint:forcetypeassert //...
		}
	}
}

func (cmd *runCommand) getSuffrageProofFunc() isaac.GetSuffrageProofFromRemoteFunc {
	return func(ctx context.Context, suffrageheight base.Height) (proof base.SuffrageProof, found bool, _ error) {
		err := util.Retry(
			ctx,
			func() (bool, error) {
				numnodes := 3 // NOTE choose top 3 sync nodes
				result := util.EmptyLocked()

				_ = isaac.ErrGroupWorkerWithSyncSourcePool(
					ctx,
					cmd.syncSourcePool,
					numnodes,
					uint64(numnodes),
					func(ctx context.Context, i, _ uint64, nci isaac.NodeConnInfo) error {
						ci, err := nci.UDPConnInfo()
						if err != nil {
							return err
						}

						switch a, b, err := cmd.client.SuffrageProof(ctx, ci, suffrageheight); {
						case err != nil:
							if quicstream.IsNetworkError(err) {
								return err
							}

							return nil
						case !b:
							return nil
						default:
							if err := a.IsValid(cmd.nodePolicy.NetworkID()); err != nil {
								return nil
							}

							_ = result.SetValue([2]interface{}{a, b})

							return errors.Errorf("stop")
						}
					},
				)

				v, isnil := result.Value()
				if isnil {
					return true, nil
				}

				i := v.([2]interface{}) //nolint:forcetypeassert //...

				proof, found = i[0].(base.SuffrageProof), i[1].(bool) //nolint:forcetypeassert //...

				return false, nil
			},
			-1,
			cmd.syncSourcesRetryInterval,
		)

		return proof, found, err
	}
}

func (cmd *runCommand) getProposalOperationFunc(
	proposal base.ProposalSignedFact,
) isaac.OperationProcessorGetOperationFunction {
	return func(ctx context.Context, operationhash util.Hash) (base.Operation, error) {
		var op base.Operation

		switch i, found, err := cmd.getProposalOperationFromPool(ctx, operationhash); {
		case err != nil:
			return nil, err
		case found:
			op = i
		}

		if op == nil {
			switch i, found, err := cmd.getProposalOperationFromRemote(ctx, proposal, operationhash); {
			case err != nil:
				return nil, err
			case !found:
				return nil, isaac.OperationNotFoundInProcessorError.Errorf("not found in remote")
			default:
				op = i
			}
		}

		if err := op.IsValid(cmd.nodePolicy.NetworkID()); err != nil {
			return nil, isaac.InvalidOperationInProcessorError.Wrap(err)
		}

		switch found, err := cmd.db.ExistsInStateOperation(op.Fact().Hash()); {
		case err != nil:
			return nil, err
		case found:
			return nil, isaac.OperationAlreadyProcessedInProcessorError.Errorf("already processed")
		default:
			return op, nil
		}
	}
}

func (cmd *runCommand) getProposalOperationFromPool(
	ctx context.Context, operationhash util.Hash,
) (base.Operation, bool, error) {
	op, found, err := cmd.pool.NewOperation(ctx, operationhash)

	switch {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	default:
		return op, true, nil
	}
}

func (cmd *runCommand) getProposalOperationFromRemote(
	ctx context.Context, proposal base.ProposalSignedFact, operationhash util.Hash,
) (base.Operation, bool, error) {
	if cmd.syncSourcePool.Len() < 1 {
		return nil, false, nil
	}

	switch isproposer, op, found, err := cmd.getProposalOperationFromRemoteProposer(ctx, proposal, operationhash); {
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

	worker := util.NewErrgroupWorker(ctx, int64(cmd.syncSourcePool.Len()))
	defer worker.Close()

	cmd.syncSourcePool.Actives(func(nci isaac.NodeConnInfo) bool {
		if proposer.Equal(nci.Address()) {
			return true
		}

		ci, err := nci.UDPConnInfo()
		if err != nil {
			return true
		}

		if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
			_, err := result.Set(func(i interface{}) (interface{}, error) {
				if i != nil {
					return nil, util.ErrLockedSetIgnore.Call()
				}

				op, found, err := cmd.client.Operation(ctx, ci, operationhash)
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

	i, isnil := result.Value()
	if isnil {
		return nil, false, err
	}

	return i.(base.Operation), true, nil //nolint:forcetypeassert //...
}

func (cmd *runCommand) getProposalOperationFromRemoteProposer(
	ctx context.Context, proposal base.ProposalSignedFact, operationhash util.Hash,
) (bool, base.Operation, bool, error) {
	proposer := proposal.ProposalFact().Proposer()

	var proposernci isaac.NodeConnInfo

	cmd.syncSourcePool.Actives(func(nci isaac.NodeConnInfo) bool {
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

	switch op, found, err := cmd.client.Operation(ctx, ci, operationhash); {
	case err != nil:
		return true, nil, false, err
	case !found:
		return true, nil, false, nil
	default:
		return true, op, true, nil
	}
}

func (cmd *runCommand) joinMemberlistForJoiningState() error {
	if len(cmd.discoveries) < 1 {
		return nil
	}

	if cmd.memberlist.IsJoined() {
		return nil
	}

	return cmd.memberlist.Join(cmd.discoveries)
}

func (cmd *runCommand) operationProcessorsMap() *hint.CompatibleSet {
	set := hint.NewCompatibleSet()

	_ = set.Add(isaacoperation.SuffrageCandidateHint, func(height base.Height) (base.OperationProcessor, error) {
		policy := cmd.db.LastNetworkPolicy()
		if policy == nil { // NOTE Usually it means empty block data
			return nil, nil
		}

		return isaacoperation.NewSuffrageCandidateProcessor(
			height,
			cmd.db.State,
			cmd.newSuffrageCandidateLimiterFunc,
			nil,
			policy.SuffrageCandidateLifespan(),
		)
	})

	_ = set.Add(isaacoperation.SuffrageJoinHint, func(height base.Height) (base.OperationProcessor, error) {
		policy := cmd.db.LastNetworkPolicy()
		if policy == nil { // NOTE Usually it means empty block data
			return nil, nil
		}

		return isaacoperation.NewSuffrageJoinProcessor(
			height,
			cmd.nodePolicy.Threshold(),
			cmd.db.State,
			nil,
			nil,
		)
	})

	return set
}

func (cmd *runCommand) newSuffrageCandidateLimiterFunc(
	height base.Height, getStateFunc base.GetStateFunc,
) (base.OperationProcessorProcessFunc, error) {
	e := util.StringErrorFunc("failed to get SuffrageCandidateLimiterFunc")

	limiter, err := cmd.loadSuffrageCandidateLimiter()
	if err != nil {
		return nil, e(err, "")
	}

	limit, err := limiter()
	if err != nil {
		return nil, e(err, "")
	}

	var existings uint64

	switch i, err := isaac.LastCandidatesFromState(height, getStateFunc); {
	case err != nil:
		return nil, e(err, "")
	default:
		existings = uint64(len(i))
	}

	if existings >= limit {
		return func(
			_ context.Context, op base.Operation, _ base.GetStateFunc,
		) (base.OperationProcessReasonError, error) {
			return base.NewBaseOperationProcessReasonError("reached limit, %d", limit), nil
		}, nil
	}

	var counted uint64

	return func(_ context.Context, op base.Operation, _ base.GetStateFunc) (base.OperationProcessReasonError, error) {
		if counted >= limit-existings {
			return base.NewBaseOperationProcessReasonError("reached limit, %d", limit), nil
		}

		counted++

		return nil, nil
	}, nil
}

func (cmd *runCommand) loadSuffrageCandidateLimiter() (base.SuffrageCandidateLimiter, error) {
	e := util.StringErrorFunc("failed to load SuffrageCandidateLimiter")

	policy := cmd.db.LastNetworkPolicy()

	if policy == nil {
		return nil, e(nil, "empty network policy")
	}

	rule := policy.SuffrageCandidateLimiterRule()

	if policy == nil {
		return nil, e(nil, "empty rule")
	}

	i := cmd.suffrageCandidateLimiterSet.Find(rule.Hint())
	if i == nil {
		return nil, e(nil, "unknown limiter rule, %q", rule.Hint())
	}

	f, ok := i.(base.SuffrageCandidateLimiterFunc)
	if !ok {
		return nil, errors.Errorf("expected SuffrageCandidateLimiterFunc, not %T", i)
	}

	return f(rule)
}

func (cmd *runCommand) whenNewBlockSaved(height base.Height) {
	l := log.With().Interface("height", height).Logger()
	l.Debug().Msg("new block saved")

	if cmd.Hold.IsSet() && height == cmd.Hold.Height() {
		l.Debug().Msg("will be stopped by hold")

		cmd.exitf(errHoldStop.Call())

		return
	}

	cmd.ballotbox.Count()
}
