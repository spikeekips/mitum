package main

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
)

func (cmd *runCommand) getSuffrageFunc() func(blockheight base.Height) (base.Suffrage, bool, error) {
	return func(blockheight base.Height) (base.Suffrage, bool, error) {
		return isaac.GetSuffrageFromDatabase(cmd.db, blockheight)
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
	return isaac.NewProposalMaker(
		cmd.local,
		cmd.nodePolicy,
		func(ctx context.Context) ([]util.Hash, error) {
			policy := cmd.db.LastNetworkPolicy()
			n := policy.MaxOperationsInProposal()
			if n < 1 {
				return nil, nil
			}

			hs, err := cmd.pool.NewOperationHashes(
				ctx,
				n,
				func(facthash util.Hash) (bool, error) {
					// FIXME if bad operation and it is failed to be processed;
					// it can be included in next proposal; it should be
					// excluded.
					// FIXME if operation has not enough fact signs, it will
					// ignored. It must be filtered for not this kind of
					// operations.
					switch found, err := cmd.db.ExistsInStateOperation(facthash); {
					case err != nil:
						return false, err
					case !found:
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

		// 	for i := range nodes {
		// 		n := nodes[i]
		// 		if n.Address().String() == "no0sas" {
		// 			return n, nil
		// 		}
		// 	}

		// 	return nil, errors.Errorf("no0sas not found")
		// }),
		cmd.proposalMaker(),
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

			switch {
			case cmd.memberlist == nil:
				return nil, false, errors.Errorf("nil memberlist")
			case !cmd.memberlist.IsJoined():
				return nil, false, errors.Errorf("memberlist; not yet joined")
			}

			var members []base.Node
			cmd.memberlist.Members(func(node quicmemberlist.Node) bool {
				if !suf.Exists(node.Address()) {
					return true
				}

				members = append(members, isaac.NewNode(node.Publickey(), node.Address()))

				return true
			})

			if len(members) < 1 {
				return nil, false, nil
			}

			return members, true, nil
		},
		func(ctx context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			// FIXME set request
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
	return func(proposal base.ProposalSignedFact, previous base.Manifest) (
		isaac.ProposalProcessor, error,
	) {
		return isaac.NewDefaultProposalProcessor(
			proposal,
			previous,
			launch.NewBlockWriterFunc(
				cmd.local, networkID, launch.LocalFSDataDirectory(cmd.design.Storage.Base), enc, cmd.db),
			cmd.db.State,
			nil,
			nil,
			cmd.pool.SetLastVoteproofs,
		)
	}
}

func (cmd *runCommand) newSyncer(
	lvps *isaacstates.LastVoteproofsHandler,
) func(height base.Height) (isaac.Syncer, error) {
	return func(height base.Height) (isaac.Syncer, error) {
		e := util.StringErrorFunc("failed newSyncer")

		// NOTE if no discoveries, moves to broken state
		if len(cmd.SyncNode) < 1 {
			return nil, e(isaacstates.ErrUnpromising.Errorf("syncer needs one or more SyncNode"), "")
		}

		var lastsuffrageproof base.SuffrageProof

		switch proof, found, err := cmd.db.LastSuffrageProof(); {
		case err != nil:
			return nil, e(err, "")
		case found:
			lastsuffrageproof = proof
		}

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
					root,
					cmd.encs,
					blockmap,
					bwdb,
					networkID,
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

		go cmd.newSyncerDeferred(height, syncer, lastsuffrageproof)

		return syncer, nil
	}
}

func (cmd *runCommand) newSyncerDeferred(
	height base.Height,
	syncer *isaacstates.Syncer,
	lastsuffrageproof base.SuffrageProof,
) {
	l := log.With().Str("module", "new-syncer").Logger()

	if err := cmd.db.MergeAllPermanent(); err != nil {
		l.Error().Err(err).Msg("failed to merge temps")

		return
	}

	var lastsuffragestate base.State
	if lastsuffrageproof != nil {
		lastsuffragestate = lastsuffrageproof.State()
	}

	if _, err := cmd.suffrageStateBuilder.Build(context.Background(), lastsuffragestate); err != nil {
		l.Error().Err(err).Msg("suffrage state builder failed")

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
	return func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
		switch pr, found, err := cmd.pool.Proposal(facthash); {
		case err != nil:
			return nil, err
		case !found:
			// FIXME if not found, request to remote node
			return nil, nil
		default:
			return pr, nil
		}
	}
}

func (cmd *runCommand) syncerLastBlockMapf() isaacstates.SyncerLastBlockMapFunc {
	return func(ctx context.Context, manifest util.Hash) (_ base.BlockMap, updated bool, _ error) {
		// FIXME sync nodes pool; if failed, selects next node
		node := cmd.SyncNode[0]

		ci, err := node.ConnInfo()
		if err != nil {
			return nil, false, err
		}

		switch m, updated, err := cmd.client.LastBlockMap(ctx, ci, manifest); {
		case err != nil, !updated:
			return m, updated, err
		default:
			if err := m.IsValid(networkID); err != nil {
				return m, updated, err
			}

			return m, updated, nil
		}
	}
}

func (cmd *runCommand) syncerBlockMapf() isaacstates.SyncerBlockMapFunc {
	return func(ctx context.Context, height base.Height) (base.BlockMap, bool, error) {
		// FIXME use multiple discoveries
		sn := cmd.SyncNode[0]

		ci, err := sn.ConnInfo()
		if err != nil {
			return nil, false, err
		}

		switch m, found, err := cmd.client.BlockMap(ctx, ci, height); {
		case err != nil, !found:
			return m, found, err
		default:
			if err := m.IsValid(networkID); err != nil {
				return m, found, err
			}

			return m, found, nil
		}
	}
}

func (cmd *runCommand) syncerBlockMapItemf() isaacstates.SyncerBlockMapItemFunc {
	// FIXME support remote item
	return func(
		ctx context.Context, height base.Height, item base.BlockMapItemType,
	) (io.ReadCloser, func() error, bool, error) {
		sn := cmd.SyncNode[0]

		ci, err := sn.ConnInfo()
		if err != nil {
			return nil, nil, false, err
		}

		r, cancel, found, err := cmd.client.BlockMapItem(ctx, ci, height, item)

		return r, cancel, found, err
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
	if err != nil {
		log.Error().Err(err).
			Interface("node_conn_info", ncis).
			Msg("failed to check sync sources")

		return
	}

	_ = cmd.syncSources.SetValue(ncis)

	log.Debug().
		Int64("called", called).
		Interface("node_conn_info", ncis).
		Msg("sync sources updated")
}

func (cmd *runCommand) latestSyncSources() []isaac.NodeConnInfo {
	i, isnil := cmd.syncSources.Value()
	if isnil {
		return nil
	}

	return i.([]isaac.NodeConnInfo) //nolint:forcetypeassert //...
}
