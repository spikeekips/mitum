package main

import (
	"context"
	"io"
	"net"
	"os"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type runCommand struct {
	db                   isaac.Database
	enc                  encoder.Encoder
	Address              launch.AddressFlag `arg:"" name:"local address" help:"node address"`
	perm                 isaac.PermanentDatabase
	local                base.LocalNode
	client               isaac.NetworkClient
	suffrageStateBuilder *isaacstates.SuffrageStateBuilder
	proposalSelector     *isaac.BaseProposalSelector
	pool                 *isaacdatabase.TempPool
	getSuffrage          func(blockheight base.Height) (base.Suffrage, bool, error)
	encs                 *encoder.Encoders
	newProposalProcessor newProposalProcessorFunc
	getLastManifest      func() (base.Manifest, bool, error)
	getSuffrageBooting   func(blockheight base.Height) (base.Suffrage, bool, error)
	quicstreamserver     *quicstream.Server
	getProposal          func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error)
	getManifest          func(height base.Height) (base.Manifest, error)
	localfsroot          string
	Discovery            []launch.ConnInfoFlag `help:"discoveries" placeholder:"ConnInfo"`
	nodePolicy           isaac.NodePolicy
	Port                 int  `arg:"" name:"port" help:"network port"`
	Hold                 bool `help:"hold consensus states"`
}

func (cmd *runCommand) Run() error {
	log.Debug().
		Interface("address", cmd.Address).
		Interface("hold", cmd.Hold).
		Interface("discovery", cmd.Discovery).
		Msg("flags")

	switch encs, enc, err := launch.PrepareEncoders(); {
	case err != nil:
		return errors.Wrap(err, "")
	default:
		cmd.encs = encs
		cmd.enc = enc
	}

	local, err := prepareLocal(cmd.Address.Address())
	if err != nil {
		return errors.Wrap(err, "failed to prepare local node")
	}

	cmd.local = local

	switch localfsroot, err := defaultLocalFSRoot(cmd.local.Address()); {
	case err != nil:
		return errors.Wrap(err, "")
	default:
		cmd.localfsroot = localfsroot

		log.Debug().Str("localfs_root", cmd.localfsroot).Msg("localfs root")
	}

	if err := cmd.prepareDatabase(); err != nil {
		return errors.Wrap(err, "")
	}

	if err := cmd.prepareNetwork(); err != nil {
		return errors.Wrap(err, "")
	}

	cmd.nodePolicy = isaac.DefaultNodePolicy(networkID)
	log.Info().
		Interface("node_policy", cmd.nodePolicy).
		Msg("node policy loaded")

	// FIXME implement isaacstates.NewSuffrageStateBuilder(cmd.nodePolicy.NetworkID(), )

	cmd.getSuffrage = cmd.getSuffrageFunc()
	// FIXME cmd.getSuffrageBooting   func(blockheight base.Height) (base.Suffrage, bool, error) {
	//}
	cmd.getManifest = cmd.getManifestFunc()
	cmd.proposalSelector = cmd.proposalSelectorFunc()
	cmd.getLastManifest = cmd.getLastManifestFunc()
	cmd.newProposalProcessor = cmd.newProposalProcessorFunc(cmd.enc)
	cmd.getProposal = cmd.getProposalFunc()

	if err := cmd.run(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (cmd *runCommand) run() error {
	log.Debug().Msg("node started")

	if err := cmd.quicstreamserver.Start(); err != nil {
		return errors.Wrap(err, "")
	}

	if cmd.Hold {
		select {} //revive:disable-line:empty-block
	}

	cmd.prepareSuffrageBuilder()

	states, err := cmd.states()
	if err != nil {
		return errors.Wrap(err, "")
	}

	log.Debug().Msg("states started")

	if err := <-states.Wait(context.Background()); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (cmd *runCommand) prepareDatabase() error {
	e := util.StringErrorFunc("failed to prepare database")

	permuri := defaultPermanentDatabaseURI()

	nodeinfo, err := launch.CheckLocalFS(networkID, cmd.localfsroot, cmd.enc)

	switch {
	case err == nil:
		if err = isaacblock.CleanBlockTempDirectory(launch.LocalFSDataDirectory(cmd.localfsroot)); err != nil {
			return e(err, "")
		}
	case errors.Is(err, os.ErrNotExist):
		if err = launch.CleanStorage(
			permuri,
			cmd.localfsroot,
			cmd.encs, cmd.enc,
		); err != nil {
			return e(err, "")
		}

		nodeinfo, err = launch.CreateLocalFS(launch.CreateDefaultNodeInfo(networkID, version), cmd.localfsroot, cmd.enc)
		if err != nil {
			return e(err, "")
		}
	default:
		return e(err, "")
	}

	db, perm, pool, err := launch.LoadDatabase(nodeinfo, permuri, cmd.localfsroot, cmd.encs, cmd.enc)
	if err != nil {
		return e(err, "")
	}

	_ = db.SetLogging(logging)

	if err := db.Start(); err != nil {
		return e(err, "")
	}

	cmd.db = db
	cmd.perm = perm
	cmd.pool = pool

	return nil
}

func (cmd *runCommand) prepareNetwork() error {
	cmd.client = launch.NewNetworkClient(cmd.encs, cmd.enc)

	handlers := isaacnetwork.NewQuicstreamHandlers(
		cmd.local,
		cmd.encs,
		cmd.enc,
		cmd.pool,
		cmd.proposalMaker(),
		func(last util.Hash) (base.SuffrageProof, bool, error) {
			switch proof, found, err := cmd.db.LastSuffrageProof(); {
			case err != nil:
				return nil, false, errors.Wrap(err, "")
			case !found:
				return nil, false, storage.NotFoundError.Errorf("last SuffrageProof not found")
			case last != nil && last.Equal(proof.Map().Manifest().Suffrage()):
				return nil, false, nil
			default:
				return proof, true, nil
			}
		},
		cmd.db.SuffrageProof,
		func(last util.Hash) (base.BlockMap, bool, error) {
			switch m, found, err := cmd.db.LastBlockMap(); {
			case err != nil:
				return nil, false, errors.Wrap(err, "")
			case !found:
				return nil, false, storage.NotFoundError.Errorf("last BlockMap not found")
			case last != nil && last.Equal(m.Manifest().Hash()):
				return nil, false, nil
			default:
				return m, true, nil
			}
		},
		cmd.db.BlockMap,
		func(height base.Height, item base.BlockMapItemType) (io.ReadCloser, bool, error) {
			e := util.StringErrorFunc("failed to get BlockMapItem")

			var enc encoder.Encoder

			switch m, found, err := cmd.db.BlockMap(height); {
			case err != nil:
				return nil, false, e(err, "")
			case !found:
				return nil, false, e(storage.NotFoundError.Errorf("BlockMap not found"), "")
			default:
				enc = cmd.encs.Find(m.Encoder())
			}

			// FIXME use cache

			reader, err := isaacblock.NewLocalFSReaderFromHeight(
				launch.LocalFSDataDirectory(cmd.localfsroot), height, enc,
			)
			if err != nil {
				return nil, false, e(err, "")
			}
			defer func() {
				_ = reader.Close()
			}()

			return reader.Reader(item)
		},
	)

	cmd.quicstreamserver = quicstream.NewServer(
		&net.UDPAddr{Port: cmd.Port},
		launch.GenerateNewTLSConfig(),
		launch.DefaultQuicConfig(),
		launch.Handlers(handlers),
	)
	_ = cmd.quicstreamserver.SetLogging(logging)

	return nil
}

func (cmd *runCommand) getSuffrageFunc() func(blockheight base.Height) (base.Suffrage, bool, error) {
	return func(blockheight base.Height) (base.Suffrage, bool, error) {
		proof, found, err := cmd.db.SuffrageProofByBlockHeight(blockheight.Prev())

		switch {
		case err != nil:
			return nil, false, errors.Wrap(err, "")
		case !found:
			return nil, false, nil
		default:
			suf, err := proof.Suffrage()
			if err != nil {
				return nil, true, errors.Wrap(err, "")
			}

			return suf, true, nil
		}
	}
}

func (cmd *runCommand) getManifestFunc() func(height base.Height) (base.Manifest, error) {
	return func(height base.Height) (base.Manifest, error) {
		switch m, found, err := cmd.db.BlockMap(height); {
		case err != nil:
			return nil, errors.Wrap(err, "")
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
						return false, errors.Wrap(err, "")
					case !found:
						return false, nil
					}

					return true, nil
				},
			)
			if err != nil {
				return nil, errors.Wrap(err, "")
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
					return nil, errors.Wrap(err, "")
				case m == nil:
					return nil, nil
				default:
					return m.Hash(), nil
				}
			},
		),
		cmd.proposalMaker(),
		cmd.getSuffrage,
		func() []base.Address { return nil },
		func(context.Context, base.Point, base.Address) (
			base.ProposalSignedFact, error,
		) {
			// FIXME set request
			return nil, nil
		},
		cmd.pool,
	)
}

func (cmd *runCommand) getLastManifestFunc() func() (base.Manifest, bool, error) {
	return func() (base.Manifest, bool, error) {
		switch m, found, err := cmd.db.LastBlockMap(); {
		case err != nil || !found:
			return nil, found, errors.Wrap(err, "")
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
			launch.NewBlockWriterFunc(cmd.local, networkID, launch.LocalFSDataDirectory(cmd.localfsroot), enc, cmd.db),
			cmd.db.State,
			nil,
			nil,
			cmd.pool.SetLastVoteproofs,
		)
	}
}

func (cmd *runCommand) states() (*isaacstates.States, error) {
	box := isaacstates.NewBallotbox(cmd.getSuffrage, cmd.nodePolicy.Threshold())
	voteFunc := func(bl base.Ballot) (bool, error) {
		voted, err := box.Vote(bl)
		if err != nil {
			return false, errors.Wrap(err, "")
		}

		return voted, nil
	}

	pps := isaac.NewProposalProcessors(cmd.newProposalProcessor, cmd.getProposal)
	_ = pps.SetLogging(logging)

	states := isaacstates.NewStates(box)
	_ = states.SetLogging(logging)

	whenNewBlockSaved := func(height base.Height) {
		box.Count()
	}

	states.
		SetHandler(isaacstates.NewBrokenHandler(cmd.local, cmd.nodePolicy)).
		SetHandler(isaacstates.NewStoppedHandler(cmd.local, cmd.nodePolicy)).
		SetHandler(isaacstates.NewBootingHandler(cmd.local, cmd.nodePolicy, cmd.getLastManifest, cmd.getSuffrage)).
		SetHandler(
			isaacstates.NewJoiningHandler(
				cmd.local, cmd.nodePolicy, cmd.proposalSelector, cmd.getLastManifest, cmd.getSuffrage, voteFunc,
			),
		).
		SetHandler(
			isaacstates.NewConsensusHandler(
				cmd.local, cmd.nodePolicy, cmd.proposalSelector,
				cmd.getManifest, cmd.getSuffrage, voteFunc, whenNewBlockSaved,
				pps,
			)).
		SetHandler(isaacstates.NewSyncingHandler(cmd.local, cmd.nodePolicy, cmd.proposalSelector, cmd.newSyncer))

	// NOTE load last init, accept voteproof and last majority voteproof
	switch ivp, avp, found, err := cmd.pool.LastVoteproofs(); {
	case err != nil:
		return nil, errors.Wrap(err, "")
	case !found:
	default:
		_ = states.LastVoteproofsHandler().Set(ivp)
		_ = states.LastVoteproofsHandler().Set(avp)
	}

	return states, nil
}

func (cmd *runCommand) newSyncer(height base.Height) (isaac.Syncer, error) {
	e := util.StringErrorFunc("failed newSyncer")

	// NOTE if no discoveries, moves to broken state
	if len(cmd.Discovery) < 1 {
		return nil, e(isaacstates.ErrUnpromising.Errorf("syncer needs one or more discoveries"), "")
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

	switch i, err := launch.NewTempSyncPoolDatabase(cmd.localfsroot, height, cmd.encs, cmd.enc); {
	case err != nil:
		return nil, e(isaacstates.ErrUnpromising.Wrap(err), "")
	default:
		tempsyncpool = i
	}

	syncer, err := isaacstates.NewSyncer(
		cmd.localfsroot,
		func(root string, blockmap base.BlockMap) (isaac.BlockImporter, error) {
			bwdb, err := cmd.db.NewBlockWriteDatabase(blockmap.Manifest().Height())
			if err != nil {
				return nil, errors.Wrap(err, "")
			}

			return isaacblock.NewBlockImporter(
				root,
				cmd.encs,
				blockmap,
				bwdb,
				cmd.perm,
				networkID,
			)
		},
		prev,
		cmd.syncerLastBlockMapf(),
		cmd.syncerBlockMapf(),
		cmd.syncerBlockMapItemf(),
		tempsyncpool,
		cmd.setLastVoteproofsf(),
	)
	if err != nil {
		return nil, e(err, "")
	}

	go func() {
		var lastsuffragestate base.State
		if lastsuffrageproof != nil {
			lastsuffragestate = lastsuffrageproof.State()
		}

		if _, err := cmd.suffrageStateBuilder.Build(context.Background(), lastsuffragestate); err != nil {
			log.Error().Err(err).Msg("suffrage state builder failed")

			return
		}

		log.Debug().Msg("SuffrageProofs built")

		err := syncer.Start()
		if err != nil {
			log.Error().Err(err).Msg("syncer stopped")

			return
		}

		_ = syncer.Add(height)
	}()

	return syncer, nil
}

func (cmd *runCommand) getProposalFunc() func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
	return func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
		switch pr, found, err := cmd.pool.Proposal(facthash); {
		case err != nil:
			return nil, errors.Wrap(err, "")
		case !found:
			// FIXME if not found, request to remote node
			return nil, nil
		default:
			return pr, nil
		}
	}
}

func (cmd *runCommand) prepareSuffrageBuilder() {
	var last util.Hash

	cmd.suffrageStateBuilder = isaacstates.NewSuffrageStateBuilder(
		networkID,
		func(ctx context.Context) (base.SuffrageProof, bool, error) {
			discovery := cmd.Discovery[0]

			ci, err := quictransport.ToQuicConnInfo(discovery.ConnInfo())
			if err != nil {
				return nil, false, errors.Wrap(err, "")
			}

			proof, updated, err := cmd.client.LastSuffrageProof(ctx, ci, last)
			switch {
			case err != nil:
				return proof, updated, errors.Wrap(err, "")
			case !updated:
				return proof, updated, nil
			default:
				if err := proof.IsValid(networkID); err != nil {
					return nil, updated, errors.Wrap(err, "")
				}

				last = proof.Map().Manifest().Suffrage()

				return proof, updated, nil
			}
		},
		func(ctx context.Context, suffrageheight base.Height) (base.SuffrageProof, bool, error) {
			discovery := cmd.Discovery[0]

			ci, err := quictransport.ToQuicConnInfo(discovery.ConnInfo())
			if err != nil {
				return nil, false, errors.Wrap(err, "")
			}

			proof, found, err := cmd.client.SuffrageProof(ctx, ci, suffrageheight)

			return proof, found, errors.Wrap(err, "")
		},
	)
}

func (cmd *runCommand) syncerLastBlockMapf() isaacstates.SyncerLastBlockMapFunc {
	return func(ctx context.Context, manifest util.Hash) (_ base.BlockMap, updated bool, _ error) {
		discovery := cmd.Discovery[0]

		ci, err := quictransport.ToQuicConnInfo(discovery.ConnInfo())
		if err != nil {
			return nil, false, errors.Wrap(err, "")
		}

		switch m, updated, err := cmd.client.LastBlockMap(ctx, ci, manifest); {
		case err != nil, !updated:
			return m, updated, errors.Wrap(err, "")
		default:
			if err := m.IsValid(networkID); err != nil {
				return m, updated, errors.Wrap(err, "")
			}

			return m, updated, nil
		}
	}
}

func (cmd *runCommand) syncerBlockMapf() isaacstates.SyncerBlockMapFunc {
	return func(ctx context.Context, height base.Height) (base.BlockMap, bool, error) {
		// FIXME use multiple discoveries
		discovery := cmd.Discovery[0]

		ci, err := quictransport.ToQuicConnInfo(discovery.ConnInfo())
		if err != nil {
			return nil, false, errors.Wrap(err, "")
		}

		switch m, found, err := cmd.client.BlockMap(ctx, ci, height); {
		case err != nil, !found:
			return m, found, errors.Wrap(err, "")
		default:
			if err := m.IsValid(networkID); err != nil {
				return m, found, errors.Wrap(err, "")
			}

			return m, found, nil
		}
	}
}

func (cmd *runCommand) syncerBlockMapItemf() isaacstates.SyncerBlockMapItemFunc {
	return func(ctx context.Context, height base.Height, item base.BlockMapItemType) (io.ReadCloser, bool, error) {
		discovery := cmd.Discovery[0]

		ci, err := quictransport.ToQuicConnInfo(discovery.ConnInfo())
		if err != nil {
			return nil, false, errors.Wrap(err, "")
		}

		r, found, err := cmd.client.BlockMapItem(ctx, ci, height, item)

		return r, found, errors.Wrap(err, "")
	}
}

func (cmd *runCommand) setLastVoteproofsf() func(isaac.BlockReader) error {
	return func(reader isaac.BlockReader) error {
		switch v, found, err := reader.Item(base.BlockMapItemTypeVoteproofs); {
		case err != nil:
			return errors.Wrap(err, "")
		case !found:
			return errors.Errorf("voteproofs not found at last")
		default:
			vps := v.([]base.Voteproof)           //nolint:forcetypeassert //...
			if err := cmd.pool.SetLastVoteproofs( //nolint:forcetypeassert //...
				vps[0].(base.INITVoteproof),
				vps[1].(base.ACCEPTVoteproof),
			); err != nil {
				return errors.Wrap(err, "")
			}

			return nil
		}
	}
}
