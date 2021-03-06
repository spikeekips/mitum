package process

import (
	"context"
	"sort"
	"time"

	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/ballot"
	"github.com/spikeekips/mitum/base/block"
	"github.com/spikeekips/mitum/base/seal"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/launch/config"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/states"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/cache"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
)

const HookNameSetNetworkHandlers = "set_network_handlers"

func HookSetNetworkHandlers(ctx context.Context) (context.Context, error) {
	if sn, err := SettingNetworkHandlersFromContext(ctx); err != nil {
		return ctx, err
	} else if err := sn.Set(); err != nil {
		return ctx, err
	} else {
		return ctx, nil
	}
}

type SettingNetworkHandlers struct {
	version   util.Version
	ctx       context.Context
	conf      config.LocalNode
	storage   storage.Storage
	blockfs   *storage.BlockFS
	policy    *isaac.LocalPolicy
	nodepool  *network.Nodepool
	suffrage  base.Suffrage
	states    states.States
	network   network.Server
	sealCache cache.Cache
	logger    logging.Logger
}

func SettingNetworkHandlersFromContext(ctx context.Context) (*SettingNetworkHandlers, error) { // nolint:funlen
	var version util.Version
	if err := LoadVersionContextValue(ctx, &version); err != nil {
		return nil, err
	}

	var conf config.LocalNode
	if err := config.LoadConfigContextValue(ctx, &conf); err != nil {
		return nil, err
	}

	var policy *isaac.LocalPolicy
	if err := LoadPolicyContextValue(ctx, &policy); err != nil {
		return nil, err
	}

	var nodepool *network.Nodepool
	if err := LoadNodepoolContextValue(ctx, &nodepool); err != nil {
		return nil, err
	}

	var st storage.Storage
	if err := LoadStorageContextValue(ctx, &st); err != nil {
		return nil, err
	}

	var blockfs *storage.BlockFS
	if err := LoadBlockFSContextValue(ctx, &blockfs); err != nil {
		return nil, err
	}

	var suffrage base.Suffrage
	if err := LoadSuffrageContextValue(ctx, &suffrage); err != nil {
		return nil, err
	}

	var consensusStates states.States
	if err := LoadConsensusStatesContextValue(ctx, &consensusStates); err != nil {
		return nil, err
	}

	var sealCache cache.Cache
	if ca, err := cache.NewGCache("lru", 100*100, time.Minute*3); err != nil {
		return nil, err
	} else {
		sealCache = ca
	}

	var nt network.Server
	var logger logging.Logger = logging.NilLogger
	if err := LoadNetworkContextValue(ctx, &nt); err != nil {
		return nil, err
	} else if l, ok := nt.(logging.HasLogger); ok {
		logger = l.Log()
	}

	return &SettingNetworkHandlers{
		ctx:       ctx,
		version:   version,
		conf:      conf,
		storage:   st,
		blockfs:   blockfs,
		policy:    policy,
		nodepool:  nodepool,
		suffrage:  suffrage,
		states:    consensusStates,
		network:   nt,
		sealCache: sealCache,
		logger:    logger,
	}, nil
}

func (sn *SettingNetworkHandlers) Set() error {
	sn.network.SetHasSealHandler(sn.networkHandlerHasSeal())
	sn.network.SetGetSealsHandler(sn.networkHandlerGetSeals())
	sn.network.SetNewSealHandler(sn.networkhandlerNewSeal())
	sn.network.SetGetManifestsHandler(sn.networkhandlerGetManifests())
	sn.network.SetGetBlocksHandler(sn.networkHandlerGetBlocks())
	sn.network.SetNodeInfoHandler(sn.networkHandlerNodeInfo())

	lc := sn.nodepool.Local().Channel().(*network.DummyChannel)
	lc.SetNewSealHandler(sn.networkhandlerNewSeal())
	lc.SetGetSealsHandler(sn.networkHandlerGetSeals())
	lc.SetGetManifestsHandler(sn.networkhandlerGetManifests())
	lc.SetGetBlocksHandler(sn.networkHandlerGetBlocks())
	lc.SetNodeInfoHandler(sn.networkHandlerNodeInfo())

	sn.logger.Debug().Msg("local channel handlers binded")

	return nil
}

func (sn *SettingNetworkHandlers) networkHandlerHasSeal() network.HasSealHandler {
	return func(h valuehash.Hash) (bool, error) {
		return sn.storage.HasSeal(h)
	}
}

func (sn *SettingNetworkHandlers) networkHandlerGetSeals() network.GetSealsHandler {
	return func(hs []valuehash.Hash) ([]seal.Seal, error) {
		var sls []seal.Seal

		if err := sn.storage.SealsByHash(hs, func(_ valuehash.Hash, sl seal.Seal) (bool, error) {
			sls = append(sls, sl)

			return true, nil
		}, true); err != nil {
			return nil, err
		}

		return sls, nil
	}
}

func (sn *SettingNetworkHandlers) networkhandlerNewSeal() network.NewSealHandler {
	return func(sl seal.Seal) error {
		sealChecker := isaac.NewSealChecker(
			sl,
			sn.storage,
			sn.policy,
			sn.sealCache,
		)
		if err := util.NewChecker("network-new-seal-checker", []util.CheckerFunc{
			sealChecker.IsKnown,
			sealChecker.IsValid,
			sealChecker.IsValidOperationSeal,
		}).Check(); err != nil {
			if xerrors.Is(err, util.IgnoreError) {
				return nil
			}

			sn.logger.Error().Err(err).Msg("seal checking failed")

			return err
		}

		if t, ok := sl.(ballot.Ballot); ok {
			checker := isaac.NewBallotChecker(
				t,
				sn.storage,
				sn.policy,
				sn.suffrage,
				sn.nodepool,
				sn.states.LastVoteproof(),
			)
			if err := util.NewChecker("network-new-ballot-checker", []util.CheckerFunc{
				checker.InSuffrage,
				checker.CheckSigning,
				checker.CheckWithLastVoteproof,
				checker.CheckProposalInACCEPTBallot,
				checker.CheckVoteproof,
			}).Check(); err != nil {
				return err
			}
		}

		go func() {
			_ = sn.states.NewSeal(sl)
		}()

		return nil
	}
}

func (sn *SettingNetworkHandlers) networkhandlerGetManifests() network.GetManifestsHandler {
	return func(heights []base.Height) ([]block.Manifest, error) {
		sort.Slice(heights, func(i, j int) bool {
			return heights[i] < heights[j]
		})

		var manifests []block.Manifest
		fetched := map[base.Height]struct{}{}
		for _, h := range heights {
			if _, found := fetched[h]; found {
				continue
			}

			fetched[h] = struct{}{}

			switch m, found, err := sn.storage.ManifestByHeight(h); {
			case !found:
				continue
			case err != nil:
				return nil, err
			default:
				manifests = append(manifests, m)
			}
		}

		return manifests, nil
	}
}

func (sn *SettingNetworkHandlers) networkHandlerGetBlocks() network.GetBlocksHandler {
	return func(heights []base.Height) ([]block.Block, error) {
		sort.Slice(heights, func(i, j int) bool {
			return heights[i] < heights[j]
		})

		var blocks []block.Block
		for _, h := range heights {
			if blk, err := sn.blockfs.Load(h); err != nil {
				if xerrors.Is(err, storage.NotFoundError) {
					break
				}

				return nil, err
			} else {
				blocks = append(blocks, blk)
			}
		}

		return blocks, nil
	}
}

func (sn *SettingNetworkHandlers) networkHandlerNodeInfo() network.NodeInfoHandler {
	return func() (network.NodeInfo, error) {
		var manifest block.Manifest
		if m, found, err := sn.storage.LastManifest(); err != nil {
			return nil, err
		} else if found {
			manifest = m
		}

		suffrageNodes := sn.suffrage.Nodes()
		nodes := make([]base.Node, len(suffrageNodes))
		for i := range suffrageNodes {
			if n, found := sn.nodepool.Node(suffrageNodes[i]); !found {
				return nil, xerrors.Errorf("suffrage node, %q not found", n.Address())
			} else {
				nodes[i] = n
			}
		}

		return network.NewNodeInfoV0(
			sn.nodepool.Local(),
			sn.policy.NetworkID(),
			sn.states.State(),
			manifest,
			sn.version,
			sn.conf.Network().URL().String(),
			sn.policy.Config(),
			nodes,
			sn.suffrage,
		), nil
	}
}
