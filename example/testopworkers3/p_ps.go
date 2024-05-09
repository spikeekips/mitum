package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/ps"
)

var hinters = []encoder.DecodeDetail{
	{Hint: OpFactHint, Instance: OpFact{}},
	{Hint: OpHint, Instance: Op{}},
	{Hint: AccountStateValueHint, Instance: AccountStateValue{}},
	{Hint: AccountStateMergeValueHint, Instance: AccountStateMergeValue{}},
	{Hint: WorkerCancelHeaderHint, Instance: WorkerCancelHeader{}},
	{Hint: WorkerDoneHeaderHint, Instance: WorkerDoneHeader{}},
	{Hint: WorkerReadyHeaderHint, Instance: WorkerReadyHeader{}},
	{Hint: WorkerNewOperationsHeaderHint, Instance: WorkerNewOperationsHeader{}},
	{Hint: WorkerInsertValuesHeaderHint, Instance: WorkerInsertValuesHeader{}},
	{Hint: WorkerPhaseDoneHeaderHint, Instance: WorkerPhaseDoneHeader{}},
	{Hint: ManagerStateOwnersHeaderHint, Instance: ManagerStateOwnersHeader{}},
	{Hint: ManagerWorkerCanceledHeaderHint, Instance: ManagerWorkerCanceledHeader{}},
	{Hint: ManagerWorkerUpdateMergedHeaderHint, Instance: ManagerWorkerUpdateMergedHeader{}},
	{Hint: ManagerWorkerStoreDoneHeaderHint, Instance: ManagerWorkerStoreDoneHeader{}},
	{Hint: WorkerSetStatesHeaderHint, Instance: WorkerSetStatesHeader{}},
	{Hint: WorkerSetOperationsHeaderHint, Instance: WorkerSetOperationsHeader{}},
	{Hint: WorkerSetStateMergeValuesHeaderHint, Instance: WorkerSetStateMergeValuesHeader{}},
	{Hint: WorkerSyncStatesHeaderHint, Instance: WorkerSyncStatesHeader{}},
}

func preparePS(
	pctx context.Context,
	name string,
	designString string,
	nops uint64,
) (
	*ps.PS,
	context.Context,
) {
	pps := ps.NewPS(name)

	_ = pps.
		AddOK(ps.NameINIT, launch.PINIT, nil)

	_ = pps.POK(ps.NameINIT).
		PostAddOK(launch.PNameLogging, launch.PLogging).
		PostAddOK(launch.PNameLoggingWithCLI, launch.PLoggingWithCLI)

	_ = pps.
		AddOK(launch.PNameEncoder, launch.PEncoder, nil).
		AddOK(launch.PNameDesign, func(pctx context.Context) (context.Context, error) {
			var encs *encoder.Encoders

			if err := util.LoadFromContextOK(pctx,
				launch.EncodersContextKey, &encs,
			); err != nil {
				return pctx, err
			}

			var design launch.NodeDesign

			if err := design.DecodeYAML([]byte(designString), encs.JSON()); err != nil {
				return pctx, err
			}

			return util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
				launch.DesignContextKey:       design,
				launch.DesignStringContextKey: designString,
			}), nil
		}, nil, launch.PNameEncoder).
		AddOK(launch.PNameLocal, launch.PLocal, nil, launch.PNameDesign).
		AddOK(launch.PNameBlockItemReaders, launch.PBlockItemReaders, nil, launch.PNameDesign).
		AddOK(launch.PNameStorage, launch.PStorage, nil, launch.PNameLocal).
		AddOK(launch.PNameProposalMaker, launch.PProposalMaker, nil, launch.PNameStorage).
		AddOK(launch.PNameNetwork, launch.PNetwork, nil, launch.PNameStorage).
		AddOK(launch.PNameStates, func(pctx context.Context) (context.Context, error) { return pctx, nil }, nil, launch.PNameNetwork).
		AddOK(launch.PNameStartNetwork, launch.PStartNetwork, launch.PCloseNetwork, launch.PNameStates).
		AddOK(launch.PNameStartStorage, launch.PStartStorage, launch.PCloseStorage, launch.PNameStartNetwork).
		AddOK(launch.PNameStatesReady, nil, launch.PCloseStates,
			launch.PNameStartStorage,
			launch.PNameStartNetwork,
			launch.PNameStates,
		)

	_ = pps.POK(launch.PNameEncoder).
		PostAddOK(launch.PNameAddHinters, launch.PAddHinters).
		PostAddOK(ps.Name("test-add-hinter"), func(pctx context.Context) (context.Context, error) {
			var encs *encoder.Encoders
			if err := util.LoadFromContextOK(pctx, launch.EncodersContextKey, &encs); err != nil {
				return pctx, err
			}

			for i := range hinters {
				if err := encs.AddDetail(hinters[i]); err != nil {
					return pctx, errors.Wrap(err, "add hinter to encoders")
				}
			}

			return pctx, nil
		})

	_ = pps.POK(launch.PNameDesign).
		PostAddOK(launch.PNameCheckDesign, launch.PCheckDesign).
		PostAddOK(ps.Name("update design"), func(pctx context.Context) (context.Context, error) {
			var isaacparams *isaac.Params

			if err := util.LoadFromContextOK(pctx,
				launch.ISAACParamsContextKey, &isaacparams,
			); err != nil {
				return pctx, err
			}

			_ = isaacparams.SetStateCacheSize(int(nops) + 33)         //nolint:gomnd //...
			_ = isaacparams.SetOperationPoolCacheSize(int(nops) + 33) //nolint:gomnd //...

			return pctx, nil
		}).
		PostAddOK(launch.PNameINITObjectCache, launch.PINITObjectCache)

	_ = pps.POK(launch.PNameBlockItemReaders).
		PreAddOK(launch.PNameBlockItemReadersDecompressFunc, launch.PBlockItemReadersDecompressFunc).
		PostAddOK(launch.PNameRemotesBlockItemReaderFunc, launch.PRemotesBlockItemReaderFunc)

	_ = pps.POK(launch.PNameStorage).
		PreAddOK(launch.PNameCleanStorage, launch.PCleanStorage).
		PreAddOK(launch.PNameCreateLocalFS, launch.PCreateLocalFS).
		PreAddOK(launch.PNameLoadDatabase, launch.PLoadDatabase).
		PostAddOK(launch.PNameCheckLeveldbStorage, launch.PCheckLeveldbStorage).
		PostAddOK(launch.PNameLoadFromDatabase, launch.PLoadFromDatabase).
		PostAddOK(launch.PNameCheckBlocksOfStorage, launch.PCheckBlocksOfStorage).
		PostAddOK(launch.PNameSuffrageCandidateLimiterSet, launch.PSuffrageCandidateLimiterSet).
		PostAddOK(launch.PNamePatchBlockItemReaders, launch.PPatchBlockItemReaders).
		PostAddOK(launch.PNameNodeInfo, launch.PNodeInfo)

	_ = pps.POK(launch.PNameStates).
		PostAddOK(launch.PNameSyncSourceChecker, func(pctx context.Context) (context.Context, error) {
			var local base.LocalNode
			var isaacparams *isaac.Params

			if err := util.LoadFromContextOK(pctx,
				launch.LocalContextKey, &local,
				launch.ISAACParamsContextKey, &isaacparams,
			); err != nil {
				return pctx, err
			}

			syncSourcePool := isaac.NewSyncSourcePool(nil)

			ballotbox := isaacstates.NewBallotbox(local.Address(), isaacparams.Threshold, nil)

			nolimit := launch.NoLimitRateLimiterRule()
			ra := launch.NewRateLimitHandlerArgs()
			ra.Rules = launch.NewRateLimiterRules()
			if err := ra.Rules.SetDefaultRuleMap(launch.NewRateLimiterRuleMap(&nolimit, nil)); err != nil {
				return pctx, err
			}

			var rh *launch.RateLimitHandler
			switch i, err := launch.NewRateLimitHandler(ra); {
			case err != nil:
				return pctx, err
			default:
				rh = i
			}

			return util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
				launch.SyncSourcePoolContextKey: syncSourcePool,
				launch.BallotboxContextKey:      ballotbox,
				launch.SuffrageVotingVoteFuncContextKey: isaac.SuffrageVoteFunc(func(base.SuffrageExpelOperation) (bool, error) {
					return false, nil
				}),
				launch.SuffrageVotingContextKey: &isaac.SuffrageVoting{},
				launch.FilterMemberlistNotifyMsgFuncContextKey: quicmemberlist.FilterNotifyMsgFunc(
					func(interface{}) (bool, error) { return true, nil },
				),
				launch.RateLimiterContextKey:  rh,
				launch.ACLContextKey:          &launch.YAMLACL{},
				launch.SuffragePoolContextKey: &launch.SuffragePool{},
			}), nil
		}).
		PostAddOK(launch.PNameOperationProcessorsMap, launch.POperationProcessorsMap).
		PostAddOK(launch.PNameNetworkHandlers, launch.PNetworkHandlers).
		PostAddOK(launch.PNameStatesNetworkHandlers, launch.PStatesNetworkHandlers)

	_ = pps.POK(launch.PNameNetwork).
		PreAddOK(launch.PNameQuicstreamClient, launch.PQuicstreamClient)

	var designFlag launch.DesignFlag
	var devFlags launch.DevFlags

	return pps,
		util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
			launch.DesignFlagContextKey: designFlag,
			launch.PrivatekeyContextKey: "",
			launch.DevFlagsContextKey:   devFlags,
			launch.MemberlistContextKey: &quicmemberlist.Memberlist{},
		})
}
