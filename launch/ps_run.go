package launch

import "github.com/spikeekips/mitum/util/ps"

func DefaultRunPS() *ps.PS {
	pps := ps.NewPS("cmd-run")

	_ = pps.
		AddOK(PNameEncoder, PEncoder, nil).
		AddOK(PNameDesign, PLoadDesign, nil, PNameEncoder).
		AddOK(PNameTimeSyncer, PStartTimeSyncer, PCloseTimeSyncer, PNameDesign).
		AddOK(PNameLocal, PLocal, nil, PNameDesign).
		AddOK(PNameStorage, PStorage, nil, PNameLocal).
		AddOK(PNameProposalMaker, PProposalMaker, nil, PNameStorage).
		AddOK(PNameNetwork, PNetwork, nil, PNameStorage).
		AddOK(PNameMemberlist, PMemberlist, nil, PNameNetwork).
		AddOK(PNameBlockItemReaders, PBlockItemReaders, nil, PNameDesign).
		AddOK(PNameStartStorage, PStartStorage, PCloseStorage, PNameStartNetwork).
		AddOK(PNameStartNetwork, PStartNetwork, PCloseNetwork, PNameStates).
		AddOK(PNameStartMemberlist, PStartMemberlist, PCloseMemberlist, PNameStartNetwork).
		AddOK(PNameStartSyncSourceChecker, PStartSyncSourceChecker, PCloseSyncSourceChecker, PNameStartNetwork).
		AddOK(PNameStartLastConsensusNodesWatcher,
			PStartLastConsensusNodesWatcher, PCloseLastConsensusNodesWatcher, PNameStartNetwork).
		AddOK(PNameStates, PStates, nil, PNameNetwork).
		AddOK(PNameStatesReady, nil, PCloseStates,
			PNameStartStorage,
			PNameStartSyncSourceChecker,
			PNameStartLastConsensusNodesWatcher,
			PNameStartMemberlist,
			PNameStartNetwork,
			PNameStates,
		)

	_ = pps.POK(PNameEncoder).
		PostAddOK(PNameAddHinters, PAddHinters)

	_ = pps.POK(PNameDesign).
		PostAddOK(PNameCheckDesign, PCheckDesign).
		PostAddOK(PNameINITObjectCache, PINITObjectCache)

	_ = pps.POK(PNameLocal).
		PostAddOK(PNameDiscoveryFlag, PDiscoveryFlag).
		PostAddOK(PNameLoadACL, PLoadACL)

	_ = pps.POK(PNameBlockItemReaders).
		PreAddOK(PNameBlockItemReadersDecompressFunc, PBlockItemReadersDecompressFunc).
		PostAddOK(PNameRemotesBlockItemReaderFunc, PRemotesBlockItemReaderFunc)

	_ = pps.POK(PNameStorage).
		PreAddOK(PNameCheckLocalFS, PCheckAndCreateLocalFS).
		PreAddOK(PNameLoadDatabase, PLoadDatabase).
		PostAddOK(PNameCheckLeveldbStorage, PCheckLeveldbStorage).
		PostAddOK(PNameLoadFromDatabase, PLoadFromDatabase).
		PostAddOK(PNameCheckBlocksOfStorage, PCheckBlocksOfStorage).
		PostAddOK(PNamePatchBlockItemReaders, PPatchBlockItemReaders).
		PostAddOK(PNameNodeInfo, PNodeInfo)

	_ = pps.POK(PNameNetwork).
		PreAddOK(PNameQuicstreamClient, PQuicstreamClient).
		PostAddOK(PNameSyncSourceChecker, PSyncSourceChecker).
		PostAddOK(PNameSuffrageCandidateLimiterSet, PSuffrageCandidateLimiterSet)

	_ = pps.POK(PNameMemberlist).
		PreAddOK(PNameLastConsensusNodesWatcher, PLastConsensusNodesWatcher).
		PreAddOK(PNameRateLimiterContextKey, PNetworkRateLimiter).
		PostAddOK(PNameBallotbox, PBallotbox).
		PostAddOK(PNameLongRunningMemberlistJoin, PLongRunningMemberlistJoin).
		PostAddOK(PNameSuffrageVoting, PSuffrageVoting).
		PostAddOK(PNameEventLoggingNetworkHandlers, PEventLoggingNetworkHandlers)

	_ = pps.POK(PNameStates).
		PreAddOK(PNameProposerSelector, PProposerSelector).
		PreAddOK(PNameOperationProcessorsMap, POperationProcessorsMap).
		PreAddOK(PNameNetworkHandlers, PNetworkHandlers).
		PreAddOK(PNameNodeInConsensusNodesFunc, PNodeInConsensusNodesFunc).
		PreAddOK(PNameProposalProcessors, PProposalProcessors).
		PreAddOK(PNameBallotStuckResolver, PBallotStuckResolver).
		PostAddOK(PNamePatchLastConsensusNodesWatcher, PPatchLastConsensusNodesWatcher).
		PostAddOK(PNameStatesSetHandlers, PStatesSetHandlers).
		PostAddOK(PNameNetworkHandlersReadWriteNode, PNetworkHandlersReadWriteNode).
		PostAddOK(PNamePatchMemberlist, PPatchMemberlist).
		PostAddOK(PNameStatesNetworkHandlers, PStatesNetworkHandlers).
		PostAddOK(PNameHandoverNetworkHandlers, PHandoverNetworkHandlers)

	return pps
}
