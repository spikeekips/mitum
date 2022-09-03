package launch

import "github.com/spikeekips/mitum/util/ps"

func DefaultRunPS() *ps.PS {
	pps := ps.NewPS("cmd-run")

	_ = pps.
		AddOK(PNameEncoder, PEncoder, nil).
		AddOK(PNameDesign, PLoadDesign, nil, PNameEncoder).
		AddOK(PNameLocal, PLocal, nil, PNameDesign).
		AddOK(PNameStorage, PStorage, nil, PNameLocal).
		AddOK(PNameProposalMaker, PProposalMaker, nil, PNameStorage).
		AddOK(PNameNetwork, PNetwork, nil, PNameStorage).
		AddOK(PNameMemberlist, PMemberlist, nil, PNameNetwork).
		AddOK(PNameStartNetwork, PStartNetwork, PCloseNetwork, PNameStates).
		AddOK(PNameStartStorage, PStartStorage, PCloseStorage, PNameStartNetwork).
		AddOK(PNameStartMemberlist, PStartMemberlist, PCloseMemberlist, PNameStartNetwork).
		AddOK(PNameStartSyncSourceChecker, PStartSyncSourceChecker, PCloseSyncSourceChecker, PNameStartNetwork).
		AddOK(PNameStartLastSuffrageProofWatcher,
			PStartLastSuffrageProofWatcher, PCloseLastSuffrageProofWatcher, PNameStartNetwork).
		AddOK(PNameStates, PStates, nil, PNameNetwork).
		AddOK(PNameStatesReady, nil, PCloseStates,
			PNameStartStorage,
			PNameStartSyncSourceChecker,
			PNameStartLastSuffrageProofWatcher,
			PNameStartMemberlist,
			PNameStartNetwork,
			PNameStates,
		)

	_ = pps.POK(PNameEncoder).
		PostAddOK(PNameAddHinters, PAddHinters)

	_ = pps.POK(PNameDesign).
		PostAddOK(PNameCheckDesign, PCheckDesign)

	_ = pps.POK(PNameLocal).
		PostAddOK(PNameDiscoveryFlag, PDiscoveryFlag)

	_ = pps.POK(PNameStorage).
		PreAddOK(PNameCheckLocalFS, PCheckLocalFS).
		PreAddOK(PNameLoadDatabase, PLoadDatabase).
		PostAddOK(PNameCheckLeveldbStorage, PCheckLeveldbStorage).
		PostAddOK(PNameCheckLoadFromDatabase, PLoadFromDatabase).
		PostAddOK(PNameNodeInfo, PNodeInfo).
		PostAddOK(PNameBallotbox, PBallotbox)

	_ = pps.POK(PNameNetwork).
		PreAddOK(PNameQuicstreamClient, PQuicstreamClient).
		PostAddOK(PNameSyncSourceChecker, PSyncSourceChecker).
		PostAddOK(PNameSuffrageCandidateLimiterSet, PSuffrageCandidateLimiterSet)

	_ = pps.POK(PNameMemberlist).
		PreAddOK(PNameLastSuffrageProofWatcher, PLastSuffrageProofWatcher).
		PostAddOK(PNamePatchLastSuffrageProofWatcherWithMemberlist, PPatchLastSuffrageProofWatcherWithMemberlist).
		PostAddOK(PNameLongRunningMemberlistJoin, PLongRunningMemberlistJoin)

	_ = pps.POK(PNameStates).
		PreAddOK(PNameOperationProcessorsMap, POperationProcessorsMap).
		PreAddOK(PNameNetworkHandlers, PNetworkHandlers).
		PreAddOK(PNameNodeInConsensusNodesFunc, PNodeInConsensusNodesFunc).
		PreAddOK(PNameProposalProcessors, PProposalProcessors).
		PostAddOK(PNameStatesSetHandlers, PStatesSetHandlers).
		PostAddOK(PNameWatchDesign, PWatchDesign)

	return pps
}
