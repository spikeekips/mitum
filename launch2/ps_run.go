package launch2

import "github.com/spikeekips/mitum/util/ps"

func DefaultRunPS() *ps.PS {
	pps := ps.NewPS()

	_ = pps.
		AddOK(PNameEncoder, PEncoder, nil).
		AddOK(PNameDesign, PDesign, nil, PNameEncoder).
		AddOK(PNameLocal, PLocal, nil, PNameDesign).
		AddOK(PNameStorage, PStorage, PCloseStorage, PNameLocal).
		AddOK(PNameProposalMaker, PProposalMaker, nil, PNameStorage).
		AddOK(PNameNetwork, PNetwork, PCloseNetwork, PNameStorage).
		AddOK(PNamePrepareMemberlist, PPrepareMemberlist, PCloseMemberlist, PNameNetwork).
		AddOK(PNameStates, PStates, PCloseStates, PNamePrepareMemberlist)

	_ = pps.POK(PNameEncoder).
		PostAddOK(PNameAddHinters, PAddHinters)

	_ = pps.POK(PNameLocal).
		PostAddOK(PNameDiscoveryFlag, PDiscoveryFlag)

	_ = pps.POK(PNameStorage).
		PreAddOK(PNameCheckLocalFS, PCheckLocalFS).
		PreAddOK(PNameLoadDatabase, PLoadDatabase).
		PostAddOK(PNameCheckLeveldbStorage, PCheckLeveldbStorage).
		PostAddOK(PNameNodeInfo, PNodeInfo).
		PostAddOK(PNameBallotbox, PBallotbox)

	_ = pps.POK(PNameNetwork).
		PreAddOK(PNameQuicstreamClient, PQuicstreamClient).
		PostAddOK(PNameSyncSourceChecker, PSyncSourceChecker).
		PostAddOK(PNamePrepareSuffrageCandidateLimiterSet, PPrepareSuffrageCandidateLimiterSet)

	_ = pps.POK(PNamePrepareMemberlist).
		PreAddOK(PNameLastSuffrageProofWatcher, PLastSuffrageProofWatcher).
		PostAddOK(PNamePatchLastSuffrageProofWatcherWithMemberlist, PPatchLastSuffrageProofWatcherWithMemberlist)

	_ = pps.POK(PNameStates).
		PreAddOK(PNameOperationProcessorsMap, POperationProcessorsMap).
		PreAddOK(PNameNetworkHandlers, PNetworkHandlers)

	return pps
}
