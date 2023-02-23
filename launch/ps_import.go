package launch

import "github.com/spikeekips/mitum/util/ps"

func DefaultImportPS() *ps.PS {
	pps := ps.NewPS("cmd-init")

	_ = pps.
		AddOK(PNameEncoder, PEncoder, nil).
		AddOK(PNameDesign, PLoadDesign, nil, PNameEncoder).
		AddOK(PNameTimeSyncer, PStartTimeSyncer, PCloseTimeSyncer, PNameDesign).
		AddOK(PNameLocal, PLocal, nil, PNameDesign).
		AddOK(PNameStorage, PStorage, PCloseStorage, PNameLocal)

	_ = pps.POK(PNameEncoder).
		PostAddOK(PNameAddHinters, PAddHinters)

	_ = pps.POK(PNameDesign).
		PostAddOK(PNameCheckDesign, PCheckDesign)

	_ = pps.POK(PNameStorage).
		PreAddOK(PNameCheckLocalFS, PCheckLocalFS).
		PreAddOK(PNameLoadDatabase, PLoadDatabase).
		PostAddOK(PNameCheckLeveldbStorage, PCheckLeveldbStorage).
		PostAddOK(PNameLoadFromDatabase, PLoadFromDatabase).
		PostAddOK(PNameCheckBlocksOfStorage, PCheckBlocksOfStorage).
		PostAddOK(PNameNodeInfo, PNodeInfo)

	return pps
}
