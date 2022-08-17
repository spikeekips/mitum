package launch

import "github.com/spikeekips/mitum/util/ps"

func DefaultINITPS() *ps.PS {
	pps := ps.NewPS()

	_ = pps.
		AddOK(PNameEncoder, PEncoder, nil).
		AddOK(PNameDesign, PDesign, nil, PNameEncoder).
		AddOK(PNameLocal, PLocal, nil, PNameDesign).
		AddOK(PNameStorage, PStorage, PCloseStorage, PNameLocal).
		AddOK(PNameGenerateGenesis, PGenerateGenesis, nil, PNameStorage)

	_ = pps.POK(PNameEncoder).
		PostAddOK(PNameAddHinters, PAddHinters)

	_ = pps.POK(PNameDesign).
		PostAddOK(PNameGenesisDesign, PGenesisDesign)

	_ = pps.POK(PNameStorage).
		PreAddOK(PNameCleanStorage, PCleanStorage).
		PreAddOK(PNameCreateLocalFS, PCreateLocalFS).
		PreAddOK(PNameLoadDatabase, PLoadDatabase)

	return pps
}
