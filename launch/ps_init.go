package launch

import "github.com/spikeekips/mitum/util/ps"

func DefaultINITPS() *ps.PS {
	pps := ps.NewPS("cmd-init")

	_ = pps.
		AddOK(PNameEncoder, PEncoder, nil).
		AddOK(PNameDesign, PLoadDesign, nil, PNameEncoder).
		AddOK(PNameTimeSyncer, PStartTimeSyncer, PCloseTimeSyncer, PNameDesign).
		AddOK(PNameLocal, PLocal, nil, PNameDesign).
		AddOK(PNameStorage, PStorage, PCloseStorage, PNameLocal).
		AddOK(PNameGenerateGenesis, PGenerateGenesis, nil, PNameStorage)

	_ = pps.POK(PNameEncoder).
		PostAddOK(PNameAddHinters, PAddHinters)

	_ = pps.POK(PNameDesign).
		PostAddOK(PNameCheckDesign, PCheckDesign).
		PostAddOK(PNameINITObjectCache, PINITObjectCache).
		PostAddOK(PNameGenesisDesign, PGenesisDesign)

	_ = pps.POK(PNameStorage).
		PreAddOK(PNameCleanStorage, PCleanStorage).
		PreAddOK(PNameCreateLocalFS, PCreateLocalFS).
		PreAddOK(PNameLoadDatabase, PLoadDatabase).
		PostAddOK(PNameBlockReadersDecompressFunc, PBlockReadersDecompressFunc).
		PostAddOK(PNameBlockReaders, PBlockReaders)

	return pps
}
