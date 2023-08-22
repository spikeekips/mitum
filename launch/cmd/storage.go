package launchcmd

type StorageCommand struct { //nolint:govet //...
	Import         ImportCommand         `cmd:"" help:"import block data files"`
	Clean          CleanCommand          `cmd:"" help:"clean storage"`
	ValidateBlocks ValidateBlocksCommand `cmd:"" help:"validate blocks in storage"`
	Status         StorageStatusCommand  `cmd:"" help:"storage status"`
	Database       DatabaseCommand       `cmd:"" help:""`
}

type DatabaseCommand struct {
	Extract DatabaseExtractCommand `cmd:"" help:"extract and print database(only leveldb supported)"`
}
