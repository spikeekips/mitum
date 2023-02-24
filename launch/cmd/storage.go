package launchcmd

type Storage struct { //nolint:govet //...
	Import         ImportCommand         `cmd:"" help:"import block data files"`
	Clean          CleanCommand          `cmd:"" help:"clean storage"`
	ValidateBlocks ValidateBlocksCommand `cmd:"" help:"validate blocks in storage"`
	Status         StorageStatusCommand  `cmd:"" help:"storage status"`
}
