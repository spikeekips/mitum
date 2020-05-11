package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/block"
)

type SyncerState uint8

const (
	_ SyncerState = iota
	SyncerCreated
	SyncerPreparing
	SyncerPrepared
	SyncerSaving
	SyncerSaved
)

func (ss SyncerState) String() string {
	switch ss {
	case SyncerCreated:
		return "syncer-created"
	case SyncerPreparing:
		return "syncer_preparing"
	case SyncerPrepared:
		return "syncer-prepared"
	case SyncerSaving:
		return "syncer-saving"
	case SyncerSaved:
		return "syncer-saved"
	default:
		return "<unknown sync state>"
	}
}

type Syncer interface {
	Prepare(block.Manifest /* base manifest */) error
	Save() error
	HeightFrom() base.Height
	HeightTo() base.Height
	State() SyncerState
	TailManifest() block.Manifest
	Close() error
}

type syncerFetchBlockError struct {
	err     error
	heights []base.Height
	node    base.Address
	missing []base.Height
	blocks  []block.Block
}

func (fm *syncerFetchBlockError) Error() string {
	if fm.err == nil {
		return ""
	}

	return fm.err.Error()
}