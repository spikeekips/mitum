//go:build test
// +build test

package isaac

import (
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

func (rs *BlockItemReaders) EmptyHeightsLock() util.LockedMap[base.Height, time.Time] {
	return rs.emptyHeightsLock
}

func (rs *BlockItemReaders) LoadAndRemoveEmptyHeightDirectories() (removed uint64, _ error) {
	return rs.loadAndRemoveEmptyHeightDirectories()
}

func (rs *BlockItemReaders) RemoveEmptyHeightsLock() (removed uint64, _ error) {
	return rs.removeEmptyHeightsLock()
}
