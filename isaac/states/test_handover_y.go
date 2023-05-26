//go:build test
// +build test

package isaacstates

import "time"

func (h *HandoverYBroker) checkSyncedDataReady() {
	ticker := time.NewTicker(time.Millisecond * 33)
	defer ticker.Stop()

	for range ticker.C {
		if synced, _ := h.isReadyToAsk.Value(); synced {
			break
		}
	}
}

func (h *HandoverYBroker) checkSyncedDataDone() {
	ticker := time.NewTicker(time.Millisecond * 33)
	defer ticker.Stop()

	for range ticker.C {
		if synced, _ := h.isDataSynced.Value(); synced {
			break
		}
	}
}
