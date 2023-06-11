//go:build test
// +build test

package isaac

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func EqualLocalParams(t *assert.Assertions, a, b *Params) {
	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	aht := a.Hint()
	bht := b.Hint()
	t.True(aht.Equal(bht), "Hint does not match: %q != %q", aht, bht)

	t.Equal(a.networkID, b.networkID, "networkID")
	t.Equal(a.threshold, b.threshold, "threshold")
	t.Equal(a.intervalBroadcastBallot, b.intervalBroadcastBallot, "intervalBroadcastBallot")
	t.Equal(a.waitPreparingINITBallot, b.waitPreparingINITBallot, "waitPreparingINITBallot")
	t.Equal(a.ballotStuckWait, b.ballotStuckWait, "ballotStuckWait")
	t.Equal(a.ballotStuckResolveAfter, b.ballotStuckResolveAfter, "ballotStuckResolveAfter")
	t.Equal(a.maxTryHandoverYBrokerSyncData, b.maxTryHandoverYBrokerSyncData, "maxTryHandoverYBrokerSyncData")
}
