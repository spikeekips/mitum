//go:build test
// +build test

package isaac

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/stretchr/testify/assert"
)

func EqualLocalParams(t *assert.Assertions, a, b *LocalParams) {
	base.EqualLocalParams(t, a, b)

	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	t.Equal(a.intervalBroadcastBallot, b.intervalBroadcastBallot, "intervalBroadcastBallot")
	t.Equal(a.waitPreparingINITBallot, b.waitPreparingINITBallot, "waitPreparingINITBallot")
	t.Equal(a.timeoutRequestProposal, b.timeoutRequestProposal, "timeoutRequestProposal")
	t.Equal(a.syncSourceCheckerInterval, b.syncSourceCheckerInterval, "syncSourceCheckerInterval")
	t.Equal(a.validProposalOperationExpire, b.validProposalOperationExpire, "validProposalOperationExpire")
	t.Equal(a.validProposalSuffrageOperationsExpire, b.validProposalSuffrageOperationsExpire, "validProposalSuffrageOperationsExpire")
	t.Equal(a.maxOperationSize, b.maxOperationSize, "maxOperationSize")
	t.Equal(a.sameMemberLimit, b.sameMemberLimit, "sameMemberLimit")
}
