//go:build test
// +build test

package isaac

import "time"

func (pps *ProposalProcessors) SetRetry(retryinterval time.Duration, retrylimit int) {
	pps.retryinterval = retryinterval
	pps.retrylimit = retrylimit
}
