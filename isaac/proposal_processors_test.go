package isaac

func (pps *proposalProcessors) isClose() bool {
	pps.RLock()
	defer pps.RUnlock()

	return pps.p == nil
}
