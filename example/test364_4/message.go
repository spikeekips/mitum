package main

type startWorkerMsg struct {
	WorkID           string   `json:"work_id"`
	Workers          []string `json:"workers"`
	ActiveOpsWorkers []string `json:"active_ops_workers"`
	MaxOpsSize       uint64   `json:"max_ops_size"`
	MinEnoughOps     uint64   `json:"min_enough_ops"`
	MaxFailedOps     uint64   `json:"max_failed_ops"`
	StatesShare      uint64   `json:"states_share"`
}

func newStartWorkerMsg(
	workID string,
	workers, activeOpsWorkers []string,
	maxOpsSize, minEnoughOps, maxFailedOps,
	statesShare uint64,
) startWorkerMsg {
	return startWorkerMsg{
		WorkID:           workID,
		Workers:          workers,
		ActiveOpsWorkers: activeOpsWorkers,
		MaxOpsSize:       maxOpsSize,
		MinEnoughOps:     minEnoughOps,
		MaxFailedOps:     maxFailedOps,
		StatesShare:      statesShare,
	}
}
