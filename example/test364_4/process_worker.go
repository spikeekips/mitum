package main

import (
	"context"

	"github.com/spikeekips/mitum/util"
)

// FIXME add failed operations to unmerged
// FIXME add empty merged states to unmerged

type (
	getValuesFunc       func(opindex uint64) ([]StateMergeValue, error)
	saveReasonFunc      func(opindex uint64, reason string) error
	stateOwnersFunc     func(_ context.Context, stateKey string) (owners []string, _ error)
	insertWorkerOpFuncs func(worker string) (insertOpFunc, func())
	saveOperationFunc   func(opindex uint64, _ Operation) error
	insertOpFunc        func(opindex uint64, oph util.Hash, _ []StateMergeValue) error

	newStateValuesProcessorFunc func(context.Context, *stateValuesProcessorArgs) (
		insert stateValuesProcessorInsertFunc,
		insertDone func(),
		wait waitWithReasonsFunc,
		_ error,
	)
	newStateValuesProcessorFuncs func() (
		*stateValuesProcessorArgs,
		newStateValuesProcessorFunc,
		error,
	)
	distributeStateMergeValuesFunc  func(_ context.Context, worker string, opindex uint64, oph util.Hash, _ []StateMergeValue) error
	distributeStateMergeValuesFuncs func() (
		distributeStateMergeValuesFunc,
		func(context.Context) error, // NOTE send to all workers
		error,
	)
	processWorkerInitialOpFuncs func() (
		insert stateValuesProcessorInsertFunc,
		insertDone func(),
	)
)
