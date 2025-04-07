package main

import (
	"context"
	"iter"

	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/util"
)

type db interface {
	ping(context.Context) error
	flushAll(context.Context) error
	saveOperationFacts(_ context.Context, semSize, bulkSize int64, ops []dop5.OperationFact) error
	getOperationFact(_ context.Context, oph util.Hash, facth *dop5.OperationFact) (bool, error)
	saveStates(_ context.Context, semSize, bulkSize int64, _ []base.State) error
	getState(_ context.Context, stateKey string, _ *base.State) (bool, error)
	getStates(context.Context) iter.Seq2[base.State, error]
}
