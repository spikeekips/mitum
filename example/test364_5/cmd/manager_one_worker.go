package main

import (
	"context"
	"iter"
	"slices"
	"strings"

	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

func managerRunOneWorker(
	ctx context.Context,
	height base.Height,
	prevManiest util.Hash,
	prevStates map[string]base.State,
	ops []dop5.OperationFact,
) (
	base.Manifest,
	map[string]base.State,
	error,
) {
	local := "worker"
	workers := []string{local}

	ophs := func(yield func(uint64, [2]util.Hash) bool) {
		for i, fact := range slices.All(ops) {
			if !yield(uint64(i), [2]util.Hash{fact.Hash(), fact.Hash()}) {
				return
			}
		}
	}

	maxOpsSize := uint64(len(ops))
	getOperationFact := func(_ context.Context, opindex uint64, oph, facth util.Hash) (dop5.OperationFact, bool, string, error) {
		if opindex >= maxOpsSize {
			return nil, false, "", nil
		}

		return ops[opindex], true, "", nil
	}

	getState := func(stateKey string) (base.State, bool, error) {
		st, found := prevStates[stateKey]

		return st, found, nil
	}
	newState := func(stateKey string, value base.StateValue, prev util.Hash, ops []util.Hash) (base.State, error) {
		return dop5.NewBaseState(height, stateKey, value, prev, ops), nil
	}

	var initialInsert dop5.WorkerInitialInsertFunc
	var wait dop5.WorkerWaitFunc
	var workerClose dutil.InErrorFunc[error]

	switch ii, _, _, w, c, err := dop5.NewFuncWorker(
		ctx,
		dop5.NewWorkerOptions(local),
		getState, newState,
		nil,
		workers, workers,
		1, 1,
		maxOpsSize,
	); {
	case err != nil:
		return nil, nil, err
	default:
		initialInsert, wait, workerClose = ii, w, c
	}

	if err := initialInsert(
		newFuncGetStateOwnersMurmur3(workers, 1),
		nil,
		getOperationFact,
		ophs,
		nil,
	); err != nil {
		return nil, nil, err
	}

	go func() {
		_ = workerClose(nil)
	}()

	createdStates := map[string]base.State{}

	generateNodesHash := fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(1 << 9)

	var opsTree, statesTree *fixedtree.Tree

	if err := wait(
		nil,
		func(
			ctx context.Context,
			reasons map[uint64]string,
			_ uint64,
			f dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
		) error {
			switch tr, err := generateOpsTree(ctx, generateNodesHash, maxOpsSize, ophs, reasons); {
			case err != nil:
				return err
			default:
				opsTree = tr
			}

			return f(func(base.State) error {
				return nil
			})
		},
		func(sts []base.State) error {
			switch tr, err := generateStatesTree(ctx, generateNodesHash, sts); {
			case err != nil:
				return err
			default:
				statesTree = tr
			}

			for st := range slices.Values(sts) {
				createdStates[st.Key()] = st
			}

			return nil
		},
	); err != nil {
		return nil, nil, err
	}

	return newManifest(height, prevManiest, opsTree, statesTree),
		createdStates,
		nil
}

func generateStatesTree(
	ctx context.Context,
	g fixedtree.GenerateNodesHashFunc,
	sts []base.State,
) (*fixedtree.Tree, error) {
	nodes := make([]fixedtree.Node, len(sts))

	for i, st := range slices.All(sts) {
		switch n, err := dop5.NewStateTreeNode(st.Key(), st.Hash()); {
		case err != nil:
			return nil, err
		default:
			nodes[i] = n
		}
	}

	slices.SortFunc(nodes, func(a, b fixedtree.Node) int {
		return strings.Compare(a.Key(), b.Key())
	})

	if err := g(ctx, nodes, nil); err != nil {
		return nil, err
	}

	return dop5.NewStatesTree(nodes), nil
}

func generateOpsTree(
	ctx context.Context,
	g fixedtree.GenerateNodesHashFunc,
	maxOpsSize uint64,
	ophs iter.Seq2[uint64, [2]util.Hash],
	reasons map[uint64]string,
) (*fixedtree.Tree, error) {
	nodes := make([]fixedtree.Node, maxOpsSize)

	for opindex, hs := range ophs {
		switch n, err := dop5.NewOperationTreeNode(hs[0], hs[1], reasons[opindex]); {
		case err != nil:
			return nil, err
		default:
			nodes[opindex] = n
		}
	}

	if err := g(ctx, nodes, nil); err != nil {
		return nil, err
	}

	tr := dop5.NewOperationsTree(nodes)

	return tr, nil
}
