package main

import (
	"context"
	"io"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	"github.com/spikeekips/mitum/example/test364_5/frame"
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
	*fixedtree.Tree,
	error,
) {
	log := dutil.LoadContextValueLogger(ctx)
	log.Debug().Msg("start one worker")

	local := "worker"
	workers := []string{local}

	ophs := func(yield func(uint64, [2]util.Hash) bool) {
		for i, fact := range slices.All(ops) {
			if !yield(dutil.SafeIntToUint64(i), [2]util.Hash{fact.Hash(), fact.Hash()}) {
				return
			}
		}
	}

	maxOpsSize := uint64(len(ops))

	getOperationFact := func(_ context.Context, opindex uint64, oph, facth util.Hash) (
		dop5.OperationFact, bool, string, error,
	) {
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
		return nil, nil, nil, err
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
		return nil, nil, nil, err
	}

	go func() {
		_ = workerClose(nil)
	}()

	mstates := map[string]base.State{}

	generateNodesHash := fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(
		min(int64(len(ops)), 1<<9)) //nolint:mnd

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

			return f(func(st base.State) error {
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

			slices.SortFunc(sts, func(a, b base.State) int {
				return strings.Compare(a.Key(), b.Key())
			})

			for st := range slices.Values(sts) {
				mstates[st.Key()] = st
			}

			return nil
		},
	); err != nil {
		return nil, nil, nil, err
	}

	return newManagerManifest(height, prevManiest, opsTree, statesTree),
		mstates,
		opsTree,
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

func saveStatesToFile(
	ctx context.Context,
	enc encoder.Encoder,
	filename string,
	sts []base.State, // NOTE sorted by key
) error {
	size := int64(len(sts))

	if size < 1 {
		return nil
	}

	var w io.Writer

	switch f, err := os.Create(filepath.Clean(filename)); {
	case err != nil:
		return errors.WithStack(err)
	default:
		defer func() { _ = f.Close() }()

		w = f
	}

	if err := dutil.WriteInt(w, size); err != nil {
		return err
	}

	newJob, jobDone, jobWait, err := defaultNewFuncJobWorker(ctx, size)
	if err != nil {
		return err
	}

	for i, st := range slices.All(sts) {
		if err := newJob(func(context.Context) error {
			ib, err := dutil.EncodeInt(dutil.SafeIntToUint64(i))
			if err != nil {
				return err
			}

			b, err := enc.Marshal(st)
			if err != nil {
				return err
			}

			return frame.WriteFixeds(w, ib, b)
		}); err != nil {
			return jobWait(err)
		}
	}

	jobDone()

	return jobWait(nil)
}

func loadStatesFromFile(
	ctx context.Context,
	enc encoder.Encoder,
	filename string,
) ([]base.State, error) {
	var r io.Reader

	switch f, err := os.Open(filepath.Clean(filename)); {
	case err != nil && !os.IsNotExist(err):
		return nil, errors.WithStack(err)
	case os.IsNotExist(err):
		return nil, nil
	default:
		defer func() { _ = f.Close() }()

		r = f
	}

	var size int64

	if err := dutil.ReadInt(r, &size); err != nil {
		return nil, err
	}

	sts := make([]base.State, size)

	newJob, jobDone, jobWait, err := defaultNewFuncJobWorker(ctx, size)
	if err != nil {
		return nil, err
	}

	for range size {
		index, b, err := readFixedWithIndex(r)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, jobWait(err)
		}

		if err := newJob(func(context.Context) error {
			if err := encoder.Unmarshal(enc, b, &sts[index]); err != nil {
				return err
			}

			return nil
		}); err != nil {
			return nil, jobWait(err)
		}
	}

	jobDone()

	if err := jobWait(nil); err != nil {
		return nil, err
	}

	slices.SortFunc(sts, func(a, b base.State) int {
		return strings.Compare(a.Key(), b.Key())
	})

	return sts, nil
}

func readFixedWithIndex(r io.Reader) (uint64, []byte, error) {
	var index uint64

	switch i, _, err := frame.ReadFixed(r); {
	case err != nil:
		return 0, nil, err
	default:
		if err := dutil.DecodeInt(i, &index); err != nil {
			return 0, nil, err
		}
	}

	b, _, err := frame.ReadFixed(r)

	return index, b, err
}

func saveManifests(
	enc encoder.Encoder,
	filename string,
	ms [2]base.Manifest,
) error {
	switch f, err := os.Create(filepath.Clean(filename)); {
	case err != nil:
		return errors.WithStack(err)
	default:
		defer func() { _ = f.Close() }()

		for m := range slices.Values(ms[:]) {
			b, err := enc.Marshal(m)
			if err != nil {
				return err
			}

			if err := frame.WriteFixed(f, b); err != nil {
				return err
			}
		}

		return nil
	}
}

func loadManifest(enc encoder.Encoder, r io.Reader) (base.Manifest, error) {
	switch b, n, err := frame.ReadFixed(r); {
	case err != nil && !errors.Is(err, io.EOF):
		return nil, err
	case n < 1 && errors.Is(err, io.EOF):
		return nil, nil //nolint:nilnil
	default:
		var manifest base.Manifest

		return manifest, encoder.Unmarshal(enc, b, &manifest)
	}
}

func loadManifests(enc encoder.Encoder, filename string) ([2]base.Manifest, error) {
	var manifests [2]base.Manifest

	switch f, err := os.Open(filepath.Clean(filename)); {
	case err != nil && !os.IsNotExist(err):
		return manifests, errors.WithStack(err)
	case os.IsNotExist(err):
		return manifests, nil
	default:
		defer func() { _ = f.Close() }()

		var err error

		manifests[0], err = loadManifest(enc, f)
		if err != nil {
			return manifests, err
		}

		if manifests[0] == nil {
			return manifests, errors.Errorf("nil manifest, 33")
		}

		manifests[1], err = loadManifest(enc, f)
		if err != nil {
			return manifests, err
		}

		if manifests[1] == nil {
			return manifests, errors.Errorf("nil manifest, 34")
		}

		return manifests, nil
	}
}

func saveOpsTreeToFile(
	ctx context.Context,
	enc encoder.Encoder,
	filename string,
	tr *fixedtree.Tree,
) error {
	size := int64(tr.Len())

	if size < 1 {
		return nil
	}

	var w io.Writer

	switch f, err := os.Create(filepath.Clean(filename)); {
	case err != nil:
		return errors.WithStack(err)
	default:
		defer func() { _ = f.Close() }()

		w = f
	}

	if err := dutil.WriteInt(w, size); err != nil {
		return err
	}

	newJob, jobDone, jobWait, err := defaultNewFuncJobWorker(ctx, size)
	if err != nil {
		return err
	}

	for i, n := range tr.Traverse() {
		if err := newJob(func(context.Context) error {
			ib, err := dutil.EncodeInt(i)
			if err != nil {
				return err
			}

			b, err := enc.Marshal(n)
			if err != nil {
				return err
			}

			return frame.WriteFixeds(w, ib, b)
		}); err != nil {
			return jobWait(err)
		}
	}

	jobDone()

	return jobWait(nil)
}

func loadOpsTreeFromFile(ctx context.Context, filename string) (*fixedtree.Tree, error) {
	var r io.Reader

	switch f, err := os.Open(filepath.Clean(filename)); {
	case err != nil && !os.IsNotExist(err):
		return nil, errors.WithStack(err)
	case os.IsNotExist(err):
		return nil, nil //nolint:nilnil
	default:
		defer func() { _ = f.Close() }()

		r = f
	}

	var size int64

	if err := dutil.ReadInt(r, &size); err != nil {
		return nil, err
	}

	nodes := make([]fixedtree.Node, size)

	newJob, jobDone, jobWait, err := defaultNewFuncJobWorker(ctx, size)
	if err != nil {
		return nil, err
	}

	for range size {
		index, b, err := readFixedWithIndex(r)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, jobWait(err)
		}

		if err := newJob(func(context.Context) error {
			n := new(dop5.OperationTreeNode)

			if err := n.UnmarshalJSON(b); err != nil {
				return err
			}

			nodes[index] = n

			return nil
		}); err != nil {
			return nil, jobWait(err)
		}
	}

	jobDone()

	if err := jobWait(nil); err != nil {
		return nil, err
	}

	return fixedtree.NewTree(dop5.OperationsTreeHint, nodes), // NOTE fake hint
		nil
}
