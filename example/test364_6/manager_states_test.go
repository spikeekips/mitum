package dop6

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dhash "github.com/spikeekips/mitum/example/test364_5/hash"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerStates struct {
	baseMergerSuite
}

func (t *testManagerStates) TestNew() {
	t.Run("zero approx states size", func() {
		_, _, err := newManagerStates(context.Background(), 0, nil)
		t.ErrorContains(err, "under 1")
	})
}

func (t *testManagerStates) prepareStates(workers []string, n int) (
	[][2]any,
	map[string][][2]any,
	*fixedtree.Tree,
	fixedtree.GenerateNodesHashFunc,
) {
	ws := map[string][][2]any{}
	nodes := make([]fixedtree.Node, n)

	sts := make([][2]any, n)
	for i := range n {
		stateKey, stateHash := fmt.Sprintf("s%d", i), dhash.RandomHex64()

		sts[i] = [2]any{stateKey, stateHash}

		n, err := dop5.NewStateTreeNode(stateKey, stateHash)
		t.NoError(err)
		nodes[i] = n
	}

	d := int(math.Ceil(float64(n) / float64(len(workers))))

	for i, worker := range slices.All(workers) {
		s := i * d
		e := (i + 1) * d
		if e > n {
			e = n
		}

		ws[worker] = sts[s:e]
	}

	slices.SortFunc(nodes, func(a, b fixedtree.Node) int {
		return strings.Compare(a.Key(), b.Key())
	})

	g := t.generateNodesHashFunc()
	t.NoError(g(context.Background(), nodes, nil))

	return sts, ws, dop5.NewOperationsTree(nodes), g
}

func (t *testManagerStates) generateNodesHashFunc() fixedtree.GenerateNodesHashFunc {
	return fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(1 << 9)
}

func (t *testManagerStates) TestTree() {
	_, workers := t.workers(3)
	_, ws, expectedTree, g := t.prepareStates(workers, 10)

	t.Run("ok", func() {
		var count int64
		workerStates, wait, err := newManagerStates(context.Background(), 9,
			func(string, error) (bool, bool, error) {
				return true, atomic.AddInt64(&count, 1) == int64(len(workers)), nil
			},
		)
		t.NoError(err)

		var wg sync.WaitGroup

		for worker := range slices.Values(workers) {
			wg.Add(1)

			go func() {
				wg.Done()

				s, d := workerStates(worker)

				for i := range slices.Values(ws[worker]) {
					err := s(
						i[0].(string),
						i[1].(util.Hash),
					)
					t.IfErrPanicf(err, "add: worker=%q", worker)
				}
				t.IfErrPanicf(d(nil), "done: worker=%q", worker)
			}()
		}

		wg.Wait()

		tr, err := wait(g)
		t.NoError(err)

		t.True(fixedtree.IsEqualTree(expectedTree, tr))
	})

	t.Run("empty states", func() {
		var count int64
		workerStates, wait, err := newManagerStates(context.Background(), 9,
			func(string, error) (bool, bool, error) {
				return true, atomic.AddInt64(&count, 1) == int64(len(workers)), nil
			},
		)
		t.NoError(err)

		var wg sync.WaitGroup

		for worker := range slices.Values(workers) {
			wg.Add(1)

			go func() {
				wg.Done()

				_, d := workerStates(worker)
				t.IfErrPanicf(d(nil), "done: worker=%q", worker)
			}()
		}

		wg.Wait()

		tr, err := wait(g)
		t.NoError(err)
		t.Nil(tr)
	})
}

func (t *testManagerStates) TestCheckFailed() {
	_, workers := t.workers(3)

	t.Run("under failed", func() {
		failedWorker := workers[2]
		_, ws, expectedTree, g := t.prepareStates(workers[:2], 10)

		var count int64
		workerStates, wait, err := newManagerStates(context.Background(), 9,
			func(worker string, err error) (bool, bool, error) {
				if err != nil {
					return true, false, nil
				}

				return true, atomic.AddInt64(&count, 1) == int64(len(workers))-1, nil
			},
		)
		t.NoError(err)

		var wg sync.WaitGroup

		for worker := range slices.Values(workers) {
			wg.Add(1)

			go func() {
				wg.Done()

				s, d := workerStates(worker)
				for i := range slices.Values(ws[worker]) {
					err := s(
						i[0].(string),
						i[1].(util.Hash),
					)
					t.IfErrPanicf(err, "add: worker=%q", worker)
				}

				if worker == failedWorker {
					_ = d(errors.New("showme"))
				} else {
					t.IfErrPanicf(d(nil), "done: worker=%q", worker)
				}
			}()
		}

		wg.Wait()

		tr, err := wait(g)
		t.NoError(err)

		t.True(fixedtree.IsEqualTree(expectedTree, tr))
	})

	t.Run("over failed", func() {
		failedWorker := workers[2]
		_, ws, _, g := t.prepareStates(workers, 10)

		workerStates, wait, err := newManagerStates(context.Background(), 9,
			func(worker string, err error) (bool, bool, error) {
				if err != nil {
					return true, false, err
				}

				return true, false, nil
			},
		)
		t.NoError(err)

		var wg sync.WaitGroup

		for worker := range slices.Values(workers) {
			wg.Add(1)

			go func() {
				wg.Done()

				s, d := workerStates(worker)
				for i := range slices.Values(ws[worker]) {
					err := s(
						i[0].(string),
						i[1].(util.Hash),
					)
					if err != nil && !errors.Is(err, context.Canceled) {
						t.IfErrPanicf(err, "add: worker=%q", worker)
					}
				}

				if worker == failedWorker {
					_ = d(errors.New("showme"))
				} else {
					_ = d(nil)
				}
			}()
		}

		wg.Wait()

		_, err = wait(g)
		t.ErrorContains(err, "showme")
	})
}

func TestManagerStates(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerStates))
}
