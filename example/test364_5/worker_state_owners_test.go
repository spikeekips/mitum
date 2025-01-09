package main

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerStateOwners struct {
	dutil.BaseSuite
	baseTestMergeOp
	baseTestWorkers
}

func (t *testWorkerStateOwners) owners(n int, prefix string) []string {
	s := make([]string, n)

	for i := range n {
		s[i] = fmt.Sprintf("%s%d", prefix, i)
	}

	return s
}

func (t *testWorkerStateOwners) randomGetStateOwners() getStateOwnersFunc {
	return func(stateKey string) []string {
		return []string{stateKey}
	}
}

func (t *testWorkerStateOwners) TestInsert() {
	local := "local"

	t.Run("no insert", func() {
		workers := t.workers(9)

		ctx, _ := t.Ctx()

		insertf, _, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		getStateOwner, stateOwnersClose, err := stateOwnersFuncByCount(workers, 2)
		t.NoError(err)
		defer stateOwnersClose()

		_, traverse, localOwners, err := insertf(getStateOwner)
		t.NoError(err)

		err = traverse(func(worker string, subws, affected []string) error {
			t.Empty(subws, worker)
			t.Empty(affected, worker)

			return nil
		})
		t.NoError(err)

		t.Empty(localOwners())

		subws, affected, err := done()
		t.NoError(err)
		t.Empty(subws)
		t.Empty(affected)
	})

	t.Run("nil sub value", func() {
		workers := t.workers(9)

		var values [][]StateMergeValue
		for range 3 {
			values = append(values, t.randomMergeValues(1, 2))
		}

		ctx, _ := t.Ctx()

		insertf, _, _, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		getStateOwner, stateOwnersClose, err := stateOwnersFuncByCount(workers, 1)
		t.NoError(err)
		defer stateOwnersClose()

		var expectedOwners []string
		for i := range slices.Values(values) {
			for j := range slices.Values(i) {
				o := getStateOwner(j.Key())
				expectedOwners = append(expectedOwners, o...)
			}
		}

		slices.Sort(expectedOwners)
		expectedOwners = slices.Compact(expectedOwners)

		insert, traverse, localOwners, err := insertf(getStateOwner)
		t.NoError(err)

		for i := range values {
			_ = insert(
				uint64(i),
				nil,
				values[i],
			)
		}

		var called atomic.Value

		t.NoError(traverse(func(worker string, subws, affected []string) error {
			t.T().Logf("worker=%q sub=%v affected=%v", worker, subws, affected)

			called.Store(true)

			return nil
		}))

		t.Nil(called.Load())

		o := localOwners()
		slices.Sort(o)
		t.Equal(expectedOwners, o)
	})

	t.Run("nil add value", func() {
		workers := t.workers(9)

		var values []StateMergeValue
		for range 3 {
			_, v := t.randomMergeValue(1)
			values = append(values, v)
		}

		ctx, _ := t.Ctx()

		insertf, _, _, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		getStateOwner, stateOwnersClose, err := stateOwnersFuncByCount(workers, 2)
		t.NoError(err)
		defer stateOwnersClose()

		var expectedOwners []string
		for i := range slices.Values(values) {
			o := getStateOwner(i.Key())
			expectedOwners = append(expectedOwners, o...)
		}

		slices.Sort(expectedOwners)
		expectedOwners = slices.Compact(expectedOwners)

		insert, traverse, localOwners, err := insertf(getStateOwner)
		t.NoError(err)

		for i := range 3 {
			_ = insert(
				uint64(i),
				values[i],
				nil,
			)
		}

		var called atomic.Value

		t.NoError(traverse(func(worker string, subws, affected []string) error {
			t.T().Logf("worker=%q sub=%v affected=%v", worker, subws, affected)

			called.Store(true)

			return nil
		}))

		t.Nil(called.Load())

		o := localOwners()
		slices.Sort(o)
		t.Equal(expectedOwners, o)
	})

	t.Run("only local insert", func() {
		workers := t.workers(9)
		local = workers[0]

		ctx, _ := t.Ctx()

		insertf, _, _, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		values := make([][2]interface{}, len(workers))
		for i := range values {
			values[i] = [2]interface{}{t.randomMergeValues(-1)[0], t.randomMergeValues(1, 2)}
		}

		getStateOwner, stateOwnersClose, err := stateOwnersFuncByCount(workers, 2)
		t.NoError(err)
		defer stateOwnersClose()

		// NOTE preload
		var expectedOwners []string

		dutil.TraverseSlice(values, func(i int, v [2]interface{}) (bool, error) {
			o := getStateOwner(v[0].(StateMergeValue).Key())
			expectedOwners = append(expectedOwners, o...)

			dutil.TraverseSlice(v[1].([]StateMergeValue), func(i int, value StateMergeValue) (bool, error) {
				o := getStateOwner(value.Key())
				expectedOwners = append(expectedOwners, o...)

				return true, nil
			})

			return true, nil
		})

		slices.Sort(expectedOwners)
		expectedOwners = slices.Compact(expectedOwners)

		expectedSubws := map[string][]string{
			"w0": {"w6", "w7"},
			"w1": {"w6", "w7"},
			"w3": {"w0", "w1"},
			"w4": {"w0", "w1"},
			"w6": {"w3", "w4"},
			"w7": {"w3", "w4"},
		}

		expectedAffected := map[string][]string{
			"w0": {"w2", "w3", "w4", "w5"},
			"w1": {"w2", "w3", "w4", "w5"},
			"w3": {"w5", "w6", "w7", "w8"},
			"w4": {"w5", "w6", "w7", "w8"},
			"w6": {"w0", "w1", "w2", "w8"},
			"w7": {"w0", "w1", "w2", "w8"},
		}

		insert, traverse, localOwners, err := insertf(getStateOwner)
		t.NoError(err)

		t.NoError(dutil.TraverseSlice(values, func(i int, v [2]interface{}) (bool, error) {
			_ = insert(
				uint64(i),
				v[0].(StateMergeValue),
				v[1].([]StateMergeValue),
			)

			return true, nil
		}))

		t.NoError(traverse(func(worker string, subws, affected []string) error {
			slices.Sort(subws)
			slices.Sort(affected)
			t.Equal(expectedSubws[worker], subws, worker)
			t.Equal(expectedAffected[worker], affected, worker)

			return nil
		}))

		o := localOwners()
		slices.Sort(o)
		t.Equal(expectedOwners, o)
	})

	t.Run("only local insert; fixed owner", func() {
		workers := t.workers(9)
		local := workers[0]
		pubs := make([]base.Publickey, len(workers))
		owners := map[string][]string{}

		dutil.TraverseSlice(pubs, func(i int, _ base.Publickey) (bool, error) {
			pub := base.NewDummyPublickey(fmt.Sprintf("p%d", i))
			pubs[i] = pub
			owners[t.newMergeValue(pub, 1).Key()] = []string{
				workers[i],
				workers[(i+1)%len(workers)],
			}

			return true, nil
		})

		getStateOwner, stateOwnersClose, err := fixedStateOwnersFunc(owners)
		t.NoError(err)
		defer stateOwnersClose()

		expectedSubs := map[string][]string{}
		expectedAffected := map[string][]string{}

		p := func(opindex int, sub StateMergeValue, adds []StateMergeValue) {
			var subs, affected []string

			dutil.TraverseSlice(getStateOwner(sub.Key()), func(_ int, owner string) (bool, error) {
				subs = append(subs, owner)

				return true, nil
			})

			dutil.TraverseSlice(adds, func(_ int, value StateMergeValue) (bool, error) {
				dutil.TraverseSlice(getStateOwner(value.Key()), func(_ int, owner string) (bool, error) {
					affected = append(affected, owner)

					return true, nil
				})

				return true, nil
			})

			slices.Sort(subs)
			subs = slices.Compact(subs)
			slices.Sort(affected)
			affected = slices.Compact(affected)

			t.Println("op owners: opindex=%d subs=%v affected=%v", opindex, subs, affected)

			dutil.TraverseSlice(subs, func(_ int, worker string) (bool, error) {
				a := expectedAffected[worker]
				a = append(a, affected...)

				expectedAffected[worker] = a

				return true, nil
			})

			dutil.TraverseSlice(affected, func(_ int, worker string) (bool, error) {
				a := expectedSubs[worker]
				a = append(a, subs...)

				expectedSubs[worker] = a

				return true, nil
			})
		}

		values := make([][2][]StateMergeValue, len(pubs)*2)
		dutil.TraverseSlice(pubs, func(i int, pub base.Publickey) (bool, error) {
			sub0 := t.newMergeValue(pub, -1)
			affected0 := []StateMergeValue{
				t.newMergeValue(pubs[(i+1)%len(pubs)], 1),
				t.newMergeValue(pubs[(i+2)%len(pubs)], 1),
			}
			values[i*2] = [2][]StateMergeValue{{sub0}, affected0}

			p(i*2, sub0, affected0)

			sub1 := t.newMergeValue(pubs[(i+1)%len(pubs)], -1)
			affected1 := []StateMergeValue{
				t.newMergeValue(pubs[(i+2)%len(pubs)], 1),
				t.newMergeValue(pubs[(i+3)%len(pubs)], 1),
			}
			values[(i*2)+1] = [2][]StateMergeValue{{sub1}, affected1}

			p((i*2)+1, sub1, affected1)

			return true, nil
		})

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			subs := expectedSubs[worker]
			slices.Sort(subs)
			expectedSubs[worker] = slices.Compact(subs)

			affected := expectedAffected[worker]
			slices.Sort(affected)
			expectedAffected[worker] = slices.Compact(affected)

			t.T().Logf("worker expected: worker=%q subs=%v affected=%v",
				worker, expectedSubs[worker], expectedAffected[worker])

			return true, nil
		})

		var expectedOwners []string
		for i := range slices.Values(values) {
			o := getStateOwner(i[0][0].Key())
			expectedOwners = append(expectedOwners, o...)

			for j := range slices.Values(i[1]) {
				o := getStateOwner(j.Key())
				expectedOwners = append(expectedOwners, o...)
			}
		}

		slices.Sort(expectedOwners)
		expectedOwners = slices.Compact(expectedOwners)

		ctx, _ := t.Ctx()

		insertf, _, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		insert, traverse, localOwners, err := insertf(getStateOwner)
		t.NoError(err)

		t.NoError(dutil.TraverseSlice(values, func(i int, v [2][]StateMergeValue) (bool, error) {
			_ = insert(uint64(i), v[0][0], v[1])

			return true, nil
		}))

		t.NoError(traverse(func(worker string, subws, affected []string) error {
			slices.Sort(subws)
			slices.Sort(affected)

			t.T().Logf("collected: worker=%q subs=%v affected=%v", worker, subws, affected)

			t.Equal(expectedSubs[worker], subws, "sub: %q", worker)
			t.Equal(expectedAffected[worker], affected, "affected: %q", worker)

			return nil
		}))

		o := localOwners()
		slices.Sort(o)
		t.Equal(expectedOwners, o)

		subws, affected, err := done()
		t.NoError(err)
		t.T().Logf("done: subs=%v affected=%v", subws, affected)
	})

	t.Run("local insert with other workers", func() {
		workers := t.workers(9)
		local := workers[0]

		ctx, _ := t.Ctx()

		insertf, add, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		values := make([][2]interface{}, len(workers))
		for i := range values {
			values[i] = [2]interface{}{t.randomMergeValues(-1)[0], t.randomMergeValues(1, 2)}
		}

		getStateOwner, stateOwnersClose, err := stateOwnersFuncByCount(workers, 2)
		t.NoError(err)
		defer stateOwnersClose()

		// NOTE preload
		var expectedOwners []string

		dutil.TraverseSlice(values, func(i int, v [2]interface{}) (bool, error) {
			o := getStateOwner(v[0].(StateMergeValue).Key())
			expectedOwners = append(expectedOwners, o...)

			dutil.TraverseSlice(v[1].([]StateMergeValue), func(i int, value StateMergeValue) (bool, error) {
				o := getStateOwner(value.Key())
				expectedOwners = append(expectedOwners, o...)

				return true, nil
			})
			return true, nil
		})

		slices.Sort(expectedOwners)
		expectedOwners = slices.Compact(expectedOwners)

		expectedSubws := map[string][]string{
			"w0": {"w6", "w7"},
			"w1": {"w6", "w7"},
			"w3": {"w0", "w1"},
			"w4": {"w0", "w1"},
			"w6": {"w3", "w4"},
			"w7": {"w3", "w4"},
		}

		expectedAffected := map[string][]string{
			"w0": {"w2", "w3", "w4", "w5"},
			"w1": {"w2", "w3", "w4", "w5"},
			"w3": {"w5", "w6", "w7", "w8"},
			"w4": {"w5", "w6", "w7", "w8"},
			"w6": {"w0", "w1", "w2", "w8"},
			"w7": {"w0", "w1", "w2", "w8"},
		}

		insert, traverse, localOwners, err := insertf(getStateOwner)
		t.NoError(err)

		t.NoError(dutil.TraverseSlice(values, func(i int, v [2]interface{}) (bool, error) {
			_ = insert(
				uint64(i),
				v[0].(StateMergeValue),
				v[1].([]StateMergeValue),
			)

			return true, nil
		}))

		err = traverse(func(worker string, subws, affected []string) error {
			slices.Sort(subws)
			slices.Sort(affected)
			t.Equal(expectedSubws[worker], subws, worker)
			t.Equal(expectedAffected[worker], affected, worker)

			return nil
		})
		t.NoError(err)

		otherSubws := func() []string {
			o := make([]string, len(workers)-1)

			for i := range workers[1:] {
				o[i] = fmt.Sprintf("s%d", i)
			}

			return o
		}()

		otherAffected := func() []string {
			o := make([]string, len(workers)-1)

			for i := range workers[1:] {
				o[i] = fmt.Sprintf("a%d", i)
			}

			return o
		}()

		dutil.TraverseSlice(workers, func(i int, worker string) (bool, error) {
			if worker == local {
				return true, nil
			}

			t.NoError(add(worker, []string{otherSubws[i-1]}, []string{otherAffected[i-1]}))

			return true, nil
		})

		o := localOwners()
		slices.Sort(o)
		t.Equal(expectedOwners, o)

		subws, affected, err := done()
		t.NoError(err)
		slices.Sort(subws)
		slices.Sort(affected)

		localExpectedSubws := slices.Concat(expectedSubws[local], otherSubws)
		slices.Sort(localExpectedSubws)
		t.Equal(localExpectedSubws, subws)

		localExpectedAffected := slices.Concat(expectedAffected[local], otherAffected)
		slices.Sort(localExpectedAffected)
		t.Equal(localExpectedAffected, affected)
	})

	t.Run("local owners", func() {
		workers := t.workers(9)
		local = workers[0]

		subs := t.randomMergeValues(1, 2)
		addvs := make([][]StateMergeValue, len(subs))

		for i := range subs {
			addvs[i] = t.randomMergeValues(1)
		}

		getStateOwner, stateOwnersClose, err := stateOwnersFuncByCount(workers, 1)
		t.NoError(err)
		defer stateOwnersClose()

		var expectedOwners []string
		for i := range slices.Values(subs) {
			o := getStateOwner(i.Key())
			expectedOwners = append(expectedOwners, o...)
		}

		for i := range slices.Values(addvs) {
			for j := range slices.Values(i) {
				o := getStateOwner(j.Key())
				expectedOwners = append(expectedOwners, o...)
			}
		}

		slices.Sort(expectedOwners)
		expectedOwners = slices.Compact(expectedOwners)

		ctx, _ := t.Ctx()

		insertf, _, _, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		insert, traverse, localOwners, err := insertf(getStateOwner)
		t.NoError(err)

		for i := range subs {
			_ = insert(uint64(i), subs[i], addvs[i])
		}

		t.NoError(traverse(func(worker string, subws, affected []string) error {
			t.T().Logf("worker=%q sub=%v affected=%v", worker, subws, affected)

			return nil
		}))

		o := localOwners()
		slices.Sort(o)
		t.T().Log("local owners:", o)

		t.Equal(expectedOwners, o)
	})
}

func (t *testWorkerStateOwners) TestAdd() {
	local := "local"

	t.Run("empty", func() {
		ctx, _ := t.Ctx()

		_, _, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		subws, affected, err := done()
		t.NoError(err)
		t.Empty(subws)
		t.Empty(affected)
	})

	t.Run("add async", func() {
		expectedSubs := t.owners(9, "s")
		expectedAffected := t.owners(9, "a")

		ctx, _ := t.Ctx()

		_, add, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		var wg sync.WaitGroup

		for i := range 3 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				t.IfErrPanic(add(
					fmt.Sprintf("w%d", i),
					expectedSubs[i*3:(i+1)*3],
					expectedAffected[i*3:(i+1)*3],
				))
			}()
		}

		wg.Wait()

		subws, affected, err := done()
		t.NoError(err)
		slices.Sort(subws)
		t.Equal(expectedSubs, subws)

		slices.Sort(affected)
		t.Equal(expectedAffected, affected)
	})

	t.Run("over max size", func() {
		expectedSubs := t.owners(9, "s")
		expectedAffected := t.owners(9, "a")

		ctx, _ := t.Ctx()

		_, add, done, err := newFuncWorkerStateOwners(ctx, local, 1)
		t.NoError(err)

		var wg sync.WaitGroup

		for i := range 3 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				t.IfErrPanic(add(
					fmt.Sprintf("w%d", i),
					expectedSubs[i*3:(i+1)*3],
					expectedAffected[i*3:(i+1)*3],
				))
			}()
		}

		wg.Wait()

		subws, affected, err := done()
		t.NoError(err)
		slices.Sort(subws)
		t.Equal(expectedSubs, subws)

		slices.Sort(affected)
		t.Equal(expectedAffected, affected)
	})

	t.Run("duplicated", func() {
		expectedSubs := t.owners(9, "s")
		expectedAffected := t.owners(9, "a")

		ctx, _ := t.Ctx()

		_, add, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		var wg sync.WaitGroup

		for i := range 3 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				t.IfErrPanic(add(fmt.Sprintf("w%d", i), expectedSubs, expectedAffected))
			}()
		}

		wg.Wait()

		subws, affected, err := done()
		t.NoError(err)
		slices.Sort(subws)
		t.Equal(expectedSubs, subws)

		slices.Sort(affected)
		t.Equal(expectedAffected, affected)
	})
}

func TestWorkerStateOwners(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerStateOwners))
}
