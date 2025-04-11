package dop5

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/spikeekips/mitum/base"
	dhash "github.com/spikeekips/mitum/example/test364_5/hash"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
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

func (t *testWorkerStateOwners) TestInsert() {
	local := "local"

	t.Run("no insert", func() {
		workers := t.workers(9)

		ctx, _ := t.Ctx()

		insertf, _, done, err := newFuncWorkerStatesOwners(ctx, local, 1<<9)
		t.NoError(err)

		getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, 2)
		t.NoError(err)
		defer stateOwnersClose()

		_, traverse, localOwners, err := insertf(getStateOwners)
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

		var values [][]SMV
		for range 3 {
			values = append(values, t.randomMergeValues(1, 2))
		}

		ctx, _ := t.Ctx()

		insertf, _, _, err := newFuncWorkerStatesOwners(ctx, local, 1<<9)
		t.NoError(err)

		getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, 1)
		t.NoError(err)
		defer stateOwnersClose()

		var expectedOwners []string
		for i := range slices.Values(values) {
			for j := range slices.Values(i) {
				o := getStateOwners(j.Key())
				expectedOwners = append(expectedOwners, o...)
			}
		}

		slices.Sort(expectedOwners)
		expectedOwners = slices.Compact(expectedOwners)

		insert, traverse, localOwners, err := insertf(getStateOwners)
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

		var values []SMV
		for range 3 {
			_, v := t.randomMergeValue(1)
			values = append(values, v)
		}

		ctx, _ := t.Ctx()

		insertf, _, _, err := newFuncWorkerStatesOwners(ctx, local, 1<<9)
		t.NoError(err)

		getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, 2)
		t.NoError(err)
		defer stateOwnersClose()

		var expectedOwners []string
		for i := range slices.Values(values) {
			o := getStateOwners(i.Key())
			expectedOwners = append(expectedOwners, o...)
		}

		slices.Sort(expectedOwners)
		expectedOwners = slices.Compact(expectedOwners)

		insert, traverse, localOwners, err := insertf(getStateOwners)
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

		insertf, _, _, err := newFuncWorkerStatesOwners(ctx, local, 1<<9)
		t.NoError(err)

		values := make([][2]interface{}, len(workers))
		for i := range values {
			values[i] = [2]interface{}{t.randomMergeValues(-1)[0], t.randomMergeValues(1, 2)}
		}

		getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, 2)
		t.NoError(err)
		defer stateOwnersClose()

		// NOTE preload
		var expectedOwners []string

		for v := range slices.Values(values) {
			o := getStateOwners(v[0].(SMV).Key())
			expectedOwners = append(expectedOwners, o...)

			for value := range slices.Values(v[1].([]SMV)) {
				o := getStateOwners(value.Key())
				expectedOwners = append(expectedOwners, o...)
			}
		}

		slices.Sort(expectedOwners)
		expectedOwners = slices.Compact(expectedOwners)

		expectedSubAffected := map[string][2][]string{
			"w0": {{"w6", "w7"}, {"w2", "w3", "w4", "w5"}},
			"w1": {{"w6", "w7"}, {"w2", "w3", "w4", "w5"}},
			"w2": {{"w0", "w1", "w6", "w7"}, nil},
			"w3": {{"w0", "w1"}, {"w5", "w6", "w7", "w8"}},
			"w4": {{"w0", "w1"}, {"w5", "w6", "w7", "w8"}},
			"w5": {{"w0", "w1", "w3", "w4"}, nil},
			"w6": {{"w3", "w4"}, {"w0", "w1", "w2", "w8"}},
			"w7": {{"w3", "w4"}, {"w0", "w1", "w2", "w8"}},
			"w8": {{"w3", "w4", "w6", "w7"}, nil},
		}

		insert, traverse, localOwners, err := insertf(getStateOwners)
		t.NoError(err)

		for i, v := range slices.All(values) {
			_ = insert(
				uint64(i),
				v[0].(SMV),
				v[1].([]SMV),
			)
		}

		t.NoError(traverse(func(worker string, subws, affected []string) error {
			slices.Sort(subws)
			slices.Sort(affected)

			t.EqualValues(expectedSubAffected[worker][0], subws, worker)
			t.EqualValues(expectedSubAffected[worker][1], affected, worker)

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

		for i := range pubs {
			pub := base.NewDummyPublickey(fmt.Sprintf("p%d", i))
			pubs[i] = pub
			owners[t.newMergeValue(pub, 1).Key()] = []string{
				workers[i],
				workers[(i+1)%len(workers)],
			}
		}

		getStateOwners, stateOwnersClose, err := fixedStateOwnersFunc(owners)
		t.NoError(err)
		defer stateOwnersClose()

		expectedSubs := map[string][]string{}
		expectedAffected := map[string][]string{}

		p := func(opindex int, sub SMV, adds []SMV) {
			var subs, affected []string

			for owner := range slices.Values(getStateOwners(sub.Key())) {
				subs = append(subs, owner)
			}

			for value := range slices.Values(adds) {
				for owner := range slices.Values(getStateOwners(value.Key())) {
					affected = append(affected, owner)
				}
			}

			slices.Sort(subs)
			subs = slices.Compact(subs)
			slices.Sort(affected)
			affected = slices.Compact(affected)

			t.Print("op owners: opindex=%d subs=%v affected=%v", opindex, subs, affected)

			for worker := range slices.Values(subs) {
				a := expectedAffected[worker]
				a = append(a, affected...)

				expectedAffected[worker] = a
			}

			for worker := range slices.Values(affected) {
				a := expectedSubs[worker]
				a = append(a, subs...)

				expectedSubs[worker] = a
			}
		}

		values := make([][2][]SMV, len(pubs)*2)

		for i, pub := range slices.All(pubs) {
			sub0 := t.newMergeValue(pub, -1)
			affected0 := []SMV{
				t.newMergeValue(pubs[(i+1)%len(pubs)], 1),
				t.newMergeValue(pubs[(i+2)%len(pubs)], 1),
			}
			values[i*2] = [2][]SMV{{sub0}, affected0}

			p(i*2, sub0, affected0)

			sub1 := t.newMergeValue(pubs[(i+1)%len(pubs)], -1)
			affected1 := []SMV{
				t.newMergeValue(pubs[(i+2)%len(pubs)], 1),
				t.newMergeValue(pubs[(i+3)%len(pubs)], 1),
			}
			values[(i*2)+1] = [2][]SMV{{sub1}, affected1}

			p((i*2)+1, sub1, affected1)
		}

		for worker := range slices.Values(workers) {
			subs := expectedSubs[worker]
			slices.Sort(subs)
			expectedSubs[worker] = slices.Compact(subs)

			affected := expectedAffected[worker]
			slices.Sort(affected)
			expectedAffected[worker] = slices.Compact(affected)

			t.T().Logf("worker expected: worker=%q subs=%v affected=%v",
				worker, expectedSubs[worker], expectedAffected[worker])
		}

		var expectedOwners []string
		for i := range slices.Values(values) {
			o := getStateOwners(i[0][0].Key())
			expectedOwners = append(expectedOwners, o...)

			for j := range slices.Values(i[1]) {
				o := getStateOwners(j.Key())
				expectedOwners = append(expectedOwners, o...)
			}
		}

		slices.Sort(expectedOwners)
		expectedOwners = slices.Compact(expectedOwners)

		ctx, _ := t.Ctx()

		insertf, _, done, err := newFuncWorkerStatesOwners(ctx, local, 1<<9)
		t.NoError(err)

		insert, traverse, localOwners, err := insertf(getStateOwners)
		t.NoError(err)

		for i, v := range slices.All(values) {
			_ = insert(uint64(i), v[0][0], v[1])
		}

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

		insertf, add, done, err := newFuncWorkerStatesOwners(ctx, local, 1<<9)
		t.NoError(err)

		values := make([][2]interface{}, len(workers))
		for i := range values {
			values[i] = [2]interface{}{t.randomMergeValues(-1)[0], t.randomMergeValues(1, 2)}
		}

		getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, 2)
		t.NoError(err)
		defer stateOwnersClose()

		// NOTE preload
		var expectedOwners []string

		for v := range slices.Values(values) {
			o := getStateOwners(v[0].(SMV).Key())
			expectedOwners = append(expectedOwners, o...)

			for value := range slices.Values(v[1].([]SMV)) {
				o := getStateOwners(value.Key())
				expectedOwners = append(expectedOwners, o...)
			}
		}

		slices.Sort(expectedOwners)
		expectedOwners = slices.Compact(expectedOwners)

		expectedSubAffected := map[string][2][]string{
			"w0": {{"w6", "w7"}, {"w2", "w3", "w4", "w5"}},
			"w2": {{"w0", "w1", "w6", "w7"}, nil},
			"w3": {{"w0", "w1"}, {"w5", "w6", "w7", "w8"}},
			"w1": {{"w6", "w7"}, {"w2", "w3", "w4", "w5"}},
			"w4": {{"w0", "w1"}, {"w5", "w6", "w7", "w8"}},
			"w5": {{"w0", "w1", "w3", "w4"}, nil},
			"w6": {{"w3", "w4"}, {"w0", "w1", "w2", "w8"}},
			"w8": {{"w3", "w4", "w6", "w7"}, nil},
			"w7": {{"w3", "w4"}, {"w0", "w1", "w2", "w8"}},
		}

		insert, traverse, localOwners, err := insertf(getStateOwners)
		t.NoError(err)

		for i, v := range slices.All(values) {
			_ = insert(
				uint64(i),
				v[0].(SMV),
				v[1].([]SMV),
			)
		}

		err = traverse(func(worker string, subws, affected []string) error {
			slices.Sort(subws)
			slices.Sort(affected)

			t.EqualValues(expectedSubAffected[worker][0], subws, worker)
			t.EqualValues(expectedSubAffected[worker][1], affected, worker)

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

		for i, worker := range slices.All(workers) {
			if worker == local {
				continue
			}

			t.NoError(add(worker, []string{otherSubws[i-1]}, []string{otherAffected[i-1]}))
		}

		o := localOwners()
		slices.Sort(o)
		t.Equal(expectedOwners, o)

		subws, affected, err := done()
		t.NoError(err)
		slices.Sort(subws)
		slices.Sort(affected)

		localExpectedSubws := slices.Concat(expectedSubAffected[local][0], otherSubws)
		slices.Sort(localExpectedSubws)
		t.Equal(localExpectedSubws, subws)

		localExpectedAffected := slices.Concat(expectedSubAffected[local][1], otherAffected)
		slices.Sort(localExpectedAffected)
		t.Equal(localExpectedAffected, affected)
	})

	t.Run("local owners", func() {
		workers := t.workers(9)
		local = workers[0]

		subs := t.randomMergeValues(1, 2)
		addvs := make([][]SMV, len(subs))

		for i := range subs {
			addvs[i] = t.randomMergeValues(1)
		}

		getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, 1)
		t.NoError(err)
		defer stateOwnersClose()

		var expectedOwners []string
		for i := range slices.Values(subs) {
			o := getStateOwners(i.Key())
			expectedOwners = append(expectedOwners, o...)
		}

		for i := range slices.Values(addvs) {
			for j := range slices.Values(i) {
				o := getStateOwners(j.Key())
				expectedOwners = append(expectedOwners, o...)
			}
		}

		slices.Sort(expectedOwners)
		expectedOwners = slices.Compact(expectedOwners)

		ctx, _ := t.Ctx()

		insertf, _, _, err := newFuncWorkerStatesOwners(ctx, local, 1<<9)
		t.NoError(err)

		insert, traverse, localOwners, err := insertf(getStateOwners)
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

		_, _, done, err := newFuncWorkerStatesOwners(ctx, local, 1<<9)
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

		_, add, done, err := newFuncWorkerStatesOwners(ctx, local, 1<<9)
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

		_, add, done, err := newFuncWorkerStatesOwners(ctx, local, 1)
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

		_, add, done, err := newFuncWorkerStatesOwners(ctx, local, 1<<9)
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

func (t *testWorkerStateOwners) TestCases() {
	t.Run("sub and adds", func() {
		/*
			     related   | ops | subs  | affected
			w0 | 0 2 3 4   | 0 3 | w1 w2 | w1
			w1 | 0 1 2 3 5 | 1 4 | w0    | w0 w2
			w2 | 1 4 5     | 2 5 | w1    | w0
		*/

		pub0 := base.NewDummyPublickey("p0")
		pub1 := base.NewDummyPublickey("p1")
		pub2 := base.NewDummyPublickey("p2")

		valuesSet := [][]SMV{
			t.newMergeValues(pub0, 1, pub1, -2), // 0
			t.newMergeValues(pub1, -2, pub2, 3), // 1: ignored
			t.newMergeValues(pub0, -1, pub1, 2), // 2
			t.newMergeValues(pub0, -1, pub1, 2), // 3: ignored
			t.newMergeValues(pub0, 1, pub2, -3), // 4
			t.newMergeValues(pub1, 1, pub2, 2),  // 5
		}

		expectedOwners := map[string][2][]string{
			"w0": {{"w1", "w2"}, {"w1"}},
			"w1": {{"w0"}, {"w0", "w2"}},
			"w2": {{"w1"}, {"w0"}},
		}

		s := slices.Collect(maps.Keys(expectedOwners))
		slices.Sort(s)

		for worker := range slices.Values(s) {
			owners := expectedOwners[worker]

			t.T().Logf("worker owners: worker=%q sub=%v affected=%v",
				worker, owners[0], owners[1])
		}

		workers := t.workers(3)

		getStateOwners := func(stateKey string) (owners []string) {
			var owner string

			switch stateKey {
			case accountStateKey(pub0):
				owner = workers[0]
			case accountStateKey(pub1):
				owner = workers[1]
			case accountStateKey(pub2):
				owner = workers[2]
			default:
				t.Panicf(nil, "unknown owner, %q", stateKey)
			}

			return []string{owner}
		}

		t.Run("insert all local", func() {
			local := workers[0]

			ctx, _ := t.Ctx()

			insertf, _, _, err := newFuncWorkerStatesOwners(ctx, local, 1<<9)
			t.NoError(err)

			insert, traverse, localOwners, err := insertf(getStateOwners)
			t.NoError(err)

			for i, vs := range slices.All(valuesSet) {
				var sub SMV
				var adds []SMV

				for v := range slices.Values(vs) {
					if v.(*AccountSMV).Balance < 0 {
						sub = v
					} else {
						adds = append(adds, v)
					}
				}

				opindex := uint64(i)
				f := insert(opindex, sub, adds)
				t.NoError(f(func(worker string, values []SMV) error {
					stateKeys := extractSMVKeysFromValues(values)

					t.T().Logf("insert worker: local=%q worker=%q opindex=%d state keys=%v", local, worker, opindex, stateKeys)

					return nil
				}))
			}

			t.NoError(traverse(func(worker string, subws, affected []string) error {
				t.T().Logf("owners: local=%q worker=%q sub=%v affected=%v", local, worker, subws, affected)

				slices.Sort(subws)
				slices.Sort(affected)

				t.Equal(expectedOwners[worker][0], subws)
				t.Equal(expectedOwners[worker][1], affected)

				return nil
			}))
			t.Equal(workers, func() []string {
				s := localOwners()
				slices.Sort(s)

				return s
			}())
		})

		t.Run("insert to workers", func() {
			ophs := make([][2]util.Hash, len(valuesSet))
			for i := range valuesSet {
				ophs[i] = [2]util.Hash{dhash.RandomHex64(), dhash.RandomHex64()}
			}

			var opsWorkers []string
			var workersOps workerOperationsFunc

			{
				o, w, err := opsNShareWorkers(workers, 1, ophs)
				t.NoError(err)
				opsWorkers = opsWorkersFromSeq(o)
				workersOps = w
			}

			ctx, _ := t.Ctx()

			inserts := map[string]extractStateOwnersSMVsFunc{}
			traverses := map[string]dutil.InErrorFunc[workerStatesOwnersSetFunc]{}
			localOwners := map[string]dutil.OutFunc[[]string]{}

			for worker := range slices.Values(workers) {
				insertf, _, _, err := newFuncWorkerStatesOwners(ctx, worker, 1<<9)
				t.NoError(err)

				inserts[worker], traverses[worker], localOwners[worker], err = insertf(getStateOwners)
				t.NoError(err)
			}

			for local := range slices.Values(opsWorkers) {
				t.NoError(workersOps(local, func(opindex uint64, _, _ util.Hash) error {
					var sub SMV
					var adds []SMV

					for v := range slices.Values(valuesSet[opindex]) {
						if v.(*AccountSMV).Balance < 0 {
							sub = v
						} else {
							adds = append(adds, v)
						}
					}

					f := inserts[local](opindex, sub, adds)

					return f(func(worker string, values []SMV) error {
						stateKeys := extractSMVKeysFromValues(values)

						t.T().Logf("insert worker: local=%q worker=%q opindex=%d state keys=%v", local, worker, opindex, stateKeys)

						return nil
					})
				}))
			}

			collectedOwners := map[string][2][]string{}

			for local, f := range maps.All(traverses) {
				t.NoError(f(func(worker string, subws, affected []string) error {
					t.T().Logf("owners: local=%q worker=%q sub=%v affected=%v", local, worker, subws, affected)

					j := collectedOwners[worker]

					j[0] = append(j[0], subws...)
					j[1] = append(j[1], affected...)

					collectedOwners[worker] = j

					return nil
				}))
			}

			s := slices.Collect(maps.Keys(collectedOwners))
			slices.Sort(s)

			for worker := range slices.Values(s) {
				os := collectedOwners[worker]
				subws := os[0]
				affected := os[1]

				slices.Sort(subws)
				slices.Sort(affected)
				subws = slices.Compact(subws)
				affected = slices.Compact(affected)

				t.T().Logf("collected owners: worker=%q sub=%v affected=%v", worker, subws, affected)

				t.Equal(expectedOwners[worker][0], subws)
				t.Equal(expectedOwners[worker][1], affected)
			}
		})
	})
}

func TestWorkerStateOwners(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerStateOwners))
}
