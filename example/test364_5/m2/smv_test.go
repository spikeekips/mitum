package m2

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerDistributeSMVs struct {
	baseMergerSuite
}

func (t *testWorkerDistributeSMVs) TestNew() {
	t.Run("empty workers", func() {
		_, _, _, err := newWorkerDistributeSMVs(context.Background(), nil, nil, nil)
		t.ErrorContains(err, "empty size")
	})
}

func (t *testWorkerDistributeSMVs) newf(workers []string) (
	NewWorkerDistributeSMVsFunc,
	func(string) map[uint64][2][]SMV,
	func(string) ([]string, []string),
) {
	smvs := dutil.NewSingleLockedMap[string, map[uint64][2][]SMV]()
	owners := dutil.NewSingleLockedMap[string, [2][]string]()

	return func(_ context.Context, worker string) (
			mergerMergeSMVsFunc,
			workerWorkerStateOwnersFunc,
			error,
		) {
			if slices.Index(workers, worker) < 0 {
				return nil, nil, util.ErrNotFound.Errorf("unknown worker")
			}

			return func(opindex uint64, subv SMV, addvs []SMV) error {
					smvs.Set(worker, func(m map[uint64][2][]SMV, _ bool) (map[uint64][2][]SMV, error) {
						if m == nil {
							m = map[uint64][2][]SMV{}
						}

						m[opindex] = [2][]SMV{{subv}, addvs}

						return m, nil
					})

					return nil
				},
				func(subs, affected []string) error {
					owners.Set(worker, func(m [2][]string, _ bool) ([2][]string, error) {
						return [2][]string{subs, affected}, nil
					})

					return nil
				},
				nil
		},
		func(worker string) map[uint64][2][]SMV {
			i, _ := smvs.Value(worker)

			return i
		},
		func(worker string) ([]string, []string) {
			i, _ := owners.Value(worker)

			return i[0], i[1]
		}
}

func (t *testWorkerDistributeSMVs) TestDistribute() {
	t.Run("empty add", func() {
		_, workers := t.workers(3)

		distribute, getSmvs, getOwners := t.newf(workers)

		_, done, wait, err := newWorkerDistributeSMVs(
			context.Background(),
			workers,
			NewFuncGetStateOwnersMurmur3(workers, 1),
			distribute,
		)
		t.NoError(err)

		done()
		allOwners, delayed, err := wait(nil)
		t.NoError(err)

		asubs, aaffected := allOwners()
		t.Empty(asubs)
		t.Empty(aaffected)
		t.NoError(delayed())

		for worker := range slices.Values(workers) {
			smvs := getSmvs(worker)
			t.Empty(smvs)

			subs, affected := getOwners(worker)
			t.Empty(subs)
			t.Empty(affected)
		}
	})

	t.Run("one worker", func() {
		local, workers := t.workers(1)

		smvs := make([][2][]SMV, 3)
		for i := range smvs {
			subv, addvs := t.randomMergeValuesWithSub(1)
			smvs[i] = [2][]SMV{{subv}, addvs}
		}

		distribute, getSmvs, getOwners := t.newf(workers)

		add, done, wait, err := newWorkerDistributeSMVs(
			context.Background(),
			workers,
			func(stateKey string) []string {
				return []string{local}
			},
			distribute,
		)
		t.NoError(err)

		for i, v := range slices.All(smvs) {
			t.NoError(add(
				uint64(i),
				v[0][0],
				v[1],
			))
		}

		done()
		allOwners, delayed, err := wait(nil)
		t.NoError(err)

		asubs, aaffected := allOwners()
		t.Equal([]string{local}, asubs)
		t.Equal([]string{local}, aaffected)

		t.NoError(delayed())

		for worker := range slices.Values(workers) {
			csmvs := getSmvs(worker)
			t.Equal(len(smvs), len(csmvs))

			for opindex, vs := range maps.All(csmvs) {
				t.Equal(smvs[opindex], vs)
			}

			subs, affected := getOwners(worker)
			t.Equal(workers, subs)
			t.Equal(workers, affected)
		}
	})

	t.Run("3 worker", func() {
		workers := []string{"w0", "w1", "w2"}

		owners := map[string]string{}
		pubs := make([]base.Publickey, 3)

		for i := range pubs {
			pub := base.NewDummyPublickey(fmt.Sprintf("p%d", i))
			pubs[i] = pub

			owners[accountStateKey(pub)] = workers[i]
		}

		smvs := make([][2][]SMV, 3)
		for i := range smvs {
			subv := t.newMergeValue(pubs[i], -1)
			addv := t.newMergeValue(pubs[(i+1)%len(pubs)], 1)

			smvs[i] = [2][]SMV{{subv}, {addv}}
		}

		getStateOwners := func(stateKey string) []string {
			i, found := owners[stateKey]

			if !found {
				panic(fmt.Sprintf("unknown state key, %q", stateKey))
			}

			return []string{i}
		}

		expectedSMVs := map[string]map[uint64][2][]SMV{}
		expectedOwners := map[string][2][]string{}
		var expectedAllOwners [2][]string
		{
			add, getOwners, allOwners, c, err := newWorkerAssignSMVs(uint64(len(workers)), getStateOwners)
			t.NoError(err)
			defer c()

			for i, vs := range slices.All(smvs) {
				opindex := uint64(i)

				var subv SMV
				if len(vs[0]) > 0 {
					subv = vs[0][0]
				}

				seq, err := add(opindex, subv, vs[1])
				t.NoError(err)

				for worker, wvs := range seq {
					m := expectedSMVs[worker]
					if m == nil {
						m = map[uint64][2][]SMV{}
					}

					m[opindex] = wvs
					expectedSMVs[worker] = m
				}
			}

			for worker := range slices.Values(workers) {
				subs, affected := getOwners(worker)
				slices.Sort(subs)
				slices.Sort(affected)
				expectedOwners[worker] = [2][]string{subs, affected}
			}

			asubs, aaffected := allOwners()
			slices.Sort(asubs)
			slices.Sort(aaffected)
			expectedAllOwners = [2][]string{asubs, aaffected}
		}

		distribute, getSmvs, getOwners := t.newf(workers)

		add, done, wait, err := newWorkerDistributeSMVs(
			context.Background(),
			workers,
			getStateOwners,
			distribute,
		)
		t.NoError(err)

		for i, v := range slices.All(smvs) {
			t.NoError(add(
				uint64(i),
				v[0][0],
				v[1],
			))
		}

		done()

		allOwners, delayed, err := wait(nil)
		t.NoError(err)

		asubs, aaffected := allOwners()
		slices.Sort(asubs)
		slices.Sort(aaffected)
		t.Equal(expectedAllOwners[0], asubs)
		t.Equal(expectedAllOwners[1], aaffected)

		t.NoError(delayed())

		for worker := range slices.Values(workers) {
			csmvs := getSmvs(worker)
			t.T().Logf("worker=%q expected=%v smvs=%v", worker,
				t.Marshal(expectedSMVs[worker]),
				t.Marshal(csmvs),
			)

			t.Equal(expectedSMVs[worker], csmvs)

			subs, affected := getOwners(worker)
			t.T().Logf("worker=%q expected=%v owners=%v", worker,
				expectedOwners[worker],
				[2][]string{subs, affected},
			)
		}
	})
}

func TestWorkerDistributeSMVs(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerDistributeSMVs))
}
