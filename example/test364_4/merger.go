package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/singleflight"
)

type StateMergeValue interface {
	Key() string
	Value() base.StateValue
	Merger(prev base.StateValue) (StateMerger, error)
}

type StateMerger interface {
	// Key is state key.
	Key() string
	// Close sets StateMerger not to get no more Merge or Unmerge.
	Close() (mergedValue base.StateValue, _ error)
	// Clone clones StateMerger itself.
	Clone() StateMerger
	// Merge merges new (partial) state value to state. If failed to merge,
	// *reason* describes the reason why merge(unmerge) opeartion is not merged.
	// *unmergeOp* is to be unmerge after merging the new state value.
	//
	// - If *commit* is nil and *reason* is not empty, *reason* indicates
	// the *opindex* operation is not merged with *reason*.
	// - If *unmergeOp* is not same with the *opindex* opeation, *reason*
	// indicates the *unmergeOp* is not merged with *reason*.
	Merge(opindex uint64, value base.StateValue) (
		commit func() error,
		unmergeOp uint64,
		reason string,
		_ error,
	)
	// Unmerge cancels merged state value. mergeOp is the operation index
	// in proposal to be merge after canceling the state value. *reason*
	// describes the reason why operation is unmerged. It can be empty without
	// error.
	//
	// - If *mergeOp* is same with the operation index, it means there is
	// nothing to cancel to merge.
	Unmerge(opindex uint64, value base.StateValue) (
		commit func() error,
		mergeOp uint64,
		_ error,
	)
}

type xMergeOpFunc func(uint64, base.StateValue) (
	commit func() error,
	next uint64,
	reason string,
	_ error,
)

func xMergeOpValues(
	ctx context.Context,
	name string,
	opindex uint64,
	values []StateMergeValue,
	getMerger func(StateMergeValue) (StateMerger, error),
	xmergeFunc func(StateMerger) xMergeOpFunc,
) (
	nextOps []uint64,
	reason string,
	_ error,
) {
	if len(values) < 1 {
		return nil, "", errors.Errorf("xmergeOp; empty values")
	}

	fmt.Println(">x", name, opindex)

	var (
		addNext    util.CompactAppendSliceAddFunc[uint64]
		doneNext   util.CompactAppendSliceDoneFunc[uint64]
		addReason  util.CompactAppendSliceAddFunc[string]
		doneReason util.CompactAppendSliceDoneFunc[string]
		addCommit  util.CompactAppendSliceAddFunc[func() error]
		doneCommit util.CompactAppendSliceDoneFunc[func() error]
	)

	switch a, d, err := UniqueCompactAppendSliceV1[uint64](uint64(len(values))); {
	case err != nil:
		return nil, "", err
	default:
		addNext = a
		doneNext = d
	}

	switch a, d, err := util.CompactAppendSliceV1[string](uint64(len(values))); { // FIXME unique string
	case err != nil:
		return nil, "", err
	default:
		defer func() {
			_ = d(nil)
		}()

		addReason = a
		doneReason = d
	}

	switch a, d, err := util.CompactAppendSliceV1[func() error](uint64(len(values))); {
	case err != nil:
		return nil, "", err
	default:
		defer func() {
			_ = d(nil)
		}()

		addCommit = a
		doneCommit = d
	}

	var notCommit bool

	if err := util.TraverseSliceV1(values, func(_ int, value StateMergeValue) (bool, error) {
		var merger StateMerger

		switch i, err := getMerger(value); {
		case err != nil:
			return false, err
		default:
			merger = i
		}

		switch c, nextOp, r, err := xmergeFunc(merger)(opindex, value.Value()); {
		case err != nil:
			return false, err
		case c == nil:
			if len(r) > 0 {
				addReason(r)
			}

			notCommit = true

			return false, nil
		default:
			if len(r) > 0 {
				addReason(r)
			}

			if nextOp != opindex {
				addNext(nextOp)
			}

			if c != nil {
				addCommit(c)
			}

			return true, nil
		}
	}); err != nil {
		return nil, "", err
	}

	_ = doneReason(func(s []string) error {
		if len(s) < 1 {
			return nil
		}

		reason = strings.Join(s, "; ")

		return nil
	})

	if notCommit {
		fmt.Println("<x", name, opindex)

		return nil, reason, nil
	}

	_ = doneNext(func(s []uint64) error {
		nextOps = slices.Clone(s)

		return nil
	})

	fmt.Println("<x", name, opindex, nextOps, reason)

	return nextOps, reason, doneCommit(func(s []func() error) error {
		switch size := int64(len(s)); {
		case size < 1:
			return nil
		case size < 2:
			return s[0]()
		}

		wk, _ := NewBaseJobWorker(ctx, int64(len(s)))
		defer wk.Close()

		if err := util.TraverseSliceV1(s, func(_ int, f func() error) (bool, error) {
			return true, wk.NewJob(func(context.Context) error {
				return f()
			})
		}); err != nil {
			return err
		}

		wk.JobDone()

		return wk.Wait()
	})
}

func mergeOp(
	ctx context.Context,
	opindex uint64,
	values []StateMergeValue,
	getMerger func(StateMergeValue) (StateMerger, error),
) (
	nextUnmergeOps []uint64,
	reason string,
	_ error,
) {
	nextUnmergeOps, reason, err := xMergeOpValues(
		ctx,
		"merge",
		opindex,
		values,
		getMerger,
		func(merger StateMerger) xMergeOpFunc {
			return merger.Merge
		},
	)

	return nextUnmergeOps, reason, err
}

func unmergeOp(
	ctx context.Context,
	opindex uint64,
	values []StateMergeValue,
	getMerger func(StateMergeValue) (StateMerger, error),
) (
	nextMergeOps []uint64,
	_ error,
) {
	nextMergeOp, _, err := xMergeOpValues(
		ctx,
		"unmerge",
		opindex,
		values,
		getMerger,
		func(merger StateMerger) xMergeOpFunc {
			return func(opindex uint64, value base.StateValue) (
				func() error,
				uint64,
				string,
				error,
			) {
				c, n, err := merger.Unmerge(opindex, value)

				return c, n, "", err
			}
		},
	)

	return nextMergeOp, err
}

func extractStateValuesKeysFunc(cacheSize int) (
	func(opindex uint64, values []StateMergeValue) (keys []string),
	func(),
) {
	cache := util.NewLFUGCache[uint64, []string](cacheSize)

	extract := func(values []StateMergeValue) (keys []string) {
		if len(values) < 1 {
			return nil
		}

		add, done, _ := util.CompactAppendSliceV1[string](uint64(len(values)))

		_ = util.TraverseSliceV1(values, func(_ int, v StateMergeValue) (bool, error) {
			add(v.Key())

			return true, nil
		})

		_ = done(func(s []string) error {
			keys = slices.Clone(s)

			return nil
		})

		return keys
	}

	return func(opindex uint64, values []StateMergeValue) (keys []string) {
			var sg singleflight.Group

			_, _, _ = sg.Do(fmt.Sprintf("%d", opindex), func() (interface{}, error) {
				if i, found := cache.Get(opindex); found {
					keys = i

					return nil, nil
				}

				keys = extract(values)
				cache.Set(opindex, keys, 0)

				return nil, nil
			})

			return keys
		}, func() {
			cache.Purge()
		}
}

func getOrCreateMergerFunc(
	m util.LockedMap[string, StateMerger],
	getState func(stateKey string) (base.State, bool, error),
) func(StateMergeValue) (StateMerger, error) {
	return func(value StateMergeValue) (merger StateMerger, _ error) {
		err := m.GetOrCreate(
			value.Key(),
			func(i StateMerger, _ bool) error {
				merger = i

				return nil
			},
			func() (StateMerger, error) {
				var stv base.StateValue

				switch i, found, err := getState(value.Key()); {
				case err != nil:
					return nil, err
				case found:
					stv = i.Value()
				}

				return value.Merger(stv)
			},
		)

		return merger, err
	}
}
