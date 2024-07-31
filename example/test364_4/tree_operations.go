package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type (
	newOperationsTreeNotInStateFunc func(notInStates []uint64) error
	newOperationsTreeNewTreeFunc    func(error) (fixedtree.Tree, error)
	newOperationsTreeFuncs          func(_ context.Context, ops [][2]util.Hash) (newOperationsTreeNotInStateFunc, newOperationsTreeNewTreeFunc, error)
)

func newOperationsTreeWriterFunc(
	newJobWorker newJobWorkerFunc,
) newOperationsTreeFuncs {
	return func(ctx context.Context, ops [][2]util.Hash) (newOperationsTreeNotInStateFunc, newOperationsTreeNewTreeFunc, error) {
		wctx, wctxCancel := ContextWithCancelCause(ctx)

		if len(ops) < 1 {
			return func([]uint64) error {
					if err := context.Cause(wctx); err != nil {
						return errors.WithStack(err)
					}

					return wctxCancel(util.ErrFound.Errorf("empty ops, but tried 'not in states'"))
				},
				func(err error) (tr fixedtree.Tree, _ error) {
					if err := context.Cause(wctx); err != nil {
						return tr, errors.WithStack(err)
					}

					if err != nil {
						return tr, wctxCancel(err)
					}

					return tr, wctxCancel(nil)
				},
				nil
		}

		opsSize := uint64(len(ops))

		var tw *fixedtree.Writer

		switch i, err := fixedtree.NewWriter(base.OperationFixedtreeHint, opsSize); {
		case err != nil:
			return nil, nil, wctxCancel(err)
		default:
			tw = i
		}

		var treeAdd func(uint64, fixedtree.Node) error
		var treeAddDone func()

		switch a, d, err := treeAddOperationNodeFunc(opsSize, tw.Add); {
		case err != nil:
			return nil, nil, wctxCancel(err)
		default:
			treeAdd = a
			treeAddDone = d
		}

		var jw JobWorker

		switch i, err := newJobWorker(wctx); {
		case err != nil:
			return nil, nil, wctxCancel(err)
		default:
			defer i.Close()

			jw = i
		}

		if err := util.TraverseSliceV1(ops, func(i int, hs [2]util.Hash) (bool, error) {
			return true, jw.NewJob(func(context.Context) error {
				return tw.Add(uint64(i), base.NewInStateOperationFixedtreeNode(hs[1], ""))
			})
		}); err != nil {
			return nil, nil, wctxCancel(err)
		}

		jw.JobDone()

		if err := jw.Wait(); err != nil {
			return nil, nil, wctxCancel(err)
		}

		setNotInState := func(opindex uint64) error {
			if opindex >= opsSize {
				return util.ErrNotFound.Errorf("out of range; opindex=%d", opindex)
			}

			return treeAdd(opindex, base.NewNotInStateOperationFixedtreeNode(ops[opindex][1], ""))
		}

		return func(notInStates []uint64) error {
				if err := context.Cause(wctx); err != nil {
					return errors.WithStack(err)
				}

				switch err := util.TraverseSliceV1(notInStates, func(_ int, opindex uint64) (bool, error) {
					return true, setNotInState(opindex)
				}); {
				case err == nil:
				case errors.Is(err, util.ErrLockedMapClosed):
					return context.Canceled
				default:
					return wctxCancel(err)
				}

				return nil
			},
			func(err error) (fixedtree.Tree, error) {
				if err := context.Cause(wctx); err != nil {
					return fixedtree.Tree{}, errors.WithStack(err)
				}

				treeAddDone()

				if err != nil {
					return fixedtree.Tree{}, wctxCancel(err)
				}

				switch tr, err := tw.Tree(); {
				case err != nil:
					return tr, wctxCancel(err)
				default:
					return tr, wctxCancel(nil)
				}
			},
			nil
	}
}

func treeAddOperationNodeFunc(
	size uint64,
	add func(uint64, fixedtree.Node) error,
) (
	func(opindex uint64, _ fixedtree.Node) error,
	func(),
	error,
) {
	var m util.LockedMap[uint64 /* opindex */, struct{}]

	switch i, err := util.NewLockedMap[uint64, struct{}](size, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	return func(opindex uint64, n fixedtree.Node) error {
			_, _, err := m.Set(opindex, func(struct{}, bool) (struct{}, error) {
				return struct{}{}, add(opindex, n)
			})

			return err
		},
		m.Close,
		nil
}
