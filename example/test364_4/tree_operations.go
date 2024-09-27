package main

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
)

// FIXME operations tree only contains unmerged ops

type (
	newOperationsTreeNotInStateFunc func(notInStates []uint64) error
	newOperationsTreeNewTreeFunc    func(error) (fixedtree.Tree, error)
	newOperationsTreeFuncs          func(_ context.Context, ops [][2]util.Hash) (newOperationsTreeNotInStateFunc, errorFunc, newOperationsTreeNewTreeFunc, error)
)

func newFuncOperationsTreeWriter() newOperationsTreeFuncs {
	return func(ctx context.Context, ops [][2]util.Hash) (newOperationsTreeNotInStateFunc, errorFunc, newOperationsTreeNewTreeFunc, error) {
		wctx, wctxCancel := ContextWithCancelCause(ctx)

		ifErrorClose := ifErrErrorFunc(wctxCancel)

		if len(ops) < 1 {
			return func([]uint64) error {
					if err := context.Cause(wctx); err != nil {
						return errors.WithStack(err)
					}

					return wctxCancel(util.ErrFound.Errorf("empty ops, but tried 'not in states'"))
				},
				dummyContextErrorFunc(wctx, func(err error) error {
					return ifErrorClose(err)
				}),
				func(err error) (tr fixedtree.Tree, _ error) {
					return tr, dummyContextErrorFunc(wctx, wctxCancel)(err)
				},
				nil
		}

		opsSize := uint64(len(ops))

		var tw *fixedtree.Writer

		switch i, err := fixedtree.NewWriter(base.OperationFixedtreeHint, opsSize); {
		case err != nil:
			return nil, nil, nil, wctxCancel(err)
		default:
			tw = i
		}

		var treeAdd func(uint64, fixedtree.Node, bool) error
		var treeAddTree func() (fixedtree.Tree, error)

		switch a, t, err := treeAddOperationNode(tw); {
		case err != nil:
			return nil, nil, nil, wctxCancel(err)
		default:
			treeAdd = a
			treeAddTree = t
		}

		_, initialTreeAddDone, initialTreeAddWait := NewDoneContextFunc(wctx)

		go func() {
			_ = initialTreeAddDone(ifErrorClose(util.TraverseSliceV1(ops, func(i int, hs [2]util.Hash) (bool, error) {
				return true, treeAdd(uint64(i), base.NewInStateOperationFixedtreeNode(hs[1], ""), false)
			})))
		}()

		_, treeAddDone, treeAddWait := NewDoneContextFunc(wctx)

		setNotInState := func(opindex uint64) error {
			if opindex >= opsSize {
				return util.ErrNotFound.Errorf("out of range; opindex=%d", opindex)
			}

			return treeAdd(opindex, base.NewNotInStateOperationFixedtreeNode(ops[opindex][1], ""), true)
		}

		closef := OnceFuncCallback(func(err error) error {
			wctxCancel(err)
			treeAddTree()

			return err
		})

		return func(notInStates []uint64) error {
				if err := context.Cause(wctx); err != nil {
					return nil // NOTE ignore
				}

				return contextCauseError(
					wctx,
					ifErrErrorFunc(closef),
					func(context.Context) error {
						switch err := util.TraverseSliceV1(notInStates, func(_ int, opindex uint64) (bool, error) {
							return true, setNotInState(opindex)
						}); {
						case errors.Is(err, util.ErrLockedMapClosed),
							errors.Is(err, context.Canceled):
							return nil
						default:
							return err
						}
					},
				)
			},
			func(err error) error {
				return contextCauseError(
					wctx,
					ifErrErrorFunc(closef),
					func(context.Context) error {
						return treeAddDone(err)
					},
				)
			},
			func(err error) (tr fixedtree.Tree, _ error) {
				return tr, contextCauseError(
					wctx,
					closef,
					func(context.Context) error {
						if err != nil {
							return err
						}

						if err := initialTreeAddWait(); err != nil {
							return err
						}

						if err := treeAddWait(); err != nil {
							return err
						}

						switch t, err := treeAddTree(); {
						case err != nil:
							return err
						default:
							tr = t

							return nil
						}
					},
				)
			},
			nil
	}
}

func treeAddOperationNode(tw *fixedtree.Writer) (
	func(opindex uint64, _ fixedtree.Node, _ bool) error,
	func() (fixedtree.Tree, error),
	error,
) {
	var m util.LockedMap[uint64 /* opindex */, struct{}]

	switch i, err := util.NewLockedMap[uint64, struct{}](uint64(tw.Len()), nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	var twlock sync.RWMutex

	return func(opindex uint64, n fixedtree.Node, notInStates bool) error {
			_, _, err := m.Set(opindex, func(_ struct{}, found bool) (struct{}, error) {
				if found && !notInStates {
					return struct{}{}, util.ErrLockedSetIgnore
				}

				twlock.RLock()
				defer twlock.RUnlock()

				return struct{}{}, tw.Add(opindex, n)
			})

			return err
		},
		func() (tr fixedtree.Tree, _ error) {
			defer m.Close()

			twlock.Lock()
			defer twlock.Unlock()

			return tw.Tree()
		},
		nil
}
