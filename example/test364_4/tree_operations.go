package main

import (
	"context"

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

func newOperationsTreeWriterFunc() newOperationsTreeFuncs {
	return func(ctx context.Context, ops [][2]util.Hash) (newOperationsTreeNotInStateFunc, errorFunc, newOperationsTreeNewTreeFunc, error) {
		wctx, wctxCancel := ContextWithCancelCause(ctx)

		if len(ops) < 1 {
			return func([]uint64) error {
					if err := context.Cause(wctx); err != nil {
						return errors.WithStack(err)
					}

					return wctxCancel(util.ErrFound.Errorf("empty ops, but tried 'not in states'"))
				},
				dummyContextErrorFunc(wctx, func(err error) error {
					if err != nil {
						return wctxCancel(err)
					}

					return nil
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
		var treeAddDone func()

		switch a, d, err := treeAddOperationNodeFunc(opsSize, tw.Add); {
		case err != nil:
			return nil, nil, nil, wctxCancel(err)
		default:
			treeAdd = a
			treeAddDone = d
		}

		_, initialTreeAdd, initialTreeAddDone := NewDoneContextFunc(wctx)
		go func() {
			err := util.TraverseSliceV1(ops, func(i int, hs [2]util.Hash) (bool, error) {
				return true, treeAdd(uint64(i), base.NewInStateOperationFixedtreeNode(hs[1], ""), false)
			})
			if err != nil {
				wctxCancel(err)
			}

			_ = initialTreeAdd(err)
		}()

		setNotInState := func(opindex uint64) error {
			if opindex >= opsSize {
				return util.ErrNotFound.Errorf("out of range; opindex=%d", opindex)
			}

			return treeAdd(opindex, base.NewNotInStateOperationFixedtreeNode(ops[opindex][1], ""), true)
		}

		closef := OnceFuncCallback(func(err error) error {
			wctxCancel(err)
			treeAddDone()

			return err
		})

		return func(notInStates []uint64) error {
				if err := context.Cause(wctx); err != nil {
					return nil // NOTE ignore
				}

				return contextCauseError(
					wctx,
					onlyErrErrorFunc(closef),
					func() error {
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
					onlyErrErrorFunc(closef),
					func() error {
						if err != nil {
							return err
						}

						return initialTreeAddDone()
					},
				)
			},
			func(err error) (tr fixedtree.Tree, _ error) {
				gerr := contextCauseError(
					wctx,
					closef,
					func() error {
						if err != nil {
							return err
						}

						treeAddDone()

						switch t, err := tw.Tree(); {
						case err != nil:
							return err
						default:
							tr = t

							return nil
						}
					},
				)

				return tr, gerr
			},
			nil
	}
}

func treeAddOperationNodeFunc(
	size uint64,
	add func(uint64, fixedtree.Node) error,
) (
	func(opindex uint64, _ fixedtree.Node, _ bool) error,
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

	return func(opindex uint64, n fixedtree.Node, notInStates bool) error {
			_, _, err := m.Set(opindex, func(_ struct{}, found bool) (struct{}, error) {
				if found && !notInStates {
					return struct{}{}, util.ErrLockedSetIgnore
				}

				return struct{}{}, add(opindex, n)
			})

			return err
		},
		m.Close,
		nil
}
