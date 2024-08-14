package main

import (
	"context"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type (
	newStatesTreeAddNodeFunc func(stateKey string) (added, closed bool, _ error)
	newStatesTreeNewTreeFunc func(error) (fixedtree.Tree, error)
	newStatesTreeFuncs       func(context.Context) (newStatesTreeAddNodeFunc, errorFunc, newStatesTreeNewTreeFunc, error)
)

func newStatesTreeWriterFunc(size uint64, grow ...uint64) newStatesTreeFuncs {
	return func(ctx context.Context) (newStatesTreeAddNodeFunc, errorFunc, newStatesTreeNewTreeFunc, error) {
		wctx, wctxCancel := ContextWithCancelCause(ctx)

		if size < 1 {
			return func(string) (bool, bool, error) {
					return false, false, wctxCancel(util.ErrFound.Errorf("zero size, but tried to add node"))
				},
				func(err error) error {
					if err := context.Cause(wctx); err != nil {
						return errors.WithStack(err)
					}

					if err != nil {
						return wctxCancel(err)
					}

					return nil
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

		var add func(stateKey string) (bool, error)
		var jobDone func()
		var done func(error) (fixedtree.Tree, error)

		switch a, j, d, err := newTreeAddStateNodeFuncs(wctx, size, grow...); {
		case err != nil:
			return nil, nil, nil, err
		default:
			add = a
			jobDone = j
			done = d
		}

		return func(stateKey string) (bool, bool, error) {
				if err := context.Cause(wctx); err != nil {
					return false, true, nil
				}

				switch added, err := add(stateKey); {
				case added:
					return true, false, nil
				case errors.Is(err, util.ErrLockedMapClosed):
					return false, true, nil
				case err != nil:
					return false, false, wctxCancel(err)
				default:
					return false, false, nil
				}
			},
			func(err error) error {
				defer jobDone()

				if err := context.Cause(wctx); err != nil {
					return errors.WithStack(err)
				}

				if err != nil {
					return wctxCancel(err)
				}

				return nil
			},
			func(err error) (tr fixedtree.Tree, _ error) {
				defer func() {
					wctxCancel(nil)
					jobDone()
					done(nil)
				}()

				if err := context.Cause(wctx); err != nil {
					return tr, errors.WithStack(err)
				}

				return done(err)
			},
			nil
	}
}

func newTreeAddStateNodeFuncs(
	ctx context.Context, size uint64, grow ...uint64,
) (
	func(stateKey string) (bool, error),
	func(),
	func(error) (fixedtree.Tree, error),
	error,
) {
	var m util.LockedMap[string /* stateKey */, struct{}]

	switch i, err := util.NewLockedMap[string, struct{}](size, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		m = i
	}

	var add util.CompactAppendSliceAddFunc[fixedtree.Node]
	var done util.CompactAppendSliceDoneFunc[fixedtree.Node]
	var jobDone func()

	_, j, wait := NewDoneContextFunc(ctx)

	switch a, d, err := util.CompactAppendSliceV1[fixedtree.Node](size, grow...); {
	case err != nil:
		return nil, nil, nil, err
	default:
		add = a
		jobDone = func() {
			m.Close()
			j(nil)
		}
		done = func(f func([]fixedtree.Node) error) error {
			defer d(nil)

			if err := wait(); err != nil {
				return err
			}

			return d(f)
		}
	}

	return func(stateKey string) (added bool, _ error) {
			_, _, err := m.Set(stateKey, func(_ struct{}, found bool) (struct{}, error) {
				if found {
					return struct{}{}, util.ErrLockedSetIgnore
				}

				added = true

				n, _ := add(fixedtree.NewBaseNode(stateKey))
				added = n > 0

				return struct{}{}, nil
			})

			return added, err
		},
		jobDone,
		func(err error) (tr fixedtree.Tree, _ error) {
			defer func() {
				m.Close()
				jobDone()
				_ = done(nil)
			}()

			if err != nil {
				return tr, err
			}

			if err := done(func(s []fixedtree.Node) error {
				if len(s) < 1 {
					return nil
				}

				slices.SortFunc(s, func(a, b fixedtree.Node) int {
					return strings.Compare(a.Key(), b.Key())
				})

				switch tw, err := fixedtree.NewWriterFromNodes(base.StateFixedtreeHint, slices.Clone(s)); {
				case err != nil:
					return err
				default:
					switch i, err := tw.Tree(); {
					case err != nil:
						return err
					default:
						tr = i

						return nil
					}
				}
			}); err != nil {
				return tr, err
			}

			return tr, nil
		},
		nil
}
