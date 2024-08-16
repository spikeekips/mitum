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

		closef := OnceFuncCallback(func(err error) error {
			wctxCancel(err)
			jobDone()
			done(err)

			return err
		})

		return func(stateKey string) (added, closed bool, _ error) {
				err := contextCauseError(
					wctx,
					onlyErrErrorFunc(closef),
					func() error {
						switch a, err := add(stateKey); {
						case a:
							added = true

							return nil
						case errors.Is(err, util.ErrLockedMapClosed):
							closed = true

							return nil
						case err != nil:
							return err
						default:
							return nil
						}
					},
				)

				if errors.Is(err, context.Canceled) {
					return false, true, nil
				}

				return added, closed, err
			},
			dummyContextErrorFunc(wctx, func(err error) error {
				defer jobDone()

				return onlyErrErrorFunc(closef)(err)
			}),
			func(err error) (tr fixedtree.Tree, _ error) {
				defer closef(nil)

				gerr := dummyContextErrorFunc(wctx, func(err error) error {
					t, herr := done(err)

					tr = t

					return closef(herr)
				})(err)

				return tr, gerr
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
