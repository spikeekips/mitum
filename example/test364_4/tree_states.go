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
	newStatesTreeAddNodeFunc func(stateKey string) (added bool, _ error)
	newStatesTreeNewTreeFunc func(error) (fixedtree.Tree, error)
	newStatesTreeFuncs       func(_ context.Context, size uint64, grow ...uint64) (newStatesTreeAddNodeFunc, newStatesTreeNewTreeFunc, error)
)

func newStatesTreeWriterFunc() newStatesTreeFuncs {
	return func(ctx context.Context, size uint64, grow ...uint64) (newStatesTreeAddNodeFunc, newStatesTreeNewTreeFunc, error) {
		wctx, wctxCancel := ContextWithCancelCause(ctx)

		if size < 1 {
			return func(string) (bool, error) {
					return false, wctxCancel(util.ErrFound.Errorf("zero size, but tried to add node"))
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

		var add util.CompactAppendSliceAddFunc[fixedtree.Node]
		done := func(func([]fixedtree.Node) error) error { return nil }
		var checkUniq func(stateKey string) (bool, error)
		var checkUniqDone func()

		switch c, d, err := checkUniqStateKeyFunc(size); {
		case err != nil:
			return nil, nil, err
		default:
			checkUniq = c
			checkUniqDone = d
		}

		if size > 0 {
			switch a, d, err := util.CompactAppendSliceV1[fixedtree.Node](size, grow...); {
			case err != nil:
				return nil, nil, err
			default:
				add = a
				done = d
			}
		}

		return func(stateKey string) (bool, error) {
				if err := context.Cause(wctx); err != nil {
					return false, errors.WithStack(err)
				}

				switch added, err := checkUniq(stateKey); {
				case added:
					n, _ := add(fixedtree.NewBaseNode(stateKey))

					return n > 0, nil
				case errors.Is(err, util.ErrLockedMapClosed):
					return false, context.Canceled
				case err != nil:
					return false, wctxCancel(err)
				default:
					return false, nil
				}
			},
			func(err error) (tr fixedtree.Tree, _ error) {
				defer func() {
					wctxCancel(nil)
					done(nil)
				}()

				if err := context.Cause(wctx); err != nil {
					return tr, errors.WithStack(err)
				}

				checkUniqDone()

				if err != nil {
					return tr, wctxCancel(err)
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
					return tr, wctxCancel(err)
				}

				return tr, nil
			},
			nil
	}
}

func checkUniqStateKeyFunc(size uint64) (func(stateKey string) (added bool, _ error), func(), error) {
	var m util.LockedMap[string, struct{}]

	switch i, err := util.NewLockedMap[string, struct{}](size, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	return func(stateKey string) (ok bool, _ error) {
			_, _, err := m.Set(stateKey, func(_ struct{}, found bool) (struct{}, error) {
				if found {
					return struct{}{}, util.ErrLockedSetIgnore
				}

				ok = true

				return struct{}{}, nil
			})

			return ok, err
		},
		m.Close,
		nil
}
