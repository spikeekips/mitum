package dop6

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type (
	GetStateValueFunc = func(string) (base.StateValue, bool, error)
	GetSVMFunc        = func(string) (SVM, bool)
	MakeSVMFunc       = func(SMV) (SVM, error)
	MergeSVMFunc      = func(SMV, dutil.InErrorFunc[SVM]) error
	NewSVMPoolFunc    = func(uint64) (
		MergeSVMFunc,
		GetSVMFunc,
		func(),
		error,
	)
	GetStateOwnersFunc     = func(stateKey string) []string
	getFactStateOwnersFunc = func(opindex uint64, subv SMV, addvs []SMV) (subs []string, affected [][]string)
	addStateOwnerSetFunc   = func(subw string, affected []string)
)

func newSVMFuncSet() (
	func(hint.Hint, MakeSVMFunc) bool,
	NewSVMPoolFunc,
	func(),
) {
	addf, getf, closef := dutil.NewHintset[MakeSVMFunc]()

	getMerger := func(value SMV) (SVM, error) {
		hinter, ok := value.(hint.Hinter)
		if !ok {
			return nil, errors.Errorf("not hint.Hinter, %T", value)
		}

		switch f, found := getf(hinter.Hint().Type(), hinter.Hint().Version().Major()); {
		case !found:
			return nil, util.ErrNotFound.Errorf("%T", value)
		default:
			return f(value)
		}
	}

	return func(ht hint.Hint, f MakeSVMFunc) bool {
			return addf(ht.Type(), ht.Version().Major(), f)
		},
		func(size uint64) (
			MergeSVMFunc,
			GetSVMFunc,
			func(),
			error,
		) {
			var pool dutil.LockedMap[string, SVM]

			switch p, err := dutil.NewLockedMap[string, SVM](nil, size); {
			case err != nil:
				return nil, nil, nil, err
			default:
				pool = p
			}

			return func(value SMV, f dutil.InErrorFunc[SVM]) error {
					var gerr error

					_, _ = pool.Set(value.Key(), func(prev SVM, found bool) (SVM, error) {
						if found {
							gerr = f(prev)

							return nil, dutil.ErrLockedSetIgnore
						}

						switch merger, err := getMerger(value); {
						case err != nil:
							return nil, err
						default:
							gerr = f(merger)

							return merger, nil
						}
					})

					return gerr
				},
				pool.Value,
				pool.Close,
				nil
		},
		closef
}

func closeSVMs(
	ctx context.Context,
	getSVM GetSVMFunc,
	collect collectOpsSeqFunc,
	f func(_ context.Context, stateKey string, _ base.StateValue, ops []uint64) error,
) error {
	size, seq := collect()
	if size < 1 {
		return nil
	}

	newJob, jobDone, jobWait := NewFuncJobWorker(ctx, int64(size))

	for stateKey, ops := range seq {
		if err := newJob(func(ctx context.Context) error {
			merger, found := getSVM(stateKey)
			if !found {
				return nil
			}

			switch v, err := merger.Close(); {
			case err != nil, v == nil:
				return err
			default:
				return f(ctx, stateKey, v, ops)
			}
		}); err != nil {
			return jobWait(err)
		}
	}

	jobDone()

	return jobWait(nil)
}

func newFactPool(size uint64) (
	dutil.In2Func[uint64, util.Hash, bool],
	func([]uint64) ([]util.Hash, error),
	func(),
	error,
) {
	var pool dutil.LockedMap[uint64, util.Hash]

	switch m, err := dutil.NewLockedMap[uint64, util.Hash](nil, min(size, 1<<9)); { //nolint:mnd
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = m
	}

	return func(opindex uint64, fact util.Hash) bool {
			return pool.SetValue(opindex, fact)
		},
		func(ops []uint64) ([]util.Hash, error) {
			// NOTE ops should be sorted
			facts := make([]util.Hash, len(ops))

			for i := range ops {
				h, found := pool.Value(ops[i])
				if !found {
					return nil, util.ErrNotFound.Errorf("%d", ops[i])
				}

				facts[i] = h
			}

			return facts, nil
		},
		pool.Close,
		nil
}
