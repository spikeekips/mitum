package m2

import (
	"iter"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type (
	GetStateValueFunc = func(string) (base.StateValue, bool, error)
	GetSVMFunc        = func(SMV) (SVM, error)
	MergeSVMFunc      = func(SMV, dutil.InErrorFunc[SVM]) error
	NewSVMPoolFunc    = func(uint64) (
		MergeSVMFunc,
		func() iter.Seq[SVM],
		error,
	)
)

func newSVMFuncSet() (
	func(hint.Hint, GetSVMFunc) bool,
	NewSVMPoolFunc,
	func(),
) {
	addf, getf, closef := dutil.NewHintset[GetSVMFunc]()

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

	return func(ht hint.Hint, f GetSVMFunc) bool {
			return addf(ht.Type(), ht.Version().Major(), f)
		},
		func(size uint64) (
			MergeSVMFunc,
			func() iter.Seq[SVM],
			error,
		) {
			var pool dutil.LockedMap[string, SVM]

			switch p, err := dutil.NewLockedMap[string, SVM](nil, size); {
			case err != nil:
				return nil, nil, err
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
				func() iter.Seq[SVM] {
					return func(yield func(SVM) bool) {
						defer pool.Close()

						pool.Seq2(func(_ string, m SVM) bool {
							return yield(m)
						})
					}
				},
				nil
		},
		closef
}
