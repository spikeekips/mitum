package dop7

import (
	"context"
	"runtime"

	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

var MaxJobWorkerSemSize uint64 = 1 << 9

func limitedJobWorkerSemSize(s uint64) int64 {
	return int64(min(max(s, 1), MaxJobWorkerSemSize))
}

func NewFuncJobWorker(ctx context.Context, semSize uint64) (
	dutil.JobWorkerNewJobFunc,
	func(),
	dutil.InErrorFunc[error],
) {
	n, d, w, err := dutil.NewFuncJobWorker(ctx, limitedJobWorkerSemSize(semSize))
	if err != nil {
		return func(dutil.InErrorFunc[context.Context]) error { return err },
			func() {},
			func(error) error { return err }
	}

	return n, d, w
}

func NewFuncErrJobWorker(ctx context.Context, semSize uint64, errf dutil.InErrorFunc[error]) (
	dutil.JobWorkerNewJobFunc,
	func(),
	dutil.InErrorFunc[error],
) {
	n, d, w, err := dutil.NewFuncErrJobWorker(ctx, limitedJobWorkerSemSize(semSize), errf)
	if err != nil {
		return func(dutil.InErrorFunc[context.Context]) error { return err },
			func() {},
			func(error) error { return err }
	}

	return n, d, w
}

func NewFuncNumCPUJobWorker(ctx context.Context) (
	dutil.JobWorkerNewJobFunc,
	func(),
	dutil.InErrorFunc[error],
) {
	return NewFuncJobWorker(ctx, uint64(runtime.NumCPU()))
}

func NewFuncNumCPUErrJobWorker(ctx context.Context, errf dutil.InErrorFunc[error]) (
	dutil.JobWorkerNewJobFunc,
	func(),
	dutil.InErrorFunc[error],
) {
	return NewFuncErrJobWorker(ctx, uint64(runtime.NumCPU()), errf)
}
