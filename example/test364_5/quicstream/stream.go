package quicstream

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

func StreamHandlerOpenSubStreams(ctx context.Context, id []byte, size uint8) (
	[]quic.Stream,
	dutil.InErrorFunc[error],
	error,
) {
	switch {
	case size < 1:
		return nil, nil, errors.Errorf("zero streams size")
	case len(id) < 1:
		return nil, nil, errors.Errorf("empty id")
	}

	var conn quic.Connection

	switch i, err := loadQuicConnectionFromContext(ctx); {
	case err != nil:
		return nil, nil, err
	default:
		conn = i
	}

	var add dutil.AppendSliceAddFunc[quic.Stream]
	var done dutil.OutFunc[[]quic.Stream]

	switch a, d, err := dutil.NewAppendSlice[quic.Stream](uint64(size), 0); {
	case err != nil:
		return nil, nil, err
	default:
		add, done = a, d
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(ctx, uint64(size)); {
	case err != nil:
		return nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	for range size {
		if err := newJob(func(ctx context.Context) error {
			switch stream, err := conn.OpenStreamSync(ctx); {
			case err != nil:
				return err
			default:
				// NOTE send prefix for streams bonding  first
				if err := writeStreamHandlerPrefix(stream, []byte(id)); err != nil {
					return err
				}

				_, _ = add(stream)

				return nil
			}
		}); err != nil {
			return nil, nil, jobWait(err)
		}
	}

	jobDone()

	if err := jobWait(nil); err != nil {
		return nil, nil, err
	}

	return done(), func(err error) error {
			streams := done()

			defer dutil.ClearSlice(streams)

			for stream := range slices.Values(streams) {
				_ = closeStreamWithError(stream, err)
			}

			return err
		},
		nil
}

func loadQuicConnectionFromContext(ctx context.Context) (quic.Connection, error) {
	switch i := ctx.Value(QuicConnectionContextKey); {
	case i == nil:
		return nil, util.ErrNotFound.Errorf("context key, %q not found", QuicConnectionContextKey)
	default:
		conn, ok := i.(quic.Connection)
		if !ok {
			return nil, util.ErrNotFound.Errorf("expected quic.Connection, but %T", i)
		}

		return conn, nil
	}
}
