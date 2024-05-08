package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
)

func newWorkerBrokerCancelFunc(height base.Height) quicstreamheader.BrokerFunc {
	return newSimpleBrokerFunc(NewWorkerCancelHeader(height))
}

func newWorkerHandlerCancelFunc(
	cancelf func(base.Height) (func(), error),
) (quicstreamheader.Handler[WorkerCancelHeader], error) { // NOTE manager WorkerCancel()
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header WorkerCancelHeader,
	) (context.Context, error) {
		switch i, err := cancelf(header.Height); {
		case err != nil:
			return ctx, err
		default:
			i()

			return ctx, broker.WriteResponseHeadOK(ctx, true, nil)
		}
	}, nil
}

func newWorkerBrokerDoneFunc(height base.Height) quicstreamheader.BrokerFunc {
	return newSimpleBrokerFunc(NewWorkerDoneHeader(height))
}

func newWorkerHandlerWorkerDoneFunc(
	donef func(base.Height) (func() error, error),
) (quicstreamheader.Handler[WorkerDoneHeader], error) { // NOTE manager WorkerDone()
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header WorkerDoneHeader,
	) (context.Context, error) {
		switch i, err := donef(header.Height); {
		case err != nil:
			return ctx, err
		default:
			err = i()

			return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
		}
	}, nil
}

func newWorkerBrokerReadyFunc(args processWorkerReadyArgs) quicstreamheader.BrokerFunc {
	return newSimpleBrokerFunc(NewWorkerReadyHeader(args))
}

func newWorkerHandlerReadyFunc(
	newWorker func(processWorkerReadyArgs) error,
) (quicstreamheader.Handler[WorkerReadyHeader], error) {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header WorkerReadyHeader,
	) (context.Context, error) {
		err := newWorker(header.Args)

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	}, nil
}

func newWorkerBrokerNewOperationsFunc(
	height base.Height,
	ops func(func(opindex uint64, op, fact util.Hash) error) error,
) quicstreamheader.BrokerFunc {
	return func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		header := NewWorkerNewOperationsHeader(height)

		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		if err := writeNewOperationsItems(
			broker.Writer,
			ops,
		); err != nil {
			return err
		}

		if err := broker.Close(); err != nil {
			return err
		}

		switch _, res, err := broker.ReadResponseHead(ctx); {
		case err != nil:
			return err
		default:
			return res.Err()
		}
	}
}

func newWorkerHandlerNewOperationsFunc(
	logf func() *zerolog.Event,
	getFuncs func(base.Height) (
		workerNewOperationsFunc,
		func() error,
		func(error),
		error,
	),
) (quicstreamheader.Handler[WorkerNewOperationsHeader], error) {
	h := func(
		ctx context.Context,
		broker *quicstreamheader.HandlerBroker,
		height base.Height,
	) error {
		var process workerNewOperationsFunc
		var done func() error
		var cancel func(error)

		switch i, j, k, err := getFuncs(height); {
		case err != nil:
			return err
		default:
			process = i
			done = j
			cancel = k
		}

		defer logging.TimeElapsedMsg(logf, "handler: new operations")()

		var ops uint64

		if err := readNewOperationsItems(broker.Reader, func(opindex uint64, op, fact util.Hash) error {
			atomic.AddUint64(&ops, 1)

			return process(opindex, op, fact)
		}); err != nil {
			cancel(err)

			return err
		}

		if atomic.LoadUint64(&ops) < 1 {
			err := errors.Errorf("empty operations")
			cancel(err)

			return err
		}

		if err := done(); err != nil {
			cancel(err)

			return err
		}

		return nil
	}

	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header WorkerNewOperationsHeader,
	) (context.Context, error) {
		err := h(ctx, broker, header.Height)

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	}, nil
}

func newWorkerBrokerInsertValuesFunc(
	height base.Height,
	worker string,
	enc encoder.Encoder,
	opindex uint64,
	fact util.Hash,
	values []StateMergeValue,
	owners map[string][]string,
) quicstreamheader.BrokerFunc {
	return func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		header := NewWorkerInsertValuesHeader(height, worker)

		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		if err := writeInsertValues(enc, broker.Writer, opindex, fact, values, owners); err != nil {
			return err
		}

		switch _, res, err := broker.ReadResponseHead(ctx); {
		case err != nil:
			return err
		default:
			return res.Err()
		}
	}
}

func newWorkerHandlerInsertValuesFunc(
	f func(base.Height) (processWorkerInsertValues, error),
) (quicstreamheader.Handler[WorkerInsertValuesHeader], error) {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header WorkerInsertValuesHeader,
	) (context.Context, error) {
		var insert processWorkerInsertValues

		switch i, err := f(header.Height); {
		case err != nil:
			return ctx, err
		default:
			insert = i
		}

		switch opindex, fact, values, owners, err := readInsertValues(broker.Encoder, broker.Reader); {
		case err != nil:
			return ctx, err
		default:
			err = insert(header.Worker, opindex, fact, values, owners)

			return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
		}
	}, nil
}

func newWorkerBrokerInsertValuesDoneFunc(height base.Height, worker string) quicstreamheader.BrokerFunc {
	return newSimpleBrokerFunc(NewWorkerInsertValuesDoneHeader(height, worker))
}

func newWorkerHandlerInsertValuesDoneFunc(
	f func(base.Height) (func(worker string) error, error),
) (quicstreamheader.Handler[WorkerInsertValuesDoneHeader], error) {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header WorkerInsertValuesDoneHeader,
	) (context.Context, error) {
		switch i, err := f(header.Height); {
		case err != nil:
			return ctx, err
		default:
			err = i(header.Worker)

			return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
		}
	}, nil
}

func newWorkerBrokerPhaseDoneFunc(
	height base.Height,
	worker string,
	phase int,
	mergedOps, unmergedOps []uint64,
) quicstreamheader.BrokerFunc {
	return func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		header := NewWorkerPhaseDoneHeader(height, worker, phase)

		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		init, add, done := util.NewFuncChain()
		if err := init(2, 0); err != nil {
			return err
		}

		add(func() error {
			return util.WriteLengthedSliceFunc(broker.Writer, uint64(len(mergedOps)),
				func(i uint64) ([]byte, error) {
					return util.Uint64ToBytes(mergedOps[i]), nil
				},
			)
		})

		add(func() error {
			return util.WriteLengthedSliceFunc(broker.Writer, uint64(len(unmergedOps)),
				func(i uint64) ([]byte, error) {
					return util.Uint64ToBytes(unmergedOps[i]), nil
				},
			)
		})

		if err := done(); err != nil {
			return err
		}

		switch _, res, err := broker.ReadResponseHead(ctx); {
		case err != nil:
			return err
		default:
			return res.Err()
		}
	}
}

func newWorkerHandlerPhaseDoneFunc(
	donef func(base.Height) (func(worker string, phase int, mergedOps, unmergedOps []uint64) error, error),
) (quicstreamheader.Handler[WorkerPhaseDoneHeader], error) {
	read := func(r io.Reader) ([]uint64, error) {
		var bs [][]byte

		switch _, i, err := util.ReadLengthedSlice(r); {
		case err != nil:
			return nil, err
		default:
			bs = i
		}

		values := make([]uint64, len(bs))

		for i := range bs {
			switch j, err := util.BytesToUint64(bs[i]); {
			case err != nil:
				return nil, err
			default:
				values[i] = j
			}
		}

		return values, nil
	}

	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header WorkerPhaseDoneHeader,
	) (context.Context, error) {
		var f func(worker string, phase int, mergedOps, unmergedOps []uint64) error

		switch i, err := donef(header.Height); {
		case err != nil:
			return ctx, err
		default:
			f = i
		}

		init, add, done := NewReaderFuncChain(broker.Reader)
		if err := init(2, 0); err != nil {
			return ctx, err
		}

		var mergedOps, unmergedOps []uint64

		add(func(r io.Reader) error {
			switch i, err := read(r); {
			case err != nil:
				return err
			default:
				mergedOps = i

				return nil
			}
		})

		add(func(r io.Reader) error {
			switch i, err := read(r); {
			case err != nil:
				return err
			default:
				unmergedOps = i

				return nil
			}
		})

		if err := done(); err != nil {
			return ctx, err
		}

		err := f(header.Worker, header.Phase, mergedOps, unmergedOps)

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	}, nil
}

func newWorkerBrokerSyncStatesFunc(height base.Height) quicstreamheader.BrokerFunc {
	return newSimpleBrokerFunc(NewWorkerSyncStatesHeader(height))
}

func newWorkerHandlerSyncStatesFunc(
	f func(context.Context, base.Height) error,
) (quicstreamheader.Handler[WorkerSyncStatesHeader], error) {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header WorkerSyncStatesHeader,
	) (context.Context, error) {
		err := f(ctx, header.Height)

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	}, nil
}

func newWorkerBrokerSetStatesFunc(
	f func(func(base.State) error) error,
) quicstreamheader.BrokerFunc {
	return func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		header := NewWorkerSetStatesHeader()

		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		if err := writeStream(
			broker.Writer,
			func(fb func([]byte) error) error {
				return f(func(st base.State) error {
					switch b, err := broker.Encoder.Marshal(st); {
					case err != nil:
						return err
					default:
						return fb(b)
					}
				})
			},
		); err != nil {
			return err
		}

		if err := broker.Close(); err != nil {
			return err
		}

		switch _, res, err := broker.ReadResponseHead(ctx); {
		case err != nil:
			return err
		default:
			return res.Err()
		}
	}
}

func newWorkerHandlerSetStatesFunc(
	f func(base.State) error,
) (quicstreamheader.Handler[WorkerSetStatesHeader], error) {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, _ WorkerSetStatesHeader,
	) (context.Context, error) {
		err := readStream(broker.Reader, func(b []byte) error {
			var st base.State

			switch err := encoder.Decode(broker.Encoder, b, &st); {
			case err != nil:
				return err
			default:
				return f(st)
			}
		})

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	}, nil
}

func newWorkerBrokerSetOperationsFunc(
	f func(func(base.Operation) error) error,
) quicstreamheader.BrokerFunc {
	return func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		header := NewWorkerSetOperationsHeader()

		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		if err := writeStream(
			broker.Writer,
			func(fb func([]byte) error) error {
				return f(func(st base.Operation) error {
					switch b, err := broker.Encoder.Marshal(st); {
					case err != nil:
						return err
					default:
						return fb(b)
					}
				})
			},
		); err != nil {
			return err
		}

		if err := broker.Close(); err != nil {
			return err
		}

		switch _, res, err := broker.ReadResponseHead(ctx); {
		case err != nil:
			return err
		default:
			return res.Err()
		}
	}
}

func newWorkerHandlerSetOperationsFunc(
	f func(base.Operation) error,
) (quicstreamheader.Handler[WorkerSetOperationsHeader], error) {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, _ WorkerSetOperationsHeader,
	) (context.Context, error) {
		err := readStream(broker.Reader, func(b []byte) error {
			var st base.Operation

			switch err := encoder.Decode(broker.Encoder, b, &st); {
			case err != nil:
				return err
			default:
				return f(st)
			}
		})

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	}, nil
}

func newWorkerBrokerSetStateMergeValuesFunc(
	f func(func(util.Hash, []StateMergeValue) error) error,
) quicstreamheader.BrokerFunc {
	return func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		header := NewWorkerSetStateMergeValuesHeader()

		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		if err := writeStream(
			broker.Writer,
			func(fb func([]byte) error) error {
				return f(func(op util.Hash, vs []StateMergeValue) error {
					buf := bytes.NewBuffer(nil)
					defer buf.Reset()

					_ = util.WriteLengthed(buf, op.Bytes())

					if err := util.WriteLengthedSliceFunc(
						buf,
						uint64(len(vs)),
						func(index uint64) ([]byte, error) {
							return broker.Encoder.Marshal(vs[index])
						},
					); err != nil {
						return err
					}

					return fb(buf.Bytes())
				})
			},
		); err != nil {
			return err
		}

		if err := broker.Close(); err != nil {
			return err
		}

		switch _, res, err := broker.ReadResponseHead(ctx); {
		case err != nil:
			return err
		default:
			return res.Err()
		}
	}
}

func newWorkerHandlerSetStateMergeValuesFunc(
	f func(util.Hash, []StateMergeValue) error,
) (quicstreamheader.Handler[WorkerSetStateMergeValuesHeader], error) {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, _ WorkerSetStateMergeValuesHeader,
	) (context.Context, error) {
		err := readStream(broker.Reader, func(b []byte) error {
			var op util.Hash

			left := bytes.NewBuffer(b)
			defer left.Reset()

			switch _, i, err := util.ReadLengthed(left); {
			case err != nil:
				return err
			default:
				op = valuehash.NewBytes(i)
			}

			var vs []StateMergeValue
			var once sync.Once

			if _, err := util.ReadLengthedSliceFunc(left, func(size, index uint64, b []byte) error {
				once.Do(func() {
					vs = make([]StateMergeValue, size)
				})

				return encoder.Decode(broker.Encoder, b, &vs[index])
			}); err != nil {
				return err
			}

			return f(op, vs)
		})

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	}, nil
}

func writeNewOperationsItem(
	opindex uint64,
	op, fact util.Hash,
	f func([]byte) error,
) error {
	buf := bytes.NewBuffer(nil)
	defer buf.Reset()

	init, add, done := util.NewFuncChain()
	if err := init(3, 0); err != nil {
		return err
	}

	add(func() error { return util.WriteLengthed(buf, util.Uint64ToBytes(opindex)) })
	add(func() error { return util.WriteLengthed(buf, op.Bytes()) })
	add(func() error { return util.WriteLengthed(buf, fact.Bytes()) })

	if err := done(); err != nil {
		return err
	}

	return f(buf.Bytes())
}

func readNewOperationsItem(b []byte) (opindex uint64, op, fact util.Hash, _ error) {
	buf := bytes.NewBuffer(b)
	defer buf.Reset()

	init, add, done := NewReaderFuncChain(buf)
	if err := init(3, 0); err != nil {
		return 0, nil, nil, err
	}

	add(ReadLengthedFunc(func(i []byte) (err error) {
		opindex, err = util.BytesToUint64(i)

		return err
	}))

	add(ReadLengthedFunc(func(i []byte) error {
		op = valuehash.NewBytes(i)

		return nil
	}))

	add(ReadLengthedFunc(func(i []byte) error {
		fact = valuehash.NewBytes(i)

		return nil
	}))

	if err := done(); err != nil {
		return 0, nil, nil, err
	}

	return opindex, op, fact, nil
}

func writeNewOperationsItems(
	w io.Writer,
	f func(func(opindex uint64, op, fact util.Hash) error) error,
) error {
	if f == nil {
		return nil
	}

	return writeStream(
		w,
		func(fb func(b []byte) error) error {
			return f(func(opindex uint64, op, fact util.Hash) error {
				return writeNewOperationsItem(opindex, op, fact, func(b []byte) error {
					return fb(b)
				})
			})
		},
	)
}

func readNewOperationsItems(
	r io.Reader,
	f func(opindex uint64, op, fact util.Hash) error,
) error {
	return readStream(r, func(b []byte) error {
		switch opindex, op, fact, err := readNewOperationsItem(b); {
		case err != nil:
			return err
		default:
			return f(opindex, op, fact)
		}
	})
}

func writeInsertValues(
	enc encoder.Encoder,
	w io.Writer,
	opindex uint64,
	fact util.Hash,
	values []StateMergeValue,
	owners map[string][]string,
) error {
	init, add, done := util.NewFuncChain()
	if err := init(4, 0); err != nil {
		return err
	}

	add(func() error { return util.WriteLengthed(w, util.Uint64ToBytes(opindex)) })
	add(func() error { return util.WriteLengthed(w, fact.Bytes()) })
	add(func() error {
		return util.WriteLengthedSliceFunc(w, uint64(len(values)),
			func(index uint64) ([]byte, error) {
				return enc.Marshal(values[index])
			},
		)
	})

	add(func() error {
		switch i, err := enc.Marshal(owners); {
		case err != nil:
			return err
		default:
			return util.WriteLengthed(w, i)
		}
	})

	return done()
}

func readInsertValues(enc encoder.Encoder, r io.Reader) (
	opindex uint64,
	fact util.Hash,
	values []StateMergeValue,
	owners map[string][]string,
	_ error,
) {
	init, add, done := NewReaderFuncChain(r)
	if err := init(4, 0); err != nil {
		return 0, nil, nil, nil, err
	}

	add(ReadLengthedFunc(func(i []byte) (err error) {
		opindex, err = util.BytesToUint64(i)

		return err
	}))

	add(ReadLengthedFunc(func(i []byte) error {
		fact = valuehash.NewBytes(i)

		return nil
	}))

	add(func(r io.Reader) error {
		var bs [][]byte

		switch _, i, err := util.ReadLengthedSlice(r); {
		case err != nil:
			return err
		default:
			bs = i
		}

		values = make([]StateMergeValue, len(bs))

		for i := range bs {
			if err := encoder.Decode(enc, bs[i], &values[i]); err != nil {
				return err
			}
		}

		return nil
	})

	add(ReadLengthedFunc(func(i []byte) error {
		return enc.Unmarshal(i, &owners)
	}))

	if err := done(); err != nil {
		return 0, nil, nil, nil, err
	}

	return opindex, fact, values, owners, nil
}
