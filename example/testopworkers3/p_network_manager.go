package main

import (
	"context"
	"io"
	"net"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

func newManagerBrokerStateOwnersFunc(
	height base.Height,
	worker string,
	stateKey string,
	f func([]string) error,
) quicstreamheader.BrokerFunc {
	return func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		header := NewManagerStateOwnersHeader(height, worker)

		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		if err := util.WriteLengthed(broker.Writer, []byte(stateKey)); err != nil {
			return err
		}

		var owners []string

		switch _, i, err := util.ReadLengthedSlice(broker.Reader); {
		case err != nil:
			return err
		case len(i) < 1:
		default:
			owners = make([]string, len(i))

			_ = util.TraverseSliceV1(i, func(j int, v []byte) (bool, error) {
				owners[j] = string(v)

				return true, nil
			})
		}

		if err := f(owners); err != nil {
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

func newManagerHandlerStateOwnersFunc(
	f func(base.Height) (func(string) ([]string, error), error),
) (quicstreamheader.Handler[ManagerStateOwnersHeader], error) {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header ManagerStateOwnersHeader,
	) (context.Context, error) {
		var sf func(string) ([]string, error)

		switch i, err := f(header.Height); {
		case err != nil:
			return ctx, err
		default:
			sf = i
		}

		var stateKey string

		switch _, i, err := util.ReadLengthed(broker.Reader); {
		case err != nil:
			return ctx, err
		case len(i) < 1:
			return ctx, errors.Errorf("empty state key")
		default:
			stateKey = string(i)
		}

		switch owners, err := sf(stateKey); {
		case err != nil:
			return ctx, err
		default:
			if err := util.WriteLengthedSliceFunc(broker.Writer, uint64(len(owners)),
				func(index uint64) ([]byte, error) {
					return []byte(owners[index]), nil
				},
			); err != nil {
				return ctx, err
			}
		}

		return ctx, broker.WriteResponseHeadOK(ctx, true, nil)
	}, nil
}

func newManagerBrokerWorkerUpdateMergedFunc(
	height base.Height,
	worker string,
	updateStatef func(func(stateKey string, stateHash util.Hash) error) error,
	updateReasonf func(func(opindex uint64, reason string) error) error,
) quicstreamheader.BrokerFunc {
	return func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		header := NewManagerWorkerUpdateMergedHeader(height, worker)

		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		if err := updateStatef(func(stateKey string, stateHash util.Hash) error {
			return util.WriteLengthedSlice(broker.Writer, [][]byte{[]byte(stateKey), stateHash.Bytes()})
		}); err != nil {
			return err
		}

		if err := util.WriteLengthedSlice(broker.Writer, nil); err != nil {
			return err
		}

		if err := updateReasonf(func(opindex uint64, reason string) error {
			return util.WriteLengthedSlice(broker.Writer, [][]byte{util.Uint64ToBytes(opindex), []byte(reason)})
		}); err != nil {
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

func newManagerHandlerWorkerUpdateMergedFunc(
	f func(_ base.Height, worker string) (
		updateState func(stateKey string, stateHash util.Hash) error,
		updateReason func(opindex uint64, reason string) error,
		done func() error,
		_ error,
	),
) (quicstreamheader.Handler[ManagerWorkerUpdateMergedHeader], error) {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header ManagerWorkerUpdateMergedHeader,
	) (context.Context, error) {
		var updateState func(stateKey string, stateHash util.Hash) error
		var updateReason func(opindex uint64, reason string) error
		var done func() error

		switch us, ur, d, err := f(header.Height, header.Worker); {
		case err != nil:
			return ctx, err
		default:
			updateState = us
			updateReason = ur
			done = d
		}

	ends:
		for {
			switch _, i, err := util.ReadLengthedSlice(broker.Reader); {
			case errors.Is(err, io.EOF):
				break ends
			case err != nil:
				return ctx, err
			case len(i) < 1:
				break ends
			case len(i) != 2:
				return ctx, errors.Errorf("invalid state slice, %d", len(i))
			default:
				if err := updateState(string(i[0]), valuehash.NewBytes(i[1])); err != nil {
					return ctx, err
				}
			}
		}

	endr:
		for {
			var iseof bool
			var bs [][]byte

			switch n, i, err := util.ReadLengthedSlice(broker.Reader); {
			case errors.Is(err, io.EOF):
				iseof = true
			case err != nil:
				return ctx, err
			case len(i) < 1:
				break endr
			case len(i) != 2:
				return ctx, errors.Errorf("invalid reason slice, %d, %d", n, len(i))
			default:
				bs = i
			}

			if iseof && len(bs) < 1 {
				break endr
			}

			switch opindex, err := util.BytesToUint64(bs[0]); {
			case err != nil:
				return ctx, err
			default:
				if err := updateReason(opindex, string(bs[1])); err != nil {
					return ctx, err
				}
			}

			if iseof {
				break endr
			}
		}

		err := done()

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	}, nil
}

func newManagerBrokerWorkerStoreDoneFunc(
	height base.Height,
	worker string,
	chunks map[base.BlockItemType]string,
) quicstreamheader.BrokerFunc {
	return func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		header := NewManagerWorkerStoreDoneHeader(height, worker)

		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		for i := range chunks {
			if err := util.WriteLengthedSlice(broker.Writer, [][]byte{
				[]byte(i),
				[]byte(chunks[i]),
			}); err != nil {
				return err
			}
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

func newManagerHandlerWorkerStoreDoneFunc(
	f func(base.Height) (func(worker string, chunks map[base.BlockItemType]string) error, error),
) (quicstreamheader.Handler[ManagerWorkerStoreDoneHeader], error) {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header ManagerWorkerStoreDoneHeader,
	) (context.Context, error) {
		var storeDone func(worker string, chunks map[base.BlockItemType]string) error

		switch i, err := f(header.Height); {
		case err != nil:
			return ctx, err
		default:
			storeDone = i
		}

		chunks := map[base.BlockItemType]string{}

	end:
		for {
			switch _, i, err := util.ReadLengthedSlice(broker.Reader); {
			case errors.Is(err, io.EOF):
				break end
			case err != nil:
				return ctx, err
			case len(i) < 1:
				break end
			case len(i) != 2:
				return ctx, errors.Errorf("invalid state slice, %d", len(i))
			default:
				chunks[base.BlockItemType(string(i[0]))] = string(i[1])
			}
		}

		err := storeDone(header.Worker, chunks)

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	}, nil
}

func newManagerBrokerWorkerCanceledFunc(height base.Height, worker string) quicstreamheader.BrokerFunc {
	return newSimpleBrokerFunc(NewManagerWorkerCanceledHeader(height, worker))
}

func newManagerHandlerWorkerCanceledFunc(
	canceledf func(base.Height) (func(worker string) error, error),
) (quicstreamheader.Handler[ManagerWorkerCanceledHeader], error) {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header ManagerWorkerCanceledHeader,
	) (context.Context, error) {
		switch i, err := canceledf(header.Height); {
		case err != nil:
			return ctx, err
		default:
			err = i(header.Worker)

			return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
		}
	}, nil
}
