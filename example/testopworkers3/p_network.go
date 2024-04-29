package main

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

func newSimpleBrokerFunc(header quicstreamheader.RequestHeader) quicstreamheader.BrokerFunc {
	return func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		if err := broker.WriteRequestHead(ctx, header); err != nil {
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

func openStreamFunc(
	encs *encoder.Encoders,
	dialf quicstream.ConnInfoDialFunc,
) func(context.Context, quicstream.ConnInfo) (quicstreamheader.StreamFunc, error) {
	hdialf := quicstreamheader.NewDialFunc(dialf, encs, encs.Default())
	return func(ctx context.Context, ci quicstream.ConnInfo) (quicstreamheader.StreamFunc, error) {
		f, _, err := hdialf(ctx, ci)

		return f, err
	}
}

func requestHandlerFunc(
	ctx context.Context,
	openStream func(ctx context.Context, ci quicstream.ConnInfo) (quicstreamheader.StreamFunc, error),
	f quicstreamheader.BrokerFunc,
	ci quicstream.ConnInfo,
) error {
	var stream quicstreamheader.StreamFunc

	switch i, err := openStream(ctx, ci); {
	case err != nil:
		return err
	default:
		stream = i
	}

	return stream(ctx, f)
}

func ensureHandlerAdd[T quicstreamheader.RequestHeader](
	pctx context.Context,
	gerr *error,
	name quicstream.HandlerName,
	handlerf func(context.Context) (quicstreamheader.Handler[T], error),
	errhandler quicstreamheader.ErrorHandler,
) {
	if err := launch.AddNetworkHandler(name); err != nil {
		panic(err)
	}

	var handler quicstreamheader.Handler[T]

	switch i, err := handlerf(pctx); {
	case err != nil:
		*gerr = err

		return
	default:
		handler = i
	}

	launch.EnsureHandlerAdd(
		pctx,
		gerr,
		name,
		handler,
		errhandler,
	)
}

func writeStream(
	w io.Writer,
	traverse func(func(b []byte) error) error,
) error {
	var bw *util.BytesFrameWriter

	switch i, err := util.NewBytesFrameWriter(w); {
	case err != nil:
		return err
	default:
		bw = i
	}

	return traverse(func(b []byte) error {
		return bw.Lengthed(b)
	})
}

func readStream(
	r io.Reader,
	f func([]byte) error,
) error {
	var br *util.BytesFrameReader

	switch i, err := util.NewBytesNoHeadersFrameReader(r); {
	case err != nil:
		return err
	default:
		br = i
	}

	for {
		switch err := br.Lengthed(func(b []byte) error {
			return f(b)
		}); {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return err
		}
	}
}

func NewReaderFuncChain(r io.Reader) (
	func(size, grow uint64) error,
	func(func(io.Reader) error),
	func() error,
) {
	init, add, done := util.NewFuncChain()

	return init,
		func(f func(io.Reader) error) {
			add(func() error {
				return f(r)
			})
		},
		done
}

func ReadLengthedFunc(
	f func([]byte) error,
) func(io.Reader) error {
	return func(r io.Reader) error {
		switch _, i, err := util.ReadLengthed(r); {
		case err != nil:
			return err
		default:
			return f(i)
		}
	}
}

func ParseConnInfo(s string) (ci quicstream.ConnInfo, err error) {
	switch ci, err = quicstream.NewConnInfoFromFullString(s); {
	case err != nil:
		return ci, err
	default:
		return quicstream.NewConnInfo(ci.UDPAddr(), true)
	}
}
