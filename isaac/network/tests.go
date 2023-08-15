//go:build test
// +build test

package isaacnetwork

import (
	"context"
	"io"
	"net"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util/encoder"
)

type DummyStreamer struct {
	r      io.Reader
	w      io.WriteCloser
	ctx    context.Context
	closef func() error
}

func (d DummyStreamer) Stream(ctx context.Context, f quicstream.StreamFunc) error {
	defer func() {
		_ = d.closef()
	}()

	return f(ctx, d.r, d.w)
}

func (d DummyStreamer) Close() error {
	if d.closef != nil {
		_ = d.closef()
	}

	return nil
}

func (d DummyStreamer) Context() context.Context {
	return d.ctx
}

func (d DummyStreamer) OpenStream(context.Context) (io.Reader, io.WriteCloser, func() error, error) {
	return d.r, d.w, d.closef, nil
}

func TestingDialFunc[T quicstreamheader.RequestHeader](encs *encoder.Encoders, prefix [32]byte, handler quicstreamheader.Handler[T]) (
	net.Addr,
	quicstream.ConnInfoDialFunc,
) {
	hr, cw := io.Pipe()
	cr, hw := io.Pipe()

	ph := quicstream.NewPrefixHandler(nil)
	ph.Add(prefix, quicstreamheader.NewHandler[T](encs, handler, nil))

	remote := quicstream.RandomUDPAddr()

	handlerf := func() error {
		defer func() {
			_ = hw.Close()
		}()

		_, err := ph.Handler(context.Background(), remote, hr, hw)

		if errors.Is(err, quicstream.ErrHandlerNotFound) {
			go func() {
				_, _ = io.ReadAll(cr)
			}()
			go func() {
				_, _ = io.ReadAll(hr)
			}()
		}

		_ = hr.Close()
		_ = hw.Close()

		return err
	}

	return remote, func(ctx context.Context, _ quicstream.ConnInfo) (quicstream.Streamer, error) {
		donech := make(chan error, 1)
		go func() {
			donech <- handlerf()
		}()

		dctx, cancel := context.WithCancel(ctx)

		return DummyStreamer{
			r: cr, w: cw, closef: func() error {
				cancel()

				_ = cr.Close()
				_ = cw.Close()

				select {
				case <-dctx.Done():
				case <-donech:
				}

				return nil
			},
			ctx: dctx,
		}, nil
	}
}
