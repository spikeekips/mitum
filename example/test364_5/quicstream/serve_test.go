package quicstream

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testServe struct {
	dutil.BaseSuite
	BaseTest
}

func (t *testServe) newServe(
	ctx context.Context,
	quicConfig *quic.Config,
) (
	dutil.InFunc[StreamHandler, dutil.ErrorFunc],
	func(context.Context, *quic.Config) (quic.Connection, error),
	dutil.ErrorFunc,
) {
	pconn, err := NewPacketConn(0)
	t.PrintIfErrPanic(err, "NewTestPacketConn: %q", pconn.LocalAddr())

	_, tlsConfig := t.NewTLSConfig()
	if quicConfig == nil {
		quicConfig = DefaultQuicConfig()
	}

	wait, closef, err := Serve(
		ctx,
		pconn,
		tlsConfig,
		quicConfig,
		RandomStatelessResetKey(),
	)
	t.PrintIfErrPanic(err, "serve")

	return func(f StreamHandler) dutil.ErrorFunc {
			w := dutil.AssertWaitChan(t.Assert(), time.Second, func() error {
				return wait(f)
			})

			return w
		},
		func(ctx context.Context, quicConfig *quic.Config) (quic.Connection, error) {
			return t.NewDial(ctx, pconn.LocalAddr(), quicConfig)
		},
		func() error {
			defer pconn.Close()

			if err := closef(); err != nil {
				return err
			}

			return pconn.Close()
		}
}

func (t *testServe) TestError() {
	t.Run("already context canceled", func() {
		pconn, err := NewPacketConn(0)
		t.NoError(err)
		defer pconn.Close()

		ctx, cancel := t.Ctx()
		cancel(nil)

		_, tlsConfig := t.NewTLSConfig()

		wait, _, err := Serve(
			ctx,
			pconn,
			tlsConfig,
			DefaultQuicConfig(),
			RandomStatelessResetKey(),
		)
		t.PrintErr(err, "serve")
		t.NoError(err)

		err = wait(nil)
		t.PrintErr(err, "wait")
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("already closed PacketConn", func() {
		pconn, err := NewPacketConn(0)
		t.NoError(err)

		t.PrintIfErrPanic(pconn.Close(), "PacketConn closed")

		_, tlsConfig := t.NewTLSConfig()

		_, _, err = Serve(
			context.Background(),
			pconn,
			tlsConfig,
			DefaultQuicConfig(),
			RandomStatelessResetKey(),
		)
		t.PrintErr(err, "serve")

		var oerr *net.OpError
		t.ErrorAs(err, &oerr)
	})
}

func (t *testServe) TestClose() {
	t.Run("wait after close", func() {
		pconn, err := NewPacketConn(0)
		t.NoError(err)
		defer pconn.Close()

		_, tlsConfig := t.NewTLSConfig()

		wait, closef, err := Serve(
			context.Background(),
			pconn,
			tlsConfig,
			DefaultQuicConfig(),
			RandomStatelessResetKey(),
		)
		t.PrintErr(err, "serve")
		t.NoError(err)

		await := dutil.AssertWaitChan(t.Assert(), time.Second, func() error {
			return wait(nil)
		})
		t.PrintIfErrPanic(closef(), "serve closed")

		err = await()
		t.PrintErr(err, "wait")
		t.NoError(err)
	})
}

func (t *testServe) TestHandleStream() {
	t.Run("ok", func() {
		wait, dial, closef := t.newServe(context.Background(), nil)
		defer closef()

		b := util.UUID().Bytes()

		done := make(chan struct{})

		await := wait(func(ctx context.Context, stream quic.Stream) (context.Context, error) {
			connUID := ctx.Value(QuicConnectionUIDContextKey).(string)
			t.Print("stream accepted: ConnectionID=%q", connUID)

			_, err := stream.Write(b)
			t.PrintIfErrPanic(err, "local stream: write")
			err = stream.Close()
			t.PrintIfErrPanic(err, "local stream: close")

			rb, err := io.ReadAll(stream)
			t.PrintIfErrPanic(err, "local stream: read all")
			t.IfTruePanic(!bytes.Equal(b, rb), "local stream: read all: equal bytes")

			close(done)

			return ctx, nil
		})

		remoteConn, err := dial(context.Background(), nil)
		t.PrintIfErrPanic(err, "remote dial")
		defer remoteConn.CloseWithError(0, "")

		remoteStream, err := remoteConn.OpenStream()
		t.PrintIfErrPanic(err, "remote conn: OpenStream")
		_, err = remoteStream.Write(b)
		t.PrintIfErrPanic(err, "remote stream: write")
		err = remoteStream.Close()
		t.PrintIfErrPanic(err, "remote stream: close")

		rb, err := io.ReadAll(remoteStream)
		t.PrintIfErrPanic(err, "remote stream: read all")
		t.Equal(b, rb)

		select {
		case <-time.After(time.Second):
			t.Fail("local stream not yet")
		case <-done:
		}

		t.NoError(closef())
		t.PrintErr(await(), "wait")
	})

	t.Run("multiple streams", func() {
		wait, dial, closef := t.newServe(context.Background(), nil)
		defer closef()

		b := util.UUID().Bytes()

		var connUID atomic.Value

		var localWg sync.WaitGroup

		await := wait(func(ctx context.Context, stream quic.Stream) (context.Context, error) {
			defer localWg.Done()

			i := ctx.Value(QuicConnectionUIDContextKey).(string)
			old := connUID.Swap(i)
			t.IfTruePanic(old != nil && old != i, "local stream: unexpected new connection: %v", old)

			_, err := stream.Write(b)
			t.IfErrPanicf(err, "local stream: write")
			err = stream.Close()
			t.IfErrPanicf(err, "local stream: close")

			rb, err := io.ReadAll(stream)
			t.IfErrPanicf(err, "local stream: read all")
			t.IfTruePanic(!bytes.Equal(b, rb), "local stream: read all: equal bytes")

			return ctx, nil
		})

		remoteConn, err := dial(context.Background(), nil)
		t.PrintIfErrPanic(err, "remote dial")
		defer remoteConn.CloseWithError(0, "")

		var remoteWg sync.WaitGroup

		for range 333 {
			remoteWg.Add(1)
			localWg.Add(1)

			go func() {
				defer remoteWg.Done()

				remoteStream, err := remoteConn.OpenStream()
				t.IfErrPanicf(err, "remote conn: OpenStream")
				_, err = remoteStream.Write(b)
				t.IfErrPanicf(err, "remote stream: write")
				err = remoteStream.Close()
				t.IfErrPanicf(err, "remote stream: close")

				rb, err := io.ReadAll(remoteStream)
				t.IfErrPanicf(err, "remote stream: read all")
				t.IfTruePanic(!bytes.Equal(b, rb), "remote stream: read all: equal bytes")
			}()
		}

		remoteWg.Wait()
		localWg.Wait()

		t.NoError(closef())

		t.PrintErr(await(), "wait")
	})
}

func TestServe(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testServe))
}
