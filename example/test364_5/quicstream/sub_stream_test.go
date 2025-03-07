package quicstream

import (
	"context"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type baseTestConnAcceptSubStreams struct {
	BaseTest
}

func (t *baseTestConnAcceptSubStreams) newFuncNewID(ctx context.Context, size uint16) (
	func() ([]quic.Stream, error),
	dutil.InErrorFunc[error],
	dutil.OutFunc[bool],
	func(),
	error,
) {
	_, _, localConn, remoteConn, closef := t.OpenConnections(
		context.Background(), context.Background(),
		nil, nil,
	)

	newID, sclose, err := newConnAcceptSubStreams(context.Background(), remoteConn)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	id := util.UUID().String()

	waitStreams, doneStreams, err := newID(ctx, id, size)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	controlStream, err := openSubStreamSync(context.Background(), localConn, id)
	t.PrintIfErrPanic(err, "local conn: open control stream")

	go func() {
		for range size - 1 {
			_, err := openSubStreamSync(context.Background(), localConn, id)
			t.PrintIfErr(err, "local conn: OpenStream")
		}

		err := frame.WriteSize(controlStream, uint32(size))
		t.PrintIfErrPanic(err, "local conn: control stream: write size, %d", size)
	}()

	return waitStreams, doneStreams, sclose, closef, nil
}

type testConnAcceptSubStreams struct {
	baseTestConnAcceptSubStreams
}

func (t *testConnAcceptSubStreams) TestError() {
	t.RunWithGoLeak("newConnAcceptSubStreams with canceled context", func() {
		_, _, _, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		ctx, cancel := t.Ctx()
		cancel(nil)

		newID, sclose, err := newConnAcceptSubStreams(ctx, remoteConn)
		t.NoError(err)
		defer sclose()

		<-time.After(time.Millisecond * 33)

		_, _, err = newID(context.Background(), util.UUID().String(), 1)
		t.ErrorIs(err, errConnAcceptSubStreamsClosed)
	})

	t.RunWithGoLeak("new id with canceled context", func() {
		_, _, _, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		newID, sclose, err := newConnAcceptSubStreams(context.Background(), remoteConn)
		t.NoError(err)
		defer sclose()

		<-time.After(time.Millisecond * 33)

		ctx, cancel := t.Ctx()
		cancel(nil)

		waitStreams, _, err := newID(ctx, util.UUID().String(), 1)
		t.NoError(err)

		streams, err := waitStreams()
		t.ErrorIs(err, context.Canceled)
		t.Empty(streams)
	})

	t.RunWithGoLeak("done streams with error", func() {
		_, _, _, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		newID, sclose, err := newConnAcceptSubStreams(context.Background(), remoteConn)
		t.NoError(err)
		defer sclose()

		<-time.After(time.Millisecond * 33)

		ctx, cancel := t.Ctx()
		defer cancel(nil)

		waitStreams, doneStreams, err := newID(ctx, util.UUID().String(), 1)
		t.NoError(err)

		t.T().Log("done streams before wait streams")
		t.ErrorContains(doneStreams(errors.Errorf("findme")), "findme")

		_, err = waitStreams()
		t.ErrorContains(err, "findme")
	})
}

func (t *testConnAcceptSubStreams) TestWaitStream() {
	t.RunWithGoLeak("wait stream; timeout", func() {
		_, _, _, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		newID, sclose, err := newConnAcceptSubStreams(context.Background(), remoteConn)
		t.NoError(err)
		defer sclose()

		<-time.After(time.Millisecond * 33)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*333)
		defer cancel()

		waitStreams, _, err := newID(ctx, util.UUID().String(), 3)
		t.NoError(err)

		waitdone := make(chan [2]interface{})
		go func() {
			streams, err := waitStreams()
			t.PrintErr(err, "wait stream: streams=%v", streams)

			waitdone <- [2]interface{}{streams, err}
		}()

		select {
		case <-time.After(time.Second):
			t.Fail("wait timeout")
		case i := <-waitdone:
			t.Empty(i[0])
			t.ErrorIs(i[1].(error), context.DeadlineExceeded)
		}
	})

	t.RunWithGoLeak("wrong received size", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		newID, sclose, err := newConnAcceptSubStreams(context.Background(), remoteConn)
		t.NoError(err)
		defer sclose()

		var size uint16 = 5
		id := util.UUID().String()

		ctx, cancel := t.Ctx()
		defer cancel(nil)

		waitStreams, doneStreams, err := newID(ctx, id, size)
		t.NoError(err)
		defer doneStreams(nil)

		controlStream, err := openSubStreamSync(ctx, localConn, id)
		t.PrintIfErrPanic(err, "local conn: open control stream")

		go func() {
			for range size - 2 {
				_, err := openSubStreamSync(ctx, localConn, id)
				t.IfErrPanicf(err, "local conn: OpenStream")
			}

			err := frame.WriteSize(controlStream, uint32(size))
			t.PrintIfErrPanic(err, "local conn: control stream: write size, %d", size)
		}()

		waitch := make(chan struct{})

		go func() {
			_, err := waitStreams()
			t.PrintIfErr(err, "wait stream")

			close(waitch)
		}()

		select {
		case <-time.After(time.Second * 2):
		case <-waitch:
			t.Fail("waitStreams shoule be blocked")
		}
	})

	t.RunWithGoLeak("one id", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		newID, sclose, err := newConnAcceptSubStreams(context.Background(), remoteConn)
		t.NoError(err)
		defer sclose()

		var size uint16 = 3
		id := util.UUID().String()

		ctx, cancel := t.Ctx()
		defer cancel(nil)

		waitStreams, doneStreams, err := newID(ctx, id, size)
		t.NoError(err)

		controlStream, err := openSubStreamSync(ctx, localConn, id)
		t.PrintIfErrPanic(err, "local conn: open control stream")

		go func() {
			for range size - 1 {
				_, err := openSubStreamSync(ctx, localConn, id)
				t.IfErrPanicf(err, "local conn: OpenStream")
			}

			err := frame.WriteSize(controlStream, uint32(size))
			t.PrintIfErrPanic(err, "local conn: control stream: write size, %d", size)
		}()

		streams, err := waitStreams()
		t.NoError(err)
		t.EqualValues(size, len(streams))
		t.NoError(doneStreams(nil))

		t.T().Log("check newID is closed")

		_, _, err = newID(ctx, util.UUID().String(), 3)
		t.NoError(err)

		t.False(sclose())
	})

	t.RunWithGoLeak("multiple ids", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		newID, sclose, err := newConnAcceptSubStreams(context.Background(), remoteConn)
		t.NoError(err)
		defer sclose()

		ids := []string{
			util.UUID().String(),
			util.UUID().String(),
			util.UUID().String(),
		}

		var size uint16 = 3

		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ws := map[string]func() ([]quic.Stream, error){}
		ds := map[string]dutil.InErrorFunc[error]{}
		cs := map[string]quic.Stream{}

		for id := range slices.Values(ids) {
			waitStreams, doneStreams, err := newID(ctx, id, size)
			t.NoError(err)

			ws[id] = waitStreams
			ds[id] = doneStreams

			cs[id], err = openSubStreamSync(ctx, localConn, id)
			t.PrintIfErrPanic(err, "local conn: open control stream")
		}

		var wg sync.WaitGroup

		for id := range slices.Values(ids) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				streams, err := ws[id]()
				t.IfErrPanicf(err, "wait streams: id=%q", id)
				t.IfTruePanic(int(size) != len(streams), "wrong streams size: id=%q size=%d streams=%d", id, size, len(streams))
			}()
		}

		for id := range slices.Values(ids) {
			for range size - 1 {
				_, err := openSubStreamSync(ctx, localConn, id)
				t.IfErrPanicf(err, "local conn: OpenStream")
			}

			err := frame.WriteSize(cs[id], uint32(size))
			t.PrintIfErrPanic(err, "local conn: control stream: write size, %d", size)
		}

		wg.Wait()

		wg = sync.WaitGroup{}

		for id := range slices.Values(ids) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				err := ds[id](nil)
				t.IfErrPanicf(err, "done streams: id=%q", id)
			}()
		}

		wg.Wait()

		t.T().Log("check newID is not closed")

		_, _, err = newID(ctx, util.UUID().String(), 3)
		t.NoError(err)

		t.False(sclose())
	})

	t.RunWithGoLeak("async", func() {
		var size uint16 = 3

		ids := make([]string, 33)
		for i := range len(ids) {
			ids[i] = util.UUID().String()
		}

		ws := dutil.NewSingleLockedMap[string, func() ([]quic.Stream, error)]()
		ds := dutil.NewSingleLockedMap[string, dutil.InErrorFunc[error]]()

		_, _, localConn, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		newID, sclose, err := newConnAcceptSubStreams(context.Background(), remoteConn)
		t.NoError(err)
		defer sclose()

		<-time.After(time.Millisecond * 333)

		var wg sync.WaitGroup

		ctx, cancel := t.Ctx()
		defer cancel(nil)

		for id := range slices.Values(ids) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				waitStreams, doneStreams, err := newID(ctx, id, size)
				t.IfErrPanicf(err, "new id")

				ws.SetValue(id, waitStreams)
				ds.SetValue(id, doneStreams)
			}()
		}

		wg.Wait()

		wg = sync.WaitGroup{}

		for id := range slices.Values(ids) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				controlStream, err := openSubStreamSync(ctx, localConn, id)
				t.PrintIfErrPanic(err, "local conn: open control stream")

				for range size - 1 {
					_, err := openSubStreamSync(ctx, localConn, id)
					t.IfErrPanicf(err, "local conn: OpenStream")
				}

				err = frame.WriteSize(controlStream, uint32(size))
				t.PrintIfErrPanic(err, "local conn: control stream: write size, %d", size)
			}()
		}

		wg.Wait()

		wg = sync.WaitGroup{}

		for id := range slices.Values(ids) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				w, _ := ws.Value(id)
				d, _ := ds.Value(id)
				defer d(nil)

				streams, err := w()
				t.IfErrPanicf(err, "wait streams: id=%q", id)
				t.IfTruePanic(int(size) != len(streams), "wrong streams size: id=%q size=%d streams=%d", id, size, len(streams))
			}()
		}

		wg.Wait()
	})
}

func (t *testConnAcceptSubStreams) TestDoneStream() {
	done := func(gerr error) (quic.Stream, func()) {
		var size uint16 = 3

		waitStreams, doneStreams, sclose, closef, err := t.newFuncNewID(context.Background(), size)
		t.NoError(err)

		streams, err := waitStreams()
		t.NoError(err)
		t.EqualValues(size, len(streams))

		doneStreams(gerr)

		return streams[0], func() {
			sclose()
			closef()
		}
	}

	t.RunWithGoLeak("stream.Read after done stream", func() {
		stream, closef := done(nil)
		defer closef()

		_, err := io.ReadAll(stream)
		t.PrintErr(err, "stream: read all")

		var serr *quic.StreamError
		t.ErrorAs(err, &serr)
		t.Equal(streamErrorCodeOK, serr.ErrorCode)
		t.False(serr.Remote)
	})

	t.RunWithGoLeak("stream.Read after done stream with error", func() {
		stream, closef := done(errors.Errorf("showme"))
		defer closef()

		_, err := io.ReadAll(stream)
		t.PrintErr(err, "stream: read all")

		var serr *quic.StreamError
		t.ErrorAs(err, &serr)
		t.Equal(streamErrorCodeUnknown, serr.ErrorCode)
		t.False(serr.Remote)
	})

	t.RunWithGoLeak("stream.Write after done stream", func() {
		stream, closef := done(nil)
		defer closef()

		_, err := stream.Write([]byte{0})
		t.PrintErr(err, "stream: write")
		t.ErrorContains(err, "write on closed stream")
	})

	t.RunWithGoLeak("stream.Write after done stream with error", func() {
		stream, closef := done(errors.Errorf("showme"))
		defer closef()

		_, err := stream.Write([]byte{0})
		t.PrintErr(err, "stream: write")

		var serr *quic.StreamError
		t.ErrorAs(err, &serr)
		t.Equal(streamErrorCodeUnknown, serr.ErrorCode)
		t.False(serr.Remote)
	})
}

func (t *testConnAcceptSubStreams) TestClose() {
	t.RunWithGoLeak("not empty ids", func() {
		_, _, _, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		newID, sclose, err := newConnAcceptSubStreams(context.Background(), remoteConn)
		t.NoError(err)

		_, doneStreams, err := newID(context.Background(), util.UUID().String(), 1)
		t.NoError(err)

		t.False(sclose())
		doneStreams(nil)
		t.True(sclose())
	})

	t.RunWithGoLeak("close before new id", func() {
		_, _, _, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		newID, sclose, err := newConnAcceptSubStreams(context.Background(), remoteConn)
		t.NoError(err)

		t.True(sclose())

		_, _, err = newID(context.Background(), util.UUID().String(), 1)
		t.ErrorIs(err, errConnAcceptSubStreamsClosed)
	})
}

func TestConnAcceptSubStreams(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testConnAcceptSubStreams))
}

type testConnsAcceptSubStreams struct {
	baseTestConnAcceptSubStreams
}

func (t *testConnsAcceptSubStreams) TestError() {
	t.RunWithGoLeak("after closed", func() {
		o, c, _ := NewConnsAcceptSubStreams(context.Background(), time.Minute)

		c()

		_, _, localConn, _, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		_, _, err := o(context.Background(), localConn, util.UUID().String(), 1)
		t.ErrorIs(err, dutil.ErrLockedMapClosed)
	})
}

func (t *testConnsAcceptSubStreams) TestNewID() {
	t.RunWithGoLeak("ok", func() {
		o, c, _ := NewConnsAcceptSubStreams(context.Background(), time.Minute)
		defer c()

		_, _, _, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		_, done, err := o(context.Background(), remoteConn, util.UUID().String(), 1)
		t.NoError(err)
		t.NoError(done(nil))

		_, done, err = o(context.Background(), remoteConn, util.UUID().String(), 1)
		t.NoError(err)
		t.NoError(done(nil))
	})
}

func (t *testConnsAcceptSubStreams) TestRemoveAfter() {
	t.RunWithGoLeak("ok", func() {
		removeAfter := time.Second * 2
		o, c, pool := NewConnsAcceptSubStreams(context.Background(), removeAfter)
		defer c()

		_, _, _, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		_, done, err := o(context.Background(), remoteConn, util.UUID().String(), 1)
		t.NoError(err)
		t.NoError(done(nil))

		t.Equal(1, pool.Len())

		<-time.After(removeAfter * 2)

		t.Equal(0, pool.Len())
	})

	t.RunWithGoLeak("expired, but not empty", func() {
		removeAfter := time.Second * 2
		o, c, pool := NewConnsAcceptSubStreams(context.Background(), removeAfter)
		defer c()

		_, _, _, remoteConn, closef := t.OpenConnections(
			context.Background(), context.Background(),
			nil, nil,
		)
		defer closef()

		_, done, err := o(context.Background(), remoteConn, util.UUID().String(), 1)
		t.NoError(err)
		defer done(nil)

		t.Equal(1, pool.Len())

		<-time.After(removeAfter * 2)

		t.Equal(1, pool.Len())
	})
}

func TestConnsAcceptSubStreams(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testConnsAcceptSubStreams))
}

type testRequestSubStreams struct {
	baseTestConnAcceptSubStreams
}

func (t *testRequestSubStreams) TestError() {
	t.RunWithGoLeak("empty id", func() {
		_, err := OpenSubStreams(context.Background(), nil, nil, nil, "", 1)
		t.ErrorContains(err, "empty id")
	})

	t.RunWithGoLeak("canceled context", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		_, _, localConn, remoteConn, closef := t.OpenConnections(
			ctx, context.Background(),
			nil, nil,
		)
		defer closef()

		var size uint16 = 3

		localStreamCh := make(chan quic.Stream)

		go func() {
			stream, err := localConn.AcceptStream(ctx)
			t.PrintIfErrPanic(err, "local conn: AcceptStream")

			_, _, err = frame.ReadFixed(stream)
			t.IfErrPanicf(err, "local stream: read")

			err = frame.WriteSize(stream, uint32(size))
			t.IfErrPanicf(err, "local stream: write size")

			localStreamCh <- stream
		}()

		remoteStream, err := remoteConn.OpenStreamSync(ctx)
		t.NoError(err)
		t.NoError(frame.WriteFixed(remoteStream, util.UUID().Bytes()))

		select {
		case <-time.After(time.Second):
			t.Fail("wait local stream")
		case <-localStreamCh:
		}

		o, c, _ := NewConnsAcceptSubStreams(ctx, time.Minute)
		defer c()

		cctx, scancel := t.Ctx()
		scancel(nil)

		_, err = OpenSubStreams(cctx, o, remoteConn, remoteStream, util.UUID().String(), size)
		t.ErrorIs(err, context.Canceled)
	})

	t.RunWithGoLeak("local close stream", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		_, _, localConn, remoteConn, closef := t.OpenConnections(
			ctx, context.Background(),
			nil, nil,
		)
		defer closef()

		var size uint16 = 3

		localStreamCh := make(chan struct{})

		go func() {
			stream, err := localConn.AcceptStream(ctx)
			t.PrintIfErrPanic(err, "local conn: AcceptStream")

			_, _, err = frame.ReadFixed(stream)
			t.IfErrPanicf(err, "local stream: read")
			t.IfErrPanicf(stream.Close(), "local stream: close")

			close(localStreamCh)
		}()

		remoteStream, err := remoteConn.OpenStreamSync(ctx)
		t.NoError(err)
		t.NoError(frame.WriteFixed(remoteStream, util.UUID().Bytes()))

		select {
		case <-time.After(time.Second):
			t.Fail("wait local stream")
		case <-localStreamCh:
		}

		o, c, _ := NewConnsAcceptSubStreams(ctx, time.Minute)
		defer c()

		cctx, scancel := t.Ctx()
		scancel(nil)

		_, err = OpenSubStreams(cctx, o, remoteConn, remoteStream, util.UUID().String(), size)
		t.ErrorIs(err, context.Canceled)
		t.ErrorContains(err, "failed to wait requested sub stream")
	})
}

func (t *testRequestSubStreams) openSubStreams(
	ctx context.Context,
	size, maxSize uint16,
	inLocal dutil.InErrorFunc[[]quic.Stream],
	inRemote dutil.InErrorFunc[[]quic.Stream],
) {
	rsize := min(size, maxSize)

	_, _, localConn, remoteConn, closef := t.OpenConnections(
		ctx, context.Background(),
		nil, nil,
	)
	defer closef()

	localStreamCh := make(chan struct{})

	go func() {
		stream, err := localConn.AcceptStream(ctx)
		t.PrintIfErrPanic(err, "local conn: AcceptStream")
		defer func() {
			stream.Close()
			stream.CancelRead(0)
		}()

		_, _, err = frame.ReadFixed(stream)
		t.IfErrPanicf(err, "local stream: read")

		close(localStreamCh)

		f, err := AcceptSubStreams(ctx, localConn, stream, maxSize)
		t.IfErrPanicf(err, "local stream: open")

		t.IfErrPanicf(f(func(streams []quic.Stream) error {
			t.IfTruePanic(len(streams) != int(rsize), "local stream: len(streams) %d != int(size) %d", len(streams), int(rsize))

			return nil
		}), "local stream: streams")
	}()

	remoteStream, err := remoteConn.OpenStreamSync(ctx)
	t.NoError(err)
	t.NoError(frame.WriteFixed(remoteStream, util.UUID().Bytes()))

	select {
	case <-time.After(time.Second):
		t.Fail("wait local stream")
	case <-localStreamCh:
	}

	o, c, _ := NewConnsAcceptSubStreams(context.Background(), time.Minute)
	defer c()

	cctx, scancel := t.Ctx()
	defer scancel(nil)

	f, err := OpenSubStreams(cctx, o, remoteConn, remoteStream, util.UUID().String(), size)

	t.NoError(f(func(streams []quic.Stream) error {
		t.EqualValues(rsize, len(streams))

		return nil
	}))
}

func (t *testRequestSubStreams) TestOpenSubStreams() {
	t.RunWithGoLeak("ok", func() {
		t.openSubStreams(
			context.Background(),
			3, 3,
			func(streams []quic.Stream) error {
				t.Print("local stream: sub streams")

				return nil
			},
			func(streams []quic.Stream) error {
				t.Print("remote stream: sub streams")

				return nil
			},
		)
	})

	t.RunWithGoLeak("ok; size = 1", func() {
		t.openSubStreams(
			context.Background(),
			1, 2,
			func(streams []quic.Stream) error {
				t.Print("local stream: sub streams")

				return nil
			},
			func(streams []quic.Stream) error {
				t.Print("remote stream: sub streams")

				return nil
			},
		)
	})

	t.RunWithGoLeak("ok; max size == size - 1", func() {
		t.openSubStreams(
			context.Background(),
			3, 2,
			func(streams []quic.Stream) error {
				t.Print("local stream: sub streams")

				return nil
			},
			func(streams []quic.Stream) error {
				t.Print("remote stream: sub streams")

				return nil
			},
		)
	})

	t.RunWithGoLeak("zero request size", func() {
		t.openSubStreams(
			context.Background(),
			0, 3,
			func(streams []quic.Stream) error {
				t.Print("local stream: sub streams")

				return nil
			},
			func(streams []quic.Stream) error {
				t.Print("remote stream: sub streams")

				return nil
			},
		)
	})

	t.RunWithGoLeak("zero received size", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		_, _, localConn, remoteConn, closef := t.OpenConnections(
			ctx, context.Background(),
			nil, nil,
		)
		defer closef()

		var size uint16 = 3

		localStreamCh := make(chan struct{})

		go func() {
			stream, err := localConn.AcceptStream(ctx)
			t.PrintIfErrPanic(err, "local conn: AcceptStream")

			_, _, err = frame.ReadFixed(stream)
			t.IfErrPanicf(err, "local stream: read")

			close(localStreamCh)

			b, _, err := frame.ReadFixed(stream)
			t.IfErrPanicf(err, "local stream: read id")
			id := string(b)

			var rsize uint32
			_, err = frame.ReadSize(stream, &rsize)
			t.PrintIfErrPanic(err, "local stream: read size, %d")
			t.IfTruePanic(int(rsize) != int(size), "local stream: rsize, %d != size, %d", rsize, size)

			controlStream, err := openSubStreamSync(ctx, localConn, id)
			t.IfErrPanicf(err, "local control stream: OpenStream")

			t.Print("local stream: send zero size, rsize=%d", rsize)
			err = frame.WriteSize(controlStream, 0)
			t.IfErrPanicf(err, "local stream: write zero size")
		}()

		remoteStream, err := remoteConn.OpenStreamSync(ctx)
		t.NoError(err)
		t.NoError(frame.WriteFixed(remoteStream, util.UUID().Bytes()))

		select {
		case <-time.After(time.Second):
			t.Fail("wait local stream")
		case <-localStreamCh:
		}

		o, c, _ := NewConnsAcceptSubStreams(ctx, time.Minute)
		defer c()

		cctx, scancel := t.Ctx()
		defer scancel(nil)

		f, err := OpenSubStreams(cctx, o, remoteConn, remoteStream, util.UUID().String(), size)
		t.NoError(err)
		t.NoError(f(func(streams []quic.Stream) error {
			t.Equal(1, len(streams)) // NOTE control stream

			return nil
		}))
	})
}

func TestRequestSubStreams(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testRequestSubStreams))
}

type testAcceptOpenSubStreams struct {
	baseTestConnAcceptSubStreams
}

func (t *testAcceptOpenSubStreams) TestZeroMaxSize() {
	ctx, cancel := t.Ctx()
	defer cancel(nil)

	_, _, localConn, remoteConn, closef := t.OpenConnections(
		ctx, context.Background(),
		nil, nil,
	)
	defer closef()

	localStreamCh := make(chan struct{})

	go func() {
		stream, err := localConn.AcceptStream(ctx)
		t.PrintIfErrPanic(err, "local conn: AcceptStream")
		defer stream.Close()

		_, _, err = frame.ReadFixed(stream)
		t.IfErrPanicf(err, "local stream: read")

		close(localStreamCh)

		err = frame.WriteFixed(stream, util.UUID().Bytes())
		t.IfErrPanicf(err, "local stream: write id")

		err = frame.WriteSize(stream, 1)
		t.IfErrPanicf(err, "local stream: write size")
	}()

	remoteStream, err := remoteConn.OpenStreamSync(ctx)
	t.NoError(err)
	t.NoError(frame.WriteFixed(remoteStream, util.UUID().Bytes()))

	select {
	case <-time.After(time.Second):
		t.Fail("wait local stream")
	case <-localStreamCh:
	}

	f, err := AcceptSubStreams(context.Background(), nil, remoteStream, 0)
	t.NoError(err)

	var called bool

	t.NoError(f(func([]quic.Stream) error {
		called = true

		return nil
	}))
	t.True(called)
}

func TestAcceptOpenSubStreams(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testAcceptOpenSubStreams))
}
