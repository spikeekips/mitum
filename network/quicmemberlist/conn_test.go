package quicmemberlist

import (
	"context"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testQConn struct {
	suite.Suite
	laddr net.Addr
	raddr net.Addr
}

func (t *testQConn) SetupTest() {
	t.laddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 0), Port: 4000}
	t.raddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 0), Port: 4001}
}

func (t *testQConn) TestNew() {
	c := newQConn(t.laddr, t.raddr, nil, nil)

	_ = interface{}(c).(net.Conn)
}

func (t *testQConn) TestRead() {
	t.Run("without write", func() {
		c := newQConn(t.laddr, t.raddr, nil, nil)
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := util.AwareContext(
			ctx,
			func(context.Context) error {
				b := make([]byte, 1024)
				_, err := c.Read(b)

				return err
			},
		)

		t.Error(err)
		t.ErrorIs(err, context.DeadlineExceeded)
	})

	t.Run("before write", func() {
		c := newQConn(t.laddr, t.raddr, nil, nil)
		defer c.Close()

		startreadch := make(chan struct{})
		readch := make(chan interface{})

		rb := make([]byte, 1024)
		go func() {
			startreadch <- struct{}{}

			n, err := c.Read(rb)
			readch <- [2]interface{}{n, err}
		}()

		<-startreadch

		b := util.UUID().Bytes()
		t.True(c.writeClose(b))

		r := <-readch
		t.NotNil(r)
		ri := r.([2]interface{})
		n := ri[0].(int)
		err := ri[1]
		t.Nil(err)

		t.Equal(b, rb[:n])
	})

	t.Run("after write", func() {
		c := newQConn(t.laddr, t.raddr, nil, nil)
		defer c.Close()

		b := util.UUID().Bytes()
		t.True(c.writeClose(b))

		rb := make([]byte, 1024)
		n, err := c.Read(rb)
		t.NoError(err)

		t.Equal(len(b), n)
		t.Equal(b, rb[:n])
	})

	t.Run("append after close", func() {
		c := newQConn(t.laddr, t.raddr, nil, nil)
		c.Close()

		t.False(c.writeClose(util.UUID().Bytes()))
	})

	t.Run("after close", func() {
		c := newQConn(t.laddr, t.raddr, nil, nil)
		c.Close()

		rb := make([]byte, 1024)
		n, err := c.Read(rb)
		t.ErrorIs(err, io.EOF)
		t.Equal(0, n)
	})
}

func (t *testQConn) TestWrite() {
	t.Run("basic", func() {
		writech := make(chan []byte, 1)
		writer := func(_ context.Context, b []byte) (int, error) {
			writech <- b

			return len(b), nil
		}

		c := newQConn(t.laddr, t.raddr, writer, nil)
		defer c.Close()

		b := util.UUID().Bytes()
		n, err := c.Write(b)
		t.NoError(err)
		t.Equal(len(b), n)

		rb := <-writech
		t.Equal(b, rb)
	})

	t.Run("after close", func() {
		writer := func(_ context.Context, b []byte) (int, error) {
			return len(b), nil
		}

		c := newQConn(t.laddr, t.raddr, writer, nil)
		c.Close()

		n, err := c.Write(util.UUID().Bytes())
		t.ErrorIs(err, net.ErrClosed)
		t.Equal(0, n)
	})
}

func (t *testQConn) TestClose() {
	t.Run("close", func() {
		c := newQConn(t.laddr, t.raddr, nil, nil)
		t.NoError(c.Close())
	})

	t.Run("close again", func() {
		c := newQConn(t.laddr, t.raddr, nil, nil)
		t.NoError(c.Close())

		err := c.Close()
		t.ErrorIs(err, net.ErrClosed)
	})
}

func (t *testQConn) TestSetReadDeadline() {
	t.Run("set read deadline", func() {
		c := newQConn(t.laddr, t.raddr, nil, nil)
		defer c.Close()

		c.SetReadDeadline(time.Now().Add(time.Millisecond * 300))
		b := make([]byte, 1024)
		n, err := c.Read(b)

		t.Equal(0, n)
		t.ErrorIs(err, os.ErrDeadlineExceeded)
	})

	t.Run("zero time", func() {
		c := newQConn(t.laddr, t.raddr, nil, nil)
		defer c.Close()

		t.NoError(c.SetReadDeadline(time.Time{}))

		_, isnil := c.dr.Value()
		t.True(isnil)
	})
}

func (t *testQConn) TestSetWriteDeadline() {
	t.Run("set write deadline", func() {
		c := newQConn(t.laddr, t.raddr,
			func(ctx context.Context, b []byte) (int, error) {
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-time.After(time.Second * 2):
					return len(b), nil
				}
			},
			nil,
		)
		defer c.Close()

		c.SetWriteDeadline(time.Now().Add(time.Millisecond * 300))
		n, err := c.Write(util.UUID().Bytes())
		t.Equal(0, n)
		t.ErrorIs(err, os.ErrDeadlineExceeded)
	})

	t.Run("zero time", func() {
		c := newQConn(t.laddr, t.raddr, nil, nil)
		defer c.Close()

		t.NoError(c.SetWriteDeadline(time.Time{}))

		_, isnil := c.dw.Value()
		t.True(isnil)
	})
}

func (t *testQConn) TestSetDeadline() {
	c := newQConn(t.laddr, t.raddr, nil, nil)
	defer c.Close()

	t.Run("check read, write", func() {
		dt := time.Now().Add(time.Millisecond * 300)
		t.NoError(c.SetDeadline(dt))

		i, _ := c.dr.Value()
		t.Equal(dt, i)
		i, _ = c.dw.Value()
		t.Equal(dt, i)
	})

	t.Run("zero time", func() {
		t.NoError(c.SetDeadline(time.Time{}))

		_, isnil := c.dr.Value()
		t.True(isnil)
		_, isnil = c.dw.Value()
		t.True(isnil)
	})
}

func TestQConn(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testQConn))
}
