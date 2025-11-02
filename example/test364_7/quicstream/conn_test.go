package quicstream

import (
	"context"
	"testing"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/suite"
)

type testNoCloseQuicConn struct {
	BaseTest
}

func (t *testNoCloseQuicConn) TestClose() {
	pconn, listener := t.RandomListener(nil, nil)
	defer func() {
		listener.Close()
		pconn.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := Dial(ctx, pconn.LocalAddr(), t.NewTLSConfig(), DefaultQuicConfig())
	t.NoError(err)
	defer conn.CloseWithError(3, "")

	nconn := newNoCloseConn(conn)

	// before closed
	//_, err = nconn.AcceptStream(ctx)
	//t.NoError(err)
	//_, err = nconn.AcceptUniStream(ctx)
	//t.NoError(err)

	tp := &quic.Transport{Conn: pconn}
	_, err = nconn.AddPath(tp)
	t.NoError(err)
	c := nconn.HandshakeComplete()
	t.NotNil(c)
	_, err = nconn.NextConnection(ctx)
	t.NoError(err)
	_, err = nconn.OpenStream()
	t.NoError(err)
	_, err = nconn.OpenStreamSync(ctx)
	t.NoError(err)
	_, err = nconn.OpenUniStream()
	t.NoError(err)
	_, err = nconn.OpenUniStreamSync(ctx)
	t.NoError(err)
	//_, err = nconn.ReceiveDatagram(ctx)
	//t.NoError(err)
	err = nconn.SendDatagram([]byte{0x00})
	t.NoError(err)

	// after closed
	nconn.CloseWithError(3, "")

	//_, err = nconn.AcceptStream(ctx)
	//t.ErrorIs(err, quic.ErrTransportClosed)
	//_, err = nconn.AcceptUniStream(ctx)
	//t.ErrorIs(err, quic.ErrTransportClosed)

	_, err = nconn.AddPath(tp)
	t.ErrorIs(err, quic.ErrTransportClosed)
	c = nconn.HandshakeComplete()
	t.Nil(c)
	_, err = nconn.NextConnection(ctx)
	t.ErrorIs(err, quic.ErrTransportClosed)
	_, err = nconn.OpenStream()
	t.ErrorIs(err, quic.ErrTransportClosed)
	_, err = nconn.OpenStreamSync(ctx)
	t.ErrorIs(err, quic.ErrTransportClosed)
	_, err = nconn.OpenUniStream()
	t.ErrorIs(err, quic.ErrTransportClosed)
	_, err = nconn.OpenUniStreamSync(ctx)
	t.ErrorIs(err, quic.ErrTransportClosed)
	//_, err = nconn.ReceiveDatagram(ctx)
	//t.ErrorIs(err, quic.ErrTransportClosed)
	err = nconn.SendDatagram([]byte{0x00})
	t.ErrorIs(err, quic.ErrTransportClosed)
}

func (t *testNoCloseQuicConn) TestCloseWithError() {
	t.Run("connErrorCodeOK", func() {
		pconn, listener := t.RandomListener(nil, nil)
		defer func() {
			listener.Close()
			pconn.Close()
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, err := Dial(ctx, pconn.LocalAddr(), t.NewTLSConfig(), DefaultQuicConfig())
		t.NoError(err)
		defer conn.CloseWithError(3, "")

		nconn := newNoCloseConn(conn).(*noCloseConn)

		t.NoError(nconn.Context().Err())
		t.NoError(nconn.Conn.Context().Err())

		t.NoError(nconn.CloseWithError(connErrorCodeOK, ""))

		t.NoError(nconn.Context().Err())
		t.NoError(nconn.Conn.Context().Err())
	})

	t.Run("other error", func() {
		pconn, listener := t.RandomListener(nil, nil)
		defer func() {
			listener.Close()
			pconn.Close()
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, err := Dial(ctx, pconn.LocalAddr(), t.NewTLSConfig(), DefaultQuicConfig())
		t.NoError(err)
		defer conn.CloseWithError(3, "")

		nconn := newNoCloseConn(conn).(*noCloseConn)

		t.NoError(nconn.Context().Err())
		t.NoError(nconn.Conn.Context().Err())

		t.NoError(nconn.CloseWithError(connErrorCodeUnknown, ""))

		err = nconn.Context().Err()
		t.ErrorIs(err, context.Canceled)
		t.Error(nconn.Conn.Context().Err())
	})
}

func TestNoCloseQuicConn(t *testing.T) {
	suite.Run(t, new(testNoCloseQuicConn))
}
