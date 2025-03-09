package quicstream

import (
	"net"
	"slices"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"golang.org/x/crypto/sha3"
)

const (
	connErrorCodeOK quic.ApplicationErrorCode = iota
	connErrorCodeUnknown
)

const (
	streamErrorCodeOK quic.StreamErrorCode = iota
	streamErrorCodeNoStreamHandler
	streamErrorCodeUnknown
)

func NewStatelessResetKey(b []byte) *quic.StatelessResetKey {
	i := quic.StatelessResetKey(sha3.Sum256(b))

	return &i
}

func DefaultQuicConfig() *quic.Config {
	return &quic.Config{
		MaxIncomingStreams:    1 << 14,
		MaxIncomingUniStreams: 1 << 14,
		MaxIdleTimeout:        time.Second,
		KeepAlivePeriod:       time.Second,
		Allow0RTT:             true,
	}
}

func NewPacketConn(port int) (net.PacketConn, error) {
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: port})

	return c, errors.WithStack(err)
}

func closeConnection(conn quic.Connection, err error) error {
	var errcode quic.ApplicationErrorCode
	var errmsg string

	switch {
	case err != nil:
		errcode = connErrorCodeUnknown
		errmsg = err.Error()
	default:
		errcode = connErrorCodeOK
	}

	return errors.WithStack(conn.CloseWithError(errcode, errmsg))
}

func cancelStream(stream quic.Stream, code quic.StreamErrorCode) error {
	stream.CancelRead(code)

	if code != streamErrorCodeOK {
		stream.CancelWrite(code)
	}

	return errors.WithStack(stream.Close())
}

func newFuncCloseStreamWithError(err error) func(quic.Stream) error {
	switch {
	case err != nil:
		return func(stream quic.Stream) error {
			_ = cancelStream(stream, streamErrorCodeUnknown)

			return err
		}
	default:
		return func(stream quic.Stream) error {
			_ = cancelStream(stream, streamErrorCodeOK)

			return nil
		}
	}
}

func closeStreamWithError(stream quic.Stream, err error) error {
	return newFuncCloseStreamWithError(err)(stream)
}

func closeStreamsWithError(streams []quic.Stream, err error) error {
	if err == nil {
		var wg sync.WaitGroup

		for stream := range slices.Values(streams) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				_ = stream.Close()
			}()
		}

		wg.Wait()
	}

	f := newFuncCloseStreamWithError(err)

	slices.Values(streams)(func(stream quic.Stream) bool {
		_ = f(stream)

		return true
	})

	return err
}
