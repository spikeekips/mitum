package launch

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/stretchr/testify/suite"
)

type testPprofHandler struct {
	suite.Suite
	isaacdatabase.BaseTestDatabase
}

func (t *testPprofHandler) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaacnetwork.ResponseHeaderHint, Instance: isaacnetwork.ResponseHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: PprofRequestHeaderHint, Instance: PprofRequestHeader{}}))
}

func (t *testPprofHandler) writef(remote net.Addr, prefix string, handler quicstream.Handler) isaacnetwork.BaseNetworkClientWriteFunc {
	return func(ctx context.Context, ci quicstream.UDPConnInfo, f quicstream.ClientWriteFunc) (io.ReadCloser, func() error, error) {
		r := bytes.NewBuffer(nil)
		if err := f(r); err != nil {
			return nil, nil, errors.WithStack(err)
		}

		uprefix, err := quicstream.ReadPrefix(r)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		if !bytes.Equal(uprefix, quicstream.HashPrefix(prefix)) {
			return nil, nil, errors.Errorf("unknown request, %q", prefix)
		}

		w := bytes.NewBuffer(nil)
		if err := handler(remote, r, w); err != nil {
			return nil, nil, errors.Wrap(err, "failed to handle request")
		}

		return io.NopCloser(w), func() error { return nil }, nil
	}
}

func (t *testPprofHandler) TestRequest() {
	handler := NetworkHandlerPprofFunc(t.Encs)

	remote := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3333}
	ci := quicstream.NewUDPConnInfo(nil, true)
	c := isaacnetwork.NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(remote, HandlerPrefixPprof, handler))

	t.Run("ok", func() {
		header := NewPprofRequestHeader("heap", 1, true)
		response, i, cancel, err := c.Request(context.Background(), ci, header, nil)
		t.NoError(err)
		defer cancel()

		r, ok := i.(io.Reader)
		t.True(ok)

		t.NoError(response.Err())
		t.True(response.OK())

		var b bytes.Buffer
		_, err = io.Copy(&b, r)
		t.NoError(err)

		t.NotEmpty(b.Bytes())
	})

	t.Run("unknown label", func() {
		header := NewPprofRequestHeader("heap0", 1, true)
		response, i, _, err := c.Request(context.Background(), ci, header, nil)
		t.Error(err)
		t.Nil(response)
		t.Nil(i)

		t.ErrorContains(err, "unknown profile label")
	})
}

func TestPprofHandler(t *testing.T) {
	suite.Run(t, new(testPprofHandler))
}
