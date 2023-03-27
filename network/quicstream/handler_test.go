package quicstream

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testPrefixHandler struct {
	BaseTest
}

func (t *testPrefixHandler) Test() {
	aprefix := HashPrefix("findme")
	bprefix := HashPrefix("showme")

	handler := NewPrefixHandler(func(_ net.Addr, r io.Reader, w io.Writer, err error) error {
		_, _ = w.Write([]byte("hehehe"))

		return nil
	})
	handler.Add(aprefix, func(_ net.Addr, r io.Reader, w io.Writer) error {
		b, _ := io.ReadAll(r)
		_, _ = w.Write(b)

		return nil
	})

	handler.Add(bprefix, func(_ net.Addr, r io.Reader, w io.Writer) error {
		b, _ := io.ReadAll(r)
		_, _ = w.Write(b)

		return nil
	})

	srv := t.NewDefaultServer(nil, Handler(handler.Handler))

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	client := t.NewClient(t.Bind)

	t.Run("findme", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		b := util.UUID().Bytes()
		r, w, err := client.OpenStream(ctx)
		t.NoError(err)

		_, err = w.Write(writeWithPrefix(aprefix, b))
		t.NoError(err)

		t.NoError(w.Close())
		defer r.Close()

		rb, err := io.ReadAll(r)
		t.NoError(err)
		t.Equal(b, rb)
	})

	t.Run("showme", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		b := util.UUID().Bytes()
		r, w, err := client.OpenStream(ctx)
		t.NoError(err)

		_, err = w.Write(writeWithPrefix(bprefix, b))
		t.NoError(err)

		t.NoError(w.Close())
		defer r.Close()

		rb, err := io.ReadAll(r)
		t.NoError(err)
		t.Equal(b, rb)
	})

	t.Run("unknown handler", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		b := util.UUID().Bytes()
		r, w, err := client.OpenStream(ctx)
		t.NoError(err)

		_, err = w.Write(writeWithPrefix([]byte("unknown"), b))
		t.NoError(err)

		t.NoError(w.Close())
		defer r.Close()

		rb, err := io.ReadAll(r)
		t.NoError(err)
		t.Equal([]byte("hehehe"), rb)
	})
}

func TestPrefixHandler(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testPrefixHandler))
}

func writeWithPrefix(prefix []byte, b []byte) []byte {
	w := bytes.NewBuffer(nil)
	defer w.Reset()

	_ = WritePrefix(w, prefix)
	_, _ = w.Write(b)

	return w.Bytes()
}
