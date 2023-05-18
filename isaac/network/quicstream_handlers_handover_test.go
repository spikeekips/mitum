package isaacnetwork

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
)

func (t *testQuicstreamHandlers) TestStartHandover() {
	xci := quicstream.RandomConnInfo()
	yci := quicstream.RandomConnInfo()

	t.Run("failed to verify node", func() {
		handler := QuicstreamHandlerStartHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			func(context.Context, base.Address, quicstream.UDPConnInfo) error { return nil },
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixStartHandover, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		started, err := c.StartHandover(context.Background(),
			yci,
			base.NewMPrivatekey(),
			t.LocalParams.NetworkID(),
			t.Local.Address(),
			xci,
		)
		t.Error(err)
		t.False(started)
		t.ErrorContains(err, "signature verification failed")
	})

	t.Run("ok", func() {
		startedch := make(chan struct{}, 1)
		handler := QuicstreamHandlerStartHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			func(context.Context, base.Address, quicstream.UDPConnInfo) error {
				startedch <- struct{}{}

				return nil
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixStartHandover, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		started, err := c.StartHandover(context.Background(),
			yci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
			t.Local.Address(),
			xci,
		)
		t.NoError(err)
		t.True(started)

		select {
		case <-time.After(time.Second):
			t.NoError(errors.Errorf("not started"))
		case <-startedch:
		}
	})

	t.Run("start error", func() {
		handler := QuicstreamHandlerStartHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			func(context.Context, base.Address, quicstream.UDPConnInfo) error {
				return errors.Errorf("hihihi")
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixStartHandover, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		started, err := c.StartHandover(context.Background(),
			yci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
			t.Local.Address(),
			xci,
		)
		t.Error(err)
		t.False(started)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testQuicstreamHandlers) TestCancelHandover() {
	localci := quicstream.RandomConnInfo()

	t.Run("failed to verify node", func() {
		handler := QuicstreamHandlerCancelHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			func() error { return nil },
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixCancelHandover, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		canceled, err := c.CancelHandover(context.Background(),
			localci,
			base.NewMPrivatekey(),
			t.LocalParams.NetworkID(),
		)
		t.Error(err)
		t.False(canceled)
		t.ErrorContains(err, "signature verification failed")
	})

	t.Run("ok", func() {
		canceledch := make(chan struct{}, 1)
		handler := QuicstreamHandlerCancelHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			func() error {
				canceledch <- struct{}{}

				return nil
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixCancelHandover, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		canceled, err := c.CancelHandover(context.Background(),
			localci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
		)
		t.NoError(err)
		t.True(canceled)

		select {
		case <-time.After(time.Second):
			t.NoError(errors.Errorf("not canceled"))
		case <-canceledch:
		}
	})

	t.Run("cancel error", func() {
		handler := QuicstreamHandlerCancelHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			func() error {
				return errors.Errorf("hihihi")
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixCancelHandover, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		canceled, err := c.CancelHandover(context.Background(),
			localci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
		)
		t.Error(err)
		t.False(canceled)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testQuicstreamHandlers) TestCheckHandover() {
	xci := quicstream.RandomConnInfo()
	yci := quicstream.RandomConnInfo()

	t.Run("failed to verify node", func() {
		handler := QuicstreamHandlerCheckHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			func(context.Context, base.Address, quicstream.UDPConnInfo) error { return nil },
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixCheckHandover, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		checked, err := c.CheckHandover(context.Background(),
			xci,
			base.NewMPrivatekey(),
			t.LocalParams.NetworkID(),
			t.Local.Address(),
			yci,
		)
		t.Error(err)
		t.False(checked)
		t.ErrorContains(err, "signature verification failed")
	})

	t.Run("ok", func() {
		checkedch := make(chan struct{}, 1)
		handler := QuicstreamHandlerCheckHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			func(context.Context, base.Address, quicstream.UDPConnInfo) error {
				checkedch <- struct{}{}

				return nil
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixCheckHandover, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		checked, err := c.CheckHandover(context.Background(),
			xci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
			t.Local.Address(),
			yci,
		)
		t.NoError(err)
		t.True(checked)

		select {
		case <-time.After(time.Second):
			t.NoError(errors.Errorf("not checked"))
		case <-checkedch:
		}
	})

	t.Run("checked error", func() {
		handler := QuicstreamHandlerCheckHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			func(context.Context, base.Address, quicstream.UDPConnInfo) error {
				return errors.Errorf("hihihi")
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixCheckHandover, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		checked, err := c.CheckHandover(context.Background(),
			xci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
			t.Local.Address(),
			yci,
		)
		t.Error(err)
		t.False(checked)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testQuicstreamHandlers) TestAskHandover() {
	xci := quicstream.RandomConnInfo()
	yci := quicstream.RandomConnInfo()

	handoverid := util.UUID().String()

	t.Run("failed to verify node", func() {
		handler := QuicstreamHandlerAskHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			xci,
			func(context.Context, base.Address, quicstream.UDPConnInfo) (string, bool, error) {
				return handoverid, true, nil
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixAskHandover, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rid, canmove, err := c.AskHandover(context.Background(),
			xci,
			base.NewMPrivatekey(),
			t.LocalParams.NetworkID(),
			t.Local.Address(),
			yci,
		)
		t.Error(err)
		t.Empty(rid)
		t.False(canmove)
		t.ErrorContains(err, "signature verification failed")
	})

	t.Run("ok", func() {
		askedch := make(chan struct{}, 1)
		handler := QuicstreamHandlerAskHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			xci,
			func(context.Context, base.Address, quicstream.UDPConnInfo) (string, bool, error) {
				askedch <- struct{}{}

				return handoverid, true, nil
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixAskHandover, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rid, canmove, err := c.AskHandover(context.Background(),
			xci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
			t.Local.Address(),
			yci,
		)
		t.NoError(err)
		t.Equal(handoverid, rid)
		t.True(canmove)

		select {
		case <-time.After(time.Second):
			t.NoError(errors.Errorf("not asked"))
		case <-askedch:
		}
	})

	t.Run("ask error", func() {
		handler := QuicstreamHandlerAskHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			xci,
			func(context.Context, base.Address, quicstream.UDPConnInfo) (string, bool, error) {
				return "", false, errors.Errorf("hihihi")
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixAskHandover, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rid, canmove, err := c.AskHandover(context.Background(),
			xci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
			t.Local.Address(),
			yci,
		)
		t.Error(err)
		t.Empty(rid)
		t.False(canmove)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testQuicstreamHandlers) TestHandoverMessage() {
	yci := quicstream.RandomConnInfo()

	handoverid := util.UUID().String()

	t.Run("ok", func() {
		sentch := make(chan isaacstates.HandoverMessage, 1)
		handler := QuicstreamHandlerHandoverMessage(
			func(msg isaacstates.HandoverMessage) error {
				sentch <- msg

				return nil
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixHandoverMessage, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		msg := isaacstates.NewHandoverMessageCancel(handoverid)

		t.NoError(c.HandoverMessage(context.Background(), yci, msg))

		select {
		case <-time.After(time.Second):
			t.NoError(errors.Errorf("not sent"))
		case <-sentch:
		}
	})

	t.Run("not HandoverMessage", func() {
		sentch := make(chan isaacstates.HandoverMessage, 1)
		handler := QuicstreamHandlerHandoverMessage(
			func(msg isaacstates.HandoverMessage) error {
				sentch <- msg

				return nil
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixHandoverMessage, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		ctx := context.Background()

		broker, err := c.Client.Broker(ctx, yci)
		t.NoError(err)

		t.NoError(broker.WriteRequestHead(ctx, NewHandoverMessageHeader()))
		t.NoError(brokerPipeEncode(ctx, broker, "showme"))

		_, h, err := broker.ReadResponseHead(ctx)
		t.NoError(err)
		t.NotNil(h)
		t.Error(h.Err())
		t.ErrorContains(h.Err(), "decode")
	})
}
