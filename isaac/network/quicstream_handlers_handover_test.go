package isaacnetwork

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
)

func (t *testQuicstreamHandlers) TestStartHandover() {
	xci := quicstream.RandomConnInfo()
	yci := quicstream.RandomConnInfo()

	t.Run("failed to verify node", func() {
		handler := QuicstreamHandlerStartHandover(
			t.Local,
			t.LocalParams.NetworkID(),
			func(context.Context, base.Address, quicstream.ConnInfo) error { return nil },
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixStartHandover, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(context.Context, base.Address, quicstream.ConnInfo) error {
				startedch <- struct{}{}

				return nil
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixStartHandover, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(context.Context, base.Address, quicstream.ConnInfo) error {
				return errors.Errorf("hihihi")
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixStartHandover, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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

		dialf := testDialFunc(t.Encs, HandlerPrefixCancelHandover, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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

		dialf := testDialFunc(t.Encs, HandlerPrefixCancelHandover, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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

		dialf := testDialFunc(t.Encs, HandlerPrefixCancelHandover, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(context.Context, base.Address, quicstream.ConnInfo) error { return nil },
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixCheckHandover, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(context.Context, base.Address, quicstream.ConnInfo) error {
				checkedch <- struct{}{}

				return nil
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixCheckHandover, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(context.Context, base.Address, quicstream.ConnInfo) error {
				return errors.Errorf("hihihi")
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixCheckHandover, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(context.Context, base.Address, quicstream.ConnInfo) (string, bool, error) {
				return handoverid, true, nil
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixAskHandover, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(context.Context, base.Address, quicstream.ConnInfo) (string, bool, error) {
				askedch <- struct{}{}

				return handoverid, true, nil
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixAskHandover, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(context.Context, base.Address, quicstream.ConnInfo) (string, bool, error) {
				return "", false, errors.Errorf("hihihi")
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixAskHandover, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			t.LocalParams.NetworkID(),
			func(msg isaacstates.HandoverMessage) error {
				sentch <- msg

				return nil
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixHandoverMessage, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		msg := isaacstates.NewHandoverMessageCancel(handoverid, nil)

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
			t.LocalParams.NetworkID(),
			func(msg isaacstates.HandoverMessage) error {
				sentch <- msg

				return nil
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixHandoverMessage, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		ctx := context.Background()

		stream, closef, err := c.Dial(ctx, yci)
		t.NoError(err)
		defer closef()

		t.NoError(stream(ctx, func(_ context.Context, broker *quicstreamheader.ClientBroker) error {
			t.NoError(broker.WriteRequestHead(ctx, NewHandoverMessageHeader()))
			t.NoError(brokerPipeEncode(ctx, broker, "showme"))

			_, h, err := broker.ReadResponseHead(ctx)
			t.NoError(err)
			t.NotNil(h)
			t.Error(h.Err())
			t.ErrorContains(h.Err(), "decode")

			return nil
		}))
	})
}

func (t *testQuicstreamHandlers) TestCheckHandoverX() {
	xci := quicstream.RandomConnInfo()

	t.Run("failed to verify node", func() {
		handler := QuicstreamHandlerCheckHandoverX(
			t.Local,
			t.LocalParams.NetworkID(),
			func(context.Context) error { return nil },
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixCheckHandoverX, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		checked, err := c.CheckHandoverX(context.Background(),
			xci,
			base.NewMPrivatekey(),
			t.LocalParams.NetworkID(),
			t.Local.Address(),
		)
		t.Error(err)
		t.False(checked)
		t.ErrorContains(err, "signature verification failed")
	})

	t.Run("ok", func() {
		checkedch := make(chan struct{}, 1)
		handler := QuicstreamHandlerCheckHandoverX(
			t.Local,
			t.LocalParams.NetworkID(),
			func(context.Context) error {
				checkedch <- struct{}{}

				return nil
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixCheckHandoverX, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		checked, err := c.CheckHandoverX(context.Background(),
			xci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
			t.Local.Address(),
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
		handler := QuicstreamHandlerCheckHandoverX(
			t.Local,
			t.LocalParams.NetworkID(),
			func(context.Context) error {
				return errors.Errorf("hihihi")
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixCheckHandoverX, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		checked, err := c.CheckHandoverX(context.Background(),
			xci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
			t.Local.Address(),
		)
		t.Error(err)
		t.False(checked)
		t.ErrorContains(err, "hihihi")
	})
}

func (t *testQuicstreamHandlers) TestLastHandoverYLogs() {
	yci := quicstream.RandomConnInfo()

	t.Run("failed to verify node", func() {
		handler := QuicstreamHandlerLastHandoverYLogs(
			t.Local,
			t.LocalParams.NetworkID(),
			func() []json.RawMessage {
				return nil
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixLastHandoverYLogs, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		err := c.LastHandoverYLogs(context.Background(),
			yci,
			base.NewMPrivatekey(),
			t.LocalParams.NetworkID(),
			func(b json.RawMessage) bool {
				return true
			},
		)
		t.Error(err)
		t.ErrorContains(err, "signature verification failed")
	})

	t.Run("ok", func() {
		js := []json.RawMessage{
			[]byte(`{"a0": 0, "a1": 1}`),
			[]byte(`{"b0": 0, "b1": 1}`),
		}

		handler := QuicstreamHandlerLastHandoverYLogs(
			t.Local,
			t.LocalParams.NetworkID(),
			func() []json.RawMessage {
				return js
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixLastHandoverYLogs, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		var rjs []json.RawMessage

		t.NoError(c.LastHandoverYLogs(context.Background(),
			yci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
			func(b json.RawMessage) bool {
				rjs = append(rjs, b)

				return true
			},
		))

		t.Equal(js, rjs)
	})

	t.Run("empty", func() {
		handler := QuicstreamHandlerLastHandoverYLogs(
			t.Local,
			t.LocalParams.NetworkID(),
			func() []json.RawMessage {
				return nil
			},
		)

		dialf := testDialFunc(t.Encs, HandlerPrefixLastHandoverYLogs, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		var rjs []json.RawMessage

		t.NoError(c.LastHandoverYLogs(context.Background(),
			yci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
			func(b json.RawMessage) bool {
				rjs = append(rjs, b)

				return true
			},
		))

		t.Equal(0, len(rjs))
	})
}
