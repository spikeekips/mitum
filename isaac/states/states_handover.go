package isaacstates

import (
	"context"

	"github.com/pkg/errors"
)

func (st *States) HandoverXBroker() *HandoverXBroker {
	v, _ := st.handoverXBroker.Value()

	return v
}

func (st *States) NewHandoverXBroker() error {
	_, err := st.handoverXBroker.Set(func(_ *HandoverXBroker, isempty bool) (*HandoverXBroker, error) {
		switch {
		case !isempty:
			return nil, errors.Errorf("already under handover x")
		case !st.AllowedConsensus():
			return nil, errors.Errorf("not allowed consensus")
		case st.HandoverYBroker() != nil:
			return nil, errors.Errorf("under handover y")
		}

		broker, err := st.args.NewHandoverXBroker(context.Background())
		if err != nil {
			st.Log().Error().Err(err).Msg("failed new handover x broker")

			return nil, err
		}

		if err := broker.patchStates(st); err != nil {
			return nil, err
		}

		return broker, nil
	})

	return err
}

func (st *States) HandoverYBroker() *HandoverYBroker {
	v, _ := st.handoverYBroker.Value()

	return v
}

func (st *States) NewHandoverYBroker(id string) error {
	_, err := st.handoverYBroker.Set(func(_ *HandoverYBroker, isempty bool) (*HandoverYBroker, error) {
		switch {
		case !isempty:
			return nil, errors.Errorf("already under handover y")
		case st.AllowedConsensus():
			return nil, errors.Errorf("allowed consensus")
		case st.HandoverXBroker() != nil:
			return nil, errors.Errorf("under handover x")
		}

		broker, err := st.args.NewHandoverYBroker(context.Background(), id)
		if err != nil {
			st.Log().Error().Err(err).Msg("failed new handover y broker")

			return nil, err
		}

		if err := broker.patchStates(st); err != nil {
			return nil, err
		}

		return broker, nil
	})

	return err
}

func (st *States) cleanHandoverBrokers() {
	_ = st.handoverXBroker.Empty(func(i *HandoverXBroker, isempty bool) error {
		if !isempty {
			st.Log().Debug().Msg("handover x broker canceled")
		}

		return nil
	})

	_ = st.handoverYBroker.Empty(func(i *HandoverYBroker, isempty bool) error {
		if !isempty {
			st.Log().Debug().Msg("handover y broker canceled")
		}

		return nil
	})
}
