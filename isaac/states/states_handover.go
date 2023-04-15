package isaacstates

import "github.com/pkg/errors"

func (st *States) UnderHandoverX() bool {
	_, isempty := st.handoverX.Value()

	return !isempty
}

func (st *States) StartHandoverX(id string) error {
	_, err := st.handoverX.Set(func(_ *HandoverXBroker, isempty bool) (*HandoverXBroker, error) {
		switch {
		case !isempty:
			return nil, errors.Errorf("already under handover x")
		case !st.AllowedConsensus():
			return nil, errors.Errorf("not allowed consensus")
		case st.UnderHandoverY():
			return nil, errors.Errorf("under handover y")
		}

		// FIXME broker, err := NewHandoverXBroker(ctx, id)
		// if err != nil {
		// 	return nil, err
		// }

		// return broker, nil
		return nil, nil
	})

	return err
}

func (st *States) UnderHandoverY() bool {
	_, isempty := st.handoverY.Value()

	return !isempty
}

func (st *States) StartHandoverY() error {
	_, err := st.handoverY.Set(func(_ *HandoverYBroker, isempty bool) (*HandoverYBroker, error) {
		switch {
		case !isempty:
			return nil, errors.Errorf("already under handover y")
		case st.AllowedConsensus():
			return nil, errors.Errorf("allowed consensus")
		case st.UnderHandoverX():
			return nil, errors.Errorf("under handover x")
		}

		// FIXME broker, err := NewHandoverYBroker()
		// if err != nil {
		// 	return nil, err
		// }

		// return broker, nil
		return nil, nil
	})

	return err
}

func (st *States) CancelHandoverY() error {
	_ = st.handoverY.EmptyValue()

	return nil
}
