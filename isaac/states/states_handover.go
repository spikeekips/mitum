package isaacstates

import "github.com/pkg/errors"

func (st *States) HandoverXBroker() *HandoverXBroker {
	v, _ := st.handoverXBroker.Value()

	return v
}

func (st *States) NewHandoverXBroker(id string) error {
	_, err := st.handoverXBroker.Set(func(_ *HandoverXBroker, isempty bool) (*HandoverXBroker, error) {
		switch {
		case !isempty:
			return nil, errors.Errorf("already under handover x")
		case !st.AllowedConsensus():
			return nil, errors.Errorf("not allowed consensus")
		case st.HandoverYBroker() != nil:
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

func (st *States) HandoverYBroker() *HandoverYBroker {
	v, _ := st.handoverYBroker.Value()

	return v
}

func (st *States) NewHandoverYBroker() error {
	_, err := st.handoverYBroker.Set(func(_ *HandoverYBroker, isempty bool) (*HandoverYBroker, error) {
		switch {
		case !isempty:
			return nil, errors.Errorf("already under handover y")
		case st.AllowedConsensus():
			return nil, errors.Errorf("allowed consensus")
		case st.HandoverXBroker() != nil:
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
