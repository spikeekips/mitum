package main

import (
	"encoding/json"
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	AccountStateValueHint      = hint.MustNewHint("test-account-state-value-v0.0.1")
	AccountStateMergeValueHint = hint.MustNewHint("test-account-state-merge-value-v0.0.1")
)

type accountStateValue struct {
	Address base.Publickey `json:"address"`
	hint.BaseHinter
	Balance int64 `json:"balance"`
}

func newAccountStateValue(address base.Publickey, balance int64) *accountStateValue {
	return &accountStateValue{
		BaseHinter: hint.NewBaseHinter(AccountStateValueHint),
		Address:    address,
		Balance:    balance,
	}
}

func (s *accountStateValue) HashBytes() []byte {
	return slices.Concat(
		s.Address.Bytes(),
		util.Int64ToBytes(s.Balance),
	)
}

func (s *accountStateValue) IsValid([]byte) error {
	if err := s.BaseHinter.IsValid(AccountStateValueHint.Type().Bytes()); err != nil {
		return err
	}

	return util.CheckIsValiders(nil, false, s.Address)
}

type accountStateValueJSONUnmarshaler struct {
	Address string `json:"address"`
	Balance int64  `json:"balance"`
}

func (s *accountStateValue) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u accountStateValueJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	switch i, err := base.DecodePublickeyFromString(u.Address, enc); {
	case err != nil:
		return err
	default:
		s.Address = i
	}

	s.Balance = u.Balance
	s.BaseHinter = hint.NewBaseHinter(AccountStateValueHint)

	return nil
}

func accountStateKey(address base.Publickey) string {
	if address == nil {
		return ""
	}

	return "ta-" + address.String()
}

type accountStateMergeValue struct {
	*accountStateValue
	key string
	hint.BaseHinter
}

func newAccountStateMergeValue(address base.Publickey, balance int64) (v *accountStateMergeValue, _ error) {
	switch {
	case address == nil:
		return v, errors.Errorf("empty address")
	case balance == 0:
		return v, errors.Errorf("zero balance")
	default:
		return &accountStateMergeValue{
			BaseHinter:        hint.NewBaseHinter(AccountStateMergeValueHint),
			key:               accountStateKey(address),
			accountStateValue: newAccountStateValue(address, balance),
		}, nil
	}
}

func (v *accountStateMergeValue) Key() string {
	return v.key
}

func (v *accountStateMergeValue) Value() base.StateValue {
	return v.accountStateValue
}

func (v *accountStateMergeValue) Merger(prev base.StateValue) (stateValueMerger, error) {
	var av *accountStateValue

	if prev != nil {
		switch i, err := util.AssertInterfaceValue[*accountStateValue](prev); {
		case err != nil:
			return nil, err
		default:
			av = i
		}
	}

	return newAccountStateValueMerger(v.key, av), nil
}

type accountStateMergeValueJSONMarshaler struct {
	Value *accountStateValue
	Key   string
}

func (v *accountStateMergeValue) MarshalJSON() ([]byte, error) {
	b, err := util.MarshalJSON(struct {
		accountStateMergeValueJSONMarshaler
		hint.BaseHinter
	}{
		BaseHinter: v.BaseHinter,
		accountStateMergeValueJSONMarshaler: accountStateMergeValueJSONMarshaler{
			Key:   v.key,
			Value: v.accountStateValue,
		},
	})

	return b, err
}

type accountStateMergeValueJSONUnmarshaler struct {
	Key   string
	Value json.RawMessage
}

func (v *accountStateMergeValue) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u accountStateMergeValueJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	if err := v.accountStateValue.DecodeJSON(u.Value, enc); err != nil {
		return err
	}

	v.key = u.Key

	return nil
}

type accountStateValueMerger struct {
	address base.Publickey
	prev    *accountStateValue
	prepops []uint64
	key     string
	sum     int64
	lastop  *uint64
}

func newAccountStateValueMerger(key string, prev *accountStateValue) *accountStateValueMerger {
	var address base.Publickey

	if prev != nil && prev.Address != nil {
		address = prev.Address
	}

	return &accountStateValueMerger{
		key:     key,
		address: address,
		prev:    prev,
	}
}

func (m *accountStateValueMerger) Key() string {
	return m.key
}

func (m *accountStateValueMerger) Close() (base.StateValue, error) {
	defer func() {
		m.lastop = nil

		dutil.ClearSlice(m.prepops)
	}()

	if m.sum == 0 {
		return nil, nil
	}

	var merged *accountStateValue

	switch {
	case m.prev != nil:
		merged = newAccountStateValue(m.prev.Address, m.prev.Balance+m.sum)
	default:
		merged = newAccountStateValue(m.address, m.sum)
	}

	if merged.Balance < 0 {
		return merged, errors.Errorf("under zero balance, state key=%q balance=%d", m.key, merged.Balance)
	}

	return merged, nil
}

func (m *accountStateValueMerger) Merge(op uint64, value base.StateValue) (
	commit func() error,
	unmergeOp uint64,
	reason string,
	_ error,
) {
	var ac *accountStateValue

	switch i, err := m.assertValue(value); {
	case err != nil:
		return nil, op, "invalid state value", err
	default:
		ac = i
	}

	unmergeOp = op

	if ac.Balance < 0 {
		balance := ac.Balance

		if m.prev != nil {
			balance += m.prev.Balance
		}

		if balance < 0 {
			return nil, op, "insufficient balance", nil
		}

		if m.lastop != nil {
			lastMerged := *m.lastop

			switch {
			case op == lastMerged:
				return nil, op, "", nil
			case op > lastMerged:
				m.addPrepOps(op)

				return nil, op, "previous issuer found", nil
			default:
				m.addPrepOps(lastMerged)

				unmergeOp = lastMerged
				reason = "previous issuer found"
			}
		}
	}

	return func() error {
		if m.address == nil {
			m.address = ac.Address
		}

		if ac.Balance < 0 {
			m.lastop = &op
		}

		m.sum += ac.Balance

		return nil
	}, unmergeOp, reason, nil
}

func (m *accountStateValueMerger) Unmerge(op uint64, value base.StateValue) (
	commit func() error,
	mergeOp uint64,
	_ error,
) {
	var ac *accountStateValue

	switch i, err := m.assertValue(value); {
	case err != nil:
		return nil, op, err
	default:
		ac = i
	}

	mergeOp = op

	if ac.Balance < 0 {
		if m.prev.Balance+ac.Balance < 0 {
			return nil, op, nil
		}

		for s := range slices.Values(m.prepops) {
			if s > op {
				mergeOp = s

				break
			}
		}
	}

	return func() error {
		if ac.Balance < 0 {
			if m.lastop != nil && *m.lastop == op {
				m.lastop = nil
			}

			if slices.Index(m.prepops, op) < 0 {
				m.prepops = append(m.prepops, op)

				slices.Sort(m.prepops)
			}
		}

		m.sum -= ac.Balance

		return nil
	}, mergeOp, nil
}

func (m *accountStateValueMerger) assertValue(value base.StateValue) (ac *accountStateValue, _ error) {
	switch i, err := util.AssertInterfaceValue[*accountStateValue](value); {
	case err != nil:
		return ac, err
	case i.Balance == 0:
		return ac, errors.Errorf("zero balance")
	case m.address != nil && !m.address.Equal(i.Address):
		return ac, errors.Errorf("address not matched")
	default:
		return i, nil
	}
}

func (m *accountStateValueMerger) addPrepOps(op uint64) {
	if slices.Index(m.prepops, op) >= 0 {
		return
	}

	m.prepops = append(m.prepops, op)

	slices.Sort(m.prepops)
}

func isSubAccountStateMergeValue(v *accountStateMergeValue) bool {
	return v.Balance < 0
}
