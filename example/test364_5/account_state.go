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

type AccountStateValue struct {
	Address base.Publickey `json:"address"`
	hint.BaseHinter
	Balance int64 `json:"balance"`
}

func NewAccountStateValue(address base.Publickey, balance int64) *AccountStateValue {
	return &AccountStateValue{
		BaseHinter: hint.NewBaseHinter(AccountStateValueHint),
		Address:    address,
		Balance:    balance,
	}
}

func (s *AccountStateValue) HashBytes() []byte {
	return slices.Concat(
		s.Address.Bytes(),
		util.Int64ToBytes(s.Balance),
	)
}

func (s *AccountStateValue) IsValid([]byte) error {
	if err := s.BaseHinter.IsValid(AccountStateValueHint.Type().Bytes()); err != nil {
		return err
	}

	return util.CheckIsValiders(nil, false, s.Address)
}

type accountStateValueJSONUnmarshaler struct {
	Address string `json:"address"`
	Balance int64  `json:"balance"`
}

func (s *AccountStateValue) DecodeJSON(b []byte, enc encoder.Encoder) error {
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

func AccountStateKey(address base.Publickey) string {
	if address == nil {
		return ""
	}

	return "ta-" + address.String()
}

type AccountStateMergeValue struct {
	*AccountStateValue
	key string
	hint.BaseHinter
}

func NewAccountStateMergeValue(address base.Publickey, balance int64) (v *AccountStateMergeValue, _ error) {
	switch {
	case address == nil:
		return v, errors.Errorf("empty address")
	case balance == 0:
		return v, errors.Errorf("zero balance")
	default:
		return &AccountStateMergeValue{
			BaseHinter:        hint.NewBaseHinter(AccountStateMergeValueHint),
			key:               AccountStateKey(address),
			AccountStateValue: NewAccountStateValue(address, balance),
		}, nil
	}
}

func (v *AccountStateMergeValue) Key() string {
	return v.key
}

func (v *AccountStateMergeValue) Value() base.StateValue {
	return v.AccountStateValue
}

func (v *AccountStateMergeValue) Merger(prev base.StateValue) (StateValueMerger, error) {
	var av *AccountStateValue

	if prev != nil {
		switch i, err := util.AssertInterfaceValue[*AccountStateValue](prev); {
		case err != nil:
			return nil, err
		default:
			av = i
		}
	}

	return NewAccountStateValueMerger(v.key, av), nil
}

type accountStateMergeValueJSONMarshaler struct {
	Value *AccountStateValue
	Key   string
}

func (v *AccountStateMergeValue) MarshalJSON() ([]byte, error) {
	b, err := util.MarshalJSON(struct {
		accountStateMergeValueJSONMarshaler
		hint.BaseHinter
	}{
		BaseHinter: v.BaseHinter,
		accountStateMergeValueJSONMarshaler: accountStateMergeValueJSONMarshaler{
			Key:   v.key,
			Value: v.AccountStateValue,
		},
	})

	return b, err
}

type accountStateMergeValueJSONUnmarshaler struct {
	Key   string
	Value json.RawMessage
}

func (v *AccountStateMergeValue) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u accountStateMergeValueJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	if err := v.AccountStateValue.DecodeJSON(u.Value, enc); err != nil {
		return err
	}

	v.key = u.Key

	return nil
}

type AccountStateValueMerger struct {
	address base.Publickey
	prev    *AccountStateValue
	prepops []uint64
	key     string
	sum     int64
	lastop  *uint64
}

func NewAccountStateValueMerger(key string, prev *AccountStateValue) *AccountStateValueMerger {
	var address base.Publickey

	if prev != nil && prev.Address != nil {
		address = prev.Address
	}

	return &AccountStateValueMerger{
		key:     key,
		address: address,
		prev:    prev,
	}
}

func (m *AccountStateValueMerger) Key() string {
	return m.key
}

func (m *AccountStateValueMerger) Close() (base.StateValue, error) {
	defer func() {
		m.lastop = nil

		dutil.ClearSlice(m.prepops)
	}()

	if m.sum == 0 {
		return nil, nil
	}

	var merged *AccountStateValue

	switch {
	case m.prev != nil:
		merged = NewAccountStateValue(m.prev.Address, m.prev.Balance+m.sum)
	default:
		merged = NewAccountStateValue(m.address, m.sum)
	}

	if merged.Balance < 0 {
		return merged, errors.Errorf("under zero balance, state key=%q balance=%d", m.key, merged.Balance)
	}

	return merged, nil
}

func (m *AccountStateValueMerger) Merge(op uint64, value base.StateValue) (
	commit func() error,
	unmergeOp uint64,
	reason string,
	_ error,
) {
	var ac *AccountStateValue

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

func (m *AccountStateValueMerger) Unmerge(op uint64, value base.StateValue) (
	commit func() error,
	mergeOp uint64,
	_ error,
) {
	var ac *AccountStateValue

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

func (m *AccountStateValueMerger) assertValue(value base.StateValue) (ac *AccountStateValue, _ error) {
	switch i, err := util.AssertInterfaceValue[*AccountStateValue](value); {
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

func (m *AccountStateValueMerger) addPrepOps(op uint64) {
	if slices.Index(m.prepops, op) >= 0 {
		return
	}

	m.prepops = append(m.prepops, op)

	slices.Sort(m.prepops)
}

func isSubAccountStateMergeValue(v *AccountStateMergeValue) bool {
	return v.Balance < 0
}
