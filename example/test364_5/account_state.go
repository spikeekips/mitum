package dop5

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
	AccountSVHint  = hint.MustNewHint("test-account-state-value-v0.0.1")
	AccountSMVHint = hint.MustNewHint("test-account-state-merge-value-v0.0.1")
)

type accountSV struct {
	Address base.Publickey `json:"address"`
	hint.BaseHinter
	Balance int64 `json:"balance"`
}

func newAccountSV(address base.Publickey, balance int64) *accountSV {
	return &accountSV{
		BaseHinter: hint.NewBaseHinter(AccountSVHint),
		Address:    address,
		Balance:    balance,
	}
}

func (s *accountSV) HashBytes() []byte {
	return slices.Concat(
		s.Address.Bytes(),
		util.Int64ToBytes(s.Balance),
	)
}

func (s *accountSV) IsValid([]byte) error {
	if err := s.BaseHinter.IsValid(AccountSVHint.Type().Bytes()); err != nil {
		return err
	}

	return util.CheckIsValiders(nil, false, s.Address)
}

type accountSVJSONUnmarshaler struct {
	Address string `json:"address"`
	Balance int64  `json:"balance"`
}

func (s *accountSV) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u accountSVJSONUnmarshaler
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
	s.BaseHinter = hint.NewBaseHinter(AccountSVHint)

	return nil
}

func accountStateKey(address base.Publickey) string {
	if address == nil {
		return ""
	}

	return "ta-" + address.String()
}

type accountSMV struct {
	*accountSV
	key string
	hint.BaseHinter
}

func newAccountSMV(address base.Publickey, balance int64) (v *accountSMV, _ error) {
	switch {
	case address == nil:
		return v, errors.Errorf("empty address")
	case balance == 0:
		return v, errors.Errorf("zero balance")
	default:
		return &accountSMV{
			BaseHinter: hint.NewBaseHinter(AccountSMVHint),
			key:        accountStateKey(address),
			accountSV:  newAccountSV(address, balance),
		}, nil
	}
}

func (v *accountSMV) Key() string {
	return v.key
}

func (v *accountSMV) Value() base.StateValue {
	return v.accountSV
}

func (v *accountSMV) Merger(prev base.StateValue) (SVM, error) {
	var av *accountSV

	if prev != nil {
		switch i, err := util.AssertInterfaceValue[*accountSV](prev); {
		case err != nil:
			return nil, err
		default:
			av = i
		}
	}

	return newAccountSVM(v.key, av), nil
}

type accountSMVJSONMarshaler struct {
	Value *accountSV
	Key   string
}

func (v *accountSMV) MarshalJSON() ([]byte, error) {
	b, err := util.MarshalJSON(struct {
		accountSMVJSONMarshaler
		hint.BaseHinter
	}{
		BaseHinter: v.BaseHinter,
		accountSMVJSONMarshaler: accountSMVJSONMarshaler{
			Key:   v.key,
			Value: v.accountSV,
		},
	})

	return b, err
}

type accountSMVJSONUnmarshaler struct {
	Key   string
	Value json.RawMessage
}

func (v *accountSMV) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u accountSMVJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	if err := v.accountSV.DecodeJSON(u.Value, enc); err != nil {
		return err
	}

	v.key = u.Key

	return nil
}

type accountSVM struct {
	address base.Publickey
	prev    *accountSV
	prepops []uint64
	key     string
	sum     int64
	lastop  *uint64
}

func newAccountSVM(key string, prev *accountSV) *accountSVM {
	var address base.Publickey

	if prev != nil && prev.Address != nil {
		address = prev.Address
	}

	return &accountSVM{
		key:     key,
		address: address,
		prev:    prev,
	}
}

func (m *accountSVM) Key() string {
	return m.key
}

func (m *accountSVM) Close() (base.StateValue, error) {
	defer func() {
		m.lastop = nil
		m.prepops = dutil.ClearSlice(m.prepops)
	}()

	if m.sum == 0 {
		return nil, nil
	}

	var merged *accountSV

	switch {
	case m.prev != nil:
		merged = newAccountSV(m.prev.Address, m.prev.Balance+m.sum)
	default:
		merged = newAccountSV(m.address, m.sum)
	}

	if merged.Balance < 0 {
		return merged, errors.Errorf("under zero balance, state key=%q balance=%d", m.key, merged.Balance)
	}

	return merged, nil
}

func (m *accountSVM) Merge(op uint64, value base.StateValue) (
	commit func() error,
	unmergeOp uint64,
	reason string,
	_ error,
) {
	var ac *accountSV

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

func (m *accountSVM) Unmerge(op uint64, value base.StateValue) (
	commit func() error,
	mergeOp uint64,
	_ error,
) {
	var ac *accountSV

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

func (m *accountSVM) assertValue(value base.StateValue) (ac *accountSV, _ error) {
	switch i, err := util.AssertInterfaceValue[*accountSV](value); {
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

func (m *accountSVM) addPrepOps(op uint64) {
	if slices.Index(m.prepops, op) >= 0 {
		return
	}

	m.prepops = append(m.prepops, op)

	slices.Sort(m.prepops)
}

func isSubAccountSMV(v *accountSMV) bool {
	return v.Balance < 0
}
