package main

import (
	"encoding/json"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
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

func NewAccountStateValue(address base.Publickey, balance int64) AccountStateValue {
	return AccountStateValue{
		BaseHinter: hint.NewBaseHinter(AccountStateValueHint),
		Address:    address,
		Balance:    balance,
	}
}

func (s AccountStateValue) HashBytes() []byte {
	return util.ConcatBytesSlice(
		s.Address.Bytes(),
		util.Int64ToBytes(s.Balance),
	)
}

func (s AccountStateValue) IsValid([]byte) error {
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
	hint.BaseHinter
	key string
	AccountStateValue
}

func NewAccountStateMergeValue(address base.Publickey, balance int64) (v AccountStateMergeValue, _ error) {
	switch {
	case address == nil:
		return v, errors.Errorf("empty address")
	case balance == 0:
		return v, errors.Errorf("zero balance")
	default:
		return AccountStateMergeValue{
			BaseHinter:        hint.NewBaseHinter(AccountStateMergeValueHint),
			key:               AccountStateKey(address),
			AccountStateValue: NewAccountStateValue(address, balance),
		}, nil
	}
}

func (v AccountStateMergeValue) Key() string {
	return v.key
}

func (v AccountStateMergeValue) Value() base.StateValue {
	return v.AccountStateValue
}

func (v AccountStateMergeValue) Merger(prev base.StateValue) (StateMerger, error) {
	var av AccountStateValue

	if prev != nil {
		switch i, err := util.AssertInterfaceValue[AccountStateValue](prev); {
		case err != nil:
			return nil, err
		default:
			av = i
		}
	}

	return NewAccountStateMerger(v.key, av), nil
}

type accountStateMergeValueJSONMarshaler struct {
	Key   string
	Value AccountStateValue
}

func (v AccountStateMergeValue) Bytes() ([]byte, error) { // FIXME remove; not used
	return util.NewLengthedBytesSlice(
		[][]byte{
			[]byte(v.key),
			v.Address.Bytes(),
			util.Int64ToBytes(v.Balance),
		},
	)
}

func AccountStateMergeValueFromReader(b []byte) (v AccountStateMergeValue, _ error) { // FIXME remove; not used
	var m [][]byte

	switch i, _, err := util.ReadLengthedBytesSlice(b); {
	case err != nil:
		return v, err
	case len(i) != 3:
		return v, errors.Errorf("wrong size, %d", len(m))
	default:
		m = i
	}

	v.key = string(m[0])

	switch i, err := base.ParseMPublickey(string(m[1])); {
	case err != nil:
		return v, err
	default:
		v.Address = i
	}

	switch i, err := util.BytesToInt64(m[2]); {
	case err != nil:
		return v, err
	default:
		v.Balance = i
	}

	v.BaseHinter = hint.NewBaseHinter(AccountStateMergeValueHint)

	return v, nil
}

func (v AccountStateMergeValue) MarshalJSON() ([]byte, error) {
	b, err := util.MarshalJSON(struct {
		hint.BaseHinter
		accountStateMergeValueJSONMarshaler
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

func (s *AccountStateMergeValue) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u accountStateMergeValueJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	if err := s.AccountStateValue.DecodeJSON(u.Value, enc); err != nil {
		return err
	}

	s.key = u.Key

	return nil
}

type AccountStateMerger struct {
	address base.Publickey
	prev    AccountStateValue
	sync.Mutex
	prepops []uint64
	key     string
	sum     int64
	lastop  *uint64
}

func NewAccountStateMerger(key string, prev AccountStateValue) *AccountStateMerger {
	var address base.Publickey

	if prev.Address != nil {
		address = prev.Address
	}

	return &AccountStateMerger{
		key:     key,
		address: address,
		prev:    prev,
	}
}

func (m *AccountStateMerger) Key() string {
	return m.key
}

func (m *AccountStateMerger) Clone() StateMerger {
	return &AccountStateMerger{
		address: m.address,
		prev:    m.prev,
		prepops: slices.Clone(m.prepops),
		key:     m.key,
		sum:     m.sum,
		lastop:  m.lastop,
	}
}

func (m *AccountStateMerger) Close() (base.StateValue, error) {
	defer func() {
		m.lastop = nil

		clear(m.prepops)
	}()

	merged := m.prev
	merged.BaseHinter = hint.NewBaseHinter(AccountStateValueHint)

	if merged.Address == nil {
		merged.Address = m.address
	}

	merged.Balance += m.sum

	return merged, nil
}

func (m *AccountStateMerger) Merge(op uint64, value base.StateValue) (
	commit func() error,
	unmergeOp uint64,
	reason string,
	_ error,
) {
	m.Lock()
	defer m.Unlock()

	unmergeOp = op

	var ac AccountStateValue

	switch i, err := m.assertValue(value); {
	case err != nil:
		return nil, unmergeOp, "invalid state value", err
	default:
		ac = i
	}

	if ac.Balance < 0 {
		if m.prev.Balance+ac.Balance < 0 {
			return nil, unmergeOp, "insufficient balance", nil
		}

		if m.lastop != nil {
			lastMerged := *m.lastop

			switch {
			case op == lastMerged:
				return nil, unmergeOp, "", nil
			case op > lastMerged:
				m.addPrepOps(op)

				return nil, unmergeOp, "previous issuer found", nil
			default:
				m.addPrepOps(lastMerged)

				unmergeOp = lastMerged
				reason = "previous issuer found"
			}
		}
	}

	return func() error {
		m.Lock()
		defer m.Unlock()

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

func (m *AccountStateMerger) Unmerge(op uint64, value base.StateValue) (
	commit func() error,
	mergeOp uint64,
	_ error,
) {
	m.Lock()
	defer m.Unlock()

	mergeOp = op

	var ac AccountStateValue

	switch i, err := m.assertValue(value); {
	case err != nil:
		return nil, mergeOp, err
	default:
		ac = i
	}

	if ac.Balance < 0 {
		if m.prev.Balance+ac.Balance < 0 {
			return nil, mergeOp, nil
		}

		_ = util.TraverseSliceV1(m.prepops, func(_ int, s uint64) (bool, error) {
			if s > op {
				mergeOp = s

				return false, nil
			}

			return true, nil
		})
	}

	return func() error {
		m.Lock()
		defer m.Unlock()

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

func (m *AccountStateMerger) assertValue(value base.StateValue) (ac AccountStateValue, _ error) {
	switch i, err := util.AssertInterfaceValue[AccountStateValue](value); {
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

func (m *AccountStateMerger) addPrepOps(op uint64) {
	if slices.Index(m.prepops, op) >= 0 {
		return
	}

	m.prepops = append(m.prepops, op)

	slices.Sort(m.prepops)
}
