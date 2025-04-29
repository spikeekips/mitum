//go:generate go run -v -race tool/set_base_hinter.go - $GOFILE

package dop5

import (
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	AccountSVHint  = hint.MustNewHint("test-account-state-value-v0.0.1")
	AccountSMVHint = hint.MustNewHint("test-account-state-merge-value-v0.0.1")
)

type AccountSV struct {
	Address base.Publickey `json:"address"`
	hint.BaseHinter
	Balance int64 `json:"balance"`
}

func NewAccountSV(address base.Publickey, balance int64) *AccountSV {
	return &AccountSV{
		BaseHinter: hint.NewBaseHinter(AccountSVHint),
		Address:    address,
		Balance:    balance,
	}
}

func (s *AccountSV) HashBytes() []byte {
	if s.Address == nil {
		return nil
	}

	b, _ := dutil.EncodeInt(s.Balance)

	return slices.Concat(s.Address.Bytes(), b)
}

func (s *AccountSV) IsValid([]byte) error {
	if err := s.BaseHinter.IsValid(AccountSVHint.Type().Bytes()); err != nil {
		return err
	}

	return util.CheckIsValiders(nil, false, s.Address)
}

var accountSVBalanceAssingJSON = dutil.NewFuncAssignJSONInts[int64]()

func AccountSVDecodeJSON(enc *jsonenc.Encoder, m map[string]any) (any, error) {
	u := new(AccountSV)

	return u, dutil.FuncChain(
		AssignJSONPublickey(m["address"], &u.Address, enc),
		accountSVBalanceAssingJSON(m["balance"], &u.Balance),
	)
}

func accountStateKey(address base.Publickey) string {
	if address == nil {
		return ""
	}

	return "ta-" + address.String()
}

type AccountSMV struct {
	*AccountSV
	key string
	hint.BaseHinter
}

func NewAccountSMV(address base.Publickey, balance int64) (*AccountSMV, error) {
	switch {
	case address == nil:
		return nil, errors.New("empty address")
	case balance == 0:
		return nil, errors.New("zero balance")
	default:
		return &AccountSMV{
			BaseHinter: hint.NewBaseHinter(AccountSMVHint),
			key:        accountStateKey(address),
			AccountSV:  NewAccountSV(address, balance),
		}, nil
	}
}

func (v *AccountSMV) Key() string {
	return v.key
}

func (v *AccountSMV) Value() base.StateValue {
	return v.AccountSV
}

func (v *AccountSMV) Merger(prev base.StateValue) (SVM, error) {
	var av *AccountSV

	if prev != nil {
		if err := encoder.Assign(prev, &av); err != nil {
			return nil, err
		}
	}

	return newAccountSVM(v.key, av), nil
}

type accountSMVJSONMarshaler struct {
	Value *AccountSV
	Key   string
}

func (v *AccountSMV) MarshalJSON() ([]byte, error) {
	b, err := util.MarshalJSON(struct {
		accountSMVJSONMarshaler
		hint.BaseHinter
	}{
		BaseHinter: v.BaseHinter,
		accountSMVJSONMarshaler: accountSMVJSONMarshaler{
			Key:   v.key,
			Value: v.AccountSV,
		},
	})

	return b, err
}

func AccountSMVDecodeJSON(enc *jsonenc.Encoder, m map[string]any) (any, error) {
	v := new(AccountSMV)

	return v, dutil.FuncChain(
		encoder.Decode(enc, m["Value"], &v.AccountSV),
		dutil.AssignJSON(m["Key"], &v.key),
	)
}

type AccountSVM struct {
	address base.Publickey
	prev    *AccountSV
	lastop  *uint64
	key     string
	prepops []uint64
	sum     int64
}

func newAccountSVM(key string, prev *AccountSV) *AccountSVM {
	var address base.Publickey

	if prev != nil && prev.Address != nil {
		address = prev.Address
	}

	return &AccountSVM{
		key:     key,
		address: address,
		prev:    prev,
	}
}

func (m *AccountSVM) Key() string {
	return m.key
}

func (m *AccountSVM) Close() (base.StateValue, error) {
	defer func() {
		m.lastop = nil
		dutil.ClearSlice(&m.prepops)
	}()

	if m.sum == 0 {
		return nil, nil //nolint:nilnil
	}

	var merged *AccountSV

	switch {
	case m.prev != nil:
		merged = NewAccountSV(m.prev.Address, m.prev.Balance+m.sum)
	default:
		merged = NewAccountSV(m.address, m.sum)
	}

	if merged.Balance < 0 {
		return merged, errors.Errorf("under zero balance, state key=%q balance=%d", m.key, merged.Balance)
	}

	return merged, nil
}

func (m *AccountSVM) Merge(op uint64, value base.StateValue) (
	func() error, /* commit */
	uint64, /* unmergeOp */
	string, /* reason */
	error,
) {
	var ac *AccountSV

	if err := m.assertValue(value, &ac); err != nil {
		return nil, op, "invalid state value", err
	}

	switch {
	case ac.Balance < 0:
		return m.mergeSub(op, ac)
	default:
		return m.mergeAdd(op, ac)
	}
}

func (m *AccountSVM) mergeSub(op uint64, ac *AccountSV) (
	func() error,
	uint64,
	string,
	error,
) {
	switch {
	case m.address == nil:
		return nil, op, "account not exists", nil
	case m.prev != nil && m.prev.Balance+ac.Balance < 0:
		return nil, op, "insufficient balance", nil
	}

	var reason string
	unmergeOp := op

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

	return func() error {
		m.address = ac.Address
		m.lastop = &op
		m.sum += ac.Balance

		return nil
	}, unmergeOp, reason, nil
}

func (m *AccountSVM) mergeAdd(op uint64, ac *AccountSV) (
	func() error,
	uint64,
	string,
	error,
) {
	return func() error {
		if m.address == nil {
			m.address = ac.Address
		}

		m.sum += ac.Balance

		return nil
	}, op, "", nil
}

func (m *AccountSVM) Unmerge(op uint64, value base.StateValue) (
	func() error, /* commit */
	uint64, /* mergeOp */
	error,
) {
	var ac *AccountSV

	if err := m.assertValue(value, &ac); err != nil {
		return nil, op, err
	}

	switch {
	case ac.Balance < 0:
		return m.unmergeSub(op, ac)
	default:
		return m.unmergeAdd(op, ac)
	}
}

func (m *AccountSVM) unmergeSub(op uint64, ac *AccountSV) (
	func() error,
	uint64,
	error,
) {
	if m.prev.Balance+ac.Balance < 0 {
		return nil, op, nil
	}

	mergeOp := op

	for s := range slices.Values(m.prepops) {
		if s > op {
			mergeOp = s

			break
		}
	}

	return func() error {
		if m.lastop != nil && *m.lastop == op {
			m.lastop = nil
		}

		if slices.Index(m.prepops, op) < 0 {
			m.prepops = append(m.prepops, op)

			slices.Sort(m.prepops)
		}

		m.sum -= ac.Balance

		return nil
	}, mergeOp, nil
}

func (m *AccountSVM) unmergeAdd(op uint64, ac *AccountSV) (
	func() error,
	uint64,
	error,
) {
	return func() error {
		m.sum -= ac.Balance

		return nil
	}, op, nil
}

func (m *AccountSVM) assertValue(value base.StateValue, t **AccountSV) error {
	if err := encoder.Assign(value, t); err != nil {
		return err
	}

	switch ac := *t; {
	case ac.Balance == 0:
		return errors.New("zero balance")
	case m.address != nil && !m.address.Equal(ac.Address):
		return errors.New("address not matched")
	default:
		return nil
	}
}

func (m *AccountSVM) addPrepOps(op uint64) {
	if slices.Index(m.prepops, op) >= 0 {
		return
	}

	m.prepops = append(m.prepops, op)

	slices.Sort(m.prepops)
}

func isSubAccountSMV(v *AccountSMV) bool {
	return v.Balance < 0
}
