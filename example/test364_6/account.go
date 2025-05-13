//go:generate go run -v -race ../test364_5/tool/set_base_hinter.go - $GOFILE

package dop6

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	dhash "github.com/spikeekips/mitum/example/test364_5/hash"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	AccountSVHint   = hint.MustNewHint("m2-account-state-value-v0.0.1")
	AccountSMVHint  = hint.MustNewHint("m2-account-state-merge-value-v0.0.1")
	AccountFactHint = hint.MustNewHint("m2-chaccount-fact-v0.0.1")
)

type AccountSV struct {
	Address base.Publickey `json:"address"`
	Balance int64          `json:"balance"`
}

func NewAccountSV(address base.Publickey, balance int64) *AccountSV {
	return &AccountSV{
		Address: address,
		Balance: balance,
	}
}

func (s *AccountSV) Hint() hint.Hint {
	return AccountSVHint
}

func (s *AccountSV) HashBytes() []byte {
	b, _ := dutil.EncodeInt(s.Balance)

	return slices.Concat(s.Address.Bytes(), b)
}

func (s *AccountSV) IsValid([]byte) error {
	return util.CheckIsValiders(nil, false, s.Address)
}

func accountStateKey(address base.Publickey) string {
	return "ta-" + address.String()
}

type AccountSMV struct {
	*AccountSV
	key string
}

func NewAccountSMV(address base.Publickey, balance int64) (*AccountSMV, error) {
	switch {
	case address == nil:
		return nil, errors.New("empty address")
	case balance == 0:
		return nil, errors.New("zero balance")
	default:
		return &AccountSMV{
			key:       accountStateKey(address),
			AccountSV: NewAccountSV(address, balance),
		}, nil
	}
}

func (s *AccountSMV) Hint() hint.Hint {
	return AccountSMVHint
}

func (v *AccountSMV) Key() string {
	return v.key
}

func (v *AccountSMV) Value() base.StateValue {
	return v.AccountSV
}

func (v *AccountSMV) Merger(prev base.StateValue) (SVM, error) {
	var ac *AccountSV

	if prev != nil {
		if err := encoder.Assign(prev, &ac); err != nil {
			return nil, err
		}
	}

	return newAccountSVM(v.key, ac), nil
}

type AccountSVM struct {
	key            string
	address        base.Publickey
	prev           int64
	sum            int64
	mergedSub      *uint64
	mergedSubValue int64
	nextSubs       []uint64
}

var _ = any(&AccountSVM{}).(SVM) //nolint:forcetypeassert

func newAccountSVM(key string, prev *AccountSV) *AccountSVM {
	var address base.Publickey

	var sum int64

	if prev != nil && prev.Address != nil {
		address = prev.Address
		sum = prev.Balance
	}

	return &AccountSVM{
		key:     key,
		address: address,
		prev:    sum,
		sum:     sum,
	}
}

func (m *AccountSVM) Key() string {
	return m.key
}

func (m *AccountSVM) Close() (base.StateValue, error) {
	defer func() {
		dutil.ClearSlice(&m.nextSubs)
	}()

	switch {
	case m.prev == m.sum:
		return nil, nil //nolint:nilnil
	case m.sum < 0:
		return nil, errors.New("under zero balance")
	case m.address == nil:
		return nil, errors.New("no address")
	default:
		return NewAccountSV(m.address, m.sum), nil
	}
}

func (m *AccountSVM) SubMerge(opindex uint64, value base.StateValue) (uint64, string, error) {
	var ac *AccountSV
	if err := encoder.Assign(value, &ac); err != nil {
		return opindex, "", err
	}

	switch {
	case ac.Balance == 0:
		return opindex, "", errors.New("zero balance")
	case ac.Balance > 0:
		return opindex, "", errors.New("not sub value")
	case m.address == nil:
		return opindex, "account not exists", nil
	case !m.address.Equal(ac.Address):
		return opindex, "", errors.New("address not matched")
	case m.prev+ac.Balance < 0:
		return opindex, "insufficient balance", nil
	}

	if m.mergedSub == nil {
		if m.address == nil {
			m.address = ac.Address
		}

		m.sum += ac.Balance
		m.mergedSub = &opindex
		m.mergedSubValue = ac.Balance

		return opindex, "", nil
	}

	switch merged := *m.mergedSub; {
	case slices.Index(m.nextSubs, opindex) >= 0:
		return opindex, "", errors.New("already sub merged and in nexts")
	case opindex == merged:
		return opindex, "", errors.New("already sub merged")
	case opindex > merged:
		m.nextSubs = append(m.nextSubs, opindex)
		slices.Sort(m.nextSubs)

		return opindex, "previous issuer found", nil
	default:
		m.nextSubs = append(m.nextSubs, merged)
		slices.Sort(m.nextSubs)

		m.sum += ac.Balance - m.mergedSubValue
		m.mergedSub = &opindex
		m.mergedSubValue = ac.Balance

		return merged, "previous issuer found", nil
	}
}

func (m *AccountSVM) AddMerge(opindex uint64, value base.StateValue) error {
	var ac *AccountSV
	if err := encoder.Assign(value, &ac); err != nil {
		return err
	}

	switch {
	case ac.Balance == 0:
		return errors.New("zero balance")
	case ac.Balance < 0:
		return errors.New("not add value")
	case m.address != nil && !m.address.Equal(ac.Address):
		return errors.New("address not matched")
	}

	if m.address == nil {
		m.address = ac.Address
	}

	m.sum += ac.Balance

	return nil
}

func (m *AccountSVM) Unmerge(opindex uint64, value base.StateValue) error {
	var ac *AccountSV
	if err := encoder.Assign(value, &ac); err != nil {
		return err
	}

	switch {
	case ac.Balance == 0:
		return errors.New("zero balance")
	case ac.Balance < 0:
		return errors.New("not add value")
	case m.address != nil && !m.address.Equal(ac.Address):
		return errors.New("address not matched")
	}

	if m.address == nil {
		m.address = ac.Address
	}

	m.sum -= ac.Balance

	return nil
}

type SVM interface { // state Value Merger
	// Key is state key.
	Key() string
	// Close closes SVM and generate new state value from merged values.
	Close() (base.StateValue, error)
	// SubMerge merges *sub* state value to state. If failed to merge, *reason*
	// describes the reason why value is not merged. *next op* is to be unmerged
	// after merging the state value.
	//
	// - If *next op* is same with the *opindex*, *unmerged reason* indicates
	// why value is not merged.
	// - If *next op* is not same with the *opindex*, *unmerged reason*
	// indicates why the *next op* is not merged with *reason*.
	SubMerge(opindex uint64, _ base.StateValue) (
		uint64, /* next op  */
		string, /* unmerged reason */
		error,
	)

	// AddMerge merges *add* state value to state.
	AddMerge(opindex uint64, _ base.StateValue) error

	// Unmerge cancels merged *add* state value.
	Unmerge(opindex uint64, _ base.StateValue) error
}

type SMV interface { // State Merge Value
	Key() string
	Value() base.StateValue
}

func NewFuncAccountSVM(getStateValue GetStateValueFunc) MakeSVMFunc {
	return func(value SMV) (SVM, error) {
		var ac *AccountSV

		switch sv, found, err := getStateValue(value.Key()); {
		case err != nil:
			return nil, err
		case found:
			if err := encoder.Assign(sv, &ac); err != nil {
				return nil, err
			}
		}

		return newAccountSVM(value.Key(), ac), nil
	}
}

type OperationFact interface {
	base.Fact
	SMVs(context.Context) (SMV, []SMV, string, error)
}

type AccountFact struct {
	h        util.Hash
	Issuer   base.Publickey
	Receiver base.Publickey
	token    base.Token
	hint.BaseHinter
	Amount int64
}

var _ = interface{}(new(AccountFact)).(OperationFact) //nolint:forcetypeassert

func NewAccountFact(token base.Token, amount int64, issuer, receiver base.Publickey) *AccountFact {
	fact := &AccountFact{
		BaseHinter: hint.NewBaseHinter(AccountFactHint),
		token:      token,
		Issuer:     issuer,
		Receiver:   receiver,
		Amount:     amount,
	}

	fact.h = fact.generateHash()

	return fact
}

func (fact *AccountFact) Hash() util.Hash {
	return fact.h
}

func (fact *AccountFact) Token() base.Token {
	return fact.token
}

func (fact *AccountFact) SMVs(context.Context) (SMV, []SMV, string, error) {
	var subv SMV
	var addvs []SMV

	if fact.Issuer != nil {
		switch i, err := NewAccountSMV(fact.Issuer, fact.Amount*-1); {
		case err != nil:
			return nil, nil, "", err
		default:
			subv = i
		}
	}

	switch i, err := NewAccountSMV(fact.Receiver, fact.Amount); {
	case err != nil:
		return nil, nil, "", err
	default:
		addvs = []SMV{i}
	}

	return subv, addvs, "", nil
}

func (fact *AccountFact) IsValid([]byte) error {
	if err := fact.BaseHinter.IsValid(fact.Hint().Type().Bytes()); err != nil {
		return err
	}

	if err := base.IsValidFact(fact, nil); err != nil {
		return err
	}

	if err := util.CheckIsValiders(nil, false, fact.Receiver); err != nil {
		return err
	}

	if fact.Amount < 1 {
		return util.ErrInvalid.Errorf("under 1")
	}

	if fact.Issuer != nil {
		if err := fact.Issuer.IsValid(nil); err != nil {
			return err
		}
	}

	if !fact.Hash().Equal(fact.generateHash()) {
		return errors.New("hash does not match")
	}

	return nil
}

func (fact *AccountFact) generateHash() util.Hash {
	w, sum := dhash.NewHex64Writer()

	w(fact.Token())

	if fact.Issuer != nil {
		w(fact.Issuer.Bytes())
	}

	w(fact.Receiver.Bytes())
	b, _ := dutil.EncodeInt(fact.Amount)
	w(b)

	return sum()
}
