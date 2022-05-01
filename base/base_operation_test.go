package base

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type dummyOperation struct {
	BaseOperation
}

func (op dummyOperation) PreProcess(context.Context, GetStateFunc) (OperationProcessReasonError, error) {
	return nil, nil
}

func (op dummyOperation) Process(context.Context, GetStateFunc) ([]StateMergeValue, OperationProcessReasonError, error) {
	return nil, nil, nil
}

type testBaseOperation struct {
	suite.Suite
	priv      Privatekey
	networkID NetworkID
}

func (t *testBaseOperation) SetupSuite() {
	t.priv = NewMPrivatekey()
	t.networkID = util.UUID().Bytes()
}

func (t *testBaseOperation) newOperation() dummyOperation {
	ht := hint.MustNewHint(util.UUID().String() + "-v0.0.3")

	fact := NewDummyFact(util.UUID().Bytes(), util.UUID().String())

	return dummyOperation{BaseOperation: NewBaseOperation(ht, fact)}
}

func (t *testBaseOperation) newSignedOperation() dummyOperation {
	op := t.newOperation()
	t.NoError(op.Sign(t.priv, t.networkID))

	return op
}

func (t *testBaseOperation) TestNew() {
	op := t.newSignedOperation()

	t.Run("invalid network di", func() {
		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, SignatureVerificationError))
	})

	_ = (interface{})(op).(Operation)
}

func (t *testBaseOperation) TestIsValid() {
	t.Run("valid", func() {
		op := t.newSignedOperation()

		t.NoError(op.IsValid(t.networkID))
	})

	t.Run("invalid fact", func() {
		op := t.newSignedOperation()

		fact := op.fact.(DummyFact)
		fact.h = valuehash.RandomSHA256()
		op.fact = fact

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "hash does not match")
	})

	t.Run("empty Signeds", func() {
		op := t.newOperation()

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty signed")
	})

	t.Run("invalid network id", func() {
		op := t.newSignedOperation()

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, SignatureVerificationError))
	})

	t.Run("duplicated signed", func() {
		op := t.newSignedOperation()
		op.signed = []Signed{op.signed[0], op.signed[0]}

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, SignatureVerificationError))
	})
}

func (t *testBaseOperation) TestSign() {
	t.Run("sign in not signed", func() {
		op := t.newOperation()

		t.NoError(op.Sign(t.priv, t.networkID))

		t.Equal(1, len(op.signed))
	})

	t.Run("duplicated sign", func() {
		op := t.newOperation()

		t.NoError(op.Sign(t.priv, t.networkID))

		oldsigned := op.signed[0]

		t.NoError(op.Sign(t.priv, t.networkID))

		newsigned := op.signed[0]

		t.True(oldsigned.Signer().Equal(newsigned.Signer()))
		t.NotEqual(oldsigned.Signature(), newsigned.Signature())
		t.NotEqual(oldsigned.SignedAt(), newsigned.SignedAt())
	})

	t.Run("new sign", func() {
		op := t.newOperation()

		t.NoError(op.Sign(t.priv, t.networkID))

		priv := NewMPrivatekey()
		t.NoError(op.Sign(priv, t.networkID))

		t.Equal(2, len(op.signed))
	})
}

func TestBaseOperation(t *testing.T) {
	suite.Run(t, new(testBaseOperation))
}

func TestBaseOperationEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	ht := hint.MustNewHint(util.UUID().String() + "-v0.0.3")
	networkID := NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		fact := NewDummyFact(util.UUID().Bytes(), util.UUID().String())

		op := NewBaseOperation(ht, fact)
		t.NoError(op.Sign(NewMPrivatekey(), networkID))

		b, err := enc.Marshal(op)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return op, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: DummyFactHint, Instance: DummyFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ht, Instance: BaseOperation{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(BaseOperation)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		ao, ok := a.(BaseOperation)
		t.True(ok)
		bo, ok := b.(BaseOperation)
		t.True(ok)

		t.NoError(bo.IsValid(networkID))

		EqualFact(t.Assert(), ao.Fact(), bo.Fact())
		EqualSigneds(t.Assert(), ao.Signed(), bo.Signed())
	}

	suite.Run(tt, t)
}
