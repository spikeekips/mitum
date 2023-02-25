package base

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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

func (op dummyOperation) PreProcess(ctx context.Context, _ GetStateFunc) (context.Context, OperationProcessReasonError, error) {
	return ctx, nil, nil
}

func (op dummyOperation) Process(context.Context, GetStateFunc) ([]StateMergeValue, OperationProcessReasonError, error) {
	return nil, nil, nil
}

type dummyNodeOperation struct {
	BaseNodeOperation
}

func (op dummyNodeOperation) PreProcess(ctx context.Context, _ GetStateFunc) (context.Context, OperationProcessReasonError, error) {
	return ctx, nil, nil
}

func (op dummyNodeOperation) Process(context.Context, GetStateFunc) ([]StateMergeValue, OperationProcessReasonError, error) {
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

	t.Run("invalid network id", func() {
		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, ErrSignatureVerification))
	})

	_ = (interface{})(op).(Operation)
}

func (t *testBaseOperation) TestIsValid() {
	t.Run("valid", func() {
		op := t.newSignedOperation()

		t.NoError(op.IsValid(t.networkID))
	})

	t.Run("wrong hash", func() {
		op := t.newSignedOperation()
		op.h = valuehash.RandomSHA256()

		err := op.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "hash does not match")
	})

	t.Run("invalid fact", func() {
		op := t.newSignedOperation()

		fact := op.fact.(DummyFact)
		fact.h = valuehash.RandomSHA256()
		op.fact = fact

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "hash does not match")
	})

	t.Run("empty signs", func() {
		op := t.newOperation()

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty signs")
	})

	t.Run("invalid network id", func() {
		op := t.newSignedOperation()

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, ErrSignatureVerification))
	})
}

func (t *testBaseOperation) TestSign() {
	t.Run("sign in not sign", func() {
		op := t.newOperation()

		t.NoError(op.Sign(t.priv, t.networkID))

		t.Equal(1, len(op.signs))
	})

	t.Run("duplicated sign", func() {
		op := t.newOperation()

		t.NoError(op.Sign(t.priv, t.networkID))

		oldsign := op.signs[0]

		<-time.After(time.Millisecond * 100)
		t.NoError(op.Sign(t.priv, t.networkID))

		newsign := op.signs[0]

		t.True(oldsign.Signer().Equal(newsign.Signer()))
		t.NotEqual(oldsign.Signature(), newsign.Signature())
		t.NotEqual(oldsign.SignedAt(), newsign.SignedAt())
	})

	t.Run("new sign", func() {
		op := t.newOperation()

		t.NoError(op.Sign(t.priv, t.networkID))

		priv := NewMPrivatekey()
		t.NoError(op.Sign(priv, t.networkID))

		t.Equal(2, len(op.signs))
	})
}

func TestBaseOperation(t *testing.T) {
	suite.Run(t, new(testBaseOperation))
}

type testBaseNodeOperation struct {
	suite.Suite
	priv      Privatekey
	networkID NetworkID
}

func (t *testBaseNodeOperation) SetupSuite() {
	t.priv = NewMPrivatekey()
	t.networkID = util.UUID().Bytes()
}

func (t *testBaseNodeOperation) newOperation() dummyNodeOperation {
	ht := hint.MustNewHint(util.UUID().String() + "-v0.0.3")

	fact := NewDummyFact(util.UUID().Bytes(), util.UUID().String())

	return dummyNodeOperation{BaseNodeOperation: NewBaseNodeOperation(ht, fact)}
}

func (t *testBaseNodeOperation) newSignedOperation(node Address) dummyNodeOperation {
	op := t.newOperation()
	t.NoError(op.NodeSign(t.priv, t.networkID, node))

	return op
}

func (t *testBaseNodeOperation) TestNew() {
	op := t.newSignedOperation(RandomAddress(""))

	t.Run("invalid network id", func() {
		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, ErrSignatureVerification))
	})

	_ = (interface{})(op).(Operation)
}

func (t *testBaseNodeOperation) TestIsValid() {
	t.Run("valid", func() {
		op := t.newSignedOperation(RandomAddress(""))

		t.NoError(op.IsValid(t.networkID))
	})

	t.Run("wrong hash", func() {
		op := t.newSignedOperation(RandomAddress(""))
		op.h = valuehash.RandomSHA256()

		err := op.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "hash does not match")
	})

	t.Run("invalid fact", func() {
		op := t.newSignedOperation(RandomAddress(""))

		fact := op.fact.(DummyFact)
		fact.h = valuehash.RandomSHA256()
		op.fact = fact

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "hash does not match")
	})

	t.Run("empty signs", func() {
		op := t.newOperation()

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty signs")
	})

	t.Run("invalid network id", func() {
		op := t.newSignedOperation(RandomAddress(""))

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, ErrSignatureVerification))
	})

	t.Run("duplicated sign", func() {
		op := t.newSignedOperation(RandomAddress(""))
		op.signs = []Sign{op.signs[0], op.signs[0]}
		op.h = op.hash()

		err := op.IsValid(t.networkID)
		t.Error(err)
		t.ErrorContains(err, "duplicated signs found")
	})
}

func (t *testBaseNodeOperation) TestSign() {
	node := RandomAddress("")

	t.Run("sign in not sign", func() {
		op := t.newOperation()

		t.NoError(op.NodeSign(t.priv, t.networkID, node))

		t.Equal(1, len(op.signs))
	})

	t.Run("duplicated sign", func() {
		op := t.newOperation()

		t.NoError(op.NodeSign(t.priv, t.networkID, node))

		oldsign := op.signs[0]

		<-time.After(time.Millisecond * 100)
		t.NoError(op.NodeSign(t.priv, t.networkID, node))

		newsign := op.signs[0]

		t.True(oldsign.Signer().Equal(newsign.Signer()))
		t.NotEqual(oldsign.Signature(), newsign.Signature())
		t.NotEqual(oldsign.SignedAt(), newsign.SignedAt())
	})

	t.Run("new sign", func() {
		op := t.newOperation()

		t.NoError(op.NodeSign(t.priv, t.networkID, node))

		priv := NewMPrivatekey()
		t.NoError(op.NodeSign(priv, t.networkID, RandomAddress("")))

		t.Equal(2, len(op.signs))
	})
}

func TestBaseNodeOperation(t *testing.T) {
	suite.Run(t, new(testBaseNodeOperation))
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
		EqualSigns(t.Assert(), ao.Signs(), bo.Signs())
	}

	suite.Run(tt, t)
}

type insideDummyOperationExtensible struct {
	hint.BaseHinter
	util.DefaultExtensibleJSON
	A string
}

func (d insideDummyOperationExtensible) MarshalJSON() ([]byte, error) {
	if b, ok := d.MarshaledJSON(); ok {
		return b, nil
	}

	return util.MarshalJSON(struct {
		hint.BaseHinter
		util.DefaultExtensibleJSON
		A string
	}{
		BaseHinter: d.BaseHinter,
		A:          d.A,
	},
	)
}

type dummyOperationExtensible struct {
	BaseOperation
	util.DefaultExtensibleJSON
	I insideDummyOperationExtensible
}

func (op dummyOperationExtensible) MarshalJSON() ([]byte, error) {
	if b, ok := op.MarshaledJSON(); ok {
		return b, nil
	}

	return util.MarshalJSON(struct {
		BaseOperationJSONMarshaler
		I insideDummyOperationExtensible
	}{
		BaseOperationJSONMarshaler: op.BaseOperation.JSONMarshaler(),
		I:                          op.I,
	},
	)
}

type dummyOperationExtensibleUnmarshaller struct {
	I json.RawMessage
}

func (op *dummyOperationExtensible) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	if err := op.BaseOperation.DecodeJSON(b, enc); err != nil {
		return err
	}

	var u dummyOperationExtensibleUnmarshaller
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	if err := encoder.Decode(enc, u.I, &op.I); err != nil {
		return err
	}

	return nil
}

func TestBaseOperationEncodeExtensible(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	ht := hint.MustNewHint("dummyoperation-v0.0.3")
	insideht := hint.MustNewHint("dummyoperation-inside-v0.0.3")

	networkID := NetworkID("dummy-network-id")

	t.Encode = func() (interface{}, []byte) {
		b := `
{
  "hash": "G8znFJSYix9UzB8fWFgEcfSnamkdnQfunh5uMRjNoBst",
  "fact": {
    "_hint": "dummyfact-v1.2.3",
    "H": "7X3TXhEhWQEp6rjPCLGxJ7QwB3GBzntEvVzZBeKhoHLr",
    "Token": "pfEb1rnJQ86UrmQ6hyxrqQ==",
    "V": "96c811bf-3921-4ab7-bd39-8fdd6c448ed7"
  },
  "signs": [
    {
      "signed_at": "2023-02-25T06:36:11.58028639Z",
      "signer": "dXXonLEHvi7ohuiZoka2etuKY3bRwzK6Kk9jU74D9nSimpu",
      "signature": "AN1rKvtRdk1qsKqrFcFS44mzNcLVTvb8ZCjJymbmbWeiK7aUgrHnHNP5TQQQ2aKC8qStjAM2RFM4bCJp8gM26yy9pPT5C3H9g"
    }
  ],
  "_hint": "dummyoperation-v0.0.3",
  "I": {
    "_hint": "dummyoperation-inside-v0.0.3",
    "A": "A",
    "killme": "eatme"
  },
  "showme": "findme"
}
`

		t.T().Log("marshaled:", b)

		return nil, []byte(b)
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: DummyFactHint, Instance: DummyFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ht, Instance: dummyOperationExtensible{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: insideht, Instance: insideDummyOperationExtensible{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(dummyOperationExtensible)
		t.True(ok)

		e, ok := i.(util.ExtensibleJSON)
		t.True(ok)

		mb, ismarshaled := e.MarshaledJSON()
		t.True(ismarshaled)
		t.NotNil(mb)

		return i
	}
	t.Compare = func(_, b interface{}) {
		_, ok := b.(util.ExtensibleJSON)
		t.True(ok)

		bo, ok := b.(dummyOperationExtensible)
		t.True(ok)

		_, ok = (interface{})(bo.I).(util.ExtensibleJSON)
		t.True(ok)

		t.NoError(bo.IsValid(networkID))

		t.Run("check MarshaledJSON", func() {
			mb, ismarshaled := bo.MarshaledJSON()
			t.True(ismarshaled)
			t.NotNil(mb)

			t.T().Log("MarshaledJSON:", string(mb))
		})

		t.Run("check extra field, `showme`", func() {
			mb, err := util.MarshalJSON(b)
			t.NoError(err)

			t.T().Log("marshaled:", string(mb))
			var m map[string]interface{}
			t.NoError(util.UnmarshalJSON(mb, &m))

			showme, found := m["showme"]
			t.True(found)
			t.Equal("findme", showme)
		})

		t.Run("check inside MarshaledJSON", func() {
			mb, ismarshaled := bo.I.MarshaledJSON()
			t.True(ismarshaled)
			t.NotNil(mb)

			t.T().Log("MarshaledJSON:", string(mb))
		})

		t.Run("check inside extra field, `killme`", func() {
			mb, err := util.MarshalJSON(bo.I)
			t.NoError(err)

			t.T().Log("marshaled:", string(mb))

			var m map[string]interface{}
			t.NoError(util.UnmarshalJSON(mb, &m))

			killme, found := m["killme"]
			t.True(found)
			t.Equal("eatme", killme)
		})
	}

	suite.Run(tt, t)
}

func TestBaseNodeOperationEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	ht := hint.MustNewHint(util.UUID().String() + "-v0.0.3")
	networkID := NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		fact := NewDummyFact(util.UUID().Bytes(), util.UUID().String())

		op := NewBaseNodeOperation(ht, fact)
		t.NoError(op.NodeSign(NewMPrivatekey(), networkID, RandomAddress("")))

		b, err := enc.Marshal(op)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return op, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: StringAddressHint, Instance: StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: DummyFactHint, Instance: DummyFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ht, Instance: BaseNodeOperation{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(BaseNodeOperation)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		ao, ok := a.(BaseNodeOperation)
		t.True(ok)
		bo, ok := b.(BaseNodeOperation)
		t.True(ok)

		t.NoError(bo.IsValid(networkID))

		EqualFact(t.Assert(), ao.Fact(), bo.Fact())
		EqualSigns(t.Assert(), ao.Signs(), bo.Signs())
	}

	suite.Run(tt, t)
}
