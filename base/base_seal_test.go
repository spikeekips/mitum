package base

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

var (
	dummySealHint     = hint.MustNewHint("dymmy-seal-v3.2.1")
	dummySealBodyHint = hint.MustNewHint("dymmy-seal-body-v3.2.1")
)

type dummySeal struct {
	BaseSeal
}

func newDummySeal(body []SealBody) dummySeal {
	return dummySeal{
		BaseSeal: NewBaseSeal(dummySealHint, body),
	}
}

func (sl dummySeal) IsValid(networkID []byte) error {
	if err := sl.BaseHinter.IsValid(dummySealHint.Type().Bytes()); err != nil {
		return util.InvalidError.Wrapf(err, "invalid dummySeal")
	}

	return sl.BaseSeal.IsValid(networkID)
}

type dummySealBody struct {
	A string
}

func (sb dummySealBody) Hint() hint.Hint {
	return dummySealBodyHint
}

func (sb dummySealBody) IsValid([]byte) error {
	if len(sb.A) < 1 {
		return util.InvalidError.Errorf("empty A in dummySealBody")
	}

	return nil
}

func (sb dummySealBody) HashBytes() []byte {
	return []byte(sb.A)
}

func (sb dummySealBody) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		hint.HintedJSONHead
		A string
	}{
		HintedJSONHead: hint.NewHintedJSONHead(sb.Hint()),
		A:              sb.A,
	})
}

type testBaseSeal struct {
	suite.Suite
	priv      Privatekey
	networkID NetworkID
}

func (t *testBaseSeal) SetupTest() {
	t.priv = NewMPrivatekey()
	t.networkID = NetworkID(util.UUID().Bytes())
}

func (t *testBaseSeal) TestNew() {
	ht := hint.MustNewHint("dummy-seal-v3.2.1")
	bodies := []SealBody{
		dummySealBody{A: util.UUID().String()},
		dummySealBody{A: util.UUID().String()},
	}

	sl := NewBaseSeal(ht, bodies)

	t.NoError(sl.Sign(t.priv, t.networkID))
	t.NoError(sl.IsValid(t.networkID))
}

func (t *testBaseSeal) TestEmptyBody() {
	ht := hint.MustNewHint("dummy-seal-v3.2.1")

	sl := NewBaseSeal(ht, nil)

	t.NoError(sl.Sign(t.priv, t.networkID))

	err := sl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "empty body in BaseSeal")
}

func (t *testBaseSeal) TestWithoutSigned() {
	ht := hint.MustNewHint("dummy-seal-v3.2.1")

	bodies := []SealBody{
		dummySealBody{A: util.UUID().String()},
		dummySealBody{A: util.UUID().String()},
	}
	sl := NewBaseSeal(ht, bodies)

	err := sl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "invalid BaseSeal")
}

func TestBaseSeal(t *testing.T) {
	suite.Run(t, new(testBaseSeal))
}

type testDummySeal struct {
	suite.Suite
	priv      Privatekey
	networkID NetworkID
}

func (t *testDummySeal) SetupTest() {
	t.priv = NewMPrivatekey()
	t.networkID = NetworkID(util.UUID().Bytes())
}

func (t *testDummySeal) TestNew() {
	bodies := []SealBody{
		dummySealBody{A: util.UUID().String()},
		dummySealBody{A: util.UUID().String()},
	}

	sl := newDummySeal(bodies)

	t.NoError(sl.Sign(t.priv, t.networkID))
	t.NoError(sl.IsValid(t.networkID))
}

func (t *testDummySeal) TestEmptyBody() {
	sl := newDummySeal(nil)

	t.NoError(sl.Sign(t.priv, t.networkID))

	err := sl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "empty body in BaseSeal")
}

func (t *testDummySeal) TestWithoutSigned() {
	bodies := []SealBody{
		dummySealBody{A: util.UUID().String()},
		dummySealBody{A: util.UUID().String()},
	}
	sl := newDummySeal(bodies)

	err := sl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "invalid BaseSeal")
}

func (t *testDummySeal) TestTypeMismatch() {
	bodies := []SealBody{
		dummySealBody{A: util.UUID().String()},
		dummySealBody{A: util.UUID().String()},
	}

	sl := newDummySeal(bodies)
	sl.BaseHinter = hint.NewBaseHinter(hint.MustNewHint("showme-v3.3.3"))

	t.NoError(sl.Sign(t.priv, t.networkID))
	err := sl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "type does not match in BaseHinter")
}

func TestDummySeal(t *testing.T) {
	suite.Run(t, new(testDummySeal))
}

type baseTestDummySealEncode struct {
	encoder.BaseTestEncode
	enc       encoder.Encoder
	priv      Privatekey
	networkID NetworkID
}

func (t *baseTestDummySealEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummySealHint, Instance: dummySeal{}}))
	t.NoError(t.enc.AddHinter(dummySealBody{}))
}

func testDummySealEncode() *baseTestDummySealEncode {
	t := new(baseTestDummySealEncode)

	t.priv = NewMPrivatekey()
	t.networkID = NetworkID(util.UUID().Bytes())

	t.Compare = func(a, b interface{}) {
		as, ok := a.(dummySeal)
		t.True(ok)
		bs, ok := b.(dummySeal)
		t.True(ok)

		t.NoError(bs.IsValid(t.networkID))
		t.True(bs.Signed().Signer().Equal(t.priv.Publickey()))

		CompareSeal(t.Assert(), as, bs)

		abs := as.Body()
		bbs := bs.Body()
		for i := range abs {
			ab, ok := abs[i].(dummySealBody)
			t.True(ok)
			bb, ok := bbs[i].(dummySealBody)
			t.True(ok)

			t.Equal(ab, bb)
		}
	}

	return t
}

func TestDummySealJSON(tt *testing.T) {
	t := testDummySealEncode()

	t.Encode = func() (interface{}, []byte) {
		bodies := []SealBody{
			dummySealBody{A: util.UUID().String()},
			dummySealBody{A: util.UUID().String()},
		}

		sl := newDummySeal(bodies)

		t.NoError(sl.Sign(t.priv, t.networkID))
		t.NoError(sl.IsValid(t.networkID))

		b, err := t.enc.Marshal(sl)
		t.NoError(err)

		return sl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(dummySeal)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}
