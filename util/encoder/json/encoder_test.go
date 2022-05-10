package jsonenc

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

type sample struct {
	A string
	B int
}

type sampleNative struct {
	A string
	hint.BaseHinter
	B int
}

type sampleJSONUnmarshaler struct {
	A string
	hint.BaseHinter
	B int
}

func (s *sampleJSONUnmarshaler) UnmarshalJSON(b []byte) error {
	var v struct {
		A string
		B int
	}

	if err := util.UnmarshalJSON(b, &v); err != nil {
		return err
	}

	s.A = v.A
	s.B = v.B

	return nil
}

type sampleTextUnmarshaler struct {
	A string
	hint.BaseHinter
	B int
}

func (s *sampleTextUnmarshaler) UnmarshalText(b []byte) error {
	var v struct {
		A string
		B int
	}

	if err := util.UnmarshalJSON(b, &v); err != nil {
		return err
	}

	s.A = v.A
	s.B = v.B

	return nil
}

type sampleDecodable struct {
	A string
	hint.BaseHinter
	B int
}

func (s *sampleDecodable) DecodeJSON(b []byte, _ *Encoder) error {
	var v struct {
		A string
		B int
	}

	if err := util.UnmarshalJSON(b, &v); err != nil {
		return err
	}

	s.A = v.A + v.A
	s.B = v.B + v.B

	return nil
}

type sampleFixedType struct {
	s string
	hint.BaseHinter
}

func (s sampleFixedType) String() string {
	return s.s + s.Hint().Type().String()
}

func (s sampleFixedType) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s *sampleFixedType) UnmarshalText(b []byte) error {
	s.s = string(b)

	return nil
}

type testJSONEncoder struct {
	suite.Suite
	enc *Encoder
}

func (t *testJSONEncoder) SetupTest() {
	t.enc = NewEncoder()
}

func (t *testJSONEncoder) TestNew() {
	_ = (interface{})(t.enc).(encoder.Encoder)
	_, ok := (interface{})(t.enc).(encoder.Encoder)
	t.True(ok)
}

func (t *testJSONEncoder) TestAddAgain() {
	d := encoder.DecodeDetail{
		Hint: hint.MustNewHint("findme-v1.2.3"),
		Decode: func([]byte, hint.Hint) (interface{}, error) {
			return nil, nil
		},
	}
	t.NoError(t.enc.Add(d))

	// add again
	err := t.enc.Add(d)
	t.True(errors.Is(err, util.DuplicatedError))
	t.ErrorContains(err, "already added")
}

func (t *testJSONEncoder) TestAddEmptyDecodeFuncAndInstance() {
	d := encoder.DecodeDetail{
		Hint: hint.MustNewHint("findme-v1.2.3"),
	}

	err := t.enc.Add(d)
	t.True(errors.Is(err, util.InvalidError))
	t.ErrorContains(err, "instance and decode func are empty")
}

func (t *testJSONEncoder) TestAddHinterAgain() {
	hr := sampleJSONUnmarshaler{
		BaseHinter: hint.NewBaseHinter(hint.MustNewHint("findme-v1.2.3")),
		A:          "A", B: 33,
	}
	t.NoError(t.enc.AddHinter(hr))

	// add again
	err := t.enc.AddHinter(hr)
	t.True(errors.Is(err, util.DuplicatedError))
	t.ErrorContains(err, "already added")
}

func (t *testJSONEncoder) TestDecodeNoneHinter() {
	ht := hint.MustNewHint("findme-v1.2.3")

	v := sample{A: "A", B: 33}

	b, err := t.enc.Marshal(struct {
		sample
		hint.HintedJSONHead
	}{
		HintedJSONHead: hint.NewHintedJSONHead(ht),
		sample:         v,
	})
	t.NoError(err)

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ht, Instance: v}))
	i, err := t.enc.Decode(b)
	t.NoError(err)

	uv, ok := i.(sample)
	t.True(ok)

	t.Equal(v, uv)
}

func (t *testJSONEncoder) TestDecodeWithHintType() {
	ht := hint.MustNewHint("findme-v1.2.3")

	v := sample{A: "A", B: 33}

	b, err := t.enc.Marshal(struct {
		sample
		hint.HintedJSONHead
	}{
		HintedJSONHead: hint.NewHintedJSONHead(ht),
		sample:         v,
	})
	t.NoError(err)

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ht, Instance: v}))
	i, err := t.enc.DecodeWithHintType(b, ht.Type())
	t.NoError(err)

	uv, ok := i.(sample)
	t.True(ok)

	t.Equal(v, uv)
}

func (t *testJSONEncoder) TestDecodeWithFixedHintType() {
	ht := hint.MustNewHint("findme-v1.2.3")

	v := sampleFixedType{BaseHinter: hint.NewBaseHinter(ht), s: util.UUID().String()}

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	var s string
	t.NoError(t.enc.Unmarshal(b, &s))

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ht, Instance: v}))
	i, err := t.enc.DecodeWithFixedHintType(s, len(ht.Type()))
	t.NoError(err)

	uv, ok := i.(sampleFixedType)
	t.True(ok)

	t.Equal(v, uv)
}

func (t *testJSONEncoder) TestDecodeWithFixedHintTypePool() {
	pool := util.NewGCacheObjectPool(10)
	_ = t.enc.SetPool(pool)

	ht := hint.MustNewHint("findme-v1.2.3")

	v := sampleFixedType{BaseHinter: hint.NewBaseHinter(ht), s: util.UUID().String()}

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	var s string
	t.NoError(t.enc.Unmarshal(b, &s))

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ht, Instance: v}))
	i, err := t.enc.DecodeWithFixedHintType(s, len(ht.Type()))
	t.NoError(err)

	uv, ok := i.(sampleFixedType)
	t.True(ok)

	t.Equal(v, uv)

	pv, found := pool.Get(v.String())
	t.True(found)
	t.Equal(v, pv)
}

func (t *testJSONEncoder) TestDecodeWithFixedHintTypePoolError() {
	pool := util.NewGCacheObjectPool(10)
	_ = t.enc.SetPool(pool)

	ht := hint.MustNewHint("findme-v1.2.3")

	v := sampleFixedType{BaseHinter: hint.NewBaseHinter(ht), s: util.UUID().String()}

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	var s string
	t.NoError(t.enc.Unmarshal(b, &s))

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ht, Instance: v}))
	i, derr := t.enc.DecodeWithFixedHintType(s, len(ht.Type())-1)
	t.Error(derr)
	t.Nil(i)
	t.ErrorContains(derr, "failed to find decoder by type")

	perr0, found := pool.Get(v.String())
	t.True(found)
	t.NotNil(perr0)
	t.Equal(derr, perr0)

	i, perr1 := t.enc.DecodeWithFixedHintType(s, len(ht.Type())-1)
	t.Error(perr1)
	t.Nil(i)
	t.True(found)
	t.NotNil(perr1)
	t.Equal(derr, perr1)
}

func (t *testJSONEncoder) TestDecodeJSONUnmarshaler() {
	ht := hint.MustNewHint("findme-v1.2.3")

	v := sampleJSONUnmarshaler{
		BaseHinter: hint.NewBaseHinter(ht),
		A:          "A",
		B:          33,
	}

	t.NoError(t.enc.AddHinter(v))

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	i, err := t.enc.Decode(b)
	t.NoError(err)

	uv, ok := i.(sampleJSONUnmarshaler)
	t.True(ok)

	t.True(ht.Equal(uv.Hint()))
	t.Equal(v, uv)
}

func (t *testJSONEncoder) TestDecodeMultipleVersions() {
	dv0 := encoder.DecodeDetail{
		Hint: hint.MustNewHint("findme-v1.2.3"),
		Decode: func(b []byte, ht hint.Hint) (interface{}, error) {
			var i map[string]json.RawMessage
			if err := util.UnmarshalJSON(b, &i); err != nil {
				return nil, err
			}

			var s sample
			if err := util.UnmarshalJSON(i["d"], &s); err != nil {
				return nil, err
			}

			return s, nil
		},
	}

	dv1 := encoder.DecodeDetail{
		Hint: hint.MustNewHint("findme-v2.2.3"),
		Decode: func(b []byte, ht hint.Hint) (interface{}, error) {
			var i map[string]json.RawMessage
			if err := util.UnmarshalJSON(b, &i); err != nil {
				return nil, err
			}

			var s sample
			if err := util.UnmarshalJSON(i["d"], &s); err != nil {
				return nil, err
			}

			s.A += "BB"
			s.B += 11

			return s, nil
		},
	}
	t.NoError(t.enc.Add(dv0))
	t.NoError(t.enc.Add(dv1))

	v := sample{A: "A", B: 33}

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	{ // v0
		nb := fmt.Sprintf(`{"%s": %q, "d": %s}`, hint.HintedJSONTag, dv0.Hint, string(b))

		uv, err := t.enc.Decode([]byte(nb))
		t.NoError(err)

		t.Equal(v, uv)
	}

	{ // v1
		nb := fmt.Sprintf(`{"%s": %q, "d": %s}`, hint.HintedJSONTag, dv1.Hint, string(b))

		i, err := t.enc.Decode([]byte(nb))
		t.NoError(err)

		uv, ok := i.(sample)
		t.True(ok)

		t.Equal(v.A+"BB", uv.A)
		t.Equal(v.B+11, uv.B)
	}
}

func (t *testJSONEncoder) TestDecodeTextUnmarshaler() {
	ht := hint.MustNewHint("findme-v1.2.3")

	v := sampleTextUnmarshaler{
		BaseHinter: hint.NewBaseHinter(ht),
		A:          "A",
		B:          33,
	}

	t.NoError(t.enc.AddHinter(v))

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	i, err := t.enc.Decode(b)
	t.NoError(err)

	uv, ok := i.(sampleTextUnmarshaler)
	t.True(ok)

	t.True(ht.Equal(uv.Hint()))
	t.Equal(v, uv)
}

func (t *testJSONEncoder) TestDecodeDecodable() {
	ht := hint.MustNewHint("findme-v1.2.3")

	v := sampleDecodable{
		BaseHinter: hint.NewBaseHinter(ht),
		A:          "A",
		B:          33,
	}

	t.NoError(t.enc.AddHinter(v))

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	i, err := t.enc.Decode(b)
	t.NoError(err)

	uv, ok := i.(sampleDecodable)
	t.True(ok)

	t.True(ht.Equal(uv.Hint()))
	t.Equal(v.A+v.A, uv.A)
	t.Equal(v.B+v.B, uv.B)
}

func (t *testJSONEncoder) TestDecodeNative() {
	ht := hint.MustNewHint("findme-v1.2.3")

	v := sampleNative{
		BaseHinter: hint.NewBaseHinter(ht),
		A:          "A",
		B:          33,
	}

	t.NoError(t.enc.AddHinter(v))

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	i, err := t.enc.Decode(b)
	t.NoError(err)

	uv, ok := i.(sampleNative)
	t.True(ok)

	t.True(ht.Equal(uv.Hint()))
	t.Equal(v, uv)
}

func (t *testJSONEncoder) TestDecodeCustomDecodeFunc() {
	ht := hint.MustNewHint("findme-v1.2.3")

	d := encoder.DecodeDetail{
		Hint: ht,
		Decode: func(b []byte, ht hint.Hint) (interface{}, error) {
			var i map[string]json.RawMessage
			if err := util.UnmarshalJSON(b, &i); err != nil {
				return nil, err
			}

			var s sample
			if err := util.UnmarshalJSON(i["d"], &s); err != nil {
				return nil, err
			}

			return s, nil
		},
	}
	t.NoError(t.enc.Add(d))

	v := sample{A: "A", B: 33}

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	nb := fmt.Sprintf(`{"%s": %q, "d": %s}`, hint.HintedJSONTag, ht, string(b))

	uv, err := t.enc.Decode([]byte(nb))
	t.NoError(err)

	t.Equal(v, uv)
}

func (t *testJSONEncoder) TestDecodeUnknown() {
	v := sampleJSONUnmarshaler{
		BaseHinter: hint.NewBaseHinter(hint.MustNewHint("findme-v1.2.3")),
		A:          "A",
		B:          33,
	}

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	_, err = t.enc.Decode(b)
	t.True(errors.Is(err, util.NotFoundError))
	t.ErrorContains(err, "failed to find decoder")
}

func (t *testJSONEncoder) TestDecodeSlice() {
	hra := sampleJSONUnmarshaler{
		BaseHinter: hint.NewBaseHinter(hint.MustNewHint("findme-v1.2.3")),
		A:          "A",
		B:          33,
	}

	hrb := sampleTextUnmarshaler{
		BaseHinter: hint.NewBaseHinter(hint.MustNewHint("showme-v1.2.3")),
		A:          "A",
		B:          33,
	}

	t.NoError(t.enc.AddHinter(hra))
	t.NoError(t.enc.AddHinter(hrb))

	b, err := t.enc.Marshal([]interface{}{hra, hrb})
	t.NoError(err)

	i, err := t.enc.DecodeSlice(b)
	t.NoError(err)

	t.Equal(2, len(i))

	uhra, ok := i[0].(sampleJSONUnmarshaler)
	t.True(ok)
	uhrb, ok := i[1].(sampleTextUnmarshaler)
	t.True(ok)

	t.Equal(hra, uhra)
	t.Equal(hrb, uhrb)
}

func TestJSONEncoder(t *testing.T) {
	suite.Run(t, new(testJSONEncoder))
}
