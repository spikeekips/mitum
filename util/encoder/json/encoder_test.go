package jsonenc

import (
	"encoding/json"
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

func (s *sampleDecodable) DecodeJSON(b []byte, _ encoder.Encoder) error {
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
	t.Error(err)
	t.ErrorContains(err, "already added")
}

func (t *testJSONEncoder) TestAddEmptyDecodeFuncAndInstance() {
	d := encoder.DecodeDetail{
		Hint: hint.MustNewHint("findme-v1.2.3"),
	}

	err := t.enc.Add(d)
	t.ErrorIs(err, util.ErrInvalid)
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
	t.Error(err)
	t.ErrorContains(err, "already added")
}

func (t *testJSONEncoder) TestDecodeNoneHinter() {
	ht := hint.MustNewHint("findme-v1.2.3")

	v := sample{A: "A", B: 33}

	b, err := t.enc.Marshal(struct {
		sample
		hint.HinterJSONHead
	}{
		HinterJSONHead: hint.NewHinterJSONHead(ht),
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
		hint.HinterJSONHead
	}{
		HinterJSONHead: hint.NewHinterJSONHead(ht),
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
	t.ErrorContains(derr, "find decoder by type")

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
	t.ErrorIs(err, util.ErrNotFound)
	t.ErrorContains(err, "find decoder")
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

func (t *testJSONEncoder) TestDecodePtr() {
	ht := hint.MustNewHint("findme-v1.2.3")

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ht, Instance: &sampleJSONUnmarshaler{}}))

	v := &sampleJSONUnmarshaler{
		BaseHinter: hint.NewBaseHinter(ht),
		A:          "A",
		B:          33,
	}

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	i, err := t.enc.Decode(b)
	t.NoError(err)

	uv, ok := i.(*sampleJSONUnmarshaler)
	t.True(ok, "%T", i)

	t.True(ht.Equal(uv.Hint()))
	t.Equal(v, uv)
}

func TestJSONEncoder(t *testing.T) {
	suite.Run(t, new(testJSONEncoder))
}

type testExtensibleJSON struct {
	suite.Suite
	enc *Encoder
}

func (t *testExtensibleJSON) SetupTest() {
	t.enc = NewEncoder()
}

type insideDummyJSONMarshaled struct {
	hint.BaseHinter
	A string
	B string
}

type dummyJSONMarshaled struct {
	util.DefaultExtensibleJSON
	insideDummyJSONMarshaled
}

func (d dummyJSONMarshaled) MarshalJSON() ([]byte, error) {
	if b, ok := d.MarshaledJSON(); ok {
		return b, nil
	}

	return util.MarshalJSON(d.insideDummyJSONMarshaled)
}

func (t *testExtensibleJSON) TestExtensibleJSONSingle() {
	ht := hint.MustNewHint("findme-v1.2.3")

	v := dummyJSONMarshaled{
		insideDummyJSONMarshaled: insideDummyJSONMarshaled{
			BaseHinter: hint.NewBaseHinter(ht),
			A:          "A",
			B:          "B",
		},
	}

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	// modify json
	var mb []byte
	{
		var m map[string]interface{}

		t.NoError(util.UnmarshalJSON(b, &m))

		m["C"] = "C"

		i, err := util.MarshalJSON(m)
		t.NoError(err)
		mb = i
	}

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ht, Instance: dummyJSONMarshaled{}}))
	i, err := t.enc.Decode(mb)
	t.NoError(err)

	uv, ok := i.(dummyJSONMarshaled)
	t.True(ok)

	t.Equal(v.A, uv.A)
	t.Equal(v.B, uv.B)

	mj, isMarshaled := uv.MarshaledJSON()
	t.True(isMarshaled)
	t.NotNil(mj)
	t.T().Log("marshaled json:", string(mj))

	var m map[string]interface{}

	t.NoError(util.UnmarshalJSON(mj, &m))

	c, found := m["C"]
	t.True(found)
	t.Equal("C", c)
}

func (t *testExtensibleJSON) TestExtensibleJSONSingleSlice() {
	ht := hint.MustNewHint("findme-v1.2.3")

	vs := make([]dummyJSONMarshaled, 3)

	for i := range vs {
		vs[i] = dummyJSONMarshaled{
			insideDummyJSONMarshaled: insideDummyJSONMarshaled{
				BaseHinter: hint.NewBaseHinter(ht),
				A:          util.UUID().String(),
				B:          util.UUID().String(),
			},
		}
	}

	// modify json
	vcs := make([]string, len(vs))
	var mb []byte
	{
		b, err := t.enc.Marshal(vs)
		t.NoError(err)

		var ms []map[string]interface{}

		t.NoError(util.UnmarshalJSON(b, &ms))

		for i := range ms {
			vcs[i] = util.UUID().String()
			ms[i]["C"] = vcs[i]
		}

		i, err := util.MarshalJSON(ms)
		t.NoError(err)
		mb = i
	}

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ht, Instance: dummyJSONMarshaled{}}))

	var us []json.RawMessage
	t.NoError(util.UnmarshalJSON(mb, &us))

	for i := range us {
		j, err := t.enc.Decode(us[i])
		t.NoError(err)

		v := vs[i]

		uv, ok := j.(dummyJSONMarshaled)
		t.True(ok)

		t.Equal(v.A, uv.A)
		t.Equal(v.B, uv.B)

		mj, isMarshaled := uv.MarshaledJSON()
		t.True(isMarshaled)
		t.NotNil(mj)
		t.T().Log("marshaled json:", string(mj))

		var m map[string]interface{}

		t.NoError(util.UnmarshalJSON(mj, &m))

		c, found := m["C"]
		t.True(found)
		t.Equal(vcs[i], c)

	}
}

type dummyJSONMarshaledDecodable struct {
	util.DefaultExtensibleJSON
	insideDummyJSONMarshaled
}

func (d dummyJSONMarshaledDecodable) MarshalJSON() ([]byte, error) {
	if b, ok := d.MarshaledJSON(); ok {
		return b, nil
	}

	return util.MarshalJSON(d.insideDummyJSONMarshaled)
}

func (d *dummyJSONMarshaledDecodable) DecodeJSON(b []byte, enc encoder.Encoder) error {
	if err := enc.Unmarshal(b, &d.insideDummyJSONMarshaled); err != nil {
		return err
	}

	return nil
}

func (t *testExtensibleJSON) TestExtensibleJSONDecodable() {
	ht := hint.MustNewHint("findme-v1.2.3")

	v := dummyJSONMarshaledDecodable{
		insideDummyJSONMarshaled: insideDummyJSONMarshaled{
			BaseHinter: hint.NewBaseHinter(ht),
			A:          "A",
			B:          "B",
		},
	}

	b, err := t.enc.Marshal(v)
	t.NoError(err)

	// modify json
	var mb []byte
	{
		var m map[string]interface{}

		t.NoError(util.UnmarshalJSON(b, &m))

		m["C"] = "C"

		i, err := util.MarshalJSON(m)
		t.NoError(err)
		mb = i
	}

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ht, Instance: dummyJSONMarshaledDecodable{}}))
	i, err := t.enc.Decode(mb)
	t.NoError(err)

	uv, ok := i.(dummyJSONMarshaledDecodable)
	t.True(ok)

	t.Equal(v.A, uv.A)
	t.Equal(v.B, uv.B)

	mj, isMarshaled := uv.MarshaledJSON()
	t.True(isMarshaled)
	t.NotNil(mj)
	t.T().Log("marshaled json:", string(mj))

	var m map[string]interface{}

	t.NoError(util.UnmarshalJSON(mj, &m))

	c, found := m["C"]
	t.True(found)
	t.Equal("C", c)
}

type fieldDummyJSONMarshaled struct {
	hint.BaseHinter
	D dummyJSONMarshaled
	E string
}

type fieldDummyJSONMarshaledUnmarshaler struct {
	D json.RawMessage
	E string
}

func (d *fieldDummyJSONMarshaled) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u fieldDummyJSONMarshaledUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	d.E = u.E

	i, err := enc.Decode(u.D)
	if err != nil {
		return err
	}

	d.D = i.(dummyJSONMarshaled)

	return nil
}

func (t *testExtensibleJSON) TestExtensibleJSONField() {
	ht := hint.MustNewHint("findme-v1.2.3")
	htdummy := hint.MustNewHint("showme-v1.2.3")

	d := fieldDummyJSONMarshaled{
		BaseHinter: hint.NewBaseHinter(ht),
		D: dummyJSONMarshaled{insideDummyJSONMarshaled: insideDummyJSONMarshaled{
			BaseHinter: hint.NewBaseHinter(htdummy),
			A:          "A", B: "B",
		}},
		E: "E",
	}

	b, err := util.MarshalJSON(d)
	t.NoError(err)

	t.T().Log("marshaled:", string(b))

	t.Run("check Marshaled", func() {
		mb, ok := d.D.MarshaledJSON()
		t.False(ok)
		t.Nil(mb)
	})

	// modify json
	var mb []byte
	{
		var m map[string]interface{}

		t.NoError(util.UnmarshalJSON(b, &m))

		md := m["D"].(map[string]interface{})
		md["C"] = "C"

		i, err := util.MarshalJSON(m)
		t.NoError(err)
		mb = i

		t.T().Log("modified:", string(mb))
	}

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ht, Instance: fieldDummyJSONMarshaled{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: htdummy, Instance: dummyJSONMarshaled{}}))

	i, err := t.enc.Decode(mb)
	t.NoError(err)

	ud, ok := i.(fieldDummyJSONMarshaled)
	t.True(ok)

	t.Run("MarshaledJSON", func() {
		i, ok := ud.D.MarshaledJSON()
		t.True(ok)
		t.NotNil(i)
	})

	t.Run("marshal again", func() {
		i, err := util.MarshalJSON(ud)
		t.NoError(err)

		t.T().Log("marshaled:", string(i))

		var m map[string]interface{}

		t.NoError(util.UnmarshalJSON(i, &m))

		md := m["D"].(map[string]interface{})
		v, found := md["C"]
		t.True(found)
		t.Equal("C", v)
	})
}

type embedDummyJSONMarshaled struct {
	dummyJSONMarshaled
	E string
}

func (d embedDummyJSONMarshaled) MarshalJSON() ([]byte, error) {
	if b, ok := d.MarshaledJSON(); ok {
		return b, nil
	}

	b, err := d.dummyJSONMarshaled.MarshalJSON()
	if err != nil {
		return nil, err
	}

	var m map[string]interface{}
	if err := util.UnmarshalJSON(b, &m); err != nil {
		return nil, err
	}

	m["E"] = d.E

	return util.MarshalJSON(m)
}

type embedDummyJSONMarshaledMarshaler struct {
	E string
}

func (d *embedDummyJSONMarshaled) DecodeJSON(b []byte, enc encoder.Encoder) error {
	if err := enc.Unmarshal(b, &d.dummyJSONMarshaled); err != nil {
		return err
	}

	var u embedDummyJSONMarshaledMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	d.E = u.E

	return nil
}

func (t *testExtensibleJSON) TestExtensibleJSONEmbeded() {
	ht := hint.MustNewHint("findme-v1.2.3")

	d := embedDummyJSONMarshaled{
		dummyJSONMarshaled: dummyJSONMarshaled{insideDummyJSONMarshaled: insideDummyJSONMarshaled{
			BaseHinter: hint.NewBaseHinter(ht),
			A:          "A", B: "B",
		}},
		E: "E",
	}

	b, err := util.MarshalJSON(d)
	t.NoError(err)

	t.T().Log("marshaled:", string(b))

	t.Run("check Marshaled", func() {
		mb, ok := d.MarshaledJSON()
		t.False(ok)
		t.Nil(mb)
	})

	// modify json
	var mb []byte
	{
		var m map[string]interface{}

		t.NoError(util.UnmarshalJSON(b, &m))

		m["C"] = "C"

		i, err := util.MarshalJSON(m)
		t.NoError(err)
		mb = i

		t.T().Log("modified:", string(mb))
	}

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ht, Instance: embedDummyJSONMarshaled{}}))

	i, err := t.enc.Decode(mb)
	t.NoError(err)

	ud, ok := i.(embedDummyJSONMarshaled)
	t.True(ok)

	t.Run("MarshaledJSON", func() {
		i, ok := ud.MarshaledJSON()
		t.True(ok)
		t.NotNil(i)
	})

	t.Run("marshal again", func() {
		i, err := util.MarshalJSON(ud)
		t.NoError(err)

		t.T().Log("marshaled:", string(i))

		var m map[string]interface{}

		t.NoError(util.UnmarshalJSON(i, &m))

		v, found := m["C"]
		t.True(found)
		t.Equal("C", v)
	})
}

func TestExtensibleJSON(t *testing.T) {
	suite.Run(t, new(testExtensibleJSON))
}
