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
	hint.BaseHinter
	A string
	B int
}

type sampleJSONUnmarshaler struct {
	hint.BaseHinter
	A string
	B int
}

type innerSampleJSONUnmarshaler struct {
	A string
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
	hint.BaseHinter
	A string
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
	hint.BaseHinter
	A string
	B int
}

func (s *sampleDecodable) DecodeJSON(b []byte, _ hint.Hint) error {
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
	t.Contains(err.Error(), "already added")
}

func (t *testJSONEncoder) TestAddEmptyDecodeFuncAndInstance() {
	d := encoder.DecodeDetail{
		Hint: hint.MustNewHint("findme-v1.2.3"),
	}

	err := t.enc.Add(d)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "instance and decode func are empty")
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
	t.Contains(err.Error(), "already added")
}

func (t *testJSONEncoder) TestDecodeNoneHinter() {
	ht := hint.MustNewHint("findme-v1.2.3")

	v := sample{A: "A", B: 33}

	b, err := t.enc.Marshal(struct {
		hint.HintedJSONHead
		sample
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
		hint.HintedJSONHead
		sample
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
	t.Equal(v, uv)
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
	t.Contains(err.Error(), "failed to find decoder")
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
