//go:generate go run -v -race ../../tool/set_base_hinter.go - $GOFILE

package jsonenc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	mencoder "github.com/spikeekips/mitum/util/encoder"
	mjsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

var bigStructHint = hint.MustNewHint("big-struct-v1.2.3")

type jm = map[string]any

type bigStruct struct {
	hint.BaseHinter
	A float64     `json:"A"`
	B string      `json:"B"`
	C []bigStruct `json:"C,omitempty"`
}

func newBigStructNested(depth int) bigStruct {
	var c []bigStruct
	if depth > 0 {
		c = []bigStruct{newBigStructNested(depth - 1)}
	}

	return bigStruct{
		BaseHinter: hint.NewBaseHinter(bigStructHint),
		A:          float64(depth),
		B:          util.UUID().String(),
		C:          c,
	}
}

func newBigStructSlices(n int) bigStruct {
	var c []bigStruct
	if n > 0 {
		c = make([]bigStruct, n)
		for i := range n {
			c[i] = newBigStructSlices(0)
		}
	}

	return bigStruct{
		BaseHinter: hint.NewBaseHinter(bigStructHint),
		A:          float64(n),
		B:          util.UUID().String(),
		C:          c,
	}
}

type bigStructInterfaceMarshaler struct {
	A float64
	B string
	C []json.RawMessage
}

func (n *bigStruct) DecodeJSON(b []byte, enc mencoder.Encoder) error {
	var u bigStructInterfaceMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	n.A = u.A
	n.B = u.B

	if len(u.C) > 0 {
		n.C = make([]bigStruct, len(u.C))
		for i, v := range slices.All(u.C) {
			if err := mencoder.Decode(enc, v, &n.C[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *bigStruct) DecodeByInterface(b []byte) error {
	var u bigStructInterfaceMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	n.A = u.A
	n.B = u.B

	if len(u.C) > 0 {
		n.C = make([]bigStruct, len(u.C))
		for i, v := range slices.All(u.C) {
			if err := n.C[i].DecodeByInterface(v); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *bigStruct) DecodeByJM(b any) error {
	var m jm

	switch t := b.(type) {
	case []byte:
		if err := util.UnmarshalJSON(t, &m); err != nil {
			return err
		}
	case jm:
		m = t
	default:
		return errors.Errorf("unexpected %T", b)
	}

	n.A = m["A"].(float64)
	n.B = m["B"].(string)

	cv, found := m["C"]
	if !found {
		return nil
	}

	var c []jm

	switch ci, ok := cv.([]any); {
	case !ok:
		return errors.Errorf("expected %T, but %T", jm{}, cv)
	default:
		c = make([]jm, len(ci))
		for i, v := range slices.All(ci) {
			cm, ok := v.(jm)
			if !ok {
				return errors.Errorf("expected %T, but %T", jm{}, cv)
			}

			c[i] = cm
		}
	}

	n.C = make([]bigStruct, len(c))
	for i, v := range slices.All(c) {
		if err := n.C[i].DecodeByJM(v); err != nil {
			return err
		}
	}

	return nil
}

func bigStructDecodeJSON(enc *Encoder, m map[string]any) (any, error) {
	var n bigStruct

	n.A = m["A"].(float64)
	n.B = m["B"].(string)

	cv, found := m["C"]
	if !found {
		return n, nil
	}

	return n, encoder.DecodeSlice(enc, cv, &n.C)()
}

var stringStructHint = hint.MustNewHint("str-v1.2.3")

type stringStruct struct {
	s string
}

func (s stringStruct) MarshalText() ([]byte, error) {
	return []byte(base64.StdEncoding.EncodeToString([]byte(s.s)) + "str"), nil
}

func stringStructDecodeJSON(enc *Encoder, s string) (any, error) {
	b, err := base64.StdEncoding.DecodeString(s[:len(s)-3])
	if err != nil {
		return nil, err
	}

	var n stringStruct

	n.s = string(b)

	return n, nil
}

var stringDHint = hint.MustNewHint("strd-v1.2.3")

type stringD string

func (s stringD) MarshalText() ([]byte, error) {
	return []byte(base64.StdEncoding.EncodeToString([]byte(s)) + "strd"), nil
}

func stringDDecodeJSON(enc *Encoder, s string) (any, error) {
	b, err := base64.StdEncoding.DecodeString(s[:len(s)-4])
	if err != nil {
		return nil, err
	}

	return stringD(string(b)), nil
}

var setHinterHint = hint.MustNewHint("set-hinter-v1.2.3")

type setHinter struct {
	HT hint.Hint `json:"_hint"`
	A  float64
}

func (s setHinter) Hint() hint.Hint {
	return s.HT
}

func (s setHinter) SetHint(ht hint.Hint) hint.Hinter {
	s.HT = ht

	return s
}

func setHinterDecodeJSON(enc *Encoder, m map[string]any) (any, error) {
	var n setHinter

	n.A = m["A"].(float64)

	return n, nil
}

var decodeJSONStructHint = hint.MustNewHint("decodejson-struct-v1.2.3")

type decodeJSONStruct struct {
	A float64
}

func (decodeJSONStruct) Hint() hint.Hint {
	return decodeJSONStructHint
}

func (s decodeJSONStruct) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		HT hint.Hint `json:"_hint"`
		A  float64
	}{
		HT: decodeJSONStructHint,
		A:  s.A,
	})
}

func (s *decodeJSONStruct) DecodeJSON(enc *Encoder, m map[string]interface{}) (any, error) {
	s = &decodeJSONStruct{}

	if err := encoder.Assign(m["A"], &s.A); err != nil {
		return nil, err
	}

	return s, nil
}

type testEncodeSubStructs struct {
	dutil.BaseSuite
	enc  *Encoder
	menc *mjsonenc.Encoder
}

func (t *testEncodeSubStructs) SetupSuite() {
	t.enc = NewEncoder(context.Background(), 1<<9)
	t.NoError(AddDecode(t.enc, bigStructHint, bigStruct{}, bigStructDecodeJSON))
	t.NoError(AddDecode(t.enc, setHinterHint, setHinter{}, setHinterDecodeJSON))

	t.menc = mjsonenc.NewEncoder()
	t.NoError(t.menc.Add(mencoder.DecodeDetail{Hint: bigStructHint, Instance: bigStruct{}}))
}

func (t *testEncodeSubStructs) TearDownSuite() {
	t.enc.Close()
}

func (t *testEncodeSubStructs) p(s string, d time.Duration) {
	t.T().Logf("%22s: %0.5fs (%s)", s, float64(d.Nanoseconds())/float64(1e9), d)
}

func (t *testEncodeSubStructs) compare(n bigStruct) {
	b, err := util.MarshalJSON(n)
	t.NoError(err)

	started := time.Now()
	var d0 bigStruct
	t.NoError(d0.DecodeByInterface(b))
	e0 := time.Since(started)

	started = time.Now()
	var d1 bigStruct
	t.NoError(d1.DecodeByJM(b))
	e1 := time.Since(started)

	started = time.Now()
	var d2 bigStruct
	t.NoError(encoder.Unmarshal(t.enc, b, &d2))
	e2 := time.Since(started)

	started = time.Now()
	var d3 bigStruct
	t.NoError(mencoder.Decode(t.menc, b, &d3))
	e3 := time.Since(started)

	t.p("interface{}", e0)
	t.p("map[string]any", e1)
	t.p("new Encoder", e2)
	t.p("old Encoder", e3)
}

func (t *testEncodeSubStructs) TestNested() {
	t.Run("one", func() { t.compare(newBigStructNested(0)) })
	t.Run("3", func() { t.compare(newBigStructNested(3)) })

	for i := 100; i <= 900; i += 100 {
		t.Run(fmt.Sprintf("n=%d", i), func() { t.compare(newBigStructNested(i)) })
	}
}

func (t *testEncodeSubStructs) TestSlices() {
	t.Run("one", func() { t.compare(newBigStructSlices(1)) })
	t.Run("3", func() { t.compare(newBigStructSlices(3)) })

	for i := 10000; i <= 90000; i += 10000 {
		t.Run(fmt.Sprintf("n=%d", i), func() { t.compare(newBigStructSlices(i)) })
	}
}

func (t *testEncodeSubStructs) TestStream() {
	do := func(n int) {
		s := make([][]byte, n)
		for i := range s {
			n := newBigStructSlices(2)
			b, err := util.MarshalJSON(n)
			t.NoError(err)
			s[i] = b
		}

		started := time.Now()
		for b := range slices.Values(s) {
			var d bigStruct
			t.NoError(d.DecodeByInterface(b))
		}
		e0 := time.Since(started)

		started = time.Now()
		for b := range slices.Values(s) {
			var d bigStruct
			t.NoError(d.DecodeByJM(b))
		}
		e1 := time.Since(started)

		started = time.Now()
		for b := range slices.Values(s) {
			var d2 bigStruct
			t.NoError(encoder.Unmarshal(t.enc, b, &d2))
		}
		e2 := time.Since(started)

		started = time.Now()
		for b := range slices.Values(s) {
			var d3 bigStruct
			t.NoError(mencoder.Decode(t.menc, b, &d3))
		}
		e3 := time.Since(started)

		t.p("interface{}", e0)
		t.p("map[string]any", e1)
		t.p("new Encoder", e2)
		t.p("old Encoder", e3)
	}

	for i := 0; i <= 30000; i += 5000 {
		if i == 0 {
			continue
		}

		t.Run(fmt.Sprintf("n=%d", i), func() { do(i) })
	}
}

func TestEncodeSubStructs(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testEncodeSubStructs))
}

type testEncoder struct {
	dutil.BaseSuite
}

func (t *testEncoder) TestAddDecode() {
	t.RunWithGoLeak("interface sample", func() {
		enc := NewEncoder(context.Background(), 1<<9)
		defer enc.Close()

		err := AddDecode(enc, stringDHint, (fmt.Stringer)(nil), stringDDecodeJSON)
		t.ErrorContains(err, "nil sample")
	})

	t.RunWithGoLeak("nil sample", func() {
		enc := NewEncoder(context.Background(), 1<<9)
		defer enc.Close()

		err := AddDecode(enc, stringDHint, (fmt.Stringer)(nil), stringDDecodeJSON)
		t.ErrorContains(err, "nil sample")
	})

	t.RunWithGoLeak("nil decode func", func() {
		enc := NewEncoder(context.Background(), 1<<9)
		defer enc.Close()

		err := AddDecode(enc, decodeJSONStructHint, decodeJSONStruct{}, nil)
		t.ErrorContains(err, "nil decode")
	})
}

func (t *testEncoder) TestHinter() {
	enc := NewEncoder(context.Background(), 1<<9)
	defer enc.Close()

	t.NoError(AddDecode(enc, bigStructHint, bigStruct{}, bigStructDecodeJSON))
	t.NoError(AddDecode(enc, stringStructHint, stringStruct{}, stringStructDecodeJSON))
	t.NoError(AddDecode(enc, stringDHint, "", stringDDecodeJSON))
	t.NoError(AddDecode(enc, setHinterHint, setHinter{}, setHinterDecodeJSON))

	t.Run("BaseHinter", func() {
		n := newBigStructNested(3)
		b, err := util.MarshalJSON(n)
		t.NoError(err)

		var d bigStruct
		t.NoError(encoder.Unmarshal(enc, b, &d))

		t.T().Logf("decoded: %s", t.MarshalIndent(d))

		t.Equal(n, d)
	})

	t.Run("SetHinter", func() {
		newht := hint.MustNewHint("set-hinter-v1.0.1")
		n := setHinter{HT: newht, A: 3}
		expected := setHinter{HT: setHinterHint, A: n.A}

		b, err := util.MarshalJSON(n)
		t.NoError(err)

		var d setHinter
		t.NoError(encoder.Unmarshal(enc, b, &d))

		t.T().Logf("decoded: %s", t.MarshalIndent(d))

		t.Equal(expected, d)
	})

	t.Run("string struct", func() {
		n := stringStruct{s: util.UUID().String()}

		b, err := util.MarshalJSON(n)
		t.NoError(err)
		t.T().Logf("marshaled: %s", t.MarshalIndent(n))

		var d stringStruct
		t.NoError(encoder.UnmarshalString(enc, b, &d, 3))

		t.T().Logf("decoded: %s", t.MarshalIndent(d))

		t.Equal(n, d)
	})

	t.Run("string", func() {
		n := stringD(util.UUID().String())

		b, err := util.MarshalJSON(n)
		t.NoError(err)
		t.T().Logf("marshaled: %s", t.MarshalIndent(n))

		var d stringD
		t.NoError(encoder.UnmarshalString(enc, b, &d, 4))

		t.T().Logf("decoded: %s", t.MarshalIndent(d))

		t.Equal(n, d)
	})
}

func TestEncoder(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testEncoder))
}
