package util

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testJSONMarshaled struct {
	suite.Suite
}

type noneMarshaledStruct struct {
	A string `json:"a"`
	B int    `json:"b"`
}

type marshaledStruct struct {
	JSONMarshaled
	A string `json:"a"`
	B int    `json:"b"`
}

func (t *testJSONMarshaled) TestNoneMarshaled() {
	i := noneMarshaledStruct{A: UUID().String(), B: 33}
	b, err := MarshalJSONWithMarshaled(i)
	t.NoError(err)

	var j noneMarshaledStruct
	t.NoError(UnmarshalJSON(b, &j))

	t.Equal(i, j)
}

func (t *testJSONMarshaled) TestMarshaled() {
	i := marshaledStruct{A: UUID().String(), B: 33}
	b, err := MarshalJSONWithMarshaled(&i)
	t.NoError(err)

	var j marshaledStruct
	t.NoError(UnmarshalJSON(b, &j))

	t.Equal(i.A, j.A)
	t.Equal(i.B, j.B)

	mb, ok := i.JSONMarshaled.Marshaled()
	t.True(ok)
	t.Equal(b, mb)

	b1, err := MarshalJSONWithMarshaled(&i)
	t.NoError(err)
	t.Equal(b, b1)

	func(k marshaledStruct) {
		mb, ok := k.JSONMarshaled.Marshaled()
		t.True(ok)
		t.Equal(b, mb)
	}(i)
}

func (t *testJSONMarshaled) TestMarshaledNotPointer() {
	i := marshaledStruct{A: UUID().String(), B: 33}
	b, err := MarshalJSONWithMarshaled(i)
	t.NoError(err)

	var j marshaledStruct
	t.NoError(UnmarshalJSON(b, &j))

	t.Equal(i.A, j.A)
	t.Equal(i.B, j.B)

	mb, ok := i.JSONMarshaled.Marshaled()
	t.False(ok)
	t.NotEqual(b, mb)

	b1, err := MarshalJSONWithMarshaled(&i)
	t.NoError(err)
	t.Equal(b, b1)
}

func TestJSONMarshaled(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testJSONMarshaled))
}
