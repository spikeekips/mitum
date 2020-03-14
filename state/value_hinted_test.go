package state

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/spikeekips/mitum/encoder"
	"github.com/spikeekips/mitum/hint"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/valuehash"
)

var dummyHintedValueHint = hint.MustHintWithType(hint.Type{0xff, 0x60}, "0.0.1", "dummy-hinted-value")

type dummyNotHinted struct {
	v int
}

type dummyNotByter struct {
	dummyNotHinted
}

func (dv dummyNotByter) Hint() hint.Hint {
	return dummyHintedValueHint
}

type dummyNotHasher struct {
	dummyNotByter
}

func (dv dummyNotHasher) Bytes() []byte {
	return util.IntToBytes(dv.v)
}

type dummy struct {
	dummyNotHasher
}

func (dv dummy) Hash() valuehash.Hash {
	return valuehash.NewSHA256(dv.Bytes())
}

func (dv dummy) MarshalJSON() ([]byte, error) {
	return util.JSONMarshal(struct {
		encoder.JSONPackHintedHead
		V int
	}{
		JSONPackHintedHead: encoder.NewJSONPackHintedHead(dv.Hint()),
		V:                  dv.v,
	})
}

func (dv *dummy) UnpackJSON(b []byte, enc *encoder.JSONEncoder) error {
	var u struct{ V int }
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	dv.v = u.V

	return nil
}

type testStateHintedValue struct {
	suite.Suite
}

func (t *testStateHintedValue) TestNewNotHinted() {
	dv := HintedValue{}
	_, err := dv.Set(dummyNotHinted{v: 1})
	t.Contains(err.Error(), "not hint.Hinter")
}

func (t *testStateHintedValue) TestNewNotByter() {
	v := dummyNotByter{}
	v.v = 1

	_, err := NewHintedValue(v)
	t.Contains(err.Error(), "not util.Byter")
}

func (t *testStateHintedValue) TestNewNotHasher() {
	v := dummyNotHasher{}
	v.v = 1

	_, err := NewHintedValue(v)
	t.Contains(err.Error(), "not valuehash.Hasher")
}

func (t *testStateHintedValue) TestNew() {
	v := dummy{}
	v.v = 1

	dv, err := NewHintedValue(v)
	t.NoError(err)

	t.Equal(dv.v, v)
	t.True(dv.Hash().Equal(v.Hash()))
	t.Equal(dv.Bytes(), v.Bytes())
}

func TestStateHintedValue(t *testing.T) {
	suite.Run(t, new(testStateHintedValue))
}