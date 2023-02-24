package util

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"
)

type insideDummyJSONMarshaled struct {
	A string
	B string
}

type dummyJSONMarshaled struct {
	DefaultExtensibleJSON
	insideDummyJSONMarshaled
}

func (d dummyJSONMarshaled) MarshalJSON() ([]byte, error) {
	if b, ok := d.MarshaledJSON(); ok {
		return b, nil
	}

	return MarshalJSON(d.insideDummyJSONMarshaled)
}

type dummyDummyJSONMarshaled struct {
	D dummyJSONMarshaled
	E string
}

type dummyDummyJSONMarshaledUnmarshaler struct {
	D json.RawMessage
	E string
}

func (d *dummyDummyJSONMarshaled) UnmarshalJSON(b []byte) error {
	var u dummyDummyJSONMarshaledUnmarshaler
	if err := UnmarshalJSON(b, &u); err != nil {
		return err
	}

	d.E = u.E

	if err := UnmarshalJSON(u.D, &d.D); err != nil {
		return err
	}

	return nil
}

type embedDummyJSONMarshaled struct {
	dummyJSONMarshaled
	E string
}

func (d embedDummyJSONMarshaled) MarshalJSON() ([]byte, error) {
	b, err := d.dummyJSONMarshaled.MarshalJSON()
	if err != nil {
		return nil, err
	}

	var m map[string]interface{}
	if err := UnmarshalJSON(b, &m); err != nil {
		return nil, err
	}

	m["E"] = d.E

	return MarshalJSON(m)
}

type embedDummyJSONMarshaledMarshaler struct {
	E string
}

func (d *embedDummyJSONMarshaled) UnmarshalJSON(b []byte) error {
	if err := UnmarshalJSON(b, &d.dummyJSONMarshaled); err != nil {
		return err
	}

	var u embedDummyJSONMarshaledMarshaler
	if err := UnmarshalJSON(b, &u); err != nil {
		return err
	}

	d.E = u.E

	return nil
}

type testMarshaled struct {
	suite.Suite
}

// FIXME test marshal pointer

func (t *testMarshaled) TestMarshal() {
	d := dummyJSONMarshaled{insideDummyJSONMarshaled: insideDummyJSONMarshaled{A: "A", B: "B"}}
	b, err := MarshalJSON(d)
	t.NoError(err)

	t.T().Log("marshaled:", string(b))

	t.Run("check Marshaled()", func() {
		mb, ok := d.MarshaledJSON()
		t.False(ok)
		t.Nil(mb)
	})

	// modify json
	var mb []byte
	{
		var m map[string]interface{}

		t.NoError(UnmarshalJSON(b, &m))

		m["C"] = "C"

		i, err := MarshalJSON(m)
		t.NoError(err)
		mb = i
	}

	var ud dummyJSONMarshaled

	t.NoError(UnmarshalJSON(mb, &ud))

	t.Run("check C", func() {
		i, ok := ud.MarshaledJSON()
		t.True(ok)
		t.NotNil(i)
	})

	t.Run("marshal again", func() {
		i, err := MarshalJSON(ud)
		t.NoError(err)

		t.T().Log("marshaled:", string(i))

		var m map[string]interface{}

		t.NoError(UnmarshalJSON(i, &m))

		v, found := m["C"]
		t.True(found)
		t.Equal("C", v)
	})
}

func (t *testMarshaled) TestField() {
	d := dummyDummyJSONMarshaled{
		D: dummyJSONMarshaled{insideDummyJSONMarshaled: insideDummyJSONMarshaled{A: "A", B: "B"}},
		E: "E",
	}

	b, err := MarshalJSON(d)
	t.NoError(err)

	t.T().Log("marshaled:", string(b))

	t.Run("check Marshaled()", func() {
		mb, ok := d.D.MarshaledJSON()
		t.False(ok)
		t.Nil(mb)
	})

	// modify json
	var mb []byte
	{
		var m map[string]interface{}

		t.NoError(UnmarshalJSON(b, &m))

		md := m["D"].(map[string]interface{})
		md["C"] = "C"

		i, err := MarshalJSON(m)
		t.NoError(err)
		mb = i

		t.T().Log("modified:", string(mb))
	}

	var ud dummyDummyJSONMarshaled

	t.NoError(UnmarshalJSON(mb, &ud))

	t.Run("check C", func() {
		i, ok := ud.D.MarshaledJSON()
		t.True(ok)
		t.NotNil(i)
	})

	t.Run("marshal again", func() {
		i, err := MarshalJSON(ud)
		t.NoError(err)

		t.T().Log("marshaled:", string(i))

		var m map[string]interface{}

		t.NoError(UnmarshalJSON(i, &m))

		md := m["D"].(map[string]interface{})
		v, found := md["C"]
		t.True(found)
		t.Equal("C", v)
	})
}

func (t *testMarshaled) TestEmbeded() {
	d := embedDummyJSONMarshaled{
		dummyJSONMarshaled: dummyJSONMarshaled{insideDummyJSONMarshaled: insideDummyJSONMarshaled{A: "A", B: "B"}},
		E:                  "E",
	}

	b, err := MarshalJSON(d)
	t.NoError(err)

	t.T().Log("marshaled:", string(b))

	t.Run("check Marshaled()", func() {
		mb, ok := d.MarshaledJSON()
		t.False(ok)
		t.Nil(mb)
	})

	// modify json
	var mb []byte
	{
		var m map[string]interface{}

		t.NoError(UnmarshalJSON(b, &m))

		m["C"] = "C"

		i, err := MarshalJSON(m)
		t.NoError(err)
		mb = i

		t.T().Log("modified:", string(mb))
	}

	var ud embedDummyJSONMarshaled

	t.NoError(UnmarshalJSON(mb, &ud))

	t.Run("check C", func() {
		i, ok := ud.MarshaledJSON()
		t.True(ok)
		t.NotNil(i)
	})

	t.Run("marshal again", func() {
		i, err := MarshalJSON(ud)
		t.NoError(err)

		t.T().Log("marshaled:", string(i))

		var m map[string]interface{}

		t.NoError(UnmarshalJSON(i, &m))

		v, found := m["C"]
		t.True(found)
		t.Equal("C", v)
	})
}

func TestMarshaled(t *testing.T) {
	suite.Run(t, new(testMarshaled))
}
