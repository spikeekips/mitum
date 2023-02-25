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

type testExtensibleJSON struct {
	suite.Suite
}

func (t *testExtensibleJSON) TestUnmarshal() {
	b := `{"A":"A","B":"B", "C": "C"}`

	t.T().Log("marshaled:", b)

	var ud dummyJSONMarshaled

	t.NoError(UnmarshalJSON([]byte(b), &ud))

	t.Run("MarshaledJSON", func() {
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

func (t *testExtensibleJSON) TestMarshal() {
	d := dummyJSONMarshaled{insideDummyJSONMarshaled: insideDummyJSONMarshaled{A: "A", B: "B"}}
	b, err := MarshalJSON(d)
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

		t.NoError(UnmarshalJSON(b, &m))

		m["C"] = "C"

		i, err := MarshalJSON(m)
		t.NoError(err)
		mb = i
	}

	var ud dummyJSONMarshaled

	t.NoError(UnmarshalJSON(mb, &ud))

	t.Run("MarshaledJSON", func() {
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

func (t *testExtensibleJSON) TestPointer() {
	d := dummyJSONMarshaled{insideDummyJSONMarshaled: insideDummyJSONMarshaled{A: "A", B: "B"}}
	b, err := MarshalJSON(&d) // NOTE set pointer
	t.NoError(err)

	t.T().Log("marshaled:", string(b))

	t.Run("check Marshaled", func() {
		mb, ok := d.MarshaledJSON()
		t.True(ok)
		t.NotNil(mb)
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

	t.Run("MarshaledJSON", func() {
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

type fieldDummyJSONMarshaled struct {
	D dummyJSONMarshaled
	E string
}

type fieldDummyJSONMarshaledUnmarshaler struct {
	D json.RawMessage
	E string
}

func (d *fieldDummyJSONMarshaled) UnmarshalJSON(b []byte) error {
	var u fieldDummyJSONMarshaledUnmarshaler
	if err := UnmarshalJSON(b, &u); err != nil {
		return err
	}

	d.E = u.E

	if err := UnmarshalJSON(u.D, &d.D); err != nil {
		return err
	}

	return nil
}

func (t *testExtensibleJSON) TestFieldUnmarshal() {
	b := `{"D":{"B":"B","C":"C","A":"A"},"E":"E"}`

	t.T().Log("marshaled:", b)

	var ud fieldDummyJSONMarshaled

	t.NoError(UnmarshalJSON([]byte(b), &ud))

	t.Run("check MarshaledJSON", func() {
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

func (t *testExtensibleJSON) TestField() {
	d := fieldDummyJSONMarshaled{
		D: dummyJSONMarshaled{insideDummyJSONMarshaled: insideDummyJSONMarshaled{A: "A", B: "B"}},
		E: "E",
	}

	b, err := MarshalJSON(d)
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

		t.NoError(UnmarshalJSON(b, &m))

		md := m["D"].(map[string]interface{})
		md["C"] = "C"

		i, err := MarshalJSON(m)
		t.NoError(err)
		mb = i

		t.T().Log("modified:", string(mb))
	}

	var ud fieldDummyJSONMarshaled

	t.NoError(UnmarshalJSON(mb, &ud))

	t.Run("check MarshaledJSON", func() {
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

func (t *testExtensibleJSON) TestEmbededUnmarshal() {
	b := `{"A":"A","B":"B","E":"E","C":"C"}`

	t.T().Log("marshaled:", b)

	var ud embedDummyJSONMarshaled

	t.NoError(UnmarshalJSON([]byte(b), &ud))

	t.Run("check MarshaledJSON", func() {
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

func (t *testExtensibleJSON) TestEmbeded() {
	d := embedDummyJSONMarshaled{
		dummyJSONMarshaled: dummyJSONMarshaled{insideDummyJSONMarshaled: insideDummyJSONMarshaled{A: "A", B: "B"}},
		E:                  "E",
	}

	b, err := MarshalJSON(d)
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

		t.NoError(UnmarshalJSON(b, &m))

		m["C"] = "C"

		i, err := MarshalJSON(m)
		t.NoError(err)
		mb = i

		t.T().Log("modified:", string(mb))
	}

	var ud embedDummyJSONMarshaled

	t.NoError(UnmarshalJSON(mb, &ud))

	t.Run("check MarshaledJSON", func() {
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

func TestExtensibleJSON(t *testing.T) {
	suite.Run(t, new(testExtensibleJSON))
}
