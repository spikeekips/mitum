package hash

import (
	"crypto/rand"
	"encoding/json"
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testBytes struct {
	suite.Suite
}

func (t *testBytes) random() []byte {
	b := make([]byte, 100)
	_, _ = rand.Read(b)

	return b
}

func (t *testBytes) TestNew() {
	t.Run("equal", func() {
		i := t.random()

		a := NewBytes(i)
		b := NewBytes(i)
		t.T().Logf("a=%q b=%q", a, b)

		t.True(a.Equal(b))
	})

	t.Run("not equal", func() {
		i := t.random()

		a := NewBytes(i)
		b := NewBytes(i[:3])
		t.T().Logf("a=%q b=%q", a, b)

		t.False(a.Equal(b))
	})
}

func (t *testBytes) TestInvalid() {
	t.Run("empty", func() {
		var h Bytes
		err := h.IsValid(nil)
		t.ErrorIs(err, util.ErrInvalid)
	})
}

func (t *testBytes) TestEncode() {
	h := NewBytes(t.random())

	b, err := json.Marshal(h)
	t.NoError(err)

	t.T().Log("marshaled:", string(b))

	var m Bytes
	t.NoError(json.Unmarshal(b, &m))

	t.T().Log("unmarshaled:", m)

	t.True(h.Equal(m))
	t.Equal(h, m)
	t.Equal(h.String(), m.String())
}

func TestBytes(t *testing.T) {
	suite.Run(t, new(testBytes))
}
