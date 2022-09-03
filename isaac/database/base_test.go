package isaacdatabase

import (
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testRecordMeta struct {
	suite.Suite
}

func (t *testRecordMeta) TestBytes() {
	t.Run("empty", func() {
		b, err := NewRecordMeta(0x00, nil)
		t.NoError(err)

		v, m, _, err := ReadRecordMetaFromBytes(b)
		t.NoError(err)
		t.Equal(byte(0x00), v)
		t.Equal(0, len(m))
	})

	t.Run("1 data", func() {
		m := [][]byte{util.UUID().Bytes()}

		b, err := NewRecordMeta(0x00, m)
		t.NoError(err)

		v, rm, _, err := ReadRecordMetaFromBytes(b)
		t.NoError(err)
		t.Equal(byte(0x00), v)
		t.Equal(len(m), len(rm))
		t.Equal(m, rm)
	})

	t.Run("over 1 data", func() {
		m := [][]byte{util.UUID().Bytes(), util.UUID().Bytes(), util.UUID().Bytes()}

		b, err := NewRecordMeta(0x03, m)
		t.NoError(err)

		v, rm, _, err := ReadRecordMetaFromBytes(b)
		t.NoError(err)
		t.Equal(byte(0x03), v)
		t.Equal(len(m), len(rm))
		t.Equal(m, rm)
	})
}

func TestRecordMeta(t *testing.T) {
	suite.Run(t, new(testRecordMeta))
}
