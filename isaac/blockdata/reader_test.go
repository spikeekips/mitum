package isaacblockdata

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

type testBlockdataReaders struct {
	suite.Suite
}

func (t *testBlockdataReaders) SetupTest() {
}

func (t *testBlockdataReaders) TestNew() {
	readers := NewBlockdataReaders()

	t.Run("unknown", func() {
		f := readers.Find(hint.MustNewHint("ab-v0.0.1"))
		t.Nil(f)
	})

	t.Run("known", func() {
		ht := hint.MustNewHint("abc-v0.0.1")

		t.NoError(readers.Add(ht, func(base.Height, encoder.Encoder) (isaac.BlockdataReader, error) { return nil, nil }))

		f := readers.Find(ht)
		t.NotNil(f)
	})

	t.Run("compatible", func() {
		ht := hint.MustNewHint("abc-v0.0.9")

		f := readers.Find(ht)
		t.NotNil(f)
	})

	t.Run("not compatible", func() {
		ht := hint.MustNewHint("abc-v1.0.1")

		f := readers.Find(ht)
		t.Nil(f)
	})
}

func (t *testBlockdataReaders) TestLoadReader() {
	encs := encoder.NewEncoders()
	enc := jsonenc.NewEncoder()
	t.NoError(encs.AddHinter(enc))

	readers := NewBlockdataReaders()

	writerhint := hint.MustNewHint("writer-v0.0.1")
	t.NoError(readers.Add(writerhint, func(base.Height, encoder.Encoder) (isaac.BlockdataReader, error) { return nil, errors.Errorf("findme") }))

	t.Run("known", func() {
		_, err := LoadBlockdataReader(readers, encs, writerhint, enc.Hint(), base.Height(66))
		t.Error(err)
		t.ErrorContains(err, "findme")
	})

	t.Run("unknown writer", func() {
		_, err := LoadBlockdataReader(readers, encs, hint.MustNewHint("hehe-v0.0.1"), enc.Hint(), base.Height(66))
		t.Error(err)
		t.ErrorContains(err, "unknown writer hint")
	})

	t.Run("unknown encodeer", func() {
		_, err := LoadBlockdataReader(readers, encs, writerhint, hint.MustNewHint("hehe-v0.0.1"), base.Height(66))
		t.Error(err)
		t.ErrorContains(err, "unknown encoder hint")
	})
}

func TestBlockdataReaders(t *testing.T) {
	suite.Run(t, new(testBlockdataReaders))
}
