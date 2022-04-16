package isaac

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

func TestManifestEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		m := NewManifest(
			base.Height(33),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			localtime.Now(),
		)

		b, err := enc.Marshal(m)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return m, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ManifestHint, Instance: Manifest{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(Manifest)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		am, ok := a.(Manifest)
		t.True(ok)
		bm, ok := b.(Manifest)
		t.True(ok)

		t.NoError(bm.IsValid(nil))

		base.EqualManifest(t.Assert(), am, bm)
	}

	suite.Run(tt, t)
}
