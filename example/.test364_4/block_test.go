package main

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type baseTestBlock struct{}

func (t *baseTestBlock) newManifest() base.DummyManifest {
	return base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
}

func (t *baseTestBlock) newItem(ty base.BlockItemType) BaseBlockMapItem {
	return newBaseBlockMapItem(ty, util.UUID().String(), true)
}

func (t *baseTestBlock) newBaseItems() map[base.BlockItemType]BlockMapItem {
	return map[base.BlockItemType]BlockMapItem{
		base.BlockItemProposal:       t.newItem(base.BlockItemProposal),
		base.BlockItemVoteproofs:     t.newItem(base.BlockItemVoteproofs),
		base.BlockItemOperationsTree: t.newItem(base.BlockItemOperationsTree),
		base.BlockItemStatesTree:     t.newItem(base.BlockItemStatesTree),
	}
}

func (t *baseTestBlock) newBlockFileChunk() blockFileChunk {
	return newBaseBlockFileChunk(util.UUID().String(), util.UUID().String(), "gz")
}

type testBlockMap struct {
	suite.Suite
	baseTestBlock
	local     base.Address
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testBlockMap) SetupSuite() {
	t.local = base.RandomAddress("")
	t.priv = base.NewMPrivatekey()
	t.networkID = util.UUID().Bytes()
}

func (t *testBlockMap) TestVerifyChecksum() {
	t.Run("empty", func() {
		checksum := baseBlockMapItemChecksum(nil)
		t.Empty(checksum)
	})

	t.Run("one", func() {
		input := util.UUID().String()
		checksum := baseBlockMapItemChecksum([]string{input})
		t.Equal(input, checksum)
	})

	t.Run("multiple", func() {
		inputs := make([]string, 33)
		for i := range inputs {
			inputs[i] = util.UUID().String()
		}

		checksum := baseBlockMapItemChecksum(inputs)
		t.NotEmpty(checksum)
	})
}

func (t *testBlockMap) TestBaseBlockMapIsValid() {
	t.Run("ok", func() {
		manifest := t.newManifest()
		items := t.newBaseItems()

		bm := newBaseBlockMap(manifest, items, time.Second)
		t.NoError(bm.Sign(t.local, t.priv, t.networkID))

		t.NoError(bm.IsValid(t.networkID))
	})

	t.Run("invalid manifest", func() {
		manifest := t.newManifest()
		manifest.Invalidf = func([]byte) error {
			return errors.Errorf("showme")
		}
		items := t.newBaseItems()

		bm := newBaseBlockMap(manifest, items, time.Second)
		t.NoError(bm.Sign(t.local, t.priv, t.networkID))

		err := bm.IsValid(t.networkID)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "showme")
	})

	t.Run("insufficient items", func() {
		manifest := t.newManifest()
		items := t.newBaseItems()
		delete(items, base.BlockItemProposal)

		bm := newBaseBlockMap(manifest, items, time.Second)
		t.NoError(bm.Sign(t.local, t.priv, t.networkID))

		err := bm.IsValid(t.networkID)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "empty proposal")
	})

	t.Run("not signed", func() {
		manifest := t.newManifest()
		items := t.newBaseItems()

		bm := newBaseBlockMap(manifest, items, time.Second)

		err := bm.IsValid(t.networkID)
		t.ErrorIs(err, util.ErrInvalid)
	})
}

func (t *testBlockMap) TestBaseBlockMapEncode() {
	tt := new(encoder.BaseTestEncode)
	enc := jsonenc.NewEncoder()

	manifest := t.newManifest()
	items := t.newBaseItems()

	bm := newBaseBlockMap(manifest, items, time.Second)
	t.NoError(bm.Sign(t.local, t.priv, t.networkID))

	tt.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: base.DummyManifestHint, Instance: base.DummyManifest{}}))
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: BlockMapHint, Instance: BaseBlockMap{}}))

		b, err := enc.Marshal(bm)
		tt.NoError(err)

		t.T().Log("marshaled:", string(b))

		return bm, b
	}
	tt.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		tt.NoError(err)

		_, ok := i.(BaseBlockMap)
		tt.True(ok)

		return i
	}
	tt.Compare = func(a, b interface{}) {
		af, ok := a.(BaseBlockMap)
		tt.True(ok)
		bf, ok := b.(BaseBlockMap)
		tt.True(ok)

		tt.NoError(bf.IsValid(t.networkID))

		EqualBlockMap(tt.Assert(), af, bf)
	}

	suite.Run(t.T(), tt)
}

func TestBlockMap(t *testing.T) {
	suite.Run(t, new(testBlockMap))
}

func EqualBlockMap(t *assert.Assertions, a, b BlockMap) {
	aht := a.(hint.Hinter).Hint()
	bht := b.(hint.Hinter).Hint()
	t.True(aht.Equal(bht), "Hint does not match")

	base.EqualManifest(t, a.Manifest(), b.Manifest())

	a.Items(func(ai BlockMapItem) bool {
		bi, found := b.Item(ai.Type())
		t.True(found, ai.Type())

		EqualBlockMapItem(t, ai, bi)

		return true
	})
}

func EqualBlockMapItem(t *assert.Assertions, a, b BlockMapItem) {
	t.Equal(a.Type(), b.Type())
	t.Equal(a.Checksum(), b.Checksum())
	t.Equal(a.IsChunked(), b.IsChunked())
}
