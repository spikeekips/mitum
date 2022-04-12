package isaac

import (
	"net/url"
	"testing"

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

func EqualBlockDataMap(t *assert.Assertions, a, b base.BlockDataMap) {
	t.True(a.Hint().Equal(b.Hint()))
	base.EqualManifest(t, a.Manifest(), b.Manifest())

	am := a.All()
	bm := b.All()

	t.Equal(len(am), len(bm))

	for k := range am {
		ai := am[k]
		bi := bm[k]

		EqualBlockDataMapItem(t, ai, bi)
	}
}

func EqualBlockDataMapItem(t *assert.Assertions, a, b base.BlockDataMapItem) {
	t.True(a.Hint().Equal(b.Hint()))
	t.Equal(a.Type(), b.Type())
	t.Equal(a.URL().String(), b.URL().String())
	t.Equal(a.Checksum(), b.Checksum())
}

type testBlockDataMap struct {
	suite.Suite
	local     base.Address
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testBlockDataMap) SetupSuite() {
	t.local = base.RandomAddress("")
	t.priv = base.NewMPrivatekey()
	t.networkID = util.UUID().Bytes()
}

func (t *testBlockDataMap) newitem(ty base.BlockDataType) BlockDataMapItem {
	return NewLocalBlockDataMapItem(ty, util.UUID().String(), util.UUID().String())
}

func (t *testBlockDataMap) newmap() BlockDataMap {
	m := NewBlockDataMap(LocalBlockDataFSWriterHint, jsonenc.JSONEncoderHint)

	for _, i := range []base.BlockDataType{
		base.BlockDataTypeProposal,
		base.BlockDataTypeOperations,
		base.BlockDataTypeOperationsTree,
		base.BlockDataTypeStates,
		base.BlockDataTypeStatesTree,
		base.BlockDataTypeVoteproofs,
	} {
		t.NoError(m.SetItem(t.newitem(i)))
	}

	manifest := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
	m.SetManifest(manifest)
	t.NoError(m.Sign(t.local, t.priv, t.networkID))

	return m
}

func (t *testBlockDataMap) TestNew() {
	m := t.newmap()
	_ = (interface{})(m).(base.BlockDataMap)

	t.NoError(m.IsValid(t.networkID))

	t.NotNil(m.Manifest())

	all := m.All()
	for k := range all {
		t.NotNil(all[k])
		t.Equal(k, all[k].Type())
	}

	t.True(LocalBlockDataFSWriterHint.Equal(m.writer))
	t.True(jsonenc.JSONEncoderHint.Equal(m.encoder))
}

func (t *testBlockDataMap) TestInvalid() {
	m := t.newmap()
	t.NoError(m.IsValid(t.networkID))

	t.Run("invalid hinter", func() {
		m := t.newmap()
		m.BaseHinter = hint.NewBaseHinter(BlockDataMapItemHint)
		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "type does not match")
	})

	t.Run("invalid manifest", func() {
		m := t.newmap()

		manifest := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
		manifest.Invalidf = func([]byte) error {
			return util.InvalidError.Errorf("kikiki")
		}

		m.manifest = manifest

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "kikiki")
	})

	t.Run("proposal not set", func() {
		m := t.newmap()
		delete(m.m, base.BlockDataTypeProposal)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "empty proposal")
	})

	t.Run("empty proposal", func() {
		m := t.newmap()
		m.m[base.BlockDataTypeProposal] = nil

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "empty proposal")
	})

	t.Run("voteproofs not set", func() {
		m := t.newmap()
		delete(m.m, base.BlockDataTypeVoteproofs)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "empty voteproofs")
	})

	t.Run("empty voteproofs", func() {
		m := t.newmap()
		m.m[base.BlockDataTypeVoteproofs] = nil

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "empty voteproofs")
	})

	t.Run("invalid item", func() {
		m := t.newmap()
		m.m[base.BlockDataTypeVoteproofs] = t.newitem(base.BlockDataType("hehe"))

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "invalid item found")
		t.Contains(err.Error(), "hehe")
	})

	t.Run("invalid signature", func() {
		m := t.newmap()

		err := m.IsValid(util.UUID().Bytes())
		t.True(errors.Is(err, util.InvalidError))
		t.True(errors.Is(err, base.SignatureVerificationError))
	})
}

func (t *testBlockDataMap) TestSetItem() {
	m := t.newmap()

	t.Run("override", func() {
		olditem, found := m.Item(base.BlockDataTypeProposal)
		t.True(found)
		t.NotNil(olditem)

		newitem := t.newitem(base.BlockDataTypeProposal)
		t.NoError(m.SetItem(newitem))

		t.NotEqual(olditem.Checksum(), newitem.Checksum())

		ritem, found := m.Item(base.BlockDataTypeProposal)
		t.True(found)
		t.NotNil(ritem)

		EqualBlockDataMapItem(t.Assert(), newitem, ritem)
	})

	t.Run("unknown data type", func() {
		newitem := t.newitem(base.BlockDataType("findme"))
		err := m.SetItem(newitem)

		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "unknown block data type")
	})
}

type testBlockDataMapEncode struct {
	encoder.BaseTestEncode
	enc *jsonenc.Encoder
}

func (t *testBlockDataMap) TestEncode() {
	tt := new(testBlockDataMapEncode)

	tt.Encode = func() (interface{}, []byte) {
		tt.enc = jsonenc.NewEncoder()
		t.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		tt.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: base.DummyManifestHint, Instance: base.DummyManifest{}}))
		tt.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: BlockDataMapItemHint, Instance: BlockDataMapItem{}}))
		tt.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: BlockDataMapHint, Instance: BlockDataMap{}}))

		m := t.newmap()

		b, err := tt.enc.Marshal(m)
		tt.NoError(err)

		return m, b
	}
	tt.Decode = func(b []byte) interface{} {
		i, err := tt.enc.Decode(b)
		tt.NoError(err)

		_, ok := i.(BlockDataMap)
		tt.True(ok)

		return i
	}
	tt.Compare = func(a, b interface{}) {
		af, ok := a.(BlockDataMap)
		tt.True(ok)
		bf, ok := b.(BlockDataMap)
		tt.True(ok)

		tt.NoError(bf.IsValid(t.networkID))

		EqualBlockDataMap(tt.Assert(), af, bf)
	}

	suite.Run(t.T(), tt)
}

func TestBlockDataMap(t *testing.T) {
	suite.Run(t, new(testBlockDataMap))
}

type testBlockDataMapItem struct {
	suite.Suite
}

func (t *testBlockDataMapItem) TestNew() {
	u, err := url.Parse("file://showme")
	t.NoError(err)
	checksum := util.UUID().String()

	item := NewBlockDataMapItem(base.BlockDataTypeProposal, *u, checksum)
	_ = (interface{})(item).(base.BlockDataMapItem)

	t.NoError(item.IsValid(nil))

	t.True(BlockDataMapItemHint.Equal(item.Hint()))
	t.Equal(base.BlockDataTypeProposal, item.Type())
	t.Equal(u.String(), item.URL().String())
	t.Equal(checksum, item.Checksum())
}

func (t *testBlockDataMapItem) TestLocal() {
	ty := base.BlockDataTypeProposal
	m0 := NewLocalBlockDataMapItem(ty, util.UUID().String(), util.UUID().String())
	m1 := NewLocalBlockDataMapItem(ty, util.UUID().String(), util.UUID().String())

	t.T().Log("fileBlockDataURL:", fileBlockDataURL.String())
	t.T().Log("m0.url:", m0.URL())
	t.T().Log("m1.url:", m1.URL())
}

func (t *testBlockDataMapItem) TestInvalid() {
	t.Run("invalid hint", func() {
		u, _ := url.Parse("file://showme")
		item := NewBlockDataMapItem(base.BlockDataTypeProposal, *u, util.UUID().String())

		item.BaseHinter = hint.NewBaseHinter(BlockDataMapHint)
		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "type does not match")
	})

	t.Run("invalid data type", func() {
		u, _ := url.Parse("file://showme")
		item := NewBlockDataMapItem(base.BlockDataType("findme"), *u, util.UUID().String())

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "unknown block data type")
	})

	t.Run("empty checksum", func() {
		u, _ := url.Parse("file://showme")
		item := NewBlockDataMapItem(base.BlockDataTypeProposal, *u, "")

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "empty checksum")
	})

	t.Run("empty url", func() {
		item := NewBlockDataMapItem(base.BlockDataTypeProposal, url.URL{}, util.UUID().String())

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "empty url")
	})

	t.Run("empty url scheme", func() {
		u, _ := url.Parse("showme")
		item := NewBlockDataMapItem(base.BlockDataTypeProposal, *u, util.UUID().String())

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "empty url")
	})

	t.Run("unsupported url scheme", func() {
		u, _ := url.Parse("showme://findme")
		item := NewBlockDataMapItem(base.BlockDataTypeProposal, *u, util.UUID().String())

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "unsupported url scheme found")
	})
}

func TestBlockDataMapItem(t *testing.T) {
	suite.Run(t, new(testBlockDataMapItem))
}

type testBlockDataMapItemEncode struct {
	encoder.BaseTestEncode
	enc *jsonenc.Encoder
}

func (t *testBlockDataMapItemEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: BlockDataMapItemHint, Instance: BlockDataMapItem{}}))
}

func TestBlockDataMapItemEncode(tt *testing.T) {
	t := new(testBlockDataMapItemEncode)

	t.Encode = func() (interface{}, []byte) {
		u, _ := url.Parse("file://showme")
		item := NewBlockDataMapItem(base.BlockDataTypeProposal, *u, util.UUID().String())

		b, err := t.enc.Marshal(item)
		t.NoError(err)

		return item, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(BlockDataMapItem)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(BlockDataMapItem)
		t.True(ok)
		bf, ok := b.(BlockDataMapItem)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		EqualBlockDataMapItem(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}
