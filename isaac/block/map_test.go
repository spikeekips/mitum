package isaacblock

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
	"github.com/stretchr/testify/suite"
)

type testBlockMap struct {
	suite.Suite
	local     base.Address
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testBlockMap) SetupSuite() {
	t.local = base.RandomAddress("")
	t.priv = base.NewMPrivatekey()
	t.networkID = util.UUID().Bytes()
}

func (t *testBlockMap) newitem(ty base.BlockMapItemType) BlockMapItem {
	return NewLocalBlockMapItem(ty, util.UUID().String())
}

func (t *testBlockMap) newmap() BlockMap {
	m := NewBlockMap(LocalFSWriterHint, jsonenc.JSONEncoderHint)

	for _, i := range []base.BlockMapItemType{
		base.BlockMapItemTypeProposal,
		base.BlockMapItemTypeOperations,
		base.BlockMapItemTypeOperationsTree,
		base.BlockMapItemTypeStates,
		base.BlockMapItemTypeStatesTree,
		base.BlockMapItemTypeVoteproofs,
	} {
		t.NoError(m.SetItem(t.newitem(i)))
	}

	manifest := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
	m.SetManifest(manifest)
	t.NoError(m.Sign(t.local, t.priv, t.networkID))

	return m
}

func (t *testBlockMap) TestNew() {
	m := t.newmap()
	_ = (interface{})(m).(base.BlockMap)

	t.NoError(m.IsValid(t.networkID))

	t.NotNil(m.Manifest())

	t.True(LocalFSWriterHint.Equal(m.writer))
	t.True(jsonenc.JSONEncoderHint.Equal(m.encoder))
}

func (t *testBlockMap) TestInvalid() {
	m := t.newmap()
	t.NoError(m.IsValid(t.networkID))

	t.Run("invalid hinter", func() {
		m := t.newmap()
		m.BaseHinter = hint.NewBaseHinter(base.StringAddressHint)
		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "type does not match")
	})

	t.Run("invalid manifest", func() {
		m := t.newmap()

		manifest := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
		manifest.Invalidf = func([]byte) error {
			return util.ErrInvalid.Errorf("kikiki")
		}

		m.manifest = manifest

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "kikiki")
	})

	t.Run("proposal not set", func() {
		m := t.newmap()
		m.items.RemoveValue(base.BlockMapItemTypeProposal)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty proposal")
	})

	t.Run("empty proposal", func() {
		m := t.newmap()
		m.items.SetValue(base.BlockMapItemTypeProposal, nil)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty proposal")
	})

	t.Run("voteproofs not set", func() {
		m := t.newmap()
		m.items.RemoveValue(base.BlockMapItemTypeVoteproofs)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty voteproofs")
	})

	t.Run("empty voteproofs", func() {
		m := t.newmap()
		m.items.SetValue(base.BlockMapItemTypeVoteproofs, nil)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty voteproofs")
	})

	t.Run("invalid item", func() {
		m := t.newmap()
		m.items.SetValue(base.BlockMapItemTypeVoteproofs, t.newitem(base.BlockMapItemType("hehe")))

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid item found")
		t.ErrorContains(err, "hehe")
	})

	t.Run("invalid signature", func() {
		m := t.newmap()

		err := m.IsValid(util.UUID().Bytes())
		t.True(errors.Is(err, util.ErrInvalid))
		t.True(errors.Is(err, base.ErrSignatureVerification))
	})
}

func (t *testBlockMap) TestSetItem() {
	m := t.newmap()

	t.Run("override", func() {
		olditem, found := m.Item(base.BlockMapItemTypeProposal)
		t.True(found)
		t.NotNil(olditem)

		newitem := t.newitem(base.BlockMapItemTypeProposal)
		t.NoError(m.SetItem(newitem))

		t.NotEqual(olditem.Checksum(), newitem.Checksum())

		ritem, found := m.Item(base.BlockMapItemTypeProposal)
		t.True(found)
		t.NotNil(ritem)

		base.EqualBlockMapItem(t.Assert(), newitem, ritem)
	})

	t.Run("unknown data type", func() {
		newitem := t.newitem(base.BlockMapItemType("findme"))
		err := m.SetItem(newitem)

		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "unknown block map item type")
	})
}

func (t *testBlockMap) TestVerify() {
	t.Run("basic", func() {
		m := t.newmap()
		t.NoError(m.IsValid(t.networkID))
	})

	t.Run("update item with same checksum", func() {
		m := t.newmap()
		t.NoError(m.IsValid(t.networkID))

		olditem, found := m.Item(base.BlockMapItemTypeProposal)
		t.True(found)
		t.NotNil(olditem)

		u := url.URL{Scheme: "https", Host: util.UUID().String(), Path: util.UUID().String()}
		newitem := NewBlockMapItem(olditem.Type(), u, olditem.Checksum())
		t.NoError(m.SetItem(newitem))

		t.NoError(m.IsValid(t.networkID))
	})
}

type testBlockMapEncode struct {
	encoder.BaseTestEncode
	enc *jsonenc.Encoder
}

func (t *testBlockMap) TestEncode() {
	tt := new(testBlockMapEncode)

	tt.Encode = func() (interface{}, []byte) {
		tt.enc = jsonenc.NewEncoder()
		t.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
		t.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		tt.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: base.DummyManifestHint, Instance: base.DummyManifest{}}))
		tt.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: BlockMapHint, Instance: BlockMap{}}))

		m := t.newmap()

		b, err := tt.enc.Marshal(m)
		tt.NoError(err)

		return m, b
	}
	tt.Decode = func(b []byte) interface{} {
		i, err := tt.enc.Decode(b)
		tt.NoError(err)

		_, ok := i.(BlockMap)
		tt.True(ok)

		return i
	}
	tt.Compare = func(a, b interface{}) {
		af, ok := a.(BlockMap)
		tt.True(ok)
		bf, ok := b.(BlockMap)
		tt.True(ok)

		tt.NoError(bf.IsValid(t.networkID))

		base.EqualBlockMap(tt.Assert(), af, bf)
	}

	suite.Run(t.T(), tt)
}

func TestBlockMap(t *testing.T) {
	suite.Run(t, new(testBlockMap))
}

type testBlockMapItem struct {
	suite.Suite
}

func (t *testBlockMapItem) TestNew() {
	u, err := url.Parse("file://showme")
	t.NoError(err)
	checksum := util.UUID().String()

	item := NewBlockMapItem(base.BlockMapItemTypeProposal, *u, checksum)
	_ = (interface{})(item).(base.BlockMapItem)

	t.NoError(item.IsValid(nil))

	t.Equal(base.BlockMapItemTypeProposal, item.Type())
	t.Equal(u.String(), item.URL().String())
	t.Equal(checksum, item.Checksum())
}

func (t *testBlockMapItem) TestLocal() {
	ty := base.BlockMapItemTypeProposal
	m0 := NewLocalBlockMapItem(ty, util.UUID().String())
	m1 := NewLocalBlockMapItem(ty, util.UUID().String())

	t.T().Log("fileBlockURL:", fileBlockURL.String())
	t.T().Log("m0.url:", m0.URL())
	t.T().Log("m1.url:", m1.URL())
}

func (t *testBlockMapItem) TestInvalid() {
	t.Run("invalid data type", func() {
		u, _ := url.Parse("file://showme")
		item := NewBlockMapItem(base.BlockMapItemType("findme"), *u, util.UUID().String())

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "unknown block map item type")
	})

	t.Run("empty checksum", func() {
		u, _ := url.Parse("file://showme")
		item := NewBlockMapItem(base.BlockMapItemTypeProposal, *u, "")

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty checksum")
	})

	t.Run("empty url", func() {
		item := NewBlockMapItem(base.BlockMapItemTypeProposal, url.URL{}, util.UUID().String())

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty url")
	})

	t.Run("empty url scheme", func() {
		u, _ := url.Parse("showme")
		item := NewBlockMapItem(base.BlockMapItemTypeProposal, *u, util.UUID().String())

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty url")
	})

	t.Run("unsupported url scheme", func() {
		u, _ := url.Parse("showme://findme")
		item := NewBlockMapItem(base.BlockMapItemTypeProposal, *u, util.UUID().String())

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "unsupported url scheme found")
	})
}

func TestBlockMapItem(t *testing.T) {
	suite.Run(t, new(testBlockMapItem))
}

type testBlockMapItemEncode struct {
	encoder.BaseTestEncode
	enc *jsonenc.Encoder
}

func (t *testBlockMapItemEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()
}

func TestBlockMapItemEncode(tt *testing.T) {
	t := new(testBlockMapItemEncode)

	t.Encode = func() (interface{}, []byte) {
		u, _ := url.Parse("file://showme")
		item := NewBlockMapItem(base.BlockMapItemTypeProposal, *u, util.UUID().String())

		b, err := t.enc.Marshal(item)
		t.NoError(err)

		return item, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BlockMapItem
		t.NoError(t.enc.Unmarshal(b, &u))

		return u
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(BlockMapItem)
		t.True(ok)
		bf, ok := b.(BlockMapItem)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		base.EqualBlockMapItem(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}
