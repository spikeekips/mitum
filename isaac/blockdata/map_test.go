package isaacblockdata

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
	return NewLocalBlockDataMapItem(ty, util.UUID().String(), 1)
}

func (t *testBlockDataMap) newmap() BlockDataMap {
	m := NewBlockDataMap(LocalFSWriterHint, jsonenc.JSONEncoderHint)

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

	t.True(LocalFSWriterHint.Equal(m.writer))
	t.True(jsonenc.JSONEncoderHint.Equal(m.encoder))
}

func (t *testBlockDataMap) TestInvalid() {
	m := t.newmap()
	t.NoError(m.IsValid(t.networkID))

	t.Run("invalid hinter", func() {
		m := t.newmap()
		m.BaseHinter = hint.NewBaseHinter(base.StringAddressHint)
		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "type does not match")
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
		t.ErrorContains(err, "kikiki")
	})

	t.Run("proposal not set", func() {
		m := t.newmap()
		m.m.RemoveValue(base.BlockDataTypeProposal)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty proposal")
	})

	t.Run("empty proposal", func() {
		m := t.newmap()
		m.m.SetValue(base.BlockDataTypeProposal, nil)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty proposal")
	})

	t.Run("voteproofs not set", func() {
		m := t.newmap()
		m.m.RemoveValue(base.BlockDataTypeVoteproofs)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty voteproofs")
	})

	t.Run("empty voteproofs", func() {
		m := t.newmap()
		m.m.SetValue(base.BlockDataTypeVoteproofs, nil)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty voteproofs")
	})

	t.Run("invalid item", func() {
		m := t.newmap()
		m.m.SetValue(base.BlockDataTypeVoteproofs, t.newitem(base.BlockDataType("hehe")))

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid item found")
		t.ErrorContains(err, "hehe")
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

		base.EqualBlockDataMapItem(t.Assert(), newitem, ritem)
	})

	t.Run("unknown data type", func() {
		newitem := t.newitem(base.BlockDataType("findme"))
		err := m.SetItem(newitem)

		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "unknown block data type")
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

		base.EqualBlockDataMap(tt.Assert(), af, bf)
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

	item := NewBlockDataMapItem(base.BlockDataTypeProposal, *u, checksum, 1)
	_ = (interface{})(item).(base.BlockDataMapItem)

	t.NoError(item.IsValid(nil))

	t.Equal(base.BlockDataTypeProposal, item.Type())
	t.Equal(u.String(), item.URL().String())
	t.Equal(checksum, item.Checksum())
}

func (t *testBlockDataMapItem) TestLocal() {
	ty := base.BlockDataTypeProposal
	m0 := NewLocalBlockDataMapItem(ty, util.UUID().String(), 1)
	m1 := NewLocalBlockDataMapItem(ty, util.UUID().String(), 1)

	t.T().Log("fileBlockDataURL:", fileBlockDataURL.String())
	t.T().Log("m0.url:", m0.URL())
	t.T().Log("m1.url:", m1.URL())
}

func (t *testBlockDataMapItem) TestInvalid() {
	t.Run("invalid data type", func() {
		u, _ := url.Parse("file://showme")
		item := NewBlockDataMapItem(base.BlockDataType("findme"), *u, util.UUID().String(), 1)

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "unknown block data type")
	})

	t.Run("empty checksum", func() {
		u, _ := url.Parse("file://showme")
		item := NewBlockDataMapItem(base.BlockDataTypeProposal, *u, "", 1)

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty checksum")
	})

	t.Run("empty url", func() {
		item := NewBlockDataMapItem(base.BlockDataTypeProposal, url.URL{}, util.UUID().String(), 1)

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty url")
	})

	t.Run("empty url scheme", func() {
		u, _ := url.Parse("showme")
		item := NewBlockDataMapItem(base.BlockDataTypeProposal, *u, util.UUID().String(), 1)

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty url")
	})

	t.Run("unsupported url scheme", func() {
		u, _ := url.Parse("showme://findme")
		item := NewBlockDataMapItem(base.BlockDataTypeProposal, *u, util.UUID().String(), 1)

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "unsupported url scheme found")
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
}

func TestBlockDataMapItemEncode(tt *testing.T) {
	t := new(testBlockDataMapItemEncode)

	t.Encode = func() (interface{}, []byte) {
		u, _ := url.Parse("file://showme")
		item := NewBlockDataMapItem(base.BlockDataTypeProposal, *u, util.UUID().String(), 1)

		b, err := t.enc.Marshal(item)
		t.NoError(err)

		return item, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BlockDataMapItem
		t.NoError(t.enc.Unmarshal(b, &u))

		return u
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(BlockDataMapItem)
		t.True(ok)
		bf, ok := b.(BlockDataMapItem)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		base.EqualBlockDataMapItem(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}
