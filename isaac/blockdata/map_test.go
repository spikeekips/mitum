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

type testBlockdataMap struct {
	suite.Suite
	local     base.Address
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testBlockdataMap) SetupSuite() {
	t.local = base.RandomAddress("")
	t.priv = base.NewMPrivatekey()
	t.networkID = util.UUID().Bytes()
}

func (t *testBlockdataMap) newitem(ty base.BlockdataType) BlockdataMapItem {
	return NewLocalBlockdataMapItem(ty, util.UUID().String(), 1)
}

func (t *testBlockdataMap) newmap() BlockdataMap {
	m := NewBlockdataMap(LocalFSWriterHint, jsonenc.JSONEncoderHint)

	for _, i := range []base.BlockdataType{
		base.BlockdataTypeProposal,
		base.BlockdataTypeOperations,
		base.BlockdataTypeOperationsTree,
		base.BlockdataTypeStates,
		base.BlockdataTypeStatesTree,
		base.BlockdataTypeVoteproofs,
	} {
		t.NoError(m.SetItem(t.newitem(i)))
	}

	manifest := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
	m.SetManifest(manifest)
	t.NoError(m.Sign(t.local, t.priv, t.networkID))

	return m
}

func (t *testBlockdataMap) TestNew() {
	m := t.newmap()
	_ = (interface{})(m).(base.BlockdataMap)

	t.NoError(m.IsValid(t.networkID))

	t.NotNil(m.Manifest())

	t.True(LocalFSWriterHint.Equal(m.writer))
	t.True(jsonenc.JSONEncoderHint.Equal(m.encoder))
}

func (t *testBlockdataMap) TestInvalid() {
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
		m.m.RemoveValue(base.BlockdataTypeProposal)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty proposal")
	})

	t.Run("empty proposal", func() {
		m := t.newmap()
		m.m.SetValue(base.BlockdataTypeProposal, nil)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty proposal")
	})

	t.Run("voteproofs not set", func() {
		m := t.newmap()
		m.m.RemoveValue(base.BlockdataTypeVoteproofs)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty voteproofs")
	})

	t.Run("empty voteproofs", func() {
		m := t.newmap()
		m.m.SetValue(base.BlockdataTypeVoteproofs, nil)

		err := m.IsValid(t.networkID)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty voteproofs")
	})

	t.Run("invalid item", func() {
		m := t.newmap()
		m.m.SetValue(base.BlockdataTypeVoteproofs, t.newitem(base.BlockdataType("hehe")))

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

func (t *testBlockdataMap) TestSetItem() {
	m := t.newmap()

	t.Run("override", func() {
		olditem, found := m.Item(base.BlockdataTypeProposal)
		t.True(found)
		t.NotNil(olditem)

		newitem := t.newitem(base.BlockdataTypeProposal)
		t.NoError(m.SetItem(newitem))

		t.NotEqual(olditem.Checksum(), newitem.Checksum())

		ritem, found := m.Item(base.BlockdataTypeProposal)
		t.True(found)
		t.NotNil(ritem)

		base.EqualBlockdataMapItem(t.Assert(), newitem, ritem)
	})

	t.Run("unknown data type", func() {
		newitem := t.newitem(base.BlockdataType("findme"))
		err := m.SetItem(newitem)

		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "unknown block data type")
	})
}

func (t *testBlockdataMap) TestVerify() {
	t.Run("basic", func() {
		m := t.newmap()
		t.NoError(m.IsValid(t.networkID))
	})

	t.Run("update item with same checksum", func() {
		m := t.newmap()
		t.NoError(m.IsValid(t.networkID))

		olditem, found := m.Item(base.BlockdataTypeProposal)
		t.True(found)
		t.NotNil(olditem)

		u := url.URL{Scheme: "https", Host: util.UUID().String(), Path: util.UUID().String()}
		newitem := NewBlockdataMapItem(olditem.Type(), u, olditem.Checksum(), olditem.Num())
		t.NoError(m.SetItem(newitem))

		t.NoError(m.IsValid(t.networkID))
	})
}

type testBlockdataMapEncode struct {
	encoder.BaseTestEncode
	enc *jsonenc.Encoder
}

func (t *testBlockdataMap) TestEncode() {
	tt := new(testBlockdataMapEncode)

	tt.Encode = func() (interface{}, []byte) {
		tt.enc = jsonenc.NewEncoder()
		t.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		tt.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: base.DummyManifestHint, Instance: base.DummyManifest{}}))
		tt.NoError(tt.enc.Add(encoder.DecodeDetail{Hint: BlockdataMapHint, Instance: BlockdataMap{}}))

		m := t.newmap()

		b, err := tt.enc.Marshal(m)
		tt.NoError(err)

		return m, b
	}
	tt.Decode = func(b []byte) interface{} {
		i, err := tt.enc.Decode(b)
		tt.NoError(err)

		_, ok := i.(BlockdataMap)
		tt.True(ok)

		return i
	}
	tt.Compare = func(a, b interface{}) {
		af, ok := a.(BlockdataMap)
		tt.True(ok)
		bf, ok := b.(BlockdataMap)
		tt.True(ok)

		tt.NoError(bf.IsValid(t.networkID))

		base.EqualBlockdataMap(tt.Assert(), af, bf)
	}

	suite.Run(t.T(), tt)
}

func TestBlockdataMap(t *testing.T) {
	suite.Run(t, new(testBlockdataMap))
}

type testBlockdataMapItem struct {
	suite.Suite
}

func (t *testBlockdataMapItem) TestNew() {
	u, err := url.Parse("file://showme")
	t.NoError(err)
	checksum := util.UUID().String()

	item := NewBlockdataMapItem(base.BlockdataTypeProposal, *u, checksum, 1)
	_ = (interface{})(item).(base.BlockdataMapItem)

	t.NoError(item.IsValid(nil))

	t.Equal(base.BlockdataTypeProposal, item.Type())
	t.Equal(u.String(), item.URL().String())
	t.Equal(checksum, item.Checksum())
}

func (t *testBlockdataMapItem) TestLocal() {
	ty := base.BlockdataTypeProposal
	m0 := NewLocalBlockdataMapItem(ty, util.UUID().String(), 1)
	m1 := NewLocalBlockdataMapItem(ty, util.UUID().String(), 1)

	t.T().Log("fileBlockdataURL:", fileBlockdataURL.String())
	t.T().Log("m0.url:", m0.URL())
	t.T().Log("m1.url:", m1.URL())
}

func (t *testBlockdataMapItem) TestInvalid() {
	t.Run("invalid data type", func() {
		u, _ := url.Parse("file://showme")
		item := NewBlockdataMapItem(base.BlockdataType("findme"), *u, util.UUID().String(), 1)

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "unknown block data type")
	})

	t.Run("empty checksum", func() {
		u, _ := url.Parse("file://showme")
		item := NewBlockdataMapItem(base.BlockdataTypeProposal, *u, "", 1)

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty checksum")
	})

	t.Run("empty url", func() {
		item := NewBlockdataMapItem(base.BlockdataTypeProposal, url.URL{}, util.UUID().String(), 1)

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty url")
	})

	t.Run("empty url scheme", func() {
		u, _ := url.Parse("showme")
		item := NewBlockdataMapItem(base.BlockdataTypeProposal, *u, util.UUID().String(), 1)

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty url")
	})

	t.Run("unsupported url scheme", func() {
		u, _ := url.Parse("showme://findme")
		item := NewBlockdataMapItem(base.BlockdataTypeProposal, *u, util.UUID().String(), 1)

		err := item.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "unsupported url scheme found")
	})
}

func TestBlockdataMapItem(t *testing.T) {
	suite.Run(t, new(testBlockdataMapItem))
}

type testBlockdataMapItemEncode struct {
	encoder.BaseTestEncode
	enc *jsonenc.Encoder
}

func (t *testBlockdataMapItemEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()
}

func TestBlockdataMapItemEncode(tt *testing.T) {
	t := new(testBlockdataMapItemEncode)

	t.Encode = func() (interface{}, []byte) {
		u, _ := url.Parse("file://showme")
		item := NewBlockdataMapItem(base.BlockdataTypeProposal, *u, util.UUID().String(), 1)

		b, err := t.enc.Marshal(item)
		t.NoError(err)

		return item, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BlockdataMapItem
		t.NoError(t.enc.Unmarshal(b, &u))

		return u
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(BlockdataMapItem)
		t.True(ok)
		bf, ok := b.(BlockdataMapItem)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		base.EqualBlockdataMapItem(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}
