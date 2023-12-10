package isaac

import (
	"net/url"
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
			localtime.Now().UTC(),
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

type testBlockItemFile struct {
	suite.Suite
}

func (t *testBlockItemFile) TestNew() {
	f := NewLocalFSBlockItemFile("proposal.gz", "")
	_ = (interface{})(f).(base.BlockItemFile)

	t.Run("proposal.gz", func() {
		f := NewLocalFSBlockItemFile("proposal.gz", "")
		t.NoError(f.IsValid(nil))

		t.Equal(LocalFSBlockItemScheme, f.URI().Scheme)
		t.Equal("/proposal.gz", f.URI().Path)

		t.Equal(f.CompressFormat(), "")
	})

	t.Run("proposal.json.gz", func() {
		f := NewLocalFSBlockItemFile("proposal.json.gz", "")
		t.NoError(f.IsValid(nil))

		t.Equal(LocalFSBlockItemScheme, f.URI().Scheme)
		t.Equal("/proposal.json.gz", f.URI().Path)

		t.Equal(f.CompressFormat(), "gz")
	})

	t.Run("url", func() {
		u, err := url.Parse("https://a/b/c/d.xz")
		t.NoError(err)

		f := NewBlockItemFile(*u, "gz")
		t.NoError(f.IsValid(nil))

		t.Equal(*u, f.URI())
		t.Equal(f.CompressFormat(), "gz")
	})
}

func (t *testBlockItemFile) TestIsValid() {
	t.Run("empty uri", func() {
		f := NewBlockItemFile(url.URL{}, "")
		err := f.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "empty uri")
	})

	t.Run("empty uri scheme", func() {
		u, err := url.Parse("https://a/b/c/d.xz")
		t.NoError(err)
		u.Scheme = ""

		f := NewBlockItemFile(*u, "")
		err = f.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "empty uri scheme")
	})

	t.Run("empty hostname in remote uri", func() {
		u, err := url.Parse("https:///d.xz")
		t.NoError(err)

		f := NewBlockItemFile(*u, "")
		err = f.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "empty hostname")
	})

	t.Run("file, localfs: empty path", func() {
		f := NewLocalFSBlockItemFile("", "")
		f.uri.Path = ""

		err := f.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "empty filename in file uri")
	})
}

func (t *testBlockItemFile) TestCompressFormat() {
	t.Run("empty", func() {
		f := NewLocalFSBlockItemFile("proposal.json", "")
		t.NoError(f.IsValid(nil))

		t.Equal("", f.CompressFormat())
	})

	t.Run("from filename", func() {
		f := NewLocalFSBlockItemFile("proposal.json.gz", "")
		t.NoError(f.IsValid(nil))

		t.Equal("gz", f.CompressFormat())
	})

	t.Run("from CompressFormat", func() {
		f := NewLocalFSBlockItemFile("proposal.gz", "xz")
		t.NoError(f.IsValid(nil))

		t.Equal("xz", f.CompressFormat())
	})
}

func TestBlockItemFile(t *testing.T) {
	suite.Run(t, new(testBlockItemFile))
}

func TestBlockItemFileEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: BlockItemFileHint, Instance: BlockItemFile{}}))

		f := NewLocalFSBlockItemFile("proposal.gz", "bz")

		t.NoError(f.IsValid(nil))

		b, err := enc.Marshal(f)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return f, b
	}
	t.Decode = func(b []byte) interface{} {
		u, err := enc.Decode(b)
		t.NoError(err)

		return u
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(BlockItemFile)
		t.True(ok)
		bf, ok := b.(BlockItemFile)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		t.Equal(af.URI(), bf.URI())
		t.Equal(af.CompressFormat(), bf.CompressFormat())
	}

	suite.Run(tt, t)
}

type testBlockItemFiles struct {
	suite.Suite
}

func (t *testBlockItemFiles) TestNew() {
	files := map[base.BlockItemType]base.BlockItemFile{
		base.BlockItemMap:        NewLocalFSBlockItemFile("m.json", ""),
		base.BlockItemProposal:   NewFileBlockItemFile("/a/b/c/p.json.gz", ""),
		base.BlockItemVoteproofs: NewLocalFSBlockItemFile("v.ndjson", ""),
	}

	fs := NewBlockItemFiles(files)

	_ = (interface{})(fs).(base.BlockItemFiles)

	t.NoError(fs.IsValid(nil))

	var i base.BlockItemFile
	var found bool

	i, found = fs.Item(base.BlockItemMap)
	t.True(found)
	t.Equal(LocalFSBlockItemScheme, i.URI().Scheme)
	t.Equal("/m.json", i.URI().Path)

	i, found = fs.Item(base.BlockItemProposal)
	t.True(found)
	t.Equal("file", i.URI().Scheme)
	t.Equal("/a/b/c/p.json.gz", i.URI().Path)

	i, found = fs.Item(base.BlockItemVoteproofs)
	t.True(found)
	t.Equal(LocalFSBlockItemScheme, i.URI().Scheme)
	t.Equal("/v.ndjson", i.URI().Path)
}

func (t *testBlockItemFiles) TestIsValid() {
	t.Run("ok", func() {
		files := map[base.BlockItemType]base.BlockItemFile{
			base.BlockItemMap:        NewLocalFSBlockItemFile("m.json", ""),
			base.BlockItemProposal:   NewFileBlockItemFile("/a/b/c/p.json.gz", ""),
			base.BlockItemVoteproofs: NewLocalFSBlockItemFile("v.ndjson", ""),
		}

		fs := NewBlockItemFiles(files)
		t.NoError(fs.IsValid(nil))
	})

	t.Run("must be file missing", func() {
		files := map[base.BlockItemType]base.BlockItemFile{
			base.BlockItemProposal:   NewFileBlockItemFile("/a/b/c/p.json.gz", ""),
			base.BlockItemVoteproofs: NewLocalFSBlockItemFile("v.ndjson", ""),
		}

		fs := NewBlockItemFiles(files)
		err := fs.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "important item file")
	})

	t.Run("wrong file", func() {
		files := map[base.BlockItemType]base.BlockItemFile{
			base.BlockItemMap:        NewLocalFSBlockItemFile("", ""),
			base.BlockItemProposal:   NewFileBlockItemFile("/a/b/c/p.json.gz", ""),
			base.BlockItemVoteproofs: NewLocalFSBlockItemFile("v.ndjson", ""),
		}

		fs := NewBlockItemFiles(files)
		err := fs.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "empty filename")
	})

	t.Run("empty items", func() {
		fs := NewBlockItemFiles(nil)

		err := fs.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "empty items")
	})
}

func (t *testBlockItemFiles) TestFile() {
	files := map[base.BlockItemType]base.BlockItemFile{
		base.BlockItemMap:        NewLocalFSBlockItemFile("m.json", ""),
		base.BlockItemProposal:   NewFileBlockItemFile("/a/b/c/p.json.gz", ""),
		base.BlockItemVoteproofs: NewLocalFSBlockItemFile("v.ndjson", ""),
	}

	fs := NewBlockItemFiles(files)

	t.Run("found", func() {
		i, found := fs.Item(base.BlockItemVoteproofs)
		t.True(found)

		t.Equal(LocalFSBlockItemScheme, i.URI().Scheme)
		t.Equal("/v.ndjson", i.URI().Path)
	})

	t.Run("not found", func() {
		i, found := fs.Item(base.BlockItemOperationsTree)
		t.False(found)
		t.Nil(i)
	})

	t.Run("Files()", func() {
		t.Equal(files, fs.Items())
	})
}

func TestBlockItemFiles(t *testing.T) {
	suite.Run(t, new(testBlockItemFiles))
}

func TestBlockItemFilesEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: BlockItemFileHint, Instance: BlockItemFile{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: BlockItemFilesHint, Instance: BlockItemFiles{}}))

		files := map[base.BlockItemType]base.BlockItemFile{
			base.BlockItemMap:            NewLocalFSBlockItemFile("m.json", ""),
			base.BlockItemProposal:       NewFileBlockItemFile("/a/b/c/p.json.gz", ""),
			base.BlockItemVoteproofs:     NewLocalFSBlockItemFile("v.ndjson", ""),
			base.BlockItemOperationsTree: NewLocalFSBlockItemFile("o.ndjson.bz", ""),
		}

		fs := NewBlockItemFiles(files)

		t.NoError(fs.IsValid(nil))

		b, err := enc.Marshal(fs)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return fs, b
	}
	t.Decode = func(b []byte) interface{} {
		u, err := enc.Decode(b)
		t.NoError(err)

		return u
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(BlockItemFiles)
		t.True(ok)
		bf, ok := b.(BlockItemFiles)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		t.Equal(af.Items(), bf.Items())
	}

	suite.Run(tt, t)
}
