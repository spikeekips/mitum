package main

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

var blockFileChunkHint = hint.MustNewHint("block-file-chunk-v0.0.1")

type blockFileChunk interface {
	URI() string
	Checksum() string
	CompressFormat() string
}

type baseBlockFileChunk struct {
	hint.BaseHinter
	uri            string
	checksum       string
	compressFormat string
}

func newBaseBlockFileChunk(uri, checksum, compressFormat string) baseBlockFileChunk {
	return baseBlockFileChunk{
		BaseHinter: hint.NewBaseHinter(blockFileChunkHint),
		uri:        uri, checksum: checksum, compressFormat: compressFormat,
	}
}

func (c baseBlockFileChunk) IsValid([]byte) error {
	switch {
	case len(c.uri) < 1:
		return util.ErrInvalid.Errorf("uri")
	case len(c.checksum) < 1:
		return util.ErrInvalid.Errorf("checksum")
	}

	return nil
}

func (c baseBlockFileChunk) URI() string            { return c.uri }
func (c baseBlockFileChunk) Checksum() string       { return c.checksum }
func (c baseBlockFileChunk) CompressFormat() string { return c.compressFormat }

type baseBlockFileChunkJSONMarshaler struct {
	hint.BaseHinter
	Uri            string `json:"uri"`
	Checksum       string `json:"checksum"`
	CompressFormat string `json:"compress_format"`
}

func (c baseBlockFileChunk) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseBlockFileChunkJSONMarshaler{
		BaseHinter:     c.BaseHinter,
		Uri:            c.uri,
		Checksum:       c.checksum,
		CompressFormat: c.compressFormat,
	})
}

func (c *baseBlockFileChunk) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u baseBlockFileChunkJSONMarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	c.uri = u.Uri
	c.checksum = u.Checksum
	c.compressFormat = u.CompressFormat

	return nil
}
