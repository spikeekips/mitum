package main

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

var blockMapItemChunkHint = hint.MustNewHint("blockmap-item-chunk-v0.0.1")

type blockMapItemChunk interface {
	URI() string
	Checksum() string
	CompressFormat() string
}

type baseBlockMapItemChunk struct {
	hint.BaseHinter
	uri            string
	checksum       string
	compressFormat string
}

func newBaseBlockMapItemChunk(uri, checksum, compressFormat string) baseBlockMapItemChunk {
	return baseBlockMapItemChunk{
		BaseHinter: hint.NewBaseHinter(blockMapItemChunkHint),
		uri:        uri, checksum: checksum, compressFormat: compressFormat,
	}
}

func (c baseBlockMapItemChunk) IsValid([]byte) error {
	switch {
	case len(c.uri) < 1:
		return util.ErrInvalid.Errorf("uri")
	case len(c.checksum) < 1:
		return util.ErrInvalid.Errorf("checksum")
	}

	return nil
}

func (c baseBlockMapItemChunk) URI() string            { return c.uri }
func (c baseBlockMapItemChunk) Checksum() string       { return c.checksum }
func (c baseBlockMapItemChunk) CompressFormat() string { return c.compressFormat }

type blockMapItemChunkJSONMarshaler struct {
	hint.BaseHinter
	Uri            string `json:"uri"`
	Checksum       string `json:"checksum"`
	CompressFormat string `json:"compress_format"`
}

func (c baseBlockMapItemChunk) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(blockMapItemChunkJSONMarshaler{
		BaseHinter:     c.BaseHinter,
		Uri:            c.uri,
		Checksum:       c.checksum,
		CompressFormat: c.compressFormat,
	})
}

func (c *baseBlockMapItemChunk) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u blockMapItemChunkJSONMarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	c.uri = u.Uri
	c.checksum = u.Checksum
	c.compressFormat = u.CompressFormat

	return nil
}
