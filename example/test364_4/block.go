package main

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type blockFileChunk interface {
	URI() string
	Checksum() string
	CompressFormat() string
}

type baseBlockFileChunk struct {
	uri            string
	checksum       string
	compressFormat string
}

func newBaseBlockFileChunk(uri, checksum, compressFormat string) baseBlockFileChunk {
	return baseBlockFileChunk{
		uri:            uri,
		checksum:       checksum,
		compressFormat: compressFormat,
	}
}

func (c baseBlockFileChunk) URI() string            { return c.uri }
func (c baseBlockFileChunk) Checksum() string       { return c.checksum }
func (c baseBlockFileChunk) CompressFormat() string { return c.compressFormat }

type ChecksumsBlockMapItem interface {
	Checksums() []string
}

type BaseChecksumsBlockMapItem struct {
	it        base.BlockItemType
	checksums []string
}

func (t BaseChecksumsBlockMapItem) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid ChecksumsBlockMapItem")

	if err := t.it.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if len(t.checksums) < 1 {
		return e.Errorf("empty checksums")
	}

	var gerr error

	if found := util.IsDuplicatedSlice(t.checksums, func(checksum string) (bool, string) {
		if len(checksum) < 1 {
			gerr = e.Errorf("empty checksum")
		}

		return gerr == nil, checksum
	}); found {
		return e.Errorf("duplicated slices")
	}

	return gerr
}

func (t BaseChecksumsBlockMapItem) Type() base.BlockItemType {
	return t.it
}

func (t BaseChecksumsBlockMapItem) Checksum() string {
	if len(t.checksums) < 1 {
		return ""
	}

	return t.checksums[0]
}

func (t BaseChecksumsBlockMapItem) Checksums() []string {
	return t.checksums
}
