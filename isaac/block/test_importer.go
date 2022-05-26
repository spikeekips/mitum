//go:build test
// +build test

package isaacblock

import (
	"context"
	"io"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
)

type DummyBlockImporter struct {
	WriteMapf     func(base.BlockMap) error
	WriteItemf    func(base.BlockMapItemType, io.Reader) error
	Savef         func(context.Context) error
	Mergef        func(context.Context) error
	CancelImportf func(context.Context) error
}

func (im *DummyBlockImporter) Reader() (isaac.BlockReader, error) {
	return nil, nil
}

func (im *DummyBlockImporter) WriteMap(m base.BlockMap) error {
	if im.WriteMapf != nil {
		return im.WriteMapf(m)
	}

	return nil
}

func (im *DummyBlockImporter) WriteItem(item base.BlockMapItemType, r io.Reader) error {
	if im.WriteItemf != nil {
		return im.WriteItemf(item, r)
	}

	return nil
}

func (im *DummyBlockImporter) Save(ctx context.Context) error {
	if im.Savef != nil {
		return im.Savef(ctx)
	}

	return nil
}

func (im *DummyBlockImporter) Merge(ctx context.Context) error {
	if im.Mergef != nil {
		return im.Mergef(ctx)
	}

	return nil
}

func (im *DummyBlockImporter) CancelImport(ctx context.Context) error {
	if im.CancelImportf != nil {
		return im.CancelImportf(ctx)
	}

	return nil
}
