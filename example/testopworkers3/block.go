package main

import (
	"net/url"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type BlockItemFile struct {
	chunks []string
}

func NewBlockItemFile(chunks []string) BlockItemFile {
	return BlockItemFile{chunks: chunks}
}

func (f BlockItemFile) IsValid([]byte) error {
	if len(f.chunks) < 1 {
		return util.ErrInvalid.Errorf("empty chunks")
	}

	if util.IsDuplicatedSlice(f.chunks, func(i string) (bool, string) {
		return true, i
	}) {
		return util.ErrInvalid.Errorf("duplicated chunks found")
	}

	if err := util.TraverseSliceV1(f.chunks, func(_ int, i string) (bool, error) {
		if len(i) < 1 {
			return false, errors.Errorf("empty chunk")
		}

		return true, nil
	}); err != nil {
		return util.ErrInvalid.Wrap(err)
	}

	return nil
}

func (f BlockItemFile) URI() url.URL {
	vs := url.Values{}

	_ = util.TraverseSliceV1(f.chunks, func(_ int, i string) (bool, error) {
		vs.Add("chunks", i)

		return true, nil
	})

	return url.URL{
		Scheme:   "workers",
		RawQuery: vs.Encode(),
	}
}

func (f BlockItemFile) CompressFormat() string {
	return ""
}
