package base

import "github.com/spikeekips/mitum/util"

type HeightDecoder struct {
	h       Height
	decoded bool
}

func (d *HeightDecoder) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal height")

	var u Height
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	d.h = u
	d.decoded = true

	return nil
}

func (d HeightDecoder) Height() Height {
	if !d.decoded {
		return NilHeight
	}

	return d.h
}
