package base

import (
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type Threshold struct {
	total     uint
	threshold uint
	ratio     float64
}

func NewThreshold(total uint, ratio float64) Threshold {
	return Threshold{
		total:     total,
		threshold: uint(math.Ceil(float64(total) * (ratio / 100))),
		ratio:     ratio,
	}
}

func (tr Threshold) Total() uint {
	return tr.total
}

func (tr Threshold) Threshold() uint {
	return tr.threshold
}

func (tr Threshold) Ratio() float64 {
	return tr.ratio
}

func (tr Threshold) Bytes() []byte {
	return util.ConcatBytesSlice(
		util.UintToBytes(tr.total),
		util.Float64ToBytes(tr.ratio),
	)
}

func (tr Threshold) String() string {
	b, _ := util.MarshalJSON(tr)
	return string(b)
}

func (tr Threshold) Equal(b Threshold) bool {
	return tr.total == b.total && tr.ratio == b.ratio && tr.threshold == b.threshold
}

func (tr Threshold) IsValid([]byte) error {
	switch {
	case tr.ratio < 1:
		return util.InvalidError.Errorf("under zero ratio, %v", tr.ratio)
	case tr.ratio > 100:
		return util.InvalidError.Errorf("over 100 ratio, %v", tr.ratio)
	}

	switch {
	case tr.total < 1:
		return util.InvalidError.Errorf("zero total in threshold")
	case tr.threshold > tr.total:
		return util.InvalidError.Errorf("threshold over total: threshold=%v total=%v", tr.threshold, tr.total)
	}

	return nil
}

type thresholdJSONMarshaler struct {
	Total     uint
	Threshold uint
	Ratio     float64
}

func (tr Threshold) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(thresholdJSONMarshaler{
		Total:     tr.total,
		Threshold: tr.threshold,
		Ratio:     tr.ratio,
	})
}

func (tr *Threshold) UnmarshalJSON(b []byte) error {
	var u thresholdJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal Threshold")
	}

	tr.total = u.Total
	tr.threshold = u.Threshold
	tr.ratio = u.Ratio

	return nil
}

func NumberOfFaultyNodes(n uint, threshold float64) int {
	if n < 1 {
		return 0
	} else if threshold >= 100 {
		return 0
	}

	return int(float64(n) - float64(n)*(threshold/100))
}
