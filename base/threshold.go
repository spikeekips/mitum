package base

import (
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type Threshold struct {
	quorum    uint
	threshold uint
	ratio     float64
}

func NewThreshold(quorum uint, ratio float64) Threshold {
	return Threshold{
		quorum:    quorum,
		threshold: uint(math.Ceil(float64(quorum) * (ratio / 100))),
		ratio:     ratio,
	}
}

func (tr Threshold) Quorum() uint {
	return tr.quorum
}

func (tr Threshold) Threshold() uint {
	return tr.threshold
}

func (tr Threshold) Ratio() float64 {
	return tr.ratio
}

func (tr Threshold) Bytes() []byte {
	return util.ConcatBytesSlice(
		util.UintToBytes(tr.quorum),
		util.Float64ToBytes(tr.ratio),
	)
}

func (tr Threshold) String() string {
	b, _ := util.MarshalJSON(tr)
	return string(b)
}

func (tr Threshold) Equal(b Threshold) bool {
	return tr.quorum == b.quorum && tr.ratio == b.ratio && tr.threshold == b.threshold
}

func (tr Threshold) IsValid([]byte) error {
	switch {
	case tr.ratio < 1:
		return util.InvalidError.Errorf("under zero ratio, %v", tr.ratio)
	case tr.ratio > 100:
		return util.InvalidError.Errorf("over 100 ratio, %v", tr.ratio)
	case tr.ratio < 67:
		return util.InvalidError.Errorf("dangerous ratio, %v < 67", tr.ratio)
	}

	switch {
	case tr.quorum < 1:
		return util.InvalidError.Errorf("zero quorum in threshold")
	case tr.threshold > tr.quorum:
		return util.InvalidError.Errorf("threshold over quorum: threshold=%v quorum=%v", tr.threshold, tr.quorum)
	}

	return nil
}

func (tr Threshold) VoteResult(set []string) (VoteResult, string) {
	return FindVoteResult(tr.quorum, tr.threshold, set)
}

type thresholdJSONMarshaler struct {
	Quorum    uint
	Threshold uint
	Ratio     float64
}

func (tr Threshold) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(thresholdJSONMarshaler{
		Quorum:    tr.quorum,
		Threshold: tr.threshold,
		Ratio:     tr.ratio,
	})
}

func (tr *Threshold) UnmarshalJSON(b []byte) error {
	var u thresholdJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal Threshold")
	}

	tr.quorum = u.Quorum
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
