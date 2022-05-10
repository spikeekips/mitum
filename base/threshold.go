package base

import (
	"math"
	"strconv"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

var (
	DefaultThreshold Threshold = 67
	MinThreshold     Threshold = 67
)

type Threshold float64

func (t Threshold) Float64() float64 {
	return float64(t)
}

func (t Threshold) Bytes() []byte {
	return []byte(t.String())
}

func (t Threshold) String() string {
	return strconv.FormatFloat(t.Float64(), 'f', 1, 64)
}

func (t Threshold) Equal(b Threshold) bool {
	return t.String() == b.String()
}

func (t Threshold) IsValid([]byte) error {
	switch {
	case t <= 0:
		return util.InvalidError.Errorf("under zero threshold, %v", t)
	case t > 100:
		return util.InvalidError.Errorf("over 100 threshold, %v", t)
	case t < MinThreshold:
		return util.InvalidError.Errorf("risky threshold, %v < 67", t)
	}

	return nil
}

func (t Threshold) Threshold(quorum uint) uint {
	return uint(math.Ceil(float64(quorum) * (t.Float64() / 100)))
}

func (t Threshold) VoteResult(quorum uint, set []string) (VoteResult, string) {
	return FindVoteResult(quorum, t.Threshold(quorum), set)
}

func (t Threshold) MarshalText() ([]byte, error) {
	return t.Bytes(), nil
}

func (t *Threshold) UnmarshalText(b []byte) error {
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return errors.WithStack(err)
	}

	*t = Threshold(f)

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
